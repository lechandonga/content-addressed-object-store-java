package com.github.lechandonga.contentaddressedobjectstorejava.store.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ContentCorruptedException;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.GarbageCollectionReport;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectNotFoundException;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectStore;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectVersion;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.PutResult;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.StoreException;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.VersionConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Filesystem-backed implementation of {@link ObjectStore}.
 *
 * <p>On-disk layout under the configured data directory:
 * <pre>
 * dataDir/
 *   blobs/ab/abcdef...              content-addressed, immutable data files
 *   objects/12/34/sha256(id).json   committed metadata (atomic replace)
 *   locks/sha256(id).lock           per-object commit locks (cross-process)
 *   tmp/                            staging area for uploads and rewrites
 *   quarantine/                     blobs parked by GC before final deletion
 * </pre>
 *
 * <p>Commit protocol: bytes are staged under {@code tmp} and fsynced, then
 * hard-linked into {@code blobs}; metadata is rewritten in {@code tmp} and
 * atomically renamed over the old metadata file while holding the object lock.
 * A crash at any point leaves only a valid old/new metadata file or debris in
 * {@code tmp}/{@code quarantine}, reconciled at startup and by GC.
 */
public final class LocalObjectStore implements ObjectStore {

    private static final Logger log = LoggerFactory.getLogger(LocalObjectStore.class);
    private static final int BUFFER_SIZE = 64 * 1024;

    private final Path blobsDir;
    private final Path objectsDir;
    private final Path locksDir;
    private final Path tmpDir;
    private final Path quarantineDir;
    private final Duration gcGracePeriod;
    private final Duration tempSweepMinAge;
    private final Clock clock;
    private final ObjectMapper mapper = new ObjectMapper();

    private final ReentrantLock gcLock = new ReentrantLock();
    private final ConcurrentMap<String, ReentrantLock> objectLocks = new ConcurrentHashMap<>();

    public LocalObjectStore(StoreConfig config) {
        this(config, Clock.systemUTC());
    }

    LocalObjectStore(StoreConfig config, Clock clock) {
        this.clock = Objects.requireNonNull(clock);
        this.gcGracePeriod = config.gcGracePeriod();
        this.tempSweepMinAge = config.tempSweepMinAge();
        Path root = config.dataDir();
        this.blobsDir = root.resolve("blobs");
        this.objectsDir = root.resolve("objects");
        this.locksDir = root.resolve("locks");
        this.tmpDir = root.resolve("tmp");
        this.quarantineDir = root.resolve("quarantine");
        for (Path dir : List.of(blobsDir, objectsDir, locksDir, tmpDir, quarantineDir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new StoreException("Cannot initialize store directory " + dir, e);
            }
        }
        recoverFromCrash();
    }

    // ------------------------------------------------------------------ put

    @Override
    public PutResult put(String objectId, InputStream content, long expectedVersion) {
        validateObjectId(objectId);
        Objects.requireNonNull(content, "content");
        String lockKey = hashHex(objectId);
        ReentrantLock localLock = objectLocks.computeIfAbsent(lockKey, k -> new ReentrantLock());
        localLock.lock();
        Path stagedBlob = null;
        Path stagedMeta = null;
        boolean committed = false;
        try (FileLocker locker = FileLocker.lock(objectLockFile(lockKey))) {
            StagedBlob staged = stageBlob(content);
            stagedBlob = staged.path();

            MetadataFile meta = readMetadataOrNull(lockKey);
            long current = meta == null ? 0 : meta.latestVersion;
            if (expectedVersion != -1 && expectedVersion != current) {
                throw new VersionConflictException(
                        "Object '" + objectId + "' is at version " + current
                                + ", expected " + expectedVersion);
            }

            String hash = staged.hash();
            boolean deduplicated = publishBlob(staged, hash);

            MetadataFile updated = meta == null ? new MetadataFile() : meta;
            updated.objectId = objectId;
            updated.latestVersion = current + 1;
            updated.versions.add(new MetadataFile.Entry(
                    current + 1, hash, staged.size(), Instant.now(clock)));
            byte[] metaBytes = mapper.writeValueAsBytes(updated);
            stagedMeta = stageMeta(metaBytes);

            Path metaPath = metadataPath(lockKey);
            Files.createDirectories(metaPath.getParent());
            IoSupport.replaceAtomically(stagedMeta, metaPath);
            committed = true;
            log.info("Committed object='{}' version={} hash={} deduplicated={}",
                    objectId, updated.latestVersion, hash, deduplicated);
            return new PutResult(objectId, updated.latestVersion, hash, deduplicated);
        } catch (VersionConflictException | ContentCorruptedException e) {
            throw e;
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw new StoreException("Failed to commit object '" + objectId + "'", e);
        } finally {
            // The staged blob is a staging link to the same inode as the
            // published blob; removing it never affects blobs/ content.
            deleteIfExistsQuietly(stagedBlob);
            if (!committed) {
                deleteIfExistsQuietly(stagedMeta);
            }
            localLock.unlock();
        }
    }

    private StagedBlob stageBlob(InputStream content) throws IOException {
        Path staged = Files.createTempFile(tmpDir, "blob-", ".part");
        MessageDigest digest = newDigest();
        long size;
        try {
            try (OutputStream out = Files.newOutputStream(staged, StandardOpenOption.WRITE)) {
                byte[] buffer = new byte[BUFFER_SIZE];
                long total = 0;
                int read;
                while ((read = content.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                    out.write(buffer, 0, read);
                    total += read;
                }
                out.flush();
                if (out instanceof java.io.FileOutputStream fos) {
                    fos.getFD().sync();
                }
                size = total;
            }
        } finally {
            content.close();
        }
        return new StagedBlob(staged, HexFormat.of().formatHex(digest.digest()), size);
    }

    /**
     * Makes the staged bytes visible as a content-addressed blob via a hard link.
     * If the blob already exists its bytes are verified first and the staged copy
     * is discarded. Returns {@code true} when content was deduplicated.
     */
    private boolean publishBlob(StagedBlob staged, String hash) throws IOException {
        Path blobPath = blobPath(hash);
        Files.createDirectories(blobPath.getParent());
        try {
            Files.createLink(blobPath, staged.path());
            IoSupport.fsyncDirectory(blobPath.getParent());
            return false;
        } catch (FileAlreadyExistsException exists) {
            verifyBlobFile(blobPath, hash);
            Files.deleteIfExists(staged.path());
            return true;
        }
    }

    private Path stageMeta(byte[] metaBytes) throws IOException {
        Path staged = Files.createTempFile(tmpDir, "meta-", ".json.tmp");
        try (OutputStream out = Files.newOutputStream(staged, StandardOpenOption.WRITE)) {
            out.write(metaBytes);
            out.flush();
            if (out instanceof java.io.FileOutputStream fos) {
                fos.getFD().sync();
            }
        }
        return staged;
    }

    // ------------------------------------------------------------------ get

    @Override
    public void get(String objectId, long version, OutputStream out) {
        validateObjectId(objectId);
        Objects.requireNonNull(out, "out");
        String lockKey = hashHex(objectId);
        ReentrantLock localLock = objectLocks.computeIfAbsent(lockKey, k -> new ReentrantLock());
        localLock.lock();
        try (FileLocker ignored = FileLocker.lock(objectLockFile(lockKey))) {
            MetadataFile meta = readMetadataOrNull(lockKey);
            if (meta == null) {
                throw new ObjectNotFoundException("Object '" + objectId + "' does not exist");
            }
            long wanted = version == -1 ? meta.latestVersion : version;
            MetadataFile.Entry entry = meta.versions.stream()
                    .filter(e -> e.version == wanted)
                    .findFirst()
                    .orElseThrow(() -> new ObjectNotFoundException(
                            "Object '" + objectId + "' has no version " + wanted));
            // Spool through a temp file so the caller's stream never receives
            // bytes that subsequently fail the digest check.
            Path verified = Files.createTempFile(tmpDir, "read-", ".part");
            try {
                MessageDigest digest = newDigest();
                long total = copyVerifying(blobPath(entry.contentHash), verified, digest);
                String actual = HexFormat.of().formatHex(digest.digest());
                if (!actual.equals(entry.contentHash) || total != entry.size) {
                    throw new ContentCorruptedException(
                            "Content integrity failure for object '" + objectId + "' version " + wanted
                                    + ": expected hash=" + entry.contentHash + " size=" + entry.size
                                    + ", actual hash=" + actual + " size=" + total);
                }
                Files.copy(verified, out);
                out.flush();
            } finally {
                Files.deleteIfExists(verified);
            }
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw new StoreException("Failed to read object '" + objectId + "' version " + version, e);
        } finally {
            localLock.unlock();
        }
    }

    private long copyVerifying(Path blob, Path destination, MessageDigest digest) throws IOException {
        if (!Files.isRegularFile(blob)) {
            throw new ContentCorruptedException(
                    "Committed blob is missing from the content area: " + blob.getFileName());
        }
        long total = 0;
        try (InputStream in = Files.newInputStream(blob);
             OutputStream out = Files.newOutputStream(destination, StandardOpenOption.WRITE)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
                out.write(buffer, 0, read);
                total += read;
            }
        }
        return total;
    }

    // ------------------------------------------------------------ versions

    @Override
    public List<ObjectVersion> listVersions(String objectId) {
        validateObjectId(objectId);
        String lockKey = hashHex(objectId);
        ReentrantLock localLock = objectLocks.computeIfAbsent(lockKey, k -> new ReentrantLock());
        localLock.lock();
        try (FileLocker ignored = FileLocker.lock(objectLockFile(lockKey))) {
            MetadataFile meta = readMetadataOrNull(lockKey);
            if (meta == null) {
                throw new ObjectNotFoundException("Object '" + objectId + "' does not exist");
            }
            return meta.versions.stream().map(MetadataFile.Entry::toApi).toList();
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw new StoreException("Failed to list versions of '" + objectId + "'", e);
        } finally {
            localLock.unlock();
        }
    }

    // ------------------------------------------------------------------ GC

    @Override
    public GarbageCollectionReport collectGarbage() {
        gcLock.lock();
        long tempRemoved;
        long reclaimed = 0;
        long rescued;
        long reachable;
        long scanned;
        try (FileLocker ignored = FileLocker.lock(locksDir.resolve("gc.lock"))) {
            Instant start = Instant.now(clock);
            tempRemoved = sweepStaging(start);

            // First referenced-set snapshot. A second snapshot after the
            // quarantine step rescues anything a racing commit publishes
            // concurrently; young blobs are additionally protected by the
            // grace-period age threshold (see below).
            Set<String> referenced = scanReferencedHashes();

            List<Path> blobs = listBlobs();
            scanned = blobs.size();
            Instant oldestAllowed = start.minus(gcGracePeriod);
            for (Path blob : blobs) {
                String hash = blob.getFileName().toString();
                if (referenced.contains(hash)) {
                    continue;
                }
                if (Files.getLastModifiedTime(blob).toInstant().isAfter(oldestAllowed)) {
                    continue; // too young: may belong to an in-flight commit
                }
                Path parked = quarantineDir.resolve(hash);
                Files.deleteIfExists(parked);
                try {
                    Files.move(blob, parked, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException atomicUnsupported) {
                    Files.move(blob, parked, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            IoSupport.fsyncDirectory(quarantineDir);

            // Final confirmation pass. Every object's commit lock is held while
            // its metadata is read, so no commit can overlap the "still not
            // referenced?" decision: a racing put either finished before this
            // scan (its hash is seen and rescued) or starts after (its blob is
            // already committed in blobs/ and was never quarantined).
            referenced.addAll(scanReferencedHashesUnderLocks());
            rescued = rescueQuarantined(referenced);
            referenced.addAll(scanReferencedHashesUnderLocks());
            rescued += rescueQuarantined(referenced);

            for (Path parked : listRegularFiles(quarantineDir)) {
                verifyQuarantineIntegrity(parked);
                Files.deleteIfExists(parked);
                reclaimed++;
            }
            reachable = referenced.size();
            IoSupport.fsyncDirectory(blobsDir);
            log.info("GC complete: scanned={} referenced={} reclaimed={} tempRemoved={} rescued={}",
                    scanned, reachable, reclaimed, tempRemoved, rescued);
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw new StoreException("Garbage collection failed", e);
        } finally {
            gcLock.unlock();
        }
        return new GarbageCollectionReport(scanned, reachable, reclaimed, tempRemoved, rescued);
    }

    /**
     * Startup reconciliation. Restores blobs parked in quarantine by a GC that
     * crashed before deletion (re-checked against current metadata), and removes
     * stale staging debris left by crashed writers. Files younger than the sweep
     * threshold are left alone in case another local process is still writing.
     */
    private void recoverFromCrash() {
        try {
            Set<String> referenced = scanReferencedHashes();
            long rescued = rescueQuarantined(referenced);
            for (Path parked : listRegularFiles(quarantineDir)) {
                Files.deleteIfExists(parked);
            }
            long swept = sweepStaging(Instant.now(clock));
            log.info("Startup recovery complete: quarantinedRescued={} stagingSwept={}", rescued, swept);
        } catch (Exception e) {
            throw new StoreException("Startup recovery failed", e);
        }
    }

    private long rescueQuarantined(Set<String> referenced) throws IOException {
        long rescued = 0;
        for (Path parked : listRegularFiles(quarantineDir)) {
            String hash = parked.getFileName().toString();
            if (referenced.contains(hash)) {
                restoreFromQuarantine(parked, hash);
                rescued++;
            }
        }
        return rescued;
    }

    private void restoreFromQuarantine(Path parked, String hash) throws IOException {
        Path target = blobPath(hash);
        Files.createDirectories(target.getParent());
        if (Files.isRegularFile(target)) {
            Files.deleteIfExists(parked);
            return;
        }
        try {
            IoSupport.atomicMove(parked, target);
        } catch (FileAlreadyExistsException alreadyThere) {
            Files.deleteIfExists(parked);
        }
        IoSupport.fsyncDirectory(target.getParent());
    }

    private long sweepStaging(Instant now) throws IOException {
        long removed = 0;
        Instant cutoff = now.minus(tempSweepMinAge);
        for (Path child : listRegularFiles(tmpDir)) {
            if (Files.getLastModifiedTime(child).toInstant().isBefore(cutoff)) {
                Files.deleteIfExists(child);
                removed++;
            }
        }
        return removed;
    }

    private Set<String> scanReferencedHashes() throws IOException {
        if (!Files.isDirectory(objectsDir)) {
            return Set.of();
        }
        try (Stream<Path> paths = Files.walk(objectsDir)) {
            return paths.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .flatMap(metaPath -> {
                        try {
                            return readMetadata(metaPath).versions.stream()
                                    .map(e -> e.contentHash);
                        } catch (IOException malformed) {
                            log.warn("Skipping unreadable metadata file during GC scan: {}", metaPath);
                            return Stream.empty();
                        }
                    })
                    .collect(Collectors.toSet());
        }
    }

    /**
     * Referenced-hash snapshot taken while every object's commit lock is held in
     * turn. Guarantees that, for each object, metadata reading cannot interleave
     * with a commit: put publishes the blob before replacing metadata, and the
     * lock makes the two steps atomic relative to this scan.
     */
    private Set<String> scanReferencedHashesUnderLocks() throws IOException {
        Set<String> hashes = new java.util.HashSet<>();
        if (!Files.isDirectory(objectsDir)) {
            return hashes;
        }
        List<Path> metadataFiles;
        try (Stream<Path> paths = Files.walk(objectsDir)) {
            metadataFiles = paths
                    .filter(p -> p.getFileName().toString().endsWith(".json"))
                    .toList();
        }
        for (Path metadata : metadataFiles) {
            String lockKey = metadata.getFileName().toString().replace(".json", "");
            try (FileLocker ignored = FileLocker.lock(objectLockFile(lockKey))) {
                try {
                    hashes.addAll(readMetadata(metadata).versions.stream()
                            .map(e -> e.contentHash).toList());
                } catch (IOException malformed) {
                    log.warn("Skipping unreadable metadata file during locked GC scan: {}", metadata);
                }
            }
        }
        return hashes;
    }

    private List<Path> listBlobs() throws IOException {
        if (!Files.isDirectory(blobsDir)) {
            return List.of();
        }
        try (Stream<Path> paths = Files.walk(blobsDir, 3)) {
            return paths.filter(Files::isRegularFile).toList();
        }
    }

    private List<Path> listRegularFiles(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            var result = new java.util.ArrayList<Path>();
            for (Path path : stream) {
                if (Files.isRegularFile(path)) {
                    result.add(path);
                }
            }
            return result;
        }
    }

    private void verifyQuarantineIntegrity(Path parked) {
        String hash = parked.getFileName().toString();
        try {
            verifyBlobFile(parked, hash);
        } catch (IOException | ContentCorruptedException problem) {
            log.warn("Quarantined blob {} failed verification; deleting it anyway", hash);
        }
    }

    // -------------------------------------------------------------- helpers

    private MetadataFile readMetadataOrNull(String lockKey) throws IOException {
        Path path = metadataPath(lockKey);
        if (!Files.isRegularFile(path)) {
            return null;
        }
        return readMetadata(path);
    }

    private MetadataFile readMetadata(Path path) throws IOException {
        try {
            MetadataFile meta = mapper.readValue(path.toFile(), MetadataFile.class);
            if (meta.versions == null || meta.latestVersion != meta.versions.size()) {
                throw new IOException("metadata version list is inconsistent");
            }
            for (int i = 0; i < meta.versions.size(); i++) {
                if (meta.versions.get(i).version != i + 1) {
                    throw new IOException("metadata version numbers are not gap-free");
                }
            }
            return meta;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("malformed metadata file " + path, e);
        }
    }

    /**
     * Verifies that an existing blob's bytes match their content hash. A mismatch
     * is reported as corruption, never silently treated as a cache hit.
     */
    private void verifyBlobFile(Path blobPath, String expectedHash) throws IOException {
        MessageDigest digest = newDigest();
        long total = 0;
        try (InputStream in = Files.newInputStream(blobPath)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
                total += read;
            }
        }
        String actual = HexFormat.of().formatHex(digest.digest());
        if (!actual.equals(expectedHash)) {
            throw new ContentCorruptedException(
                    "Stored blob content corrupt: expected " + expectedHash + " but was " + actual
                            + " (" + total + " bytes)");
        }
    }

    private Path metadataPath(String lockKey) {
        return objectsDir.resolve(lockKey.substring(0, 2))
                .resolve(lockKey.substring(2, 4))
                .resolve(lockKey + ".json");
    }

    private Path blobPath(String hash) {
        return blobsDir.resolve(hash.substring(0, 2)).resolve(hash.substring(2, 4)).resolve(hash);
    }

    private Path objectLockFile(String lockKey) {
        return locksDir.resolve(lockKey + ".lock");
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new StoreException("SHA-256 is not available in this JVM", e);
        }
    }

    private static String hashHex(String text) {
        return HexFormat.of().formatHex(
                newDigest().digest(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    private static void validateObjectId(String objectId) {
        if (objectId == null || objectId.isBlank()) {
            throw new IllegalArgumentException("objectId must not be blank");
        }
        if (objectId.length() > 512) {
            throw new IllegalArgumentException("objectId must not exceed 512 characters");
        }
        if (objectId.contains("\0")) {
            throw new IllegalArgumentException("objectId contains illegal NUL character");
        }
    }

    private static void deleteIfExistsQuietly(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
                // Staging debris is swept at startup / GC; nothing else to do.
            }
        }
    }

    private record StagedBlob(Path path, String hash, long size) {
    }
}
