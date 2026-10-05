package com.github.lechandonga.contentaddressedobjectstorejava.tests;

import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ContentCorruptedException;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectNotFoundException;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectStore;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectVersion;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.PutResult;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.VersionConflictException;
import com.github.lechandonga.contentaddressedobjectstorejava.store.engine.LocalObjectStore;
import com.github.lechandonga.contentaddressedobjectstorejava.store.engine.StoreConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Engine-level tests for the key guarantees: content deduplication, integrity
 * failures, version ordering, optimistic concurrency conflicts, crash recovery
 * and safe garbage collection. Each case logs its scenario and result.
 */
@DisplayName("Scenario suite: local content-addressed object store")
class LocalObjectStoreTest {

    private static final Logger log = LoggerFactory.getLogger(LocalObjectStoreTest.class);

    @TempDir
    Path dataDir;

    private ObjectStore store;

    @BeforeEach
    void setUp() {
        store = newStore(Duration.ZERO, Duration.ZERO);
    }

    @Nested
    @DisplayName("Scenario 5: safe garbage collection")
    class GarbageCollection {

        @Test
        @DisplayName("case 5.1 GC deletes unreferenced blobs but keeps every version-referenced one")
        void gcReclaimsOnlyUnreferencedBlobs() throws Exception {
            log.info("CASE 5.1 START create referenced + orphan blobs, run GC");
            byte[] retained = "referenced-content".getBytes(StandardCharsets.UTF_8);
            byte[] historical = "old-revision".getBytes(StandardCharsets.UTF_8);
            byte[] orphan = "never-committed-after-rewind".getBytes(StandardCharsets.UTF_8);
            store.put("gc-doc", streamOf(historical), 0);
            store.put("gc-doc", streamOf(retained), 1);
            String orphanHash = sha256Hex(orphan);
            Path orphanBlob = findBlob(orphanHash);
            Files.createDirectories(orphanBlob.getParent());
            Files.write(orphanBlob, orphan);
            // Age the orphan so it is eligible even with a zero-ish grace period.
            Files.setLastModifiedTime(orphanBlob,
                    java.nio.file.attribute.FileTime.from(
                            java.time.Instant.now().minus(Duration.ofMinutes(30))));

            long blobsBefore = countBlobs();
            var report = store.collectGarbage();
            long blobsAfter = countBlobs();
            log.info("CASE 5.1 report: {}", report);

            assertEquals(blobsBefore - 1, blobsAfter, "exactly the orphan blob must disappear");
            assertFalse(Files.exists(orphanBlob));
            assertArrayEquals(historical, readAll(store, "gc-doc", 1),
                    "old version still readable: its blob survives GC");
            assertArrayEquals(retained, readAll(store, "gc-doc", 2));
            assertEquals(1, report.blobsReclaimed());
            assertTrue(report.blobsReachable() >= 2);
            log.info("CASE 5.1 PASS orphan removed, committed version blobs retained");
        }

        @Test
        @DisplayName("case 5.2 young blobs are protected by the grace window from a racing GC")
        void gcGraceProtectsYoungBlobs() throws Exception {
            log.info("CASE 5.2 START publish young orphan, run GC with 1h grace");
            byte[] young = "just-staged".getBytes(StandardCharsets.UTF_8);
            Path youngBlob = findBlob(sha256Hex(young));
            Files.createDirectories(youngBlob.getParent());
            Files.write(youngBlob, young);

            ObjectStore gcWithGrace = newStore(Duration.ofHours(1), Duration.ofHours(1));
            gcWithGrace.collectGarbage();
            assertTrue(Files.exists(youngBlob),
                    "blobs younger than the grace period must not be reclaimed");
            log.info("CASE 5.2 PASS in-flight-age blob protected by grace window");
        }
    }

    @Nested
    @DisplayName("Scenario 4: interruption and restart recovery")
    class CrashRecovery {

        @Test
        @DisplayName("case 4.1 staged debris from a killed writer never appears committed and is swept")
        void debrisFromKilledUploadIsRecovered() throws Exception {
            log.info("CASE 4.1 START commit a version, leave stale staging debris, reopen store");
            store.put("keep", streamOf("committed-bytes"), 0);
            Path debrisA = Files.createTempFile(dataDir.resolve("tmp"), "blob-", ".part");
            Path debrisB = Files.createTempFile(dataDir.resolve("tmp"), "meta-", ".json.tmp");
            Files.writeString(debrisA, "half-uploaded");
            Files.writeString(debrisB, "{not-json");
            var old = java.nio.file.attribute.FileTime.from(
                    java.time.Instant.now().minus(Duration.ofHours(1)));
            Files.setLastModifiedTime(debrisA, old);
            Files.setLastModifiedTime(debrisB, old);

            ObjectStore reopened = newStore(Duration.ofSeconds(1), Duration.ofSeconds(1));
            try (Stream<Path> tmpFiles = Files.list(dataDir.resolve("tmp"))) {
                assertEquals(0, tmpFiles.count(), "old staging debris must be swept at startup");
            }
            assertArrayEquals("committed-bytes".getBytes(StandardCharsets.UTF_8),
                    readAll(reopened, "keep", 1));
            assertThrows(ObjectNotFoundException.class,
                    () -> reopened.listVersions("keep-unknown-debris"));
            try (Stream<Path> debris = Files.list(dataDir.resolve("tmp"))
                    .filter(p -> p.getFileName().toString().startsWith("blob-")
                            || p.getFileName().toString().startsWith("meta-"))) {
                assertEquals(0, debris.count(), "no staged debris may be exposed as committed data");
            }
            log.info("CASE 4.1 PASS committed data intact, staging debris invisible and swept");
        }

        @Test
        @DisplayName("case 4.2 GC interrupted after quarantine restores still-referenced blobs on reopen")
        void quarantineInterruptedIsReconciled() throws Exception {
            log.info("CASE 4.2 START park a referenced blob in quarantine, then reopen");
            byte[] referenced = "must-survive".getBytes(StandardCharsets.UTF_8);
            PutResult committed = store.put("alive", streamOf(referenced), 0);
            String hash = committed.contentHash();
            Path blob = findBlob(hash);
            Path parked = dataDir.resolve("quarantine").resolve(hash);
            Files.move(blob, parked);

            ObjectStore reopened = newStore(Duration.ZERO, Duration.ZERO);
            assertArrayEquals(referenced, readAll(reopened, "alive", 1),
                    "blob referenced by a committed version must be rescued from quarantine");
            assertFalse(Files.exists(parked));
            log.info("CASE 4.2 PASS referenced blob rescued after interrupted GC");
        }

        @Test
        @DisplayName("case 4.3 reopen after multiple commits leaves every version byte-identical")
        void reopenKeepsFullHistory() throws Exception {
            log.info("CASE 4.3 START three versions then restart the store");
            store.put("multi", streamOf("a"), 0);
            store.put("multi", streamOf("bb"), 1);
            store.put("multi", streamOf("ccc"), 2);
            ObjectStore reopened = newStore(Duration.ZERO, Duration.ZERO);
            assertEquals(3, reopened.listVersions("multi").size());
            assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), readAll(reopened, "multi", 1));
            assertArrayEquals("bb".getBytes(StandardCharsets.UTF_8), readAll(reopened, "multi", 2));
            assertArrayEquals("ccc".getBytes(StandardCharsets.UTF_8), readAll(reopened, "multi", 3));
            log.info("CASE 4.3 PASS all three versions readable after restart");
        }
    }

    @Nested
    @DisplayName("Scenario 3: concurrent conditional writes")
    class Concurrency {

        @Test
        @DisplayName("case 3.1 N racing conditional updates: exactly one winner, no lost versions")
        void racingWritersExactlyOneWins() throws Exception {
            log.info("CASE 3.1 START 8 writers race on object 'hot', each expecting version 0");
            int writers = 8;
            ExecutorService pool = Executors.newFixedThreadPool(writers);
            CountDownLatch ready = new CountDownLatch(writers);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger conflicts = new AtomicInteger();
            AtomicInteger wins = new AtomicInteger();
            try {
                List<Future<?>> futures = new java.util.ArrayList<>();
                for (int i = 0; i < writers; i++) {
                    byte[] content = ("writer-" + i).getBytes(StandardCharsets.UTF_8);
                    futures.add(pool.submit((Callable<Void>) () -> {
                        ready.countDown();
                        start.await();
                        try {
                            PutResult result = store.put("hot", streamOf(content), 0);
                            wins.incrementAndGet();
                            log.info("CASE 3.1 writer committed version {}", result.version());
                        } catch (VersionConflictException conflict) {
                            conflicts.incrementAndGet();
                            log.info("CASE 3.1 writer lost the race: {}", conflict.getMessage());
                        }
                        return null;
                    }));
                }
                ready.await(5, TimeUnit.SECONDS);
                start.countDown();
                for (Future<?> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            assertEquals(1, wins.get(), "exactly one writer must commit");
            assertEquals(writers - 1, conflicts.get(), "all losers must receive an explicit conflict");
            List<ObjectVersion> history = store.listVersions("hot");
            assertEquals(1, history.size(), "no losing upload may create a version");
            byte[] actual = readAll(store, "hot", 1);
            String winnerText = new String(actual, StandardCharsets.UTF_8);
            assertTrue(winnerText.startsWith("writer-"));
            assertEquals(sha256Hex(actual), history.get(0).contentHash());
            log.info("CASE 3.1 PASS winner='{}', conflicts={}, versions retained=1",
                    winnerText, conflicts.get());
        }

        @Test
        @DisplayName("case 3.2 stale expected-version on an existing object conflicts")
        void staleExpectedVersionConflicts() {
            log.info("CASE 3.2 START update with out-of-date expected version");
            store.put("seq", streamOf("v1"), 0);
            store.put("seq", streamOf("v2"), 1);
            VersionConflictException error = assertThrows(VersionConflictException.class,
                    () -> store.put("seq", streamOf("stale"), 1));
            assertTrue(error.getMessage().contains("version 2"));
            assertThrows(VersionConflictException.class,
                    () -> store.put("seq", streamOf("new-on-existing"), 0));
            assertEquals(2, store.listVersions("seq").size());
            log.info("CASE 3.2 PASS stale conditional commit rejected: {}", error.getMessage());
        }
    }

    private ObjectStore newStore(Duration gcGrace, Duration sweepMinAge) {
        return new LocalObjectStore(new StoreConfig(dataDir, gcGrace, sweepMinAge));
    }

    private static InputStream streamOf(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    private static InputStream streamOf(byte[] bytes) {
        return new ByteArrayInputStream(bytes);
    }

    private static byte[] readAll(ObjectStore target, String id, long version) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        target.get(id, version, out);
        return out.toByteArray();
    }

    private static String sha256Hex(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private long countBlobs() throws IOException {
        try (Stream<Path> walk = Files.walk(dataDir.resolve("blobs"))) {
            return walk.filter(Files::isRegularFile).count();
        }
    }

    private Path findBlob(String hash) {
        return dataDir.resolve("blobs").resolve(hash.substring(0, 2))
                .resolve(hash.substring(2, 4)).resolve(hash);
    }

    @Nested
    @DisplayName("Scenario 1: content deduplication and sequential versions")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class DeduplicationAndVersions {

        @Test
        @DisplayName("case 1.1 identical uploads share one blob; history is gap-free and readable")
        void sameContentAcrossObjectsAndRevisionsIsDeduplicated() throws Exception {
            log.info("CASE 1.1 START repeated + cross-object upload of identical bytes");
            byte[] payload = "hello-content-addressable-world".getBytes(StandardCharsets.UTF_8);
            String hash = sha256Hex(payload);

            PutResult first = store.put("obj-a", streamOf(payload), 0);
            PutResult second = store.put("obj-a", streamOf(payload), 1);
            PutResult otherObject = store.put("obj-b", streamOf(payload), 0);
            log.info("CASE 1.1 commits: {} / {} / {}", first, second, otherObject);

            assertFalse(first.deduplicated(), "first publish must store a new blob");
            assertTrue(second.deduplicated(), "identical revision must dedupe");
            assertTrue(otherObject.deduplicated(), "cross-object reuse must dedupe");
            assertEquals(1, countBlobs(), "exactly one blob on disk for identical bytes");
            assertTrue(Files.isRegularFile(findBlob(hash)));

            byte[] changed = "different bytes".getBytes(StandardCharsets.UTF_8);
            PutResult third = store.put("obj-a", streamOf(changed), 2);
            assertEquals(2, countBlobs(), "new content adds exactly one blob");

            List<ObjectVersion> history = store.listVersions("obj-a");
            assertEquals(3, history.size());
            assertEquals(1, history.get(0).version());
            assertEquals(2, history.get(1).version());
            assertEquals(3, history.get(2).version());

            assertArrayEquals(payload, readAll(store, "obj-a", 1));
            assertArrayEquals(payload, readAll(store, "obj-a", 2));
            assertArrayEquals(changed, readAll(store, "obj-a", 3));
            assertArrayEquals(changed, readAll(store, "obj-a", -1));
            log.info("CASE 1.1 PASS all historical versions byte-identical, {} blobs total", countBlobs());
        }
    }

    @Nested
    @DisplayName("Scenario 2: integrity verification on read")
    class Integrity {

        @Test
        @DisplayName("case 2.1 corrupted blob yields ContentCorruptedException and no bytes to caller")
        void corruptedBlobIsRejected() throws Exception {
            log.info("CASE 2.1 START tamper committed blob on disk, then read");
            byte[] payload = new byte[200_000];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (i % 251);
            }
            PutResult committed = store.put("doc", streamOf(payload), 0);
            Path blob = findBlob(committed.contentHash());
            Files.write(blob, new byte[]{0, 1, 2, 3},
                    java.nio.file.StandardOpenOption.WRITE,
                    java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);

            ByteArrayOutputStream sink = new ByteArrayOutputStream();
            ContentCorruptedException error = assertThrows(ContentCorruptedException.class,
                    () -> store.get("doc", 1, sink),
                    "tampered content must be reported as corruption");
            assertEquals(0, sink.size(), "caller stream must receive zero bytes on integrity failure");
            assertTrue(error.getMessage().contains(committed.contentHash()));
            log.info("CASE 2.1 PASS rejected corrupt read with error: {}", error.getMessage());
        }

        @Test
        @DisplayName("case 2.2 deleted committed blob is reported as corruption, not wrong bytes")
        void missingBlobIsRejected() throws Exception {
            log.info("CASE 2.2 START delete committed blob underneath the store");
            PutResult committed = store.put("doc2", streamOf("vanish-me"), 0);
            Files.delete(findBlob(committed.contentHash()));
            assertThrows(ContentCorruptedException.class,
                    () -> store.get("doc2", -1, new ByteArrayOutputStream()));
            assertThrows(ObjectNotFoundException.class,
                    () -> store.get("missing-object", -1, new ByteArrayOutputStream()));
            log.info("CASE 2.2 PASS missing blob -> corruption; missing object -> not found");
        }
    }
}
