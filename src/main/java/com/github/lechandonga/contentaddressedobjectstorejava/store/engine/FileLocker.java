package com.github.lechandonga.contentaddressedobjectstorejava.store.engine;

import com.github.lechandonga.contentaddressedobjectstorejava.store.api.VersionConflictException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Exclusive cross-process lock backed by {@link FileChannel#tryLock}.
 * Used per object to serialize the read-modify-write commit sequence across
 * JVMs; JVM-local locks in {@link LocalObjectStore} handle same-JVM contention.
 * The lock is blocking: writers queue instead of failing, and the final
 * compare-and-swap check against the metadata version decides the winner.
 */
final class FileLocker implements AutoCloseable {

    private final FileChannel channel;
    private final FileLock lock;

    private FileLocker(FileChannel channel, FileLock lock) {
        this.channel = channel;
        this.lock = lock;
    }

    static FileLocker lock(Path lockFile) {
        try {
            Files.createDirectories(lockFile.getParent());
            FileChannel channel = FileChannel.open(lockFile,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            try {
                FileLock fileLock;
                do {
                    fileLock = channel.tryLock();
                    if (fileLock == null) {
                        Thread.sleep(5);
                    }
                } while (fileLock == null);
                return new FileLocker(channel, fileLock);
            } catch (OverlappingFileLockException overlapping) {
                // Another thread in THIS JVM already holds the lock; the JVM
                // lock prevents this in practice, but report rather than spin.
                channel.close();
                throw new VersionConflictException("Object is locked by another local writer");
            }
        } catch (IOException e) {
            throw new com.github.lechandonga.contentaddressedobjectstorejava.store.api.StoreException(
                    "Cannot acquire file lock " + lockFile, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new com.github.lechandonga.contentaddressedobjectstorejava.store.api.StoreException(
                    "Interrupted while waiting for file lock " + lockFile, e);
        }
    }

    @Override
    public void close() {
        try {
            lock.release();
        } catch (IOException ignored) {
            // closing the channel releases the lock regardless
        }
        try {
            channel.close();
        } catch (IOException ignored) {
            // best effort
        }
    }
}
