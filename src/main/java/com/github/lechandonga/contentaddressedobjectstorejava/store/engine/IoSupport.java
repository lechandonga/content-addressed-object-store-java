package com.github.lechandonga.contentaddressedobjectstorejava.store.engine;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/** Low-level filesystem helpers: durable temp-file writes, atomic renames, fsync. */
final class IoSupport {

    private IoSupport() {
    }

    /**
     * Writes {@code data} durably into {@code target}: bytes land in a temp file
     * in the same directory, are fsynced, then atomically renamed over the target.
     * Readers therefore see either the complete old file or the complete new file.
     */
    static void writeDurable(Path target, byte[] data) throws IOException {
        Path dir = target.getParent();
        Files.createDirectories(dir);
        Path tmp = Files.createTempFile(dir, target.getFileName().toString(), ".tmp");
        boolean moved = false;
        try {
            try (OutputStream out = Files.newOutputStream(tmp, StandardOpenOption.WRITE)) {
                out.write(data);
                out.flush();
                if (out instanceof java.io.FileOutputStream fos) {
                    fos.getFD().sync();
                }
            }
            replaceAtomically(tmp, target);
            moved = true;
        } finally {
            if (!moved) {
                Files.deleteIfExists(tmp);
            }
        }
    }

    /** Durable replacement: destination either keeps the old bytes or shows all new bytes. */
    static void replaceAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
        fsyncDirectory(target.getParent());
    }

    static void atomicMove(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target);
        } catch (FileAlreadyExistsException e) {
            throw e;
        }
    }

    static void fsyncDirectory(Path dir) {
        try (var in = new java.io.FileInputStream(dir.toFile())) {
            in.getFD().sync();
        } catch (Exception ignored) {
            // Directory fsync is best-effort (unsupported on some platforms);
            // atomic rename still guarantees readers never see torn files.
        }
    }

    static byte[] toUtf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
