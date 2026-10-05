package com.github.lechandonga.contentaddressedobjectstorejava.store.api;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Local-disk, content-addressed, versioned object store.
 *
 * <p>Content is deduplicated by SHA-256 digest: identical bytes uploaded for any
 * object (or repeatedly for one object) occupy a single blob on disk. Each upload
 * that changes an object creates a new immutable, sequentially numbered version.
 * Updates are compare-and-swap against {@code expectedVersion}; concurrent
 * writers that race produce exactly one winner and one
 * {@link VersionConflictException}.
 */
public interface ObjectStore {

    /**
     * Streams content into the store and commits it as a new version.
     *
     * @param objectId        target object identifier
     * @param content         content bytes; the stream is always closed
     * @param expectedVersion the version the caller based its update on:
     *                        {@code 0} to require that the object does not exist yet,
     *                        or the latest known version number. Use {@code -1} to
     *                        append unconditionally.
     * @return commit result including the assigned version number
     * @throws VersionConflictException if the object's current version differs
     * @throws ContentCorruptedException if the uploaded stream does not match a
     *                                   caller-supplied expectation, or IO fails
     *                                   before a durable blob exists
     */
    PutResult put(String objectId, InputStream content, long expectedVersion);

    /**
     * Writes the exact bytes of a committed version into {@code out}. The bytes
     * are checksum-verified before they are handed out; on mismatch a
     * {@link ContentCorruptedException} is thrown and {@code out} receives no data.
     *
     * @param objectId object identifier
     * @param version  version number; use {@code -1} for the latest version
     * @param out      destination stream (not closed by the store)
     */
    void get(String objectId, long version, OutputStream out);

    /** Returns the ordered version history of an object, oldest first. */
    List<ObjectVersion> listVersions(String objectId);

    /**
     * Reclaims unreferenced blobs and stale staging files. Blobs still referenced
     * by any committed version are never deleted. Concurrent writes are safe:
     * blobs observed in-flight are protected by a grace window, and blobs
     * committed while reclaim is running are rescued if they were quarantined.
     */
    GarbageCollectionReport collectGarbage();
}
