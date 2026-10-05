package com.github.lechandonga.contentaddressedobjectstorejava.store.api;

import java.time.Instant;

/**
 * Immutable record describing one committed version of an object.
 *
 * @param version     monotonically increasing, gap-free version number (1-based)
 * @param contentHash SHA-256 digest of the content, hex encoded
 * @param size        content length in bytes
 * @param committedAt commit timestamp assigned by the store
 */
public record ObjectVersion(long version, String contentHash, long size, Instant committedAt) {
}
