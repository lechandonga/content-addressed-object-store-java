package com.github.lechandonga.contentaddressedobjectstorejava.store.api;

/**
 * Outcome of a successful upload.
 *
 * @param objectId object the content was attached to
 * @param version  the newly committed version number
 * @param contentHash SHA-256 digest of the uploaded bytes
 * @param deduplicated {@code true} when the blob already existed and no bytes
 *                     were written to the content area
 */
public record PutResult(String objectId, long version, String contentHash, boolean deduplicated) {
}
