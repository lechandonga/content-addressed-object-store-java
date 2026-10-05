package com.github.lechandonga.contentaddressedobjectstorejava.store.api;

/**
 * Raised when a conditional update loses the optimistic-concurrency race:
 * another writer committed a newer version in the meantime. No version is
 * overwritten and the losing upload is fully discarded.
 */
public class VersionConflictException extends StoreException {
    public VersionConflictException(String message) {
        super(message);
    }
}
