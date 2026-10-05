package com.github.lechandonga.contentaddressedobjectstorejava.store.api;

/**
 * Raised when on-disk bytes of an already committed blob fail their checksum
 * verification, or when uploaded bytes do not match the expected digest.
 * The store never returns the corrupted bytes to the caller.
 */
public class ContentCorruptedException extends StoreException {
    public ContentCorruptedException(String message) {
        super(message);
    }

    public ContentCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
