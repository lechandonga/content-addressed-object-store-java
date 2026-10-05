package com.github.lechandonga.contentaddressedobjectstorejava.store.api;

/**
 * Base type for all failures raised by the object store. Callers can distinguish
 * integrity failures, optimistic-concurrency conflicts and not-found cases via
 * the concrete subtypes instead of parsing error messages.
 */
public class StoreException extends RuntimeException {
    public StoreException(String message) {
        super(message);
    }

    public StoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
