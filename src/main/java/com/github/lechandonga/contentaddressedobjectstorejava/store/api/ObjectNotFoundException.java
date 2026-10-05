package com.github.lechandonga.contentaddressedobjectstorejava.store.api;

/** Raised when an object, object version or blob is not present in the store. */
public class ObjectNotFoundException extends StoreException {
    public ObjectNotFoundException(String message) {
        super(message);
    }
}
