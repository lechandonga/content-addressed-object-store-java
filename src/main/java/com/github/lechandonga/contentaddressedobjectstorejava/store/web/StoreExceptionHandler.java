package com.github.lechandonga.contentaddressedobjectstorejava.store.web;

import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ContentCorruptedException;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectNotFoundException;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.StoreException;
import com.github.lechandonga.contentaddressedobjectstorejava.store.api.VersionConflictException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/** Maps store failure types to distinct HTTP status codes. */
@RestControllerAdvice
public class StoreExceptionHandler {

    @ExceptionHandler(VersionConflictException.class)
    public ResponseEntity<Map<String, String>> conflict(VersionConflictException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "version_conflict", "message", e.getMessage()));
    }

    @ExceptionHandler(ContentCorruptedException.class)
    public ResponseEntity<Map<String, String>> corrupt(ContentCorruptedException e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(Map.of("error", "content_corrupt", "message", e.getMessage()));
    }

    @ExceptionHandler(ObjectNotFoundException.class)
    public ResponseEntity<Map<String, String>> notFound(ObjectNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "not_found", "message", e.getMessage()));
    }

    @ExceptionHandler(StoreException.class)
    public ResponseEntity<Map<String, String>> storeFailure(StoreException e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "store_failure", "message", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest()
                .body(Map.of("error", "bad_request", "message", e.getMessage()));
    }
}
