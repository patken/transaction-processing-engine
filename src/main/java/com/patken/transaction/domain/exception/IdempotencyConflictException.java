package com.patken.transaction.domain.exception;

/**
 * Thrown when a {@code businessId} is reused with a business payload that differs
 * from the transaction originally stored under that key (ADR-003, amended after
 * review). A same-key-same-payload retry is a safe idempotent replay (200); a
 * same-key-different-payload request is a client bug that must surface, not be
 * silently answered with the original transaction. Mapped to HTTP 409.
 */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String businessId) {
        super("businessId " + businessId + " was already used with a different transaction payload");
    }
}
