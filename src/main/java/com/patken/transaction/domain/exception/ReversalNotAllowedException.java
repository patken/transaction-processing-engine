package com.patken.transaction.domain.exception;

/**
 * Thrown when a REVERSAL request violates the rules in ADR-008 (original not
 * COMPLETED, original already reversed, reversal of a reversal, amount mismatch).
 * Mapped to HTTP 409 — the referenced transaction's state is what blocks the request.
 */
public class ReversalNotAllowedException extends RuntimeException {

    public ReversalNotAllowedException(String message) {
        super(message);
    }
}
