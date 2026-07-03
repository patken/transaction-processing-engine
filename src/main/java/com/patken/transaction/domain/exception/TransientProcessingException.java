package com.patken.transaction.domain.exception;

/**
 * A backend processing step failed in a way that's worth retrying (a transient/simulated
 * backend error, as opposed to a permanent business rejection). The consumer catches
 * this to drive the retry → DLQ path (Phase 5, ADR-004).
 */
public class TransientProcessingException extends RuntimeException {

    public TransientProcessingException(String message) {
        super(message);
    }
}
