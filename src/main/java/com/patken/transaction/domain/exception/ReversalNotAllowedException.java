package com.patken.transaction.domain.exception;

import com.patken.transaction.domain.annotation.ProblemMapping;

/**
 * Thrown when a REVERSAL request violates the rules in ADR-008 (original not
 * COMPLETED, original already reversed, reversal of a reversal, amount mismatch).
 * Mapped to HTTP 409 — the referenced transaction's state is what blocks the request.
 */
@ProblemMapping(status = 409, title = "Reversal not allowed")
public class ReversalNotAllowedException extends DomainException {

    public ReversalNotAllowedException(String message) {
        super(message);
    }

    public ReversalNotAllowedException(String message, Throwable cause) {
        super(message, cause);
    }
}
