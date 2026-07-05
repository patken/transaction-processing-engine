package com.patken.transaction.domain.exception;

import com.patken.transaction.domain.annotation.ProblemMapping;

/**
 * Base for client-facing business errors — the ones that must become a well-formed
 * RFC 7807 response rather than a generic 500. Every concrete subclass carries a
 * {@link ProblemMapping} declaring its HTTP status and title; {@code GlobalExceptionHandler}
 * handles the whole family through one {@code @ExceptionHandler(DomainException.class)}.
 *
 * <p>Deliberately excludes {@link TransientProcessingException}: that one is an internal
 * retry signal for the consumer, never a synchronous API response.
 */
public abstract class DomainException extends RuntimeException {

    protected DomainException(String message) {
        super(message);
    }

    protected DomainException(String message, Throwable cause) {
        super(message, cause);
    }
}
