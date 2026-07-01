package com.patken.transaction.domain.exception;

/**
 * Thrown for cross-field validation that the DTO's bean-validation annotations can't
 * express alone (e.g. sourceAccount == targetAccount, originalTransactionId set on a
 * non-REVERSAL). Mapped to HTTP 400 — the request itself is malformed, not a state conflict.
 */
public class InvalidTransactionRequestException extends RuntimeException {

    public InvalidTransactionRequestException(String message) {
        super(message);
    }
}
