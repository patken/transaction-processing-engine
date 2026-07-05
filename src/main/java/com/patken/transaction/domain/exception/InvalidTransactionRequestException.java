package com.patken.transaction.domain.exception;

import com.patken.transaction.domain.annotation.ProblemMapping;

/**
 * Thrown for cross-field validation that the DTO's bean-validation annotations can't
 * express alone (e.g. sourceAccount == targetAccount, originalTransactionId set on a
 * non-REVERSAL). Mapped to HTTP 400 — the request itself is malformed, not a state conflict.
 */
@ProblemMapping(status = 400, title = "Invalid transaction request")
public class InvalidTransactionRequestException extends DomainException {

    public InvalidTransactionRequestException(String message) {
        super(message);
    }
}
