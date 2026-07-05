package com.patken.transaction.domain.exception;

import com.patken.transaction.domain.annotation.ProblemMapping;

import java.util.UUID;

/** Mapped to HTTP 404. */
@ProblemMapping(status = 404, title = "Transaction not found")
public class TransactionNotFoundException extends DomainException {

    public TransactionNotFoundException(UUID transactionId) {
        super("No transaction found with id " + transactionId);
    }

    public TransactionNotFoundException(String businessId) {
        super("No transaction found with businessId " + businessId);
    }
}
