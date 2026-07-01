package com.patken.transaction.domain.exception;

import java.util.UUID;

/** Mapped to HTTP 404. */
public class TransactionNotFoundException extends RuntimeException {

    public TransactionNotFoundException(UUID transactionId) {
        super("No transaction found with id " + transactionId);
    }

    public TransactionNotFoundException(String businessId) {
        super("No transaction found with businessId " + businessId);
    }
}
