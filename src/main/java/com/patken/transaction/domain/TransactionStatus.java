package com.patken.transaction.domain;

/**
 * Valid transitions between statuses are enforced by {@link TransactionStateMachine},
 * not here — this enum only lists the possible values.
 */
public enum TransactionStatus {
    RECEIVED,
    VALIDATED,
    DISPATCHED,
    PROCESSING,
    COMPLETED,
    FAILED,
    RETRY,
    DEAD_LETTERED
}
