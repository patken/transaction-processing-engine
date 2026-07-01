package com.patken.transaction.domain;

import com.patken.transaction.domain.annotation.Terminal;

/**
 * Valid transitions between statuses are enforced by {@link TransactionStateMachine},
 * not here — this enum only lists the possible values.
 */
public enum TransactionStatus {
    RECEIVED,
    VALIDATED,
    DISPATCHED,
    PROCESSING,
    @Terminal COMPLETED,
    FAILED,
    RETRY,
    @Terminal DEAD_LETTERED;

    public boolean isTerminal() {
        try {
            return getClass().getField(name()).isAnnotationPresent(Terminal.class);
        } catch (NoSuchFieldException e) {
            throw new AssertionError("Enum constant field must exist for " + name(), e);
        }
    }
}
