package com.patken.transaction.domain;

import com.patken.transaction.domain.annotation.Terminal;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

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

    // Computed once at class-init, not per call — this is read on every consumer
    // message (idempotency check before reprocessing, ADR-005). getDeclaringClass()
    // rather than getClass(): if a constant ever grows a body (an anonymous subclass,
    // e.g. `COMPLETED { ... }`), getClass() would return that subclass and getField
    // would throw; getDeclaringClass() always returns TransactionStatus itself.
    private static final Set<TransactionStatus> TERMINAL = Arrays.stream(values())
            .filter(status -> {
                try {
                    return status.getDeclaringClass().getField(status.name()).isAnnotationPresent(Terminal.class);
                } catch (NoSuchFieldException e) {
                    throw new AssertionError("Enum constant field must exist for " + status.name(), e);
                }
            })
            .collect(Collectors.toUnmodifiableSet());

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }
}
