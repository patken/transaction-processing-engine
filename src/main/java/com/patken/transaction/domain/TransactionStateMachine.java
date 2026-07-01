package com.patken.transaction.domain;

import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Validates transitions between {@link TransactionStatus} values. See
 * docs/implementation-plan.md section 1.2 for the rationale behind this exact table.
 */
@Component
public class TransactionStateMachine {

    private static final Map<TransactionStatus, Set<TransactionStatus>> VALID_TRANSITIONS =
            new EnumMap<>(TransactionStatus.class);

    static {
        VALID_TRANSITIONS.put(TransactionStatus.RECEIVED,
                EnumSet.of(TransactionStatus.VALIDATED, TransactionStatus.FAILED));
        VALID_TRANSITIONS.put(TransactionStatus.VALIDATED,
                EnumSet.of(TransactionStatus.DISPATCHED, TransactionStatus.FAILED));
        VALID_TRANSITIONS.put(TransactionStatus.DISPATCHED,
                EnumSet.of(TransactionStatus.PROCESSING, TransactionStatus.FAILED));
        VALID_TRANSITIONS.put(TransactionStatus.PROCESSING,
                EnumSet.of(TransactionStatus.COMPLETED, TransactionStatus.FAILED));
        VALID_TRANSITIONS.put(TransactionStatus.FAILED,
                EnumSet.of(TransactionStatus.RETRY, TransactionStatus.DEAD_LETTERED));
        VALID_TRANSITIONS.put(TransactionStatus.RETRY,
                EnumSet.of(TransactionStatus.DISPATCHED, TransactionStatus.DEAD_LETTERED));
        VALID_TRANSITIONS.put(TransactionStatus.COMPLETED, EnumSet.noneOf(TransactionStatus.class));
        VALID_TRANSITIONS.put(TransactionStatus.DEAD_LETTERED, EnumSet.noneOf(TransactionStatus.class));
    }

    public boolean isValidTransition(TransactionStatus from, TransactionStatus to) {
        return VALID_TRANSITIONS.getOrDefault(from, Set.of()).contains(to);
    }

    /**
     * @throws InvalidStateTransitionException if {@code from -> to} is not allowed.
     */
    public void assertValidTransition(TransactionStatus from, TransactionStatus to) {
        if (!isValidTransition(from, to)) {
            throw new InvalidStateTransitionException(from, to);
        }
    }

    public void transition(Transaction transaction, TransactionStatus to) {
        assertValidTransition(transaction.getStatus(), to);
        transaction.setStatus(to);
    }
}
