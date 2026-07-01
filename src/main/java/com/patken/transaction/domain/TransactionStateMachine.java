package com.patken.transaction.domain;

import com.patken.transaction.domain.exception.InvalidStateTransitionException;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.patken.transaction.domain.TransactionStatus.COMPLETED;
import static com.patken.transaction.domain.TransactionStatus.DEAD_LETTERED;
import static com.patken.transaction.domain.TransactionStatus.DISPATCHED;
import static com.patken.transaction.domain.TransactionStatus.FAILED;
import static com.patken.transaction.domain.TransactionStatus.PROCESSING;
import static com.patken.transaction.domain.TransactionStatus.RECEIVED;
import static com.patken.transaction.domain.TransactionStatus.RETRY;
import static com.patken.transaction.domain.TransactionStatus.VALIDATED;

/**
 * Validates transitions between {@link TransactionStatus} values. See
 * docs/implementation-plan.md section 1.2 for the rationale behind this exact table.
 */
@Component
public class TransactionStateMachine {

    private record Transition(TransactionStatus from, TransactionStatus to) {
    }

    private static final List<Transition> TRANSITIONS = List.of(
            new Transition(RECEIVED, VALIDATED),
            new Transition(RECEIVED, FAILED),
            new Transition(VALIDATED, DISPATCHED),
            new Transition(VALIDATED, FAILED),
            new Transition(DISPATCHED, PROCESSING),
            new Transition(DISPATCHED, FAILED),
            new Transition(PROCESSING, COMPLETED),
            new Transition(PROCESSING, FAILED),
            new Transition(FAILED, RETRY),
            new Transition(FAILED, DEAD_LETTERED),
            new Transition(RETRY, DISPATCHED),
            new Transition(RETRY, DEAD_LETTERED)
    );

    private static final Map<TransactionStatus, Set<TransactionStatus>> VALID_TRANSITIONS =
            Stream.of(TransactionStatus.values())
                    .collect(Collectors.toMap(
                            Function.identity(),
                            status -> TRANSITIONS.stream()
                                    .filter(transition -> transition.from() == status)
                                    .map(Transition::to)
                                    .collect(Collectors.toUnmodifiableSet()),
                            (a, b) -> a,
                            () -> new EnumMap<>(TransactionStatus.class)));

    static {
        // Fail fast if @Terminal ever drifts from the transition table above, rather
        // than letting an inconsistency surface later as a subtle production bug.
        Stream.of(TransactionStatus.values()).forEach(status -> {
            boolean hasOutgoingTransition = !VALID_TRANSITIONS.get(status).isEmpty();
            if (status.isTerminal() == hasOutgoingTransition) {
                throw new ExceptionInInitializerError(
                        "%s: @Terminal (%s) disagrees with the transition table (hasOutgoingTransition=%s)"
                                .formatted(status, status.isTerminal(), hasOutgoingTransition));
            }
        });
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
