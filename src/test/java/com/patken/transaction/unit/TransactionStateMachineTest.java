package com.patken.transaction.unit;

import com.patken.transaction.domain.InvalidStateTransitionException;
import com.patken.transaction.domain.TransactionStateMachine;
import com.patken.transaction.domain.TransactionStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exhaustively verifies every (from, to) pair against the transition table decided in
 * docs/implementation-plan.md section 1.2 — not just the happy path.
 */
class TransactionStateMachineTest {

    private static final Set<TransitionCase> VALID_TRANSITIONS = Set.of(
            new TransitionCase(TransactionStatus.RECEIVED, TransactionStatus.VALIDATED),
            new TransitionCase(TransactionStatus.RECEIVED, TransactionStatus.FAILED),
            new TransitionCase(TransactionStatus.VALIDATED, TransactionStatus.DISPATCHED),
            new TransitionCase(TransactionStatus.VALIDATED, TransactionStatus.FAILED),
            new TransitionCase(TransactionStatus.DISPATCHED, TransactionStatus.PROCESSING),
            new TransitionCase(TransactionStatus.DISPATCHED, TransactionStatus.FAILED),
            new TransitionCase(TransactionStatus.PROCESSING, TransactionStatus.COMPLETED),
            new TransitionCase(TransactionStatus.PROCESSING, TransactionStatus.FAILED),
            new TransitionCase(TransactionStatus.FAILED, TransactionStatus.RETRY),
            new TransitionCase(TransactionStatus.FAILED, TransactionStatus.DEAD_LETTERED),
            new TransitionCase(TransactionStatus.RETRY, TransactionStatus.DISPATCHED),
            new TransitionCase(TransactionStatus.RETRY, TransactionStatus.DEAD_LETTERED)
    );

    private final TransactionStateMachine stateMachine = new TransactionStateMachine();

    static Stream<TransitionCase> allPossiblePairs() {
        return EnumSet.allOf(TransactionStatus.class).stream()
                .flatMap(from -> EnumSet.allOf(TransactionStatus.class).stream()
                        .map(to -> new TransitionCase(from, to)));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("allPossiblePairs")
    void everyPairMatchesTheDecidedTransitionTable(TransitionCase transitionCase) {
        boolean expectedValid = VALID_TRANSITIONS.contains(transitionCase);

        assertThat(stateMachine.isValidTransition(transitionCase.from(), transitionCase.to()))
                .as("isValidTransition(%s, %s)", transitionCase.from(), transitionCase.to())
                .isEqualTo(expectedValid);

        if (expectedValid) {
            stateMachine.assertValidTransition(transitionCase.from(), transitionCase.to());
        } else {
            assertThatThrownBy(() -> stateMachine.assertValidTransition(transitionCase.from(), transitionCase.to()))
                    .isInstanceOf(InvalidStateTransitionException.class);
        }
    }

    @Test
    void completedIsTerminal() {
        for (TransactionStatus to : TransactionStatus.values()) {
            assertThat(stateMachine.isValidTransition(TransactionStatus.COMPLETED, to)).isFalse();
        }
    }

    @Test
    void deadLetteredIsTerminal() {
        for (TransactionStatus to : TransactionStatus.values()) {
            assertThat(stateMachine.isValidTransition(TransactionStatus.DEAD_LETTERED, to)).isFalse();
        }
    }

    @Test
    void exceptionCarriesFromAndTo() {
        InvalidStateTransitionException exception = org.junit.jupiter.api.Assertions.assertThrows(
                InvalidStateTransitionException.class,
                () -> stateMachine.assertValidTransition(TransactionStatus.COMPLETED, TransactionStatus.DISPATCHED));

        assertThat(exception.getFrom()).isEqualTo(TransactionStatus.COMPLETED);
        assertThat(exception.getTo()).isEqualTo(TransactionStatus.DISPATCHED);
    }

    private record TransitionCase(TransactionStatus from, TransactionStatus to) {
        @Override
        public String toString() {
            return from + " -> " + to;
        }
    }
}
