package com.patken.transaction.domain.exception;

import com.patken.transaction.domain.TransactionStateMachine;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.annotation.ProblemMapping;

/**
 * Thrown when a transition is attempted that {@link TransactionStateMachine} does not
 * consider valid. Mapped to HTTP 409 Conflict by the global exception handler — the
 * current state of the resource is what prevents the operation, not the request shape.
 */
@ProblemMapping(status = 409, title = "Invalid state transition")
public class InvalidStateTransitionException extends DomainException {

    private final TransactionStatus from;
    private final TransactionStatus to;

    public InvalidStateTransitionException(TransactionStatus from, TransactionStatus to) {
        super("Transaction cannot move from %s to %s".formatted(from, to));
        this.from = from;
        this.to = to;
    }

    public TransactionStatus getFrom() {
        return from;
    }

    public TransactionStatus getTo() {
        return to;
    }
}
