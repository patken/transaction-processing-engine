package com.patken.transaction.messaging.consumer;

import com.patken.transaction.domain.FailureType;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionFailureAudit;
import com.patken.transaction.domain.TransactionStateMachine;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.domain.exception.TransientProcessingException;
import com.patken.transaction.persistence.TransactionFailureAuditRepository;
import com.patken.transaction.persistence.TransactionRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

/**
 * One processing attempt, run under a pessimistic row lock (ADR-002) in a single
 * transaction: lock → drive status → record outcome → commit. Deliberately a separate
 * bean from {@link TransactionCommandConsumer} so the consumer's retry loop can invoke
 * {@link #attemptOnce} across the proxy boundary and actually get {@code @Transactional}
 * semantics (self-invocation wouldn't).
 *
 * <p>Because the row is loaded <em>after</em> the lock is acquired and mutated as a
 * managed entity (single flush at commit), this is where the Phase 4 {@code
 * kafka_published} race is structurally closed: any concurrent writer to the same row
 * (the API thread's {@code markKafkaPublished}) is serialized by the lock, so a
 * whole-row flush here can no longer clobber it.
 */
@Component
public class TransactionProcessor {

    public enum Result { COMPLETED, ALREADY_TERMINAL, RETRY_SCHEDULED, DEAD_LETTERED, LOCK_SKIPPED }

    /**
     * @param transaction the (now-detached, post-commit) row, for the caller to build the
     *                    outbound event / DLQ payload; null for LOCK_SKIPPED.
     * @param previousStatus only set for COMPLETED (the status the completion event reports moving from).
     * @param reason only set for DEAD_LETTERED (the dead-letter reason).
     */
    public record Outcome(Result result, Transaction transaction, TransactionStatus previousStatus, String reason) {
    }

    private final TransactionRepository repository;
    private final TransactionFailureAuditRepository auditRepository;
    private final TransactionStateMachine stateMachine;
    private final BackendSimulator backendSimulator;
    private final int maxRetries;

    public TransactionProcessor(TransactionRepository repository,
                                TransactionFailureAuditRepository auditRepository,
                                TransactionStateMachine stateMachine,
                                BackendSimulator backendSimulator,
                                @Value("${transaction-engine.processing.max-retries:3}") int maxRetries) {
        this.repository = repository;
        this.auditRepository = auditRepository;
        this.stateMachine = stateMachine;
        this.backendSimulator = backendSimulator;
        this.maxRetries = maxRetries;
    }

    @Transactional
    public Outcome attemptOnce(UUID transactionId) {
        Optional<Transaction> locked = repository.lockForProcessing(transactionId);
        if (locked.isEmpty()) {
            // Another worker holds the row (ADR-002) — skipped, not waited on.
            return new Outcome(Result.LOCK_SKIPPED, null, null, null);
        }
        Transaction txn = locked.get();

        if (txn.getStatus().isTerminal()) {
            // At-least-once redelivery of an already-finished transaction (ADR-001/005).
            return new Outcome(Result.ALREADY_TERMINAL, txn, null, null);
        }

        // Permanent business rejection (not a transient failure) — no retry, straight to
        // DEAD_LETTERED. See the async-revalidation rationale below.
        if (txn.getType() == TransactionType.REVERSAL) {
            String rejection = reversalRejectionReason(txn);
            if (rejection != null) {
                return recordFailureAndDecide(txn, rejection, /* forceDeadLetter */ true);
            }
        }

        try {
            TransactionStatus previousStatus = driveToCompleted(txn);
            return new Outcome(Result.COMPLETED, txn, previousStatus, null);
        } catch (TransientProcessingException e) {
            return recordFailureAndDecide(txn, e.getMessage(), /* forceDeadLetter */ false);
        }
    }

    /**
     * Drives the managed entity forward to COMPLETED from wherever it currently is —
     * a fresh RECEIVED, a RETRY re-entry (RETRY → DISPATCHED, ADR-005), or a
     * crash-mid-flight resume. {@link #advance} skips transitions already applied, so
     * the same fixed sequence is safe to replay.
     *
     * <p>The simulator's {@code dispatch}/{@code process} are called unconditionally
     * (not gated on whether the transition was a no-op): re-running them on a resume is
     * safe here because the simulator is side-effect-free beyond a delay. A real backend
     * would need idempotency keys to make the re-dispatch/re-process safe.
     *
     * @return the status the completion moves from (always PROCESSING), for the event.
     */
    private TransactionStatus driveToCompleted(Transaction txn) {
        advance(txn, TransactionStatus.VALIDATED);
        advance(txn, TransactionStatus.DISPATCHED);
        backendSimulator.dispatch(txn);
        advance(txn, TransactionStatus.PROCESSING);
        backendSimulator.process(txn); // may throw TransientProcessingException
        TransactionStatus previousStatus = txn.getStatus();
        advance(txn, TransactionStatus.COMPLETED);
        return previousStatus;
    }

    private Outcome recordFailureAndDecide(Transaction txn, String errorMessage, boolean forceDeadLetter) {
        int attempt = txn.getRetryCount() + 1;
        advance(txn, TransactionStatus.FAILED);
        txn.setRetryCount(attempt);
        txn.setErrorMessage(errorMessage);
        auditRepository.save(new TransactionFailureAudit(txn.getId(), FailureType.PROCESSING, attempt, errorMessage));

        if (forceDeadLetter || attempt >= maxRetries) {
            advance(txn, TransactionStatus.DEAD_LETTERED);
            return new Outcome(Result.DEAD_LETTERED, txn, null, errorMessage);
        }
        advance(txn, TransactionStatus.RETRY);
        return new Outcome(Result.RETRY_SCHEDULED, txn, null, errorMessage);
    }

    /**
     * In the current domain model COMPLETED is terminal (ADR-005), so this should never
     * trip — defense in depth against acting on a reversal whose original changed since
     * the synchronous create-time check.
     */
    private String reversalRejectionReason(Transaction txn) {
        Transaction original = repository.findById(txn.getOriginalTransactionId()).orElse(null);
        if (original == null || original.getStatus() != TransactionStatus.COMPLETED) {
            return "Original transaction " + txn.getOriginalTransactionId()
                    + " is no longer eligible for reversal at processing time";
        }
        return null;
    }

    /** Mutates the managed entity's status if the transition is valid; a no-op otherwise. */
    private void advance(Transaction txn, TransactionStatus to) {
        if (stateMachine.isValidTransition(txn.getStatus(), to)) {
            stateMachine.transition(txn, to);
        }
    }
}
