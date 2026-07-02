package com.patken.transaction.messaging.consumer;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStateMachine;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.messaging.KafkaTopics;
import com.patken.transaction.messaging.TransactionCommandMessage;
import com.patken.transaction.messaging.producer.KafkaTransactionProducer;
import com.patken.transaction.persistence.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Drives a transaction from RECEIVED to COMPLETED on the happy path (Phase 4 — no
 * retry/DLQ yet, that's Phase 5). Manual ack (KafkaConfig): the offset only commits
 * after the DB work and the outbound event both succeed, per section 1.4 of
 * notes/implementation-plan.md.
 */
@Component
public class TransactionCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionCommandConsumer.class);

    private final TransactionRepository repository;
    private final TransactionStateMachine stateMachine;
    private final BackendSimulator backendSimulator;
    private final KafkaTransactionProducer producer;

    public TransactionCommandConsumer(TransactionRepository repository, TransactionStateMachine stateMachine,
                                       BackendSimulator backendSimulator, KafkaTransactionProducer producer) {
        this.repository = repository;
        this.stateMachine = stateMachine;
        this.backendSimulator = backendSimulator;
        this.producer = producer;
    }

    @KafkaListener(topics = KafkaTopics.COMMANDS)
    public void onMessage(TransactionCommandMessage message, Acknowledgment acknowledgment) {
        Transaction transaction = repository.findById(message.transactionId())
                .orElseThrow(() -> new IllegalStateException(
                        "Received a command for unknown transaction " + message.transactionId()));

        if (transaction.getStatus().isTerminal()) {
            // Redelivery of an already-COMPLETED/DEAD_LETTERED transaction (at-least-once
            // delivery, ADR-001) — a no-op, not an error. ADR-005.
            log.debug("Ignoring redelivered command for terminal transaction {}", transaction.getId());
            acknowledgment.acknowledge();
            return;
        }

        // Async re-validation, distinct from TransactionCommandService's synchronous check
        // at creation time: a consumer shouldn't blindly trust an invariant checked by a
        // different code path, possibly minutes earlier, before acting on money movement.
        // CREDIT/DEBIT have nothing further to re-verify given the current domain model
        // (no account balance/limit system exists yet); REVERSAL depends on the original
        // transaction's state, which is at least conceivable to have changed by now.
        if (transaction.getType() == TransactionType.REVERSAL) {
            String rejectionReason = reversalRejectionReason(transaction);
            if (rejectionReason != null) {
                fail(transaction, rejectionReason);
                log.warn("Transaction {} failed async re-validation: {}", transaction.getId(), rejectionReason);
                acknowledgment.acknowledge();
                return;
            }
        }

        try {
            transaction = advance(transaction, TransactionStatus.VALIDATED);

            transaction = advance(transaction, TransactionStatus.DISPATCHED);
            backendSimulator.dispatch(transaction);

            transaction = advance(transaction, TransactionStatus.PROCESSING);
            backendSimulator.process(transaction);

            TransactionStatus previousStatus = transaction.getStatus();
            transaction = advance(transaction, TransactionStatus.COMPLETED);
            producer.publishEvent(transaction, previousStatus);

            acknowledgment.acknowledge();
        } catch (KafkaTransactionProducer.PublishException e) {
            // Phase 5 adds retry/backoff/DLQ here. For now: don't ack, let the broker
            // redeliver — the transaction is already COMPLETED in the DB, so redelivery
            // is a safe no-op per the terminal-status check above.
            log.error("Failed to publish transaction.events for {}", transaction.getId(), e);
        }
    }

    /**
     * @return a human-readable rejection reason if the original is no longer eligible
     * (missing, or not COMPLETED), or {@code null} if the reversal is still valid.
     * In the current domain model COMPLETED is truly terminal (ADR-005), so this should
     * never actually trip — the check exists as defense in depth, not because the
     * happy path is expected to fail it.
     */
    private String reversalRejectionReason(Transaction transaction) {
        Transaction original = repository.findById(transaction.getOriginalTransactionId()).orElse(null);
        if (original == null || original.getStatus() != TransactionStatus.COMPLETED) {
            return "Original transaction " + transaction.getOriginalTransactionId()
                    + " is no longer eligible for reversal at processing time";
        }
        return null;
    }

    /** RECEIVED, VALIDATED, DISPATCHED, and PROCESSING can all transition to FAILED. */
    private void fail(Transaction transaction, String reason) {
        stateMachine.transition(transaction, TransactionStatus.FAILED);
        repository.updateStatusWithError(transaction.getId(), TransactionStatus.FAILED, reason);
    }

    /**
     * Skips the transition if {@code transaction} is already past {@code to} — a
     * redelivery (at-least-once, ADR-001) can resume mid-flight if an earlier attempt
     * got partway through this same fixed sequence before failing, and blindly
     * replaying an already-applied step would hit an invalid transition.
     *
     * <p>Writes {@code status} via a targeted update ({@link TransactionRepository#updateStatus})
     * rather than {@code repository.save(transaction)} — found the hard way in Phase 4
     * verification: a full-row save can race the API request thread's
     * {@link TransactionRepository#markKafkaPublished} call (they happen close
     * together on the happy path) and clobber the flag back to {@code false} with the
     * stale in-memory copy this consumer read at the start of message handling.
     * Treating status and kafka_published as independently-owned columns, each with
     * its own targeted write, avoids both that and the separate stale-{@code @Version}
     * merge/save gotcha a whole-row write would otherwise carry.
     */
    private Transaction advance(Transaction transaction, TransactionStatus to) {
        if (!stateMachine.isValidTransition(transaction.getStatus(), to)) {
            return transaction;
        }
        stateMachine.transition(transaction, to);
        repository.updateStatus(transaction.getId(), to);
        return transaction;
    }
}
