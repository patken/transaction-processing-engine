package com.patken.transaction.recovery;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.messaging.consumer.TransactionProcessor;
import com.patken.transaction.messaging.producer.KafkaTransactionProducer;
import com.patken.transaction.persistence.TransactionRepository;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Recovers transactions that started processing but went quiet — a consumer that died
 * mid-flight, or a re-dispatch whose republish was lost (ADR-006). Runs on a coarse
 * cadence (this is a safety net, not the primary retry path — that's the consumer's own
 * loop, Phase 5) and single-instance via ShedLock.
 *
 * <p>Each stuck transaction is treated as one more failed attempt through the shared
 * {@link TransactionProcessor#recoverStuck} path: under the ceiling it's re-dispatched
 * to Kafka (the consumer resumes RETRY → DISPATCHED, ADR-005/D2); at the ceiling it's
 * dead-lettered. Re-using that path keeps the retry ceiling and audit trail identical
 * whether a failure was caught inline by the consumer or swept up here.
 */
@Component
public class StuckTransactionScheduler {

    private static final Logger log = LoggerFactory.getLogger(StuckTransactionScheduler.class);

    private static final List<TransactionStatus> IN_FLIGHT =
            List.of(TransactionStatus.DISPATCHED, TransactionStatus.PROCESSING, TransactionStatus.RETRY);

    private final TransactionRepository repository;
    private final TransactionProcessor processor;
    private final KafkaTransactionProducer producer;
    private final Duration stuckTimeout;
    private final int batchSize;

    public StuckTransactionScheduler(TransactionRepository repository, TransactionProcessor processor,
                                     KafkaTransactionProducer producer,
                                     @Value("${transaction-engine.recovery.stuck-timeout:PT10M}") Duration stuckTimeout,
                                     @Value("${transaction-engine.recovery.batch-size:100}") int batchSize) {
        this.repository = repository;
        this.processor = processor;
        this.producer = producer;
        this.stuckTimeout = stuckTimeout;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${transaction-engine.recovery.poll-interval-ms:300000}")
    @SchedulerLock(name = "stuckTransactionRecovery")
    public void recoverStuckTransactions() {
        Instant cutoff = Instant.now().minus(stuckTimeout);
        List<Transaction> stuck = repository.findStuck(IN_FLIGHT, cutoff, PageRequest.of(0, batchSize));
        if (stuck.isEmpty()) {
            return;
        }
        log.info("Recovering {} stuck transaction(s) (in-flight and idle for > {})", stuck.size(), stuckTimeout);
        stuck.forEach(this::recover);
    }

    private void recover(Transaction transaction) {
        TransactionProcessor.Outcome outcome = processor.recoverStuck(transaction.getId());
        switch (outcome.result()) {
            case RETRY_SCHEDULED -> redispatch(outcome.transaction());
            case DEAD_LETTERED -> {
                log.warn("Stuck transaction {} exhausted retries; dead-lettering", transaction.getId());
                publishToDlqBestEffort(outcome.transaction(), outcome.reason());
            }
            // LOCK_SKIPPED (a consumer is genuinely still working it) / ALREADY_TERMINAL
            // (progressed since the batch was read): leave it for the next cycle.
            default -> { }
        }
    }

    private void redispatch(Transaction transaction) {
        try {
            producer.publishCommand(transaction);
        } catch (KafkaTransactionProducer.PublishException e) {
            // The transaction is at RETRY in the DB; if the republish failed (e.g. Kafka
            // still down), the next cycle finds it stuck again and retries the dispatch.
            log.error("Failed to re-dispatch stuck transaction {}; will retry next cycle", transaction.getId(), e);
        }
    }

    private void publishToDlqBestEffort(Transaction transaction, String reason) {
        try {
            producer.publishToDlq(transaction, reason);
        } catch (KafkaTransactionProducer.PublishException e) {
            log.error("Failed to publish dead-lettered transaction {} to the DLQ topic", transaction.getId(), e);
        }
    }
}
