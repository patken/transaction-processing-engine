package com.patken.transaction.messaging.consumer;

import com.patken.transaction.messaging.KafkaTopics;
import com.patken.transaction.messaging.TransactionCommandMessage;
import com.patken.transaction.messaging.producer.KafkaTransactionProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka glue + retry orchestration. Each attempt's transactional work is delegated to
 * {@link TransactionProcessor}; this class owns the retry cadence (backoff between
 * attempts) and the post-commit Kafka side effects (completion event / DLQ publish).
 *
 * <p>Retry is driven here in the app (backoff loop over {@link TransactionProcessor#attemptOnce})
 * rather than via Spring Kafka's error handler, because the retry state must be
 * persistent — status RETRY, the {@code retry_count} column, the failure-audit rows —
 * which the Phase 6 recovery scheduler reads. Backoffs are short (well under
 * {@code max.poll.interval.ms}); the Phase 6 scheduler is the coarse-grained safety net
 * for anything a crash leaves behind.
 */
@Component
public class TransactionCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionCommandConsumer.class);

    private static final int MAX_LOCK_SKIP_RETRIES = 5;
    private static final long LOCK_SKIP_BACKOFF_MS = 50;

    private final TransactionProcessor processor;
    private final KafkaTransactionProducer producer;
    private final long backoffBaseMs;
    private final double backoffMultiplier;

    public TransactionCommandConsumer(TransactionProcessor processor, KafkaTransactionProducer producer,
                                      @Value("${transaction-engine.processing.backoff-ms:100}") long backoffBaseMs,
                                      @Value("${transaction-engine.processing.backoff-multiplier:2.0}") double backoffMultiplier) {
        this.processor = processor;
        this.producer = producer;
        this.backoffBaseMs = backoffBaseMs;
        this.backoffMultiplier = backoffMultiplier;
    }

    @KafkaListener(topics = KafkaTopics.COMMANDS)
    public void onMessage(TransactionCommandMessage message, Acknowledgment acknowledgment) {
        // Ack only after the message is fully handled to a terminal-for-this-consumer
        // outcome. An unexpected exception (a real bug / DB outage) propagates and the
        // offset isn't committed — the record is redelivered rather than silently dropped.
        handle(message);
        acknowledgment.acknowledge();
    }

    private void handle(TransactionCommandMessage message) {
        int lockSkips = 0;
        while (true) {
            TransactionProcessor.Outcome outcome = processor.attemptOnce(message.transactionId());
            switch (outcome.result()) {
                case COMPLETED -> {
                    publishEventBestEffort(outcome);
                    return;
                }
                case ALREADY_TERMINAL -> {
                    return;
                }
                case DEAD_LETTERED -> {
                    log.warn("Transaction {} dead-lettered: {}", message.transactionId(), outcome.reason());
                    publishDlqBestEffort(message, outcome.reason());
                    return;
                }
                case RETRY_SCHEDULED -> sleep(backoffFor(outcome.transaction().getRetryCount()));
                case LOCK_SKIPPED -> {
                    if (++lockSkips > MAX_LOCK_SKIP_RETRIES) {
                        // Persistently locked by another worker — leave it for that worker
                        // or the Phase 6 recovery scheduler rather than spinning.
                        log.warn("Could not acquire processing lock for {} after {} tries; leaving for recovery",
                                message.transactionId(), lockSkips);
                        return;
                    }
                    sleep(LOCK_SKIP_BACKOFF_MS);
                }
            }
        }
    }

    /** Exponential backoff before the (retryCount+1)-th attempt: base * multiplier^(retryCount-1). */
    private long backoffFor(int retryCount) {
        return Math.round(backoffBaseMs * Math.pow(backoffMultiplier, Math.max(0, retryCount - 1)));
    }

    private void publishEventBestEffort(TransactionProcessor.Outcome outcome) {
        try {
            producer.publishEvent(outcome.transaction(), outcome.previousStatus());
        } catch (KafkaTransactionProducer.PublishException e) {
            // The transaction is COMPLETED in the DB (source of truth); the event is a
            // downstream notification. Not retried here — logged, consistent with Phase 4.
            log.error("Failed to publish completion event for {}", outcome.transaction().getId(), e);
        }
    }

    private void publishDlqBestEffort(TransactionCommandMessage message, String reason) {
        try {
            producer.publishToDlq(message, reason);
        } catch (KafkaTransactionProducer.PublishException e) {
            // Already DEAD_LETTERED in the DB (source of truth, ADR-004); the DLQ topic is
            // an ops-replay convenience. Logged, not retried.
            log.error("Failed to publish transaction {} to the DLQ topic", message.transactionId(), e);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during retry backoff", e);
        }
    }
}
