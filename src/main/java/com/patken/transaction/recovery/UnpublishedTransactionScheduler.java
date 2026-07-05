package com.patken.transaction.recovery;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.messaging.consumer.TransactionProcessor;
import com.patken.transaction.messaging.producer.KafkaTransactionProducer;
import com.patken.transaction.observability.SchedulerHealthIndicator;
import com.patken.transaction.observability.SchedulerHeartbeat;
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
 * The outbox recovery half of persist-before-publish (ADR-001): retries the Kafka
 * publish for transactions that are durable in the DB but never made it onto
 * {@code transaction.commands}. Single-instance via ShedLock.
 *
 * <p>Retries are capped at the same {@code max-retries} as processing failures
 * (ADR-004, revised): past the ceiling the transaction is DEAD_LETTERED with a
 * {@code failure_type = PUBLISH} audit row, rather than being republished forever on a
 * prolonged Kafka outage. The publish itself runs outside any DB transaction so a slow
 * or failing Kafka send never holds a row lock.
 */
@Component
public class UnpublishedTransactionScheduler {

    private static final Logger log = LoggerFactory.getLogger(UnpublishedTransactionScheduler.class);

    private final TransactionRepository repository;
    private final TransactionProcessor processor;
    private final KafkaTransactionProducer producer;
    private final SchedulerHeartbeat heartbeat;
    private final Duration minAge;
    private final int batchSize;

    public UnpublishedTransactionScheduler(TransactionRepository repository, TransactionProcessor processor,
                                           KafkaTransactionProducer producer, SchedulerHeartbeat heartbeat,
                                           @Value("${transaction-engine.recovery.unpublished-min-age:PT1M}") Duration minAge,
                                           @Value("${transaction-engine.recovery.batch-size:100}") int batchSize) {
        this.repository = repository;
        this.processor = processor;
        this.producer = producer;
        this.heartbeat = heartbeat;
        this.minAge = minAge;
        this.batchSize = batchSize;
    }

    @Scheduled(
            fixedDelayString = "${transaction-engine.recovery.poll-interval-ms:300000}",
            initialDelayString = "${transaction-engine.recovery.initial-delay-ms:0}")
    @SchedulerLock(name = SchedulerHealthIndicator.UNPUBLISHED_JOB)
    public void recoverUnpublishedTransactions() {
        heartbeat.recordRun(SchedulerHealthIndicator.UNPUBLISHED_JOB);
        Instant cutoff = Instant.now().minus(minAge);
        List<Transaction> unpublished = repository.findUnpublished(cutoff, PageRequest.of(0, batchSize));
        if (unpublished.isEmpty()) {
            return;
        }
        log.info("Retrying publish for {} unpublished transaction(s)", unpublished.size());
        unpublished.forEach(this::retryPublish);
    }

    private void retryPublish(Transaction transaction) {
        try {
            producer.publishCommand(transaction);
            repository.markKafkaPublished(transaction.getId());
        } catch (KafkaTransactionProducer.PublishException e) {
            boolean deadLettered = processor.recordPublishFailure(transaction.getId(), e.getMessage());
            if (deadLettered) {
                log.warn("Unpublished transaction {} exhausted publish retries; dead-lettered", transaction.getId());
            } else {
                log.warn("Publish retry failed for transaction {}; will retry next cycle", transaction.getId());
            }
        }
    }
}
