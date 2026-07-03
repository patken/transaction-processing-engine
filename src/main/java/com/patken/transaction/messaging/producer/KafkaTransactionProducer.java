package com.patken.transaction.messaging.producer;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.messaging.KafkaTopics;
import com.patken.transaction.messaging.TransactionCommandMessage;
import com.patken.transaction.messaging.TransactionEventMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes commands and events. {@code correlationId} travels both as a Kafka header
 * (infra-level tracing without deserializing the payload) and in the payload itself
 * (application logic downstream) — section 1.4 of notes/implementation-plan.md.
 *
 * <p>Publish is synchronous (bounded wait on the send future): the caller needs to know
 * the outcome immediately to decide whether to mark {@code kafka_published = true}
 * (ADR-001). A failure here is not retried inline — persist-before-publish means the
 * row is already durable, and the Phase 6 recovery scheduler picks up anything left
 * with {@code kafka_published = false}.
 */
@Component
public class KafkaTransactionProducer {

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaTransactionProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishCommand(Transaction transaction) throws PublishException {
        TransactionCommandMessage message = new TransactionCommandMessage(
                transaction.getId(),
                transaction.getBusinessId(),
                transaction.getType(),
                transaction.getAmount(),
                transaction.getCurrency(),
                transaction.getSourceAccount(),
                transaction.getTargetAccount(),
                transaction.getOriginalTransactionId(),
                transaction.getCorrelationId(),
                Instant.now()
        );
        send(KafkaTopics.COMMANDS, transaction.getId().toString(), transaction.getCorrelationId(), message);
    }

    public void publishEvent(Transaction transaction, TransactionStatus previousStatus) throws PublishException {
        TransactionEventMessage message = new TransactionEventMessage(
                transaction.getId(),
                transaction.getBusinessId(),
                transaction.getStatus(),
                previousStatus,
                transaction.getCorrelationId(),
                Instant.now()
        );
        send(KafkaTopics.EVENTS, transaction.getId().toString(), transaction.getCorrelationId(), message);
    }

    /**
     * Publishes the original command to {@link KafkaTopics#DLQ} for ops replay (ADR-004),
     * with the dead-letter reason as a header. The transaction is already DEAD_LETTERED
     * in the DB (the source of truth) before this runs — a DLQ publish failure is logged
     * by the caller, not retried; the DB state is what matters.
     */
    public void publishToDlq(TransactionCommandMessage command, String reason) throws PublishException {
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(KafkaTopics.DLQ, null, command.transactionId().toString(), command);
        record.headers().add(new RecordHeader("dlq-reason", reason.getBytes(StandardCharsets.UTF_8)));
        if (command.correlationId() != null) {
            record.headers().add(
                    new RecordHeader("correlationId", command.correlationId().getBytes(StandardCharsets.UTF_8)));
        }
        awaitSend(KafkaTopics.DLQ, record);
    }

    private void send(String topic, String key, String correlationId, Object payload) throws PublishException {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, null, key, payload);
        if (correlationId != null) {
            record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));
        }
        awaitSend(topic, record);
    }

    private void awaitSend(String topic, ProducerRecord<String, Object> record) throws PublishException {
        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishException("Interrupted while publishing to " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new PublishException("Failed to publish to " + topic, e);
        }
    }

    /** Checked on purpose: callers must decide explicitly whether a publish failure is fatal. */
    public static class PublishException extends Exception {
        public PublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
