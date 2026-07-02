package com.patken.transaction.messaging;

import com.patken.transaction.domain.TransactionStatus;

import java.time.Instant;
import java.util.UUID;

/** Wire format for {@link KafkaTopics#EVENTS} — one message per status change. */
public record TransactionEventMessage(
        UUID transactionId,
        String businessId,
        TransactionStatus status,
        TransactionStatus previousStatus,
        String correlationId,
        Instant timestamp
) {
}
