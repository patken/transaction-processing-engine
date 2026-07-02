package com.patken.transaction.messaging;

import com.patken.transaction.domain.TransactionType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Wire format for {@link KafkaTopics#COMMANDS}. {@code correlationId} travels here in
 * the payload (for application logic downstream) and also as a Kafka header (for
 * infra-level tracing without deserializing the payload) — see ADR and section 1.4 of
 * notes/implementation-plan.md.
 */
public record TransactionCommandMessage(
        UUID transactionId,
        String businessId,
        TransactionType type,
        BigDecimal amount,
        String currency,
        String sourceAccount,
        String targetAccount,
        UUID originalTransactionId,
        String correlationId,
        Instant timestamp
) {
}
