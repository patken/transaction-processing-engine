package com.patken.transaction.service;

import com.patken.transaction.api.generated.dto.CreateTransactionRequest;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.domain.exception.InvalidTransactionRequestException;
import com.patken.transaction.domain.exception.ReversalNotAllowedException;
import com.patken.transaction.domain.exception.TransactionNotFoundException;
import com.patken.transaction.messaging.producer.KafkaTransactionProducer;
import com.patken.transaction.observability.TransactionMetrics;
import com.patken.transaction.persistence.TransactionGateway;
import com.patken.transaction.persistence.TransactionRepository;
import com.patken.transaction.service.mapper.TransactionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

/**
 * Validates and persists transaction commands, then publishes to Kafka
 * (persist-before-publish, ADR-001) — transactions are created with status RECEIVED.
 */
@Service
public class TransactionCommandService {

    private static final Logger log = LoggerFactory.getLogger(TransactionCommandService.class);

    private final TransactionRepository repository;
    private final TransactionGateway gateway;
    private final TransactionMapper mapper;
    private final KafkaTransactionProducer producer;
    private final TransactionMetrics metrics;

    public TransactionCommandService(TransactionRepository repository, TransactionGateway gateway,
                                      TransactionMapper mapper, KafkaTransactionProducer producer,
                                      TransactionMetrics metrics) {
        this.repository = repository;
        this.gateway = gateway;
        this.mapper = mapper;
        this.producer = producer;
        this.metrics = metrics;
    }

    public CommandResult create(CreateTransactionRequest request, String correlationId) {
        validate(request);

        Transaction transaction = new Transaction(
                UUID.randomUUID(),
                request.getBusinessId(),
                TransactionType.valueOf(request.getType().name()),
                TransactionStatus.RECEIVED,
                request.getAmount(),
                request.getCurrency(),
                request.getSourceAccount(),
                request.getTargetAccount(),
                request.getOriginalTransactionId(),
                normalizeMetadata(request.getMetadata()),
                correlationId
        );

        TransactionGateway.PersistResult result = gateway.persistIdempotent(transaction);
        if (result.created()) {
            metrics.recordCreated(result.transaction().getType());
            publish(result.transaction());
        }
        return new CommandResult(mapper.toResponse(result.transaction()), result.created());
    }

    /**
     * Not retried inline: the transaction is already durable (ADR-001), so a failure
     * here just leaves {@code kafka_published = false} for the Phase 6 recovery
     * scheduler to pick up — the API response still succeeds either way.
     */
    private void publish(Transaction transaction) {
        try {
            producer.publishCommand(transaction);
            repository.markKafkaPublished(transaction.getId());
        } catch (KafkaTransactionProducer.PublishException e) {
            log.warn("Failed to publish transaction {} to Kafka; the Phase 6 recovery scheduler will retry it",
                    transaction.getId(), e);
        }
    }

    // C5: the generated DTO defaults metadata to an empty map rather than null, so
    // "no metadata supplied" would otherwise be stored as '{}'::jsonb instead of NULL —
    // breaking `metadata IS NULL` queries and the "not provided" semantics.
    private static Map<String, Object> normalizeMetadata(Map<String, Object> metadata) {
        return (metadata == null || metadata.isEmpty()) ? null : metadata;
    }

    private void validate(CreateTransactionRequest request) {
        // B5: NUMERIC(19,4) would otherwise round a higher-scale amount silently —
        // a rounding error on a financial amount must be a 400, not a DB side effect.
        if (request.getAmount().scale() > 4) {
            throw new InvalidTransactionRequestException(
                    "amount scale must not exceed 4 decimal places, got " + request.getAmount());
        }

        TransactionType type = TransactionType.valueOf(request.getType().name());

        if (type == TransactionType.REVERSAL) {
            validateReversal(request);
        } else if (request.getOriginalTransactionId() != null) {
            throw new InvalidTransactionRequestException(
                    "originalTransactionId is only valid when type = REVERSAL");
        } else if (request.getSourceAccount().equals(request.getTargetAccount())) {
            throw new InvalidTransactionRequestException(
                    "sourceAccount and targetAccount must differ for " + type);
        }
    }

    private void validateReversal(CreateTransactionRequest request) {
        UUID originalId = request.getOriginalTransactionId();
        if (originalId == null) {
            throw new InvalidTransactionRequestException(
                    "originalTransactionId is required when type = REVERSAL");
        }

        Transaction original = repository.findById(originalId)
                .orElseThrow(() -> new TransactionNotFoundException(originalId));

        if (original.getType() == TransactionType.REVERSAL) {
            throw new ReversalNotAllowedException("Cannot create a REVERSAL of a REVERSAL: " + originalId);
        }
        if (original.getStatus() != TransactionStatus.COMPLETED) {
            throw new ReversalNotAllowedException(
                    "Transaction " + originalId + " must be COMPLETED to be reversed, was " + original.getStatus());
        }
        if (original.getAmount().compareTo(request.getAmount()) != 0) {
            throw new ReversalNotAllowedException(
                    "Reversal amount must equal the original transaction amount (" + original.getAmount() + ")");
        }
        // B3 (ADR-008 amended): a reversal mirrors the original's money movement, so
        // its accounts must be the original's swapped — not client-chosen. This also
        // transitively guarantees source != target, since the original already does.
        if (!request.getSourceAccount().equals(original.getTargetAccount())
                || !request.getTargetAccount().equals(original.getSourceAccount())) {
            throw new ReversalNotAllowedException(
                    "Reversal accounts must mirror the original transaction: sourceAccount="
                            + original.getTargetAccount() + ", targetAccount=" + original.getSourceAccount());
        }
        if (repository.existsByOriginalTransactionId(originalId)) {
            throw new ReversalNotAllowedException("Transaction " + originalId + " has already been reversed");
        }
    }

    public record CommandResult(com.patken.transaction.api.generated.dto.TransactionResponse transaction,
                                 boolean created) {
    }
}
