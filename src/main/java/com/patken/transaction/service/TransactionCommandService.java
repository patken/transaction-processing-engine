package com.patken.transaction.service;

import com.patken.transaction.api.generated.dto.CreateTransactionRequest;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.domain.exception.InvalidTransactionRequestException;
import com.patken.transaction.domain.exception.ReversalNotAllowedException;
import com.patken.transaction.domain.exception.TransactionNotFoundException;
import com.patken.transaction.persistence.TransactionGateway;
import com.patken.transaction.persistence.TransactionRepository;
import com.patken.transaction.service.mapper.TransactionMapper;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

/**
 * Validates and persists transaction commands. Kafka publishing is wired in Phase 4 —
 * for now, transactions are created with status RECEIVED and {@code kafka_published =
 * false}; the Phase 6 recovery scheduler will pick them up once the producer exists.
 */
@Service
public class TransactionCommandService {

    private final TransactionRepository repository;
    private final TransactionGateway gateway;
    private final TransactionMapper mapper;

    public TransactionCommandService(TransactionRepository repository, TransactionGateway gateway,
                                      TransactionMapper mapper) {
        this.repository = repository;
        this.gateway = gateway;
        this.mapper = mapper;
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
        return new CommandResult(mapper.toResponse(result.transaction()), result.created());
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
