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
                request.getMetadata(),
                correlationId
        );

        TransactionGateway.PersistResult result = gateway.persistIdempotent(transaction);
        return new CommandResult(mapper.toResponse(result.transaction()), result.created());
    }

    private void validate(CreateTransactionRequest request) {
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
        if (repository.existsByOriginalTransactionId(originalId)) {
            throw new ReversalNotAllowedException("Transaction " + originalId + " has already been reversed");
        }
    }

    public record CommandResult(com.patken.transaction.api.generated.dto.TransactionResponse transaction,
                                 boolean created) {
    }
}
