package com.patken.transaction.service;

import com.patken.transaction.api.generated.dto.TransactionResponse;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.exception.TransactionNotFoundException;
import com.patken.transaction.persistence.TransactionRepository;
import com.patken.transaction.service.mapper.TransactionMapper;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Read-only lookups. Pagination/filtering ({@code listTransactions}) is added in Phase 3.
 */
@Service
public class TransactionQueryService {

    private final TransactionRepository repository;
    private final TransactionMapper mapper;

    public TransactionQueryService(TransactionRepository repository, TransactionMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    public TransactionResponse getById(UUID transactionId) {
        Transaction transaction = repository.findById(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId));
        return mapper.toResponse(transaction);
    }

    public TransactionResponse getByBusinessId(String businessId) {
        Transaction transaction = repository.findByBusinessId(businessId)
                .orElseThrow(() -> new TransactionNotFoundException(businessId));
        return mapper.toResponse(transaction);
    }
}
