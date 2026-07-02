package com.patken.transaction.service;

import com.patken.transaction.api.generated.dto.TransactionResponse;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.exception.TransactionNotFoundException;
import com.patken.transaction.persistence.TransactionRepository;
import com.patken.transaction.service.mapper.TransactionMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Read-only lookups: single-record fetches and the paginated/filtered listing.
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

    /**
     * Most recent first — matches the {@code idx_transactions_status_created_at}
     * composite index (D5, architecture review) when {@code status} is provided.
     */
    public Page<TransactionResponse> list(TransactionStatus status, int page, int limit) {
        Pageable pageable = PageRequest.of(page, limit, Sort.by(Sort.Direction.DESC, "createdAt"));
        Page<Transaction> results = status == null
                ? repository.findAll(pageable)
                : repository.findByStatus(status, pageable);
        return results.map(mapper::toResponse);
    }
}
