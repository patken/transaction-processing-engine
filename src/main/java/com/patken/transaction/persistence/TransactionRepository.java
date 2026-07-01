package com.patken.transaction.persistence;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface TransactionRepository extends JpaRepository<Transaction, UUID> {

    Optional<Transaction> findByBusinessId(String businessId);

    Page<Transaction> findByStatus(TransactionStatus status, Pageable pageable);

    /**
     * Only REVERSAL rows ever have a non-null original_transaction_id (DB CHECK
     * constraint), so any match here means the original already has a reversal.
     */
    boolean existsByOriginalTransactionId(UUID originalTransactionId);
}
