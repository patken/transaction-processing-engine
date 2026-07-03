package com.patken.transaction.persistence;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

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

    /**
     * Targeted update (D3, architecture review, ADR-001) — the API request thread marks
     * a transaction published without touching {@code version} or any other column.
     * The consumer, which processes under a pessimistic row lock (ADR-002,
     * {@link #lockForProcessing}), never races this: it loads the row <em>after</em>
     * acquiring the lock, so a concurrent {@code markKafkaPublished} is either already
     * committed (and thus seen) or blocked behind the lock until the consumer commits.
     */
    @Modifying
    @Transactional
    @Query("UPDATE Transaction t SET t.kafkaPublished = true WHERE t.id = :id")
    void markKafkaPublished(@Param("id") UUID id);

    /**
     * {@code SELECT ... FOR UPDATE SKIP LOCKED} (ADR-002) — claims a transaction row for
     * exclusive processing. Returns empty if another worker already holds the lock
     * (the row is skipped, not waited on — no blocking, no deadlocks). Native query so
     * the {@code SKIP LOCKED} clause is guaranteed in the emitted SQL rather than left
     * to Hibernate dialect/hint translation. Must be called within a transaction; the
     * lock is held until that transaction commits or rolls back.
     */
    @Query(value = "SELECT * FROM transactions WHERE id = :id FOR UPDATE SKIP LOCKED", nativeQuery = true)
    Optional<Transaction> lockForProcessing(@Param("id") UUID id);
}
