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

import java.time.Instant;
import java.util.Collection;
import java.util.List;
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

    /**
     * Stuck transactions for {@code StuckTransactionScheduler} (ADR-006): still in a
     * non-terminal in-flight state and untouched for longer than the timeout. RETRY is
     * included so a re-dispatch whose republish was lost (e.g. Kafka was down during
     * recovery) is picked up again — a consumer's own mid-backoff RETRY is far younger
     * than the timeout, so it's never mistaken for stuck. {@code Pageable} bounds the
     * batch so one cycle can't try to load the whole table.
     */
    @Query("SELECT t FROM Transaction t WHERE t.status IN :statuses AND t.updatedAt < :cutoff ORDER BY t.updatedAt ASC")
    List<Transaction> findStuck(@Param("statuses") Collection<TransactionStatus> statuses,
                                @Param("cutoff") Instant cutoff, Pageable pageable);

    /**
     * Persisted-but-unpublished transactions for {@code UnpublishedTransactionScheduler}
     * (ADR-001) — the outbox scan. The {@code created_at} age gate keeps a just-created
     * row (whose API thread is still mid-publish) out of the batch. Restricted to
     * RECEIVED: a transaction is only ever unpublished before it's been consumed, and
     * once publish retries are exhausted it becomes DEAD_LETTERED — which still has
     * {@code kafka_published = false} but must not be rescanned. Served by the partial
     * index {@code idx_transactions_kafka_published} on {@code kafka_published = FALSE}.
     */
    @Query("SELECT t FROM Transaction t WHERE t.kafkaPublished = false AND t.status = "
            + "com.patken.transaction.domain.TransactionStatus.RECEIVED AND t.createdAt < :cutoff ORDER BY t.createdAt ASC")
    List<Transaction> findUnpublished(@Param("cutoff") Instant cutoff, Pageable pageable);
}
