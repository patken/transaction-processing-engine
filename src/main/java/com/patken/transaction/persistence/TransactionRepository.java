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
     * Targeted update (D3, architecture review, ADR-001) — does not touch {@code
     * version}. Going through the regular entity save/flush cycle instead would bump
     * the optimistic lock and could collide with a consumer updating the same row's
     * status at nearly the same moment, since publish and consume happen close
     * together on the happy path.
     */
    @Modifying
    @Transactional
    @Query("UPDATE Transaction t SET t.kafkaPublished = true WHERE t.id = :id")
    void markKafkaPublished(@Param("id") UUID id);

    /**
     * Targeted update, same reasoning as {@link #markKafkaPublished} — found the hard
     * way in Phase 4 verification: the consumer originally advanced status via
     * {@code repository.save(transaction)}, a full-row write. Since the consumer reads
     * its copy of the row at the start of message handling, it can race the API
     * request thread's {@link #markKafkaPublished} call and hold a stale (false)
     * in-memory {@code kafkaPublished}; saving the whole entity then clobbers the flag
     * the producer had just set. Updating only {@code status} avoids the collision —
     * consistent with treating each concern (publish flag, status) as independently
     * owned rather than bundled into one whole-row write.
     */
    @Modifying
    @Transactional
    @Query("UPDATE Transaction t SET t.status = :status WHERE t.id = :id")
    void updateStatus(@Param("id") UUID id, @Param("status") TransactionStatus status);

    /**
     * Same targeted-update reasoning as {@link #updateStatus}, plus the error reason in
     * the same write — used when the consumer's async re-validation fails (e.g. a
     * REVERSAL whose original is no longer COMPLETED by the time it's processed).
     */
    @Modifying
    @Transactional
    @Query("UPDATE Transaction t SET t.status = :status, t.errorMessage = :errorMessage WHERE t.id = :id")
    void updateStatusWithError(@Param("id") UUID id, @Param("status") TransactionStatus status,
                                @Param("errorMessage") String errorMessage);
}
