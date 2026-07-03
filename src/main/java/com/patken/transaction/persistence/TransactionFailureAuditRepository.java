package com.patken.transaction.persistence;

import com.patken.transaction.domain.TransactionFailureAudit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TransactionFailureAuditRepository extends JpaRepository<TransactionFailureAudit, UUID> {

    /** Full incident timeline for a transaction (ADR-004 ops query), oldest attempt first. */
    List<TransactionFailureAudit> findByTransactionIdOrderByAttemptNumberAsc(UUID transactionId);
}
