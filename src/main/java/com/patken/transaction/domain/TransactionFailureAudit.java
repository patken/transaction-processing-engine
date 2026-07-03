package com.patken.transaction.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.UUID;

/**
 * One row per failed attempt (ADR-004). Append-only — never updated, so no {@code @Version}
 * and no need for the {@link org.springframework.data.domain.Persistable} dance
 * {@link Transaction} needs; the DB generates the id.
 */
@Entity
@Table(name = "transaction_failure_audit")
public class TransactionFailureAudit {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    @Enumerated(EnumType.STRING)
    @Column(name = "failure_type", nullable = false, length = 20)
    private FailureType failureType;

    @Column(name = "attempt_number", nullable = false)
    private int attemptNumber;

    @Column(name = "error_message", nullable = false)
    private String errorMessage;

    @CreationTimestamp
    @Column(name = "failed_at", nullable = false, updatable = false)
    private Instant failedAt;

    protected TransactionFailureAudit() {
        // JPA
    }

    public TransactionFailureAudit(UUID transactionId, FailureType failureType, int attemptNumber, String errorMessage) {
        this.transactionId = transactionId;
        this.failureType = failureType;
        this.attemptNumber = attemptNumber;
        this.errorMessage = errorMessage;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public FailureType getFailureType() {
        return failureType;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Instant getFailedAt() {
        return failedAt;
    }
}
