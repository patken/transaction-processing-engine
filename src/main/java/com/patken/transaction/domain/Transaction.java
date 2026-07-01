package com.patken.transaction.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "transactions")
public class Transaction {

    @Id
    private UUID id;

    @Column(name = "business_id", nullable = false, unique = true)
    private String businessId;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false, length = 20)
    private TransactionType type;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private TransactionStatus status;

    @Column(name = "amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "source_account", nullable = false, length = 100)
    private String sourceAccount;

    @Column(name = "target_account", nullable = false, length = 100)
    private String targetAccount;

    @Column(name = "original_transaction_id")
    private UUID originalTransactionId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata")
    private Map<String, Object> metadata;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "kafka_published", nullable = false)
    private boolean kafkaPublished;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "error_message")
    private String errorMessage;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    protected Transaction() {
        // JPA
    }

    public Transaction(UUID id, String businessId, TransactionType type, TransactionStatus status,
                        BigDecimal amount, String currency, String sourceAccount, String targetAccount,
                        UUID originalTransactionId, Map<String, Object> metadata, String correlationId) {
        this.id = id;
        this.businessId = businessId;
        this.type = type;
        this.status = status;
        this.amount = amount;
        this.currency = currency;
        this.sourceAccount = sourceAccount;
        this.targetAccount = targetAccount;
        this.originalTransactionId = originalTransactionId;
        this.metadata = metadata;
        this.correlationId = correlationId;
        this.kafkaPublished = false;
        this.retryCount = 0;
    }

    public UUID getId() {
        return id;
    }

    public String getBusinessId() {
        return businessId;
    }

    public TransactionType getType() {
        return type;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getSourceAccount() {
        return sourceAccount;
    }

    public String getTargetAccount() {
        return targetAccount;
    }

    public UUID getOriginalTransactionId() {
        return originalTransactionId;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public boolean isKafkaPublished() {
        return kafkaPublished;
    }

    public void setKafkaPublished(boolean kafkaPublished) {
        this.kafkaPublished = kafkaPublished;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public long getVersion() {
        return version;
    }
}
