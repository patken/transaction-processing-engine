package com.patken.transaction.unit;

import com.patken.transaction.domain.FailureType;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionFailureAudit;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.domain.exception.TransactionNotFoundException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Value-object level checks for the domain entities: they carry business meaning
 * (a fresh transaction is transient and unpublished; an audit row is append-only and
 * remembers its attempt number), so pin that here rather than only exercising it
 * incidentally through the Testcontainers suites.
 */
class DomainModelTest {

    @Test
    void newTransactionIsTransientUnpublishedAndCarriesItsFields() {
        UUID id = UUID.randomUUID();
        Transaction txn = new Transaction(
                id, "biz-1", TransactionType.CREDIT, TransactionStatus.RECEIVED,
                new BigDecimal("150.0000"), "CAD", "ACC-1", "ACC-2",
                null, Map.of("channel", "web"), "corr-1");

        assertThat(txn.getId()).isEqualTo(id);
        assertThat(txn.getBusinessId()).isEqualTo("biz-1");
        assertThat(txn.getType()).isEqualTo(TransactionType.CREDIT);
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.RECEIVED);
        assertThat(txn.getAmount()).isEqualByComparingTo("150.0000");
        assertThat(txn.getCurrency()).isEqualTo("CAD");
        assertThat(txn.getSourceAccount()).isEqualTo("ACC-1");
        assertThat(txn.getTargetAccount()).isEqualTo("ACC-2");
        assertThat(txn.getOriginalTransactionId()).isNull();
        assertThat(txn.getMetadata()).containsEntry("channel", "web");
        assertThat(txn.getCorrelationId()).isEqualTo("corr-1");
        assertThat(txn.isKafkaPublished()).isFalse();
        assertThat(txn.getRetryCount()).isZero();
        // No @CreationTimestamp fired (never persisted) → transient per Persistable.isNew().
        assertThat(txn.getCreatedAt()).isNull();
        assertThat(txn.getUpdatedAt()).isNull();
        assertThat(txn.getVersion()).isZero();
        assertThat(txn.isNew()).isTrue();
    }

    @Test
    void mutableProcessingFieldsRoundTrip() {
        Transaction txn = new Transaction(
                UUID.randomUUID(), "biz-2", TransactionType.DEBIT, TransactionStatus.RECEIVED,
                BigDecimal.TEN, "USD", "ACC-1", "ACC-2", null, null, "corr-2");

        txn.setStatus(TransactionStatus.PROCESSING);
        txn.setRetryCount(2);
        txn.setKafkaPublished(true);
        txn.setErrorMessage("backend timeout");

        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.PROCESSING);
        assertThat(txn.getRetryCount()).isEqualTo(2);
        assertThat(txn.isKafkaPublished()).isTrue();
        assertThat(txn.getErrorMessage()).isEqualTo("backend timeout");
    }

    @Test
    void failureAuditRemembersAttemptTypeAndReason() {
        UUID txnId = UUID.randomUUID();
        TransactionFailureAudit audit =
                new TransactionFailureAudit(txnId, FailureType.PUBLISH, 3, "kafka down");

        assertThat(audit.getTransactionId()).isEqualTo(txnId);
        assertThat(audit.getFailureType()).isEqualTo(FailureType.PUBLISH);
        assertThat(audit.getAttemptNumber()).isEqualTo(3);
        assertThat(audit.getErrorMessage()).isEqualTo("kafka down");
        // id / failedAt are DB-generated — null until persisted.
        assertThat(audit.getId()).isNull();
        assertThat(audit.getFailedAt()).isNull();
    }

    @Test
    void failureTypeHasProcessingAndPublish() {
        assertThat(FailureType.values()).containsExactly(FailureType.PROCESSING, FailureType.PUBLISH);
        assertThat(FailureType.valueOf("PROCESSING")).isEqualTo(FailureType.PROCESSING);
    }

    @Test
    void notFoundByBusinessIdCarriesTheKeyInItsMessage() {
        assertThat(new TransactionNotFoundException("biz-404").getMessage())
                .contains("biz-404");
    }
}
