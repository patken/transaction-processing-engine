package com.patken.transaction.unit;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.domain.exception.IdempotencyConflictException;
import com.patken.transaction.domain.exception.ReversalNotAllowedException;
import com.patken.transaction.persistence.TransactionGateway;
import com.patken.transaction.persistence.TransactionRepository;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies ADR-003 (optimistic insert + catch, no check-then-insert, strict payload
 * match) and ADR-008's constraint-routing (B2) at the {@link TransactionGateway}
 * level, where the idempotency mechanics actually live.
 */
class IdempotencyTest {

    @Mock
    private TransactionRepository repository;

    private TransactionGateway gateway;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        gateway = new TransactionGateway(repository);
    }

    @AfterEach
    void clearTransactionSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void newBusinessIdIsPersistedAndReportedAsCreated() {
        Transaction transaction = newTransaction("biz-1", BigDecimal.valueOf(100));
        when(repository.saveAndFlush(transaction)).thenReturn(transaction);

        TransactionGateway.PersistResult result = gateway.persistIdempotent(transaction);

        assertThat(result.created()).isTrue();
        assertThat(result.transaction()).isSameAs(transaction);
        verify(repository).saveAndFlush(transaction);
    }

    @Test
    void duplicateBusinessIdWithSamePayloadReturnsExistingRowInsteadOfFailing() {
        Transaction incoming = newTransaction("biz-2", BigDecimal.valueOf(100));
        Transaction existing = newTransaction("biz-2", BigDecimal.valueOf(100));

        doThrow(businessIdViolation()).when(repository).saveAndFlush(incoming);
        when(repository.findByBusinessId("biz-2")).thenReturn(Optional.of(existing));

        TransactionGateway.PersistResult result = gateway.persistIdempotent(incoming);

        assertThat(result.created()).isFalse();
        assertThat(result.transaction()).isSameAs(existing);
    }

    @Test
    void duplicateBusinessIdWithDifferentAmountIsRejected() {
        // B1 (ADR-003 amended): same key, different payload — not a safe replay.
        Transaction incoming = newTransaction("biz-2b", BigDecimal.valueOf(999));
        Transaction existing = newTransaction("biz-2b", BigDecimal.valueOf(100));

        doThrow(businessIdViolation()).when(repository).saveAndFlush(incoming);
        when(repository.findByBusinessId("biz-2b")).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> gateway.persistIdempotent(incoming))
                .isInstanceOf(IdempotencyConflictException.class)
                .hasMessageContaining("biz-2b");
    }

    @Test
    void concurrentReversalOfSameOriginalIsRejectedNotMisreadAsReplay() {
        // B2: a uq_reversal_per_original violation must never be routed through the
        // businessId replay path (different businessId => findByBusinessId would find
        // nothing and the original DB error would leak as a 500 instead of 409).
        Transaction incoming = newTransaction("biz-3", BigDecimal.valueOf(100));

        doThrow(reversalPerOriginalViolation()).when(repository).saveAndFlush(incoming);

        assertThatThrownBy(() -> gateway.persistIdempotent(incoming))
                .isInstanceOf(ReversalNotAllowedException.class);

        verify(repository, never()).findByBusinessId(any());
    }

    @Test
    void unmappedConstraintViolationIsRethrownAsIs() {
        Transaction incoming = newTransaction("biz-4", BigDecimal.valueOf(100));
        DataIntegrityViolationException violation = new DataIntegrityViolationException(
                "some other constraint",
                new ConstraintViolationException("boom", new SQLException("boom"), "chk_reversal_has_original"));

        doThrow(violation).when(repository).saveAndFlush(incoming);

        assertThatThrownBy(() -> gateway.persistIdempotent(incoming)).isSameAs(violation);
    }

    @Test
    void doesNotCheckBeforeInserting() {
        // ADR-003: no findByBusinessId call before the insert attempt — the DB constraint
        // is the source of truth, not an application-level pre-check (that would be a TOCTOU race).
        Transaction transaction = newTransaction("biz-5", BigDecimal.valueOf(100));
        when(repository.saveAndFlush(transaction)).thenReturn(transaction);

        gateway.persistIdempotent(transaction);

        verify(repository, never()).findByBusinessId(any());
    }

    @Test
    void rethrowsOriginalExceptionIfRowVanishesBetweenViolationAndReread() {
        // Defensive edge case: the constraint fired but a re-fetch by businessId found
        // nothing (e.g. concurrent delete). Surface the original DB error rather than NPE.
        Transaction incoming = newTransaction("biz-6", BigDecimal.valueOf(100));
        DataIntegrityViolationException violation = businessIdViolation();

        doThrow(violation).when(repository).saveAndFlush(incoming);
        when(repository.findByBusinessId("biz-6")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> gateway.persistIdempotent(incoming))
                .isSameAs(violation);
    }

    @Test
    void refusesToRunInsideAnEnclosingTransaction() {
        // C3: the catch-after-flush pattern isn't composable into a larger
        // @Transactional method — fail fast instead of a mysterious
        // UnexpectedRollbackException surfacing far from this call site.
        Transaction transaction = newTransaction("biz-7", BigDecimal.valueOf(100));
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);

        assertThatThrownBy(() -> gateway.persistIdempotent(transaction))
                .isInstanceOf(IllegalStateException.class);

        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    private static DataIntegrityViolationException businessIdViolation() {
        return new DataIntegrityViolationException("duplicate key",
                new ConstraintViolationException("duplicate key", new SQLException("duplicate key"), "uq_business_id"));
    }

    private static DataIntegrityViolationException reversalPerOriginalViolation() {
        return new DataIntegrityViolationException("duplicate key",
                new ConstraintViolationException("duplicate key", new SQLException("duplicate key"),
                        "uq_reversal_per_original"));
    }

    private Transaction newTransaction(String businessId, BigDecimal amount) {
        return new Transaction(
                UUID.randomUUID(),
                businessId,
                TransactionType.CREDIT,
                TransactionStatus.RECEIVED,
                amount,
                "CAD",
                "ACC-001",
                "ACC-002",
                null,
                null,
                "correlation-1"
        );
    }
}
