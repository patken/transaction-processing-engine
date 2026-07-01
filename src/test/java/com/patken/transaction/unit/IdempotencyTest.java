package com.patken.transaction.unit;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.persistence.TransactionGateway;
import com.patken.transaction.persistence.TransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies ADR-003 (optimistic insert + catch, no check-then-insert) at the
 * {@link TransactionGateway} level, where the idempotency mechanics actually live.
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

    @Test
    void newBusinessIdIsPersistedAndReportedAsCreated() {
        Transaction transaction = newTransaction("biz-1");
        when(repository.saveAndFlush(transaction)).thenReturn(transaction);

        TransactionGateway.PersistResult result = gateway.persistIdempotent(transaction);

        assertThat(result.created()).isTrue();
        assertThat(result.transaction()).isSameAs(transaction);
        verify(repository).saveAndFlush(transaction);
    }

    @Test
    void duplicateBusinessIdReturnsExistingRowInsteadOfFailing() {
        Transaction incoming = newTransaction("biz-2");
        Transaction existing = newTransaction("biz-2");

        doThrow(new DataIntegrityViolationException("uq_business_id"))
                .when(repository).saveAndFlush(incoming);
        when(repository.findByBusinessId("biz-2")).thenReturn(Optional.of(existing));

        TransactionGateway.PersistResult result = gateway.persistIdempotent(incoming);

        assertThat(result.created()).isFalse();
        assertThat(result.transaction()).isSameAs(existing);
    }

    @Test
    void doesNotCheckBeforeInserting() {
        // ADR-003: no findByBusinessId call before the insert attempt — the DB constraint
        // is the source of truth, not an application-level pre-check (that would be a TOCTOU race).
        Transaction transaction = newTransaction("biz-3");
        when(repository.saveAndFlush(transaction)).thenReturn(transaction);

        gateway.persistIdempotent(transaction);

        verify(repository, org.mockito.Mockito.never()).findByBusinessId(any());
    }

    @Test
    void rethrowsOriginalExceptionIfRowVanishesBetweenViolationAndReread() {
        // Defensive edge case: the constraint fired but a re-fetch by businessId found
        // nothing (e.g. concurrent delete). Surface the original DB error rather than NPE.
        Transaction incoming = newTransaction("biz-4");
        DataIntegrityViolationException violation = new DataIntegrityViolationException("uq_business_id");

        doThrow(violation).when(repository).saveAndFlush(incoming);
        when(repository.findByBusinessId("biz-4")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> gateway.persistIdempotent(incoming))
                .isSameAs(violation);
    }

    private Transaction newTransaction(String businessId) {
        return new Transaction(
                UUID.randomUUID(),
                businessId,
                TransactionType.CREDIT,
                TransactionStatus.RECEIVED,
                BigDecimal.valueOf(100),
                "CAD",
                "ACC-001",
                "ACC-002",
                null,
                null,
                "correlation-1"
        );
    }
}
