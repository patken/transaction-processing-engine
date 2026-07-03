package com.patken.transaction.persistence;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.exception.IdempotencyConflictException;
import com.patken.transaction.domain.exception.ReversalNotAllowedException;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import java.util.Objects;

/**
 * Owns idempotent persistence (ADR-003). Retry for transient DB failures (Spring
 * Retry) and {@code SELECT FOR UPDATE SKIP LOCKED} locking are added in Phase 5 —
 * this is the extension point for both.
 *
 * <p>No explicit {@code @Transactional} here: {@link TransactionRepository#saveAndFlush}
 * is already transactional per Spring Data JPA's {@code SimpleJpaRepository}, and
 * {@code saveAndFlush} (rather than plain {@code save}) forces the constraint violation
 * to surface immediately instead of being deferred to a later flush. This shape only
 * works outside an enclosing transaction — see {@link #persistIdempotent} — so it is
 * not composable into a larger {@code @Transactional} method; call it as a leaf.
 */
@Component
public class TransactionGateway {

    private static final String BUSINESS_ID_CONSTRAINT = "uq_business_id";
    private static final String REVERSAL_PER_ORIGINAL_CONSTRAINT = "uq_reversal_per_original";

    private final TransactionRepository repository;

    public TransactionGateway(TransactionRepository repository) {
        this.repository = repository;
    }

    /**
     * Optimistic insert. On a {@code business_id} unique-constraint violation, re-reads
     * and returns the existing row instead of failing — no check-then-insert, per ADR-003.
     * On a {@code uq_reversal_per_original} violation (a concurrent reversal of the same
     * original won the race), fails with {@link ReversalNotAllowedException} instead of
     * being misread as an idempotent replay (ADR-008).
     *
     * @return the persisted transaction, and whether it was newly created (false = idempotent replay)
     * @throws IdempotencyConflictException if {@code businessId} is reused with a different payload
     */
    @Retryable(retryFor = TransientDataAccessException.class, maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2))
    public PersistResult persistIdempotent(Transaction transaction) {
        Assert.state(!TransactionSynchronizationManager.isActualTransactionActive(),
                "persistIdempotent must run outside any existing transaction: the catch-after-flush "
                        + "idempotency pattern (ADR-003) marks an enclosing transaction rollback-only on a "
                        + "constraint violation, turning the re-read below into an UnexpectedRollbackException "
                        + "far from this call site instead of the clean 200/409 it's meant to produce.");

        try {
            Transaction saved = repository.saveAndFlush(transaction);
            return new PersistResult(saved, true);
        } catch (DataIntegrityViolationException e) {
            return switch (Objects.requireNonNullElse(constraintNameOf(e), "")) {
                case BUSINESS_ID_CONSTRAINT -> replayOrConflict(transaction, e);
                case REVERSAL_PER_ORIGINAL_CONSTRAINT -> throw new ReversalNotAllowedException(
                        "Transaction " + transaction.getOriginalTransactionId() + " has already been reversed", e);
                default -> throw e;
            };
        }
    }

    private PersistResult replayOrConflict(Transaction attempted, DataIntegrityViolationException original) {
        Transaction existing = repository.findByBusinessId(attempted.getBusinessId())
                .orElseThrow(() -> original);

        if (!businessPayloadMatches(attempted, existing)) {
            throw new IdempotencyConflictException(attempted.getBusinessId());
        }
        return new PersistResult(existing, false);
    }

    /**
     * ADR-003 (amended): a businessId replay is only a safe no-op if the payload
     * matches what's already stored. correlationId and metadata are excluded
     * deliberately — they're not part of the transaction's financial identity.
     */
    private boolean businessPayloadMatches(Transaction a, Transaction b) {
        return a.getType() == b.getType()
                && a.getAmount().compareTo(b.getAmount()) == 0
                && a.getCurrency().equals(b.getCurrency())
                && a.getSourceAccount().equals(b.getSourceAccount())
                && a.getTargetAccount().equals(b.getTargetAccount())
                && Objects.equals(a.getOriginalTransactionId(), b.getOriginalTransactionId());
    }

    private static String constraintNameOf(DataIntegrityViolationException e) {
        return e.getCause() instanceof ConstraintViolationException cve ? cve.getConstraintName() : null;
    }

    public record PersistResult(Transaction transaction, boolean created) {
    }
}
