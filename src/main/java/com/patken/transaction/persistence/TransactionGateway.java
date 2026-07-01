package com.patken.transaction.persistence;

import com.patken.transaction.domain.Transaction;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

/**
 * Owns idempotent persistence (ADR-003). Retry for transient DB failures (Spring
 * Retry) and {@code SELECT FOR UPDATE SKIP LOCKED} locking are added in Phase 5 —
 * this is the extension point for both.
 *
 * <p>No explicit {@code @Transactional} here: {@link TransactionRepository#saveAndFlush}
 * is already transactional per Spring Data JPA's {@code SimpleJpaRepository}, and
 * {@code saveAndFlush} (rather than plain {@code save}) forces the constraint violation
 * to surface immediately instead of being deferred to a later flush.
 */
@Component
public class TransactionGateway {

    private final TransactionRepository repository;

    public TransactionGateway(TransactionRepository repository) {
        this.repository = repository;
    }

    /**
     * Optimistic insert. On a {@code business_id} unique-constraint violation, re-reads
     * and returns the existing row instead of failing — no check-then-insert, per ADR-003.
     *
     * @return the persisted transaction, and whether it was newly created (false = idempotent replay)
     */
    public PersistResult persistIdempotent(Transaction transaction) {
        try {
            Transaction saved = repository.saveAndFlush(transaction);
            return new PersistResult(saved, true);
        } catch (DataIntegrityViolationException e) {
            Transaction existing = repository.findByBusinessId(transaction.getBusinessId())
                    .orElseThrow(() -> e);
            return new PersistResult(existing, false);
        }
    }

    public record PersistResult(Transaction transaction, boolean created) {
    }
}
