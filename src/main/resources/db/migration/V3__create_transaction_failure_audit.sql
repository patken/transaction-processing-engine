-- ADR-004: one row per failed attempt (processing or publish), the full failure
-- history that transactions.error_message (last-value-only) can't hold. The transactions
-- table stays the single source of truth for current state (status = DEAD_LETTERED,
-- retry_count); this table is the audit trail for investigation.
CREATE TABLE transaction_failure_audit (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id   UUID NOT NULL REFERENCES transactions (id),
    failure_type     VARCHAR(20) NOT NULL,          -- PROCESSING, PUBLISH
    attempt_number   INT NOT NULL,
    error_message    TEXT NOT NULL,
    failed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()  -- TIMESTAMPTZ, consistent with V2 (B4)
);

CREATE INDEX idx_failure_audit_transaction_id ON transaction_failure_audit (transaction_id);
