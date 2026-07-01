CREATE TABLE transactions (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    business_id             VARCHAR(255) NOT NULL,
    type                    VARCHAR(20) NOT NULL,          -- CREDIT, DEBIT, REVERSAL
    status                  VARCHAR(20) NOT NULL,          -- RECEIVED, VALIDATED, DISPATCHED, PROCESSING, COMPLETED, FAILED, RETRY, DEAD_LETTERED
    amount                  DECIMAL(19,4) NOT NULL,
    currency                VARCHAR(3) NOT NULL,
    source_account          VARCHAR(100) NOT NULL,
    target_account          VARCHAR(100) NOT NULL,
    original_transaction_id UUID,
    metadata                JSONB,
    correlation_id          VARCHAR(255),
    kafka_published         BOOLEAN NOT NULL DEFAULT FALSE,
    retry_count             INT NOT NULL DEFAULT 0,
    error_message           TEXT,
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    version                 BIGINT NOT NULL DEFAULT 0,      -- optimistic locking

    CONSTRAINT uq_business_id UNIQUE (business_id),
    CONSTRAINT fk_original_transaction FOREIGN KEY (original_transaction_id) REFERENCES transactions (id),

    -- ADR-008: original_transaction_id is required for REVERSAL, forbidden otherwise.
    CONSTRAINT chk_reversal_has_original CHECK (
        (type = 'REVERSAL' AND original_transaction_id IS NOT NULL)
        OR (type <> 'REVERSAL' AND original_transaction_id IS NULL)
    )
);

CREATE INDEX idx_transactions_status ON transactions (status);
CREATE INDEX idx_transactions_kafka_published ON transactions (kafka_published) WHERE kafka_published = FALSE;
CREATE INDEX idx_transactions_created_at ON transactions (created_at);

-- ADR-008: at most one REVERSAL per original transaction.
CREATE UNIQUE INDEX uq_reversal_per_original ON transactions (original_transaction_id) WHERE type = 'REVERSAL';
