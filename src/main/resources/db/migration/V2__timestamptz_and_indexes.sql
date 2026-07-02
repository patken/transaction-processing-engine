-- B4 (architecture review): recovery schedulers (Phase 6, ADR-006) compare
-- updated_at/created_at against NOW(). Naive TIMESTAMP columns silently shift that
-- comparison by the DB server's timezone offset when it isn't UTC. TIMESTAMPTZ makes
-- the comparison timezone-correct regardless of server configuration.
ALTER TABLE transactions
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ USING updated_at AT TIME ZONE 'UTC';

-- D5 (architecture review): the listing query (GET /transactions?status=&page=&limit=,
-- Phase 3) filters by status and orders by created_at. A composite index serves that
-- query directly instead of relying on the two separate single-column indexes.
CREATE INDEX idx_transactions_status_created_at ON transactions (status, created_at);
