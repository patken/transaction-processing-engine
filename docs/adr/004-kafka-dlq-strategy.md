# ADR-004: Failure Handling Strategy — DLQ and Publish Retry Ceiling

**Context:** The spec originally covered only one failure path in detail: a Kafka consumer that fails to process a command after `max-retries` routes the message to the `transaction.dlq` topic and moves the transaction to `DEAD_LETTERED`. The second failure path — persist-before-publish (ADR-001) where the Kafka *publish* itself fails — was left uncapped: `UnpublishedTransactionScheduler` was specified to retry every 5 minutes indefinitely, with no ceiling, no escalation, and no transition to `DEAD_LETTERED`. Left as-is, a prolonged Kafka outage would retry forever with no operator visibility beyond the `kafka.publish.failures` counter ticking up.

**Decision:**
1. **Publish failures get the same ceiling as processing failures.** `UnpublishedTransactionScheduler` retries up to `max-retries` (the same configured value used by consumer processing retries). Once exceeded, the transaction transitions to `DEAD_LETTERED` through the existing `TransactionStateMachine` — no separate mechanism, no separate status.
2. **No separate DLQ table for transaction state.** The `transactions` table remains the single source of truth for a transaction's terminal state (`status = DEAD_LETTERED`, `retry_count`, `error_message`). The Kafka `transaction.dlq` topic is not a substitute state store — it exists purely so ops tooling can consume/replay the raw failed *command message* (headers, `correlationId`, payload) independently of the DB. Duplicating status into a second DB table would create two systems of record that can drift.
3. **A `transaction_failure_audit` table is added for the failure history that `error_message` (last-value-only) cannot capture.** One row per failed attempt — both processing and publish failures — rather than one row per transaction. This is the audit trail a financial system needs (when did each attempt fail, and why), without turning the hot `transactions` table into a growing log.

   ```sql
   CREATE TABLE transaction_failure_audit (
       id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       transaction_id   UUID NOT NULL REFERENCES transactions (id),
       failure_type     VARCHAR(20) NOT NULL,  -- PROCESSING, PUBLISH
       attempt_number   INT NOT NULL,
       error_message    TEXT NOT NULL,
       failed_at        TIMESTAMP NOT NULL DEFAULT NOW()
   );

   CREATE INDEX idx_failure_audit_transaction_id ON transaction_failure_audit (transaction_id);
   ```

   Ships as its own Flyway migration in Phase 5/6 (when retry/DLQ handling is actually implemented), not bundled into the Phase 1 `V1__create_transactions_table.sql` — no premature schema for a mechanism that doesn't exist yet.

**Rationale:** The two failure paths (processing vs. publish) are conceptually the same shape — bounded retries, then a terminal dead-letter state — so they should share one mechanism instead of the publish path being a special case with weaker guarantees. Splitting "current state" (on `transactions`) from "failure history" (on `transaction_failure_audit`) avoids both extremes: no dead-letter table duplicating live status, and no unbounded growth of `error_message`/`retry_count` columns trying to hold a full history on the hot table.

**Consequences:**
- `max-retries` becomes a shared config value read by both the Kafka consumer's retry logic and `UnpublishedTransactionScheduler`.
- Every failed attempt (processing or publish) writes a row to `transaction_failure_audit` in the same transaction as the `transactions` update — no separate at-least-once concern, since it's a local DB write, not a second message bus.
- `transactions.error_message` keeps holding the *latest* error for cheap access (e.g., in the API response or a dashboard) without joining to the audit table; the audit table is for the full history / investigation view.
- Query pattern for ops: `SELECT * FROM transaction_failure_audit WHERE transaction_id = ? ORDER BY attempt_number` for a full incident timeline; `SELECT * FROM transactions WHERE status = 'DEAD_LETTERED'` for the current dead-letter backlog.
