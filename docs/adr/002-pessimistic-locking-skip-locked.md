# ADR-002: Pessimistic Locking with SKIP LOCKED

**Context:** Multiple consumer instances (and the recovery scheduler) may attempt to process the same transaction concurrently. Processing must be exclusive per transaction, without turning contention into blocking or deadlocks.

**Decision:** When a consumer picks up a transaction for processing, it acquires the row with `SELECT ... FOR UPDATE SKIP LOCKED`. Rows locked by another worker are skipped, not waited on; skipped work is picked up on the next poll or recovery cycle.

**Rationale:** Plain `SELECT FOR UPDATE` makes competing workers queue behind each other — under load, that serializes the fleet on the hottest rows and invites lock-wait timeouts. `SKIP LOCKED` turns the transactions table into a work queue: every worker always makes progress on *some* unlocked row. No deadlocks by construction (no lock ordering across multiple rows is ever needed), no wasted wait time.

**Division of labor with optimistic locking:** the schema also carries a `version` column (`@Version`). The two mechanisms are not redundant:
- `SKIP LOCKED` (pessimistic) guards **consumer-side processing** — one worker per transaction at a time.
- `version` (optimistic) guards **cross-component writes** to the same row (e.g., API-side updates vs. consumer-side status updates) where holding a DB lock across the interaction would be wrong.

**Consequences:**
- Implemented in `TransactionGateway` (the designated extension point) when consumer processing lands — via a native query or Hibernate's `@Lock(PESSIMISTIC_WRITE)` with the SKIP LOCKED hint; the generated SQL must be asserted in an integration test, not assumed.
- A transaction being processed is invisible to other workers rather than an error — retried naturally on the next cycle.
- Lock scope must stay short: acquire → process → update status → commit, within one DB transaction.
