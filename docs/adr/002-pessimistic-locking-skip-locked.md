# ADR-002: Pessimistic Locking with SKIP LOCKED

**Context:** Multiple consumer instances (and the recovery scheduler) may attempt to process the same transaction concurrently. Processing must be exclusive per transaction, without turning contention into blocking or deadlocks.

**Decision:** When a consumer picks up a transaction for processing, it acquires the row with `SELECT ... FOR UPDATE SKIP LOCKED`. Rows locked by another worker are skipped, not waited on; skipped work is picked up on the next poll or recovery cycle.

**Rationale:** Plain `SELECT FOR UPDATE` makes competing workers queue behind each other — under load, that serializes the fleet on the hottest rows and invites lock-wait timeouts. `SKIP LOCKED` turns the transactions table into a work queue: every worker always makes progress on *some* unlocked row. No deadlocks by construction (no lock ordering across multiple rows is ever needed), no wasted wait time.

**Division of labor with optimistic locking:** the schema also carries a `version` column (`@Version`). The two mechanisms are not redundant:
- `SKIP LOCKED` (pessimistic) guards **consumer-side processing** — one worker per transaction at a time.
- `version` (optimistic) guards **cross-component writes** to the same row (e.g., API-side updates vs. consumer-side status updates) where holding a DB lock across the interaction would be wrong.

**Consequences:**
- Implemented as `TransactionRepository.lockForProcessing` — a **native query** (`SELECT ... FOR UPDATE SKIP LOCKED`) so the SKIP LOCKED clause is guaranteed in the emitted SQL rather than left to Hibernate hint/dialect translation. The consumer's per-attempt logic lives in `TransactionProcessor` (not `TransactionGateway`, which stays focused on the create-path idempotent insert); the gateway and the processor are two distinct persistence concerns.
- A transaction being processed is invisible to other workers rather than an error — retried naturally on the next cycle.
- Lock scope stays short and covers one attempt: acquire → drive status → record outcome → commit, within one DB transaction (`TransactionProcessor.attemptOnce`, `@Transactional`).
- **This also structurally closed the Phase 4 `kafka_published` write race.** Because the consumer loads the row *after* acquiring the lock and mutates it as a managed entity, the API thread's `markKafkaPublished` update is serialized behind the lock — a whole-row flush by the consumer can no longer clobber the flag, so the targeted per-column update the Phase 4 consumer needed is gone.
- Verified by `TransactionLockingIT`: a second worker gets an empty result immediately (skips) while a first holds the lock, rather than blocking.
