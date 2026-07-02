# ADR-005: State Machine Transitions

**Context:** Transaction statuses form a lifecycle, and invalid moves (e.g., reopening a `COMPLETED` transaction) must be structurally impossible, not just discouraged. The valid-transition set has to be explicit, exhaustive, and enforced in exactly one place.

**Decision:** `TransactionStateMachine` enforces this transition table — anything not listed throws `InvalidStateTransitionException`, mapped to **HTTP 409 Conflict** (the resource's current state blocks the operation; the request itself is well-formed, so 400 would be wrong):

| From | Valid targets |
|---|---|
| RECEIVED | VALIDATED, FAILED |
| VALIDATED | DISPATCHED, FAILED |
| DISPATCHED | PROCESSING, FAILED |
| PROCESSING | COMPLETED, FAILED |
| FAILED | RETRY, DEAD_LETTERED |
| RETRY | DISPATCHED, DEAD_LETTERED |
| COMPLETED | *(terminal)* |
| DEAD_LETTERED | *(terminal)* |

Terminal statuses are marked with a custom `@Terminal` annotation on the enum constants; a static consistency check fails class-initialization if the annotation ever disagrees with the table (a terminal status with an outgoing transition, or vice versa). The table itself is covered by an exhaustive parameterized test over all 64 (from, to) pairs.

**Load-bearing invariant — terminal statuses make checks race-free:** because `COMPLETED` has no outgoing transition, any check of the form "this transaction must be COMPLETED before X" (e.g., reversal eligibility, ADR-008) cannot be invalidated by a concurrent status change after the check passes. No lock is needed for those reads. **Adding an outgoing transition from a terminal status would silently break this reasoning** — which is precisely what the static consistency check and the annotation exist to prevent.

**Recovery path is deliberate, not missing:** stuck-transaction recovery (ADR-006) re-dispatches transactions stuck in `PROCESSING` — but there is intentionally no `PROCESSING → DISPATCHED` or `PROCESSING → RETRY` edge. Recovery must walk `PROCESSING → FAILED → RETRY → DISPATCHED`, recording the stuck timeout as the failure reason. This keeps every recovery visible in the failure history (`error_message`, `transaction_failure_audit` per ADR-004) instead of teleporting transactions back into the pipeline with no trace.

**Consequences:**
- Consumers must check `status.isTerminal()` before processing a redelivered Kafka message (at-least-once delivery, ADR-001) — reprocessing a terminal transaction is a no-op, not an exception.
- Every status write goes through `TransactionStateMachine.transition(...)`; direct `setStatus` calls outside it are a code-review reject.
- New statuses require updating the table, the annotation where relevant, and the exhaustive test — all three fail loudly if forgotten.
