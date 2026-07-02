# ADR-008: REVERSAL Modeling

**Context:** The spec lists `REVERSAL` as a transaction type but defines no business rules: whether it mutates the original transaction or creates a new record, whether partial reversals are allowed, whether reversal chains or duplicate reversals are permitted, and what its accounts mean relative to the original.

**Decision:**
- A `REVERSAL` is a **new transaction record**, never a mutation of the original. It carries a required `original_transaction_id` (FK to `transactions.id`).
- The reversal amount must **equal** the original transaction's amount exactly. Partial reversals are out of scope.
- A transaction of type `REVERSAL` cannot itself be reversed (no reversal chains).
- An original transaction can be reversed **at most once** — enforced by a partial unique index: `CREATE UNIQUE INDEX uq_reversal_per_original ON transactions(original_transaction_id) WHERE type = 'REVERSAL'`.
- The original transaction must be in status `COMPLETED` to be eligible for reversal; otherwise the request is rejected with `409 Conflict`.
- **Amended after review — accounts are validated against the original:** a reversal reverses the original money movement, so its `sourceAccount`/`targetAccount` must equal the original's `targetAccount`/`sourceAccount` (swapped). Any other pair is rejected with `409 Conflict`. Client-supplied accounts are kept in the contract (rather than derived server-side) as an explicit intent check — a mismatch reveals a confused client early. This also transitively guarantees `sourceAccount != targetAccount` for reversals, since the original already satisfied it.

**Rationale:** Treating reversals as new append-only records is consistent with ledger/accounting practice — every state change is a new fact, nothing is overwritten, and the full history is auditable from the `transactions` table alone. Enforcing "one reversal per original" at the database level (not just in application code) prevents the race where two concurrent reversal requests both pass an application-level check before either commits — the same TOCTOU concern addressed for idempotency in ADR-003, applied here via a partial unique index.

**Load-bearing invariant (see ADR-005):** the "original must be COMPLETED" eligibility check is race-free *without any lock*, because `COMPLETED` is a terminal status — once the check passes, no concurrent transition can invalidate it. This holds only as long as terminal statuses stay terminal, which the state machine's static consistency check enforces.

**Consequences:**
- `original_transaction_id` is `NULL` for `CREDIT`/`DEBIT` and `NOT NULL` for `REVERSAL` — validated at the service level and enforced by a database `CHECK` constraint.
- Reversal creation goes through the same idempotency path as any other transaction (its own `businessId`), so a client retrying a reversal request is safe.
- **The application-level "already reversed" check is a fast-fail courtesy, not the guarantee.** The partial unique index is the guarantee. Consequently, the losing side of a concurrent-reversal race surfaces as a constraint violation on `uq_reversal_per_original` — the persistence layer must route that violation (by constraint name, per ADR-003) to a `409 Conflict`, not misread it as an idempotent replay or let it escape as a 500.
- No support for partial reversals or multi-step reversal chains — if a future requirement needs this, it's a schema change (e.g., a `reversed_amount` running total) and a new ADR, not a silent extension of this one.
