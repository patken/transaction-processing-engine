# ADR-003: Idempotency via businessId

**Context:** Clients retry transaction requests after network failures and timeouts. A retry must never double-process funds. The deduplication mechanism must hold under concurrency (two identical requests racing), not just in the sequential case.

**Decision:**
- Every transaction carries a client-provided `businessId`, unique per transaction, enforced by a `UNIQUE` constraint (`uq_business_id`) in PostgreSQL.
- The service performs an **optimistic insert**: attempt the INSERT, catch the constraint violation, re-read by `businessId`, return the existing row with HTTP 200 (vs. 201 for a fresh create). There is deliberately **no check-then-insert** — an application-level pre-check is a TOCTOU race; the database constraint is the only source of truth. (A unit test pins this: the gateway must not call `findByBusinessId` before inserting.)
- **Amended after review:** an idempotent replay is only honored if the payload matches. If the same `businessId` arrives with a *different* business payload (type, amount, currency, accounts, originalTransactionId), the request is rejected with **409 Conflict** and an explicit problem detail — not silently answered with the existing transaction. Same key + same payload = safe retry (200); same key + different payload = client bug that must surface, not be masked.

**Rationale:** The constraint-first approach is the only scheme that survives concurrent duplicates. The payload-match amendment closes the remaining semantic hole: without it, a client that erroneously reuses a key for a *different* payment intent receives a 200 and believes its second intent was accepted. Mature payment APIs (e.g., Stripe's idempotency keys) treat key reuse with different parameters as an error for exactly this reason.

**Consequences:**
- Clients must generate and track their own `businessId`; retries are safe by construction.
- The catch-the-violation path must **route by constraint name** (via the underlying `ConstraintViolationException`): only a `uq_business_id` violation means "idempotent replay". Other integrity violations on the same insert path (e.g., `uq_reversal_per_original`, see ADR-008) have different meanings and different status codes — treating every `DataIntegrityViolationException` as a duplicate businessId misroutes them.
- The replay path costs one extra SELECT — only on the duplicate path, not on the hot path.
- The payload comparison must use business fields only (not timestamps/correlationId), and `BigDecimal` comparison must be value-based (`compareTo`, not `equals`).
