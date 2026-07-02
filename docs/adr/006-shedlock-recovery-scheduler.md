# ADR-006: ShedLock Recovery Schedulers

**Context:** Two failure modes leave transactions in a non-final state with nobody driving them forward: a publish that never succeeded (`kafka_published = FALSE`, ADR-001) and a transaction stuck mid-processing (consumer crashed after taking it). Recovery must be automatic, and must not run concurrently on every node of a multi-instance deployment.

**Decision:** Two scheduled jobs, each guarded by **ShedLock** (DB-backed distributed lock — one dedicated lock table, created by its own Flyway migration) so that exactly one instance executes each job per cycle:

1. **Unpublished-transaction recovery** (every 5 min): find `kafka_published = FALSE AND created_at < NOW() - INTERVAL '1 minute'`, re-attempt the publish. Retries are **capped at the shared `max-retries`** (ADR-004); beyond the cap the transaction is dead-lettered through the state machine, with a `PUBLISH` row in `transaction_failure_audit`. No infinite retry loop on a prolonged Kafka outage.
2. **Stuck-transaction recovery** (every 5 min): find `status IN ('DISPATCHED','PROCESSING') AND updated_at < NOW() - INTERVAL '10 minutes'`. Under `max-retries`: walk the legal state-machine path `→ FAILED → RETRY → DISPATCHED` (ADR-005 — no teleporting; the stuck timeout is recorded as the failure reason) and re-dispatch. At the cap: dead-letter.

**Rationale:** ShedLock over alternatives (Quartz clustered, leader election) because the lock store is the database we already operate — no new infrastructure, and lock state is inspectable with plain SQL. The 5-minute cadence is deliberately coarse: recovery is a safety net, not the primary path.

**Consequences:**
- Both jobs' `NOW()`-based cutoffs compare against timestamps written by the application in UTC. This requires **`TIMESTAMPTZ` columns**: with naive `TIMESTAMP`, a Postgres server not running in UTC silently shifts every recovery window by the timezone offset. (Migration V2 converts `created_at`/`updated_at` accordingly.)
- Recovery re-publishing means at-least-once delivery end-to-end; consumer idempotency (ADR-005 terminal check) is what makes that safe.
- ShedLock guards *scheduler execution*, not row-level access — row exclusivity during processing remains `SKIP LOCKED`'s job (ADR-002). The two locks protect different things and both are needed.
- Health checks expose scheduler liveness so a silently dead scheduler is an alert, not a discovery made during an incident.
