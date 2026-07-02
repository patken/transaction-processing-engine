# ADR-001: Persist Before Publish (State-Based Transactional Outbox)

**Context:** When a transaction command is accepted by the API, it must reach two systems: PostgreSQL (source of truth) and Kafka (processing pipeline). Writing to two systems cannot be atomic — one of them can always fail after the other succeeded. The order and the recovery mechanism must be chosen deliberately.

**Decision:** Always persist to PostgreSQL first, then publish to Kafka. A `kafka_published` flag on the `transactions` row tracks whether the publish succeeded; a recovery scheduler (ADR-006) retries unpublished rows, with retries capped per ADR-004.

This is the **transactional outbox pattern, state-based variant**: instead of a separate outbox table holding pending messages, the business row itself carries the publication state. The partial index on `kafka_published = FALSE` keeps the "pending outbox" scan cheap regardless of table size.

**Alternatives considered:**
- *Publish first, persist second* — rejected: if the DB write fails after Kafka accepted the message, a consumer processes a transaction that officially doesn't exist. Data loss risk inverted to the worse side.
- *Dedicated outbox table + message relay* — the classic outbox shape. More decoupled (business row stays pure), but adds a table, a relay component, and a cleanup policy. Not justified at this scale; the state-based variant demonstrates the same guarantee with less machinery.
- *Change Data Capture (Debezium)* — production-grade choice for high-volume systems; publishes from the WAL, no dual-write at all. Rejected for scope: it adds Kafka Connect infrastructure and moves the interesting logic out of the application. Worth naming as the natural evolution path.

**Consequences:**
- No transaction is ever silently lost: worst case, it sits in PostgreSQL with `kafka_published = FALSE` until the scheduler retries or dead-letters it (ADR-004).
- Publishing is at-least-once: a crash between successful publish and flag update causes a re-publish on recovery. Consumers must be idempotent (they are — see ADR-005: terminal statuses make reprocessing a no-op).
- The post-publish flag update must not go through the regular JPA save path: reloading and saving the entity bumps the optimistic-lock `version` and can collide with a consumer updating the status of the same row at the same moment (the happy path makes this likely — publish and consume are near-simultaneous). The flag is set via a targeted `@Modifying` update (`UPDATE ... SET kafka_published = true WHERE id = ?`) that touches neither `version` nor the entity cache.
- Slight write-latency increase (DB commit before publish) — acceptable for a system whose stated priority is correctness over latency.
