# transaction-processing-engine

Event-driven transaction processing engine built with Java 21, Spring Boot, Kafka, and PostgreSQL — featuring idempotency, pessimistic locking, failure recovery, and observability.

A simplified, open-source take on a real-world transaction command platform, built to demonstrate financial-grade correctness, reliability, and resilience patterns.

## Status

Work in progress.

## Documentation

- [Architecture overview](docs/architecture.md) — system flow, status lifecycle, key patterns (transactional outbox, SKIP LOCKED work queue, constraint-backed idempotency), data model, testing strategy.
- [Architecture Decision Records](docs/adr/) — one ADR per non-obvious decision, with context, alternatives considered, and consequences (ADR-001 through ADR-008).

## Quickstart

```bash
docker compose up -d --build
curl http://localhost:8080/actuator/health
```

## Try it locally (authentication)

Every business endpoint requires a JWT — `GET` included, since transaction records are
financial data (ADR-007). There's no external Identity Provider to sign up for: the
app signs its own demo-grade tokens locally, and exposes one open endpoint to get one:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/dev/token | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

curl http://localhost:8080/api/v1/transactions -H "Authorization: Bearer $TOKEN"
```

`/dev/token` and `/actuator/health|info|prometheus` are the only endpoints that don't
require a token. This is explicitly a demo-grade setup (see ADR-007's consequences) —
no external IdP, no refresh tokens, no scopes/roles beyond a single "authenticated"
claim, one-hour expiry. Swapping in a real IdP later is a config-only change (Spring
Security's Resource Server already abstracts this).

## Tech Stack

- Java 21, Spring Boot 3.5.6
- PostgreSQL (Flyway migrations)
- Apache Kafka (KRaft mode)
- Docker / docker-compose
- GitHub Actions CI
- OpenAPI 3 (contract-first, `openapi-generator-maven-plugin`)
- Spring Security (OAuth2 Resource Server, locally-signed JWT — ADR-007)
- ShedLock (DB-backed distributed lock for the recovery schedulers — ADR-006)
- Testcontainers (PostgreSQL, Kafka) for integration tests — `mvn test` for unit tests, `mvn verify` runs both

## API

Contract-first, defined in `src/main/resources/openapi/oas3.yaml`. Transaction commands
(`CREDIT`, `DEBIT`, `REVERSAL`) move through a validated status lifecycle:
`RECEIVED → VALIDATED → DISPATCHED → PROCESSING → COMPLETED`, with `FAILED → RETRY →
DEAD_LETTERED` as the failure path. Creating a transaction drives it through this full
lifecycle automatically — no manual intervention: the API persists it (`RECEIVED`),
publishes it to Kafka, and a consumer takes it the rest of the way to `COMPLETED`.

A processing failure is retried with exponential backoff (each attempt claims the row
with `SELECT ... FOR UPDATE SKIP LOCKED`, ADR-002); after the configured ceiling the
transaction is `DEAD_LETTERED`, the original command is published to the `transaction.dlq`
topic for ops replay, and every failed attempt is recorded in a `transaction_failure_audit`
row (ADR-004). To see it live, start the stack with a forced failure rate:

```bash
BACKEND_SIMULATOR_FAILURE_RATE=1.0 docker compose up -d --build
# every transaction now fails processing 3× then lands on transaction.dlq as DEAD_LETTERED
```

Two ShedLock-guarded recovery schedulers (ADR-006) are the safety net behind that
primary retry path: one re-dispatches transactions stuck in-flight (a consumer that
died mid-processing), the other retries the Kafka publish for transactions that are
durable in the DB but never made it onto the topic (persist-before-publish, ADR-001) —
both capped at the same retry ceiling before dead-lettering. ShedLock (a DB lock table)
ensures exactly one instance runs each job per cycle in a multi-node deployment. The
cadence and thresholds are tunable (`RECOVERY_POLL_INTERVAL_MS`, `RECOVERY_STUCK_TIMEOUT`,
…) — set them low to watch recovery act in a demo.

Endpoints implemented so far (base path `/api/v1`, all requiring `Authorization: Bearer $TOKEN` — see above):

```bash
# Create a transaction command — idempotent on businessId (resubmitting returns 200 + the existing transaction)
curl -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "businessId": "unique-client-id-123",
    "type": "CREDIT",
    "amount": 150.00,
    "currency": "CAD",
    "sourceAccount": "ACC-001",
    "targetAccount": "ACC-002"
  }'

# Get by transaction id
curl http://localhost:8080/api/v1/transactions/{transactionId} -H "Authorization: Bearer $TOKEN"

# Get by businessId
curl http://localhost:8080/api/v1/transactions/business/{businessId} -H "Authorization: Bearer $TOKEN"

# List, paginated and filterable by status
curl "http://localhost:8080/api/v1/transactions?status=RECEIVED&page=0&limit=20" -H "Authorization: Bearer $TOKEN"
```

Idempotency (ADR-003) is strict: resubmitting a `businessId` with the exact same
payload replays the original transaction (200); resubmitting it with a *different*
payload (amount, accounts, type, originalTransactionId) is rejected as a conflict
rather than silently accepted. Reversal accounts must mirror the original transaction's,
swapped (ADR-008).

RFC 7807 error responses for not-found / validation / conflict cases are not wired up
yet (Phase 9) — until then, unhandled business exceptions surface as a generic 500.
