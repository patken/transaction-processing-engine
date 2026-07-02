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

## Tech Stack

- Java 21, Spring Boot 3.5.6
- PostgreSQL (Flyway migrations)
- Apache Kafka (KRaft mode)
- Docker / docker-compose
- GitHub Actions CI
- OpenAPI 3 (contract-first, `openapi-generator-maven-plugin`)
- Testcontainers (PostgreSQL) for integration tests — `mvn test` for unit tests, `mvn verify` runs both

## API

Contract-first, defined in `src/main/resources/openapi/oas3.yaml`. Transaction commands
(`CREDIT`, `DEBIT`, `REVERSAL`) move through a validated status lifecycle:
`RECEIVED → VALIDATED → DISPATCHED → PROCESSING → COMPLETED`, with `FAILED → RETRY →
DEAD_LETTERED` as the failure path.

Endpoints implemented so far (base path `/api/v1`):

```bash
# Create a transaction command — idempotent on businessId (resubmitting returns 200 + the existing transaction)
curl -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "businessId": "unique-client-id-123",
    "type": "CREDIT",
    "amount": 150.00,
    "currency": "CAD",
    "sourceAccount": "ACC-001",
    "targetAccount": "ACC-002"
  }'

# Get by transaction id
curl http://localhost:8080/api/v1/transactions/{transactionId}

# Get by businessId
curl http://localhost:8080/api/v1/transactions/business/{businessId}

# List, paginated and filterable by status
curl "http://localhost:8080/api/v1/transactions?status=RECEIVED&page=0&limit=20"
```

Idempotency (ADR-003) is strict: resubmitting a `businessId` with the exact same
payload replays the original transaction (200); resubmitting it with a *different*
payload (amount, accounts, type, originalTransactionId) is rejected as a conflict
rather than silently accepted. Reversal accounts must mirror the original transaction's,
swapped (ADR-008).

Authentication (JWT, required on every endpoint per [ADR-007](docs/adr/007-security-strategy.md))
and RFC 7807 error responses for not-found / validation / conflict cases are not wired
up yet — until then, endpoints are open and unhandled business exceptions surface as a
generic 500.
