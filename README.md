# transaction-processing-engine

Event-driven transaction processing engine built with Java 21, Spring Boot, Kafka, and PostgreSQL — featuring idempotency, pessimistic locking, failure recovery, and observability.

A simplified, open-source take on a real-world transaction command platform, built to demonstrate financial-grade correctness, reliability, and resilience patterns.

## Status

Work in progress.

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

## API

Contract-first, defined in `src/main/resources/openapi/oas3.yaml`. Transaction commands
(`CREDIT`, `DEBIT`, `REVERSAL`) move through a validated status lifecycle:
`RECEIVED → VALIDATED → DISPATCHED → PROCESSING → COMPLETED`, with `FAILED → RETRY →
DEAD_LETTERED` as the failure path.
