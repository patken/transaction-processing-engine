# transaction-processing-engine

Event-driven transaction processing engine built with Java 21, Spring Boot, Kafka, and PostgreSQL — featuring idempotency, pessimistic locking, failure recovery, and observability.

A simplified, open-source take on a real-world transaction command platform, built to demonstrate financial-grade correctness, reliability, and resilience patterns.

## Status

Work in progress — see `docs/implementation-status.md` for the current phase.

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
