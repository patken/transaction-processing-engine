package com.patken.transaction.integration;

import com.patken.transaction.domain.FailureType;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionFailureAudit;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.messaging.producer.KafkaTransactionProducer;
import com.patken.transaction.persistence.TransactionFailureAuditRepository;
import com.patken.transaction.persistence.TransactionRepository;
import com.patken.transaction.recovery.UnpublishedTransactionScheduler;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

/**
 * The publish-retry ceiling (ADR-004, revised): with the Kafka producer forced to keep
 * failing, an unpublished transaction that reaches {@code max-retries} is DEAD_LETTERED
 * with a {@code failure_type = PUBLISH} audit row — instead of being republished forever
 * on a prolonged Kafka outage. Producer is mocked for deterministic failure; no real
 * Kafka needed.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
class UnpublishedPublishExhaustionIT {

    private static final int MAX_RETRIES = 3;

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("transaction_engine")
            .withUsername("transaction_engine")
            .withPassword("transaction_engine");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("transaction-engine.processing.max-retries", () -> MAX_RETRIES);
        registry.add("transaction-engine.recovery.poll-interval-ms", () -> 3_600_000);
    }

    @MockitoBean
    private KafkaTransactionProducer producer;

    @Autowired
    private TransactionRepository repository;

    @Autowired
    private TransactionFailureAuditRepository auditRepository;

    @Autowired
    private UnpublishedTransactionScheduler scheduler;

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void unpublishedTransactionPastTheCeilingIsDeadLetteredWithAPublishAudit() throws Exception {
        Mockito.doThrow(new KafkaTransactionProducer.PublishException("kafka down", new RuntimeException()))
                .when(producer).publishCommand(any());

        // Already failed publish twice; this cycle is the third and final attempt.
        Transaction seeded = new Transaction(UUID.randomUUID(), "publish-exhaust-" + UUID.randomUUID(),
                TransactionType.CREDIT, TransactionStatus.RECEIVED, BigDecimal.valueOf(100), "CAD",
                "ACC-001", "ACC-002", null, null, "corr");
        seeded.setRetryCount(MAX_RETRIES - 1);
        seeded.setKafkaPublished(false);
        UUID id = repository.save(seeded).getId();
        jdbc.update("UPDATE transactions SET created_at = ? WHERE id = ?",
                java.sql.Timestamp.from(Instant.now().minus(Duration.ofHours(1))), id);

        scheduler.recoverUnpublishedTransactions();

        Transaction txn = repository.findById(id).orElseThrow();
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.DEAD_LETTERED);
        assertThat(txn.getRetryCount()).isEqualTo(MAX_RETRIES);

        List<TransactionFailureAudit> audit = auditRepository.findByTransactionIdOrderByAttemptNumberAsc(id);
        assertThat(audit).hasSize(1);
        assertThat(audit.getFirst().getFailureType()).isEqualTo(FailureType.PUBLISH);
    }
}
