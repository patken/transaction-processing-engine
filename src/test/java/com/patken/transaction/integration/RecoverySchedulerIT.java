package com.patken.transaction.integration;

import com.patken.transaction.domain.FailureType;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.persistence.TransactionFailureAuditRepository;
import com.patken.transaction.persistence.TransactionRepository;
import com.patken.transaction.recovery.StuckTransactionScheduler;
import com.patken.transaction.recovery.UnpublishedTransactionScheduler;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Real Postgres + Kafka. Exercises the ShedLock-guarded recovery schedulers (ADR-006)
 * by seeding transactions artificially aged past the recovery thresholds (backdating
 * timestamps via JDBC) and invoking the schedulers directly. The poll interval is set
 * very high so the auto-scheduled runs don't race the tests.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
class RecoverySchedulerIT {

    private static final int MAX_RETRIES = 3;

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("transaction_engine")
            .withUsername("transaction_engine")
            .withPassword("transaction_engine");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("transaction-engine.processing.max-retries", () -> MAX_RETRIES);
        registry.add("transaction-engine.recovery.poll-interval-ms", () -> 3_600_000); // don't auto-fire during the test
    }

    @Autowired
    private TransactionRepository repository;

    @Autowired
    private TransactionFailureAuditRepository auditRepository;

    @Autowired
    private StuckTransactionScheduler stuckScheduler;

    @Autowired
    private UnpublishedTransactionScheduler unpublishedScheduler;

    @Autowired
    private LockProvider lockProvider;

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void stuckTransactionUnderTheCeilingIsRedispatchedAndCompletes() {
        // Consumer died mid-PROCESSING; row hasn't moved in an hour.
        UUID id = seed(TransactionStatus.PROCESSING, 0, true);
        ageTimestamps(id, Duration.ofHours(1));

        stuckScheduler.recoverStuckTransactions();

        // Re-dispatched → the (live, failure-free) consumer resumes it to COMPLETED.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Transaction txn = repository.findById(id).orElseThrow();
            assertThat(txn.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
            assertThat(txn.getRetryCount()).isEqualTo(1); // the stuck sweep counted as one attempt
        });
        assertThat(auditRepository.findByTransactionIdOrderByAttemptNumberAsc(id)).hasSize(1);
    }

    @Test
    void stuckTransactionAtTheCeilingIsDeadLettered() {
        UUID id = seed(TransactionStatus.PROCESSING, MAX_RETRIES - 1, true);
        ageTimestamps(id, Duration.ofHours(1));

        stuckScheduler.recoverStuckTransactions();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Transaction txn = repository.findById(id).orElseThrow();
            assertThat(txn.getStatus()).isEqualTo(TransactionStatus.DEAD_LETTERED);
            assertThat(txn.getRetryCount()).isEqualTo(MAX_RETRIES);
        });
    }

    @Test
    void unpublishedTransactionIsRepublishedAndCompletes() {
        // Persisted (RECEIVED) but never published; older than the min-age gate.
        UUID id = seed(TransactionStatus.RECEIVED, 0, false);
        ageTimestamps(id, Duration.ofHours(1));

        unpublishedScheduler.recoverUnpublishedTransactions();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Transaction txn = repository.findById(id).orElseThrow();
            assertThat(txn.isKafkaPublished()).isTrue();
            assertThat(txn.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
        });
    }

    @Test
    void shedLockAllowsOnlyOneHolderAtATime() {
        // Two logical instances racing for the same named job: only one may hold the lock.
        LockConfiguration config = new LockConfiguration(
                Instant.now(), "recovery-exclusion-test", Duration.ofMinutes(5), Duration.ZERO);

        Optional<SimpleLock> first = lockProvider.lock(config);
        assertThat(first).as("first instance acquires the lock").isPresent();

        Optional<SimpleLock> second = lockProvider.lock(config);
        assertThat(second).as("second instance is excluded while the first holds it").isEmpty();

        first.get().unlock();

        Optional<SimpleLock> third = lockProvider.lock(config);
        assertThat(third).as("acquirable again once released").isPresent();
        third.get().unlock();
    }

    private UUID seed(TransactionStatus status, int retryCount, boolean kafkaPublished) {
        Transaction txn = new Transaction(UUID.randomUUID(), "recovery-it-" + UUID.randomUUID(),
                TransactionType.CREDIT, status, BigDecimal.valueOf(100), "CAD", "ACC-001", "ACC-002", null, null, "corr");
        txn.setRetryCount(retryCount);
        txn.setKafkaPublished(kafkaPublished);
        return repository.save(txn).getId();
    }

    /** Backdate created_at/updated_at past the recovery thresholds (they're @…Timestamp otherwise). */
    private void ageTimestamps(UUID id, Duration by) {
        Instant past = Instant.now().minus(by);
        jdbc.update("UPDATE transactions SET created_at = ?, updated_at = ? WHERE id = ?",
                java.sql.Timestamp.from(past), java.sql.Timestamp.from(past), id);
    }
}
