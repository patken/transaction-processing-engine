package com.patken.transaction.integration;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.persistence.TransactionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@code SELECT ... FOR UPDATE SKIP LOCKED} (ADR-002) actually skips rather than
 * blocks: one worker holds the row lock while a second tries to acquire it and gets an
 * empty result immediately, instead of waiting for the first to release. Real Postgres —
 * SKIP LOCKED is Postgres-specific and can't be exercised against a substitute.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
class TransactionLockingIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("transaction_engine")
            .withUsername("transaction_engine")
            .withPassword("transaction_engine");

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private TransactionRepository repository;

    @Autowired
    private PlatformTransactionManager txManager;

    @Test
    void skipLockedSkipsARowAlreadyLockedByAnotherWorker() throws Exception {
        Transaction seeded = repository.save(newTransaction());
        UUID id = seeded.getId();

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService holderThread = Executors.newSingleThreadExecutor();

        // Worker A: acquire and hold the lock for up to 2s.
        Future<?> holder = holderThread.submit(() -> new TransactionTemplate(txManager).executeWithoutResult(s -> {
            assertThat(repository.lockForProcessing(id)).isPresent();
            locked.countDown();
            awaitQuietly(release);
        }));

        assertThat(locked.await(2, TimeUnit.SECONDS)).isTrue();

        // Worker B: try to acquire the same lock — should skip (empty) and return fast.
        long start = System.currentTimeMillis();
        Optional<Transaction> workerB = new TransactionTemplate(txManager)
                .execute(s -> repository.lockForProcessing(id));
        long elapsedMs = System.currentTimeMillis() - start;

        assertThat(workerB).as("second worker should skip the locked row").isEmpty();
        assertThat(elapsedMs).as("SKIP LOCKED should return immediately, not block on the held lock").isLessThan(1000);

        release.countDown();
        holder.get(2, TimeUnit.SECONDS);
        holderThread.shutdown();

        // Once the holder releases, the row is acquirable again.
        Optional<Transaction> afterRelease = new TransactionTemplate(txManager)
                .execute(s -> repository.lockForProcessing(id));
        assertThat(afterRelease).isPresent();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Transaction newTransaction() {
        return new Transaction(UUID.randomUUID(), "lock-it-" + UUID.randomUUID(), TransactionType.CREDIT,
                TransactionStatus.RECEIVED, BigDecimal.valueOf(100), "CAD", "ACC-001", "ACC-002", null, null, "corr");
    }
}
