package com.patken.transaction.integration;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.persistence.TransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.UUID;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.nullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Real PostgreSQL via Testcontainers — no H2. Pagination and status filtering rely on
 * Postgres-specific behavior (the composite index, JSONB column) worth exercising for
 * real, not against a substitute that wouldn't prove anything (see notes/architecture-review.md).
 *
 * <p>{@code disabledWithoutDocker = true}: skips gracefully (not a failure) when no usable
 * Docker environment is detected, e.g. the docker-java/Docker Desktop macOS API-version
 * incompatibility noted in notes/implementation-status.md — rather than failing the build
 * over a local environment issue unrelated to this code.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
class TransactionQueryIT {

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
    private MockMvc mockMvc;

    @Autowired
    private TransactionRepository repository;

    @BeforeEach
    void seedTransactions() {
        repository.deleteAll();
        for (int i = 0; i < 5; i++) {
            TransactionStatus status = i < 3 ? TransactionStatus.COMPLETED : TransactionStatus.RECEIVED;
            repository.save(newTransaction("biz-query-" + i, status));
        }
    }

    @Test
    void firstPageReturnsLimitRecordsAndANextLink() throws Exception {
        mockMvc.perform(get("/api/v1/transactions").param("page", "0").param("limit", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.records.length()").value(2))
                .andExpect(jsonPath("$.total").value(5))
                .andExpect(jsonPath("$.next").value(endsWith("page=1&limit=2")));
    }

    @Test
    void lastPageHasNoNextLink() throws Exception {
        mockMvc.perform(get("/api/v1/transactions").param("page", "2").param("limit", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.records.length()").value(1))
                .andExpect(jsonPath("$.total").value(5))
                .andExpect(jsonPath("$.next").value(nullValue()));
    }

    @Test
    void filtersByStatus() throws Exception {
        mockMvc.perform(get("/api/v1/transactions").param("status", "COMPLETED").param("limit", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.records.length()").value(3))
                .andExpect(jsonPath("$.total").value(3));

        mockMvc.perform(get("/api/v1/transactions").param("status", "RECEIVED").param("limit", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.records.length()").value(2))
                .andExpect(jsonPath("$.total").value(2));
    }

    private Transaction newTransaction(String businessId, TransactionStatus status) {
        return new Transaction(
                UUID.randomUUID(),
                businessId,
                TransactionType.CREDIT,
                status,
                BigDecimal.valueOf(100),
                "CAD",
                "ACC-001",
                "ACC-002",
                null,
                null,
                "corr-" + businessId
        );
    }
}
