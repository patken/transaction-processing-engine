package com.patken.transaction.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.patken.transaction.api.generated.dto.TransactionResponse;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.integration.support.AuthTestSupport;
import com.patken.transaction.persistence.TransactionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Real PostgreSQL + Kafka via Testcontainers — the happy path end to end, no manual
 * intervention: create via the API, then the consumer alone drives the transaction to
 * COMPLETED (Phase 4). Retry/DLQ/failure paths are Phase 5.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
class TransactionKafkaFlowIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("transaction_engine")
            .withUsername("transaction_engine")
            .withPassword("transaction_engine");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TransactionRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void createdTransactionReachesCompletedWithoutManualIntervention() throws Exception {
        String requestBody = """
                {
                  "businessId": "kafka-flow-it-1",
                  "type": "CREDIT",
                  "amount": 150.00,
                  "currency": "CAD",
                  "sourceAccount": "ACC-001",
                  "targetAccount": "ACC-002"
                }
                """;

        String token = AuthTestSupport.fetchTestToken(mockMvc);

        MvcResult createResult = mockMvc.perform(post("/api/v1/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header(HttpHeaders.AUTHORIZATION, AuthTestSupport.bearer(token))
                        .content(requestBody))
                .andExpect(status().isCreated())
                .andReturn();

        TransactionResponse created = objectMapper.readValue(
                createResult.getResponse().getContentAsString(), TransactionResponse.class);
        UUID transactionId = created.getTransactionId();

        assertThat(created.getStatus()).isEqualTo(com.patken.transaction.api.generated.dto.TransactionStatus.RECEIVED);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Transaction transaction = repository.findById(transactionId).orElseThrow();
            assertThat(transaction.getStatus()).isEqualTo(com.patken.transaction.domain.TransactionStatus.COMPLETED);
            assertThat(transaction.isKafkaPublished()).isTrue();
        });
    }
}
