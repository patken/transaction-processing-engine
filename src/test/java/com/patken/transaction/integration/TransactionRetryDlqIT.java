package com.patken.transaction.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.patken.transaction.api.generated.dto.TransactionResponse;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.exception.TransientProcessingException;
import com.patken.transaction.integration.support.AuthTestSupport;
import com.patken.transaction.messaging.KafkaTopics;
import com.patken.transaction.messaging.consumer.BackendSimulator;
import com.patken.transaction.persistence.TransactionFailureAuditRepository;
import com.patken.transaction.persistence.TransactionRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Real Postgres + Kafka. Exercises the Phase 5 failure paths by mocking the backend to
 * fail deterministically (a probabilistic failure-rate would make the assertions flaky):
 * a transient failure that recovers before the ceiling ends COMPLETED; one that never
 * recovers ends DEAD_LETTERED, on the DLQ topic, with one audit row per attempt (ADR-004).
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
class TransactionRetryDlqIT {

    private static final int MAX_RETRIES = 3;

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
        registry.add("transaction-engine.processing.max-retries", () -> MAX_RETRIES);
        registry.add("transaction-engine.processing.backoff-ms", () -> 50);
    }

    // Replace the real backend so failures are deterministic instead of probabilistic.
    @MockitoBean
    private BackendSimulator backendSimulator;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TransactionRepository repository;

    @Autowired
    private TransactionFailureAuditRepository auditRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void transientFailureThatRecoversBeforeTheCeilingCompletes() throws Exception {
        // Fail once, then succeed.
        Mockito.doThrow(new TransientProcessingException("boom")).doNothing()
                .when(backendSimulator).process(any());

        UUID id = createTransaction("retry-recovers");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Transaction txn = repository.findById(id).orElseThrow();
            assertThat(txn.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
            assertThat(txn.getRetryCount()).isEqualTo(1);
        });
        assertThat(auditRepository.findByTransactionIdOrderByAttemptNumberAsc(id)).hasSize(1);
    }

    @Test
    void failureThatNeverRecoversIsDeadLetteredAndSentToTheDlq() throws Exception {
        Mockito.doThrow(new TransientProcessingException("permanent boom")).when(backendSimulator).process(any());

        UUID id = createTransaction("retry-exhausts");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Transaction txn = repository.findById(id).orElseThrow();
            assertThat(txn.getStatus()).isEqualTo(TransactionStatus.DEAD_LETTERED);
            assertThat(txn.getRetryCount()).isEqualTo(MAX_RETRIES);
        });
        assertThat(auditRepository.findByTransactionIdOrderByAttemptNumberAsc(id)).hasSize(MAX_RETRIES);
        assertThat(dlqContainsKey(id.toString())).as("the dead-lettered command should be on the DLQ topic").isTrue();
    }

    private UUID createTransaction(String businessId) throws Exception {
        String token = AuthTestSupport.fetchTestToken(mockMvc);
        String body = """
                {
                  "businessId": "%s",
                  "type": "CREDIT",
                  "amount": 150.00,
                  "currency": "CAD",
                  "sourceAccount": "ACC-001",
                  "targetAccount": "ACC-002"
                }
                """.formatted(businessId);

        String response = mockMvc.perform(post("/api/v1/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header(HttpHeaders.AUTHORIZATION, AuthTestSupport.bearer(token))
                        .content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readValue(response, TransactionResponse.class).getTransactionId();
    }

    private boolean dlqContainsKey(String expectedKey) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlq-assert-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(KafkaTopics.DLQ));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (expectedKey.equals(record.key())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
