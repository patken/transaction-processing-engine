package com.patken.transaction.integration;

import com.patken.transaction.integration.support.AuthTestSupport;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * ADR-007: every business endpoint requires a valid JWT, GET included; {@code
 * /dev/token} and the open actuator endpoints are the only exceptions. Real Postgres +
 * Kafka via Testcontainers — Kafka is needed because {@code /actuator/health} now
 * aggregates the Kafka connectivity indicator (Phase 7), so a genuine 200 requires it up.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
class SecurityIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("transaction_engine")
            .withUsername("transaction_engine")
            .withPassword("transaction_engine");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    void devTokenEndpointIsOpen() throws Exception {
        mockMvc.perform(post("/dev/token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.access_token").exists())
                .andExpect(jsonPath("$.token_type").value("Bearer"));
    }

    @Test
    void actuatorHealthIsOpen() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk());
    }

    @Test
    void getWithoutTokenIsRejected() throws Exception {
        mockMvc.perform(get("/api/v1/transactions"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void postWithoutTokenIsRejected() throws Exception {
        mockMvc.perform(post("/api/v1/transactions").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void requestWithGarbageTokenIsRejected() throws Exception {
        mockMvc.perform(get("/api/v1/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer not-a-real-jwt"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void requestWithValidTokenIsAccepted() throws Exception {
        String token = AuthTestSupport.fetchTestToken(mockMvc);

        mockMvc.perform(get("/api/v1/transactions")
                        .header(HttpHeaders.AUTHORIZATION, AuthTestSupport.bearer(token)))
                .andExpect(status().isOk());
    }
}
