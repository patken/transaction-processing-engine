package com.patken.transaction.unit;

import com.patken.transaction.api.GlobalExceptionHandler;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.exception.IdempotencyConflictException;
import com.patken.transaction.domain.exception.InvalidStateTransitionException;
import com.patken.transaction.domain.exception.InvalidTransactionRequestException;
import com.patken.transaction.domain.exception.ReversalNotAllowedException;
import com.patken.transaction.domain.exception.TransactionNotFoundException;
import com.patken.transaction.observability.MdcKeys;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Web-layer unit test: a probe controller throws each exception; assertions pin the
 * RFC 7807 mapping (status, {@code application/problem+json}, title, correlationId).
 * No Spring context, no DB — standalone MockMvc + the advice under test.
 */
class GlobalExceptionHandlerTest {

    private final MockMvc mockMvc = MockMvcBuilders
            .standaloneSetup(new ProbeController())
            .setControllerAdvice(new GlobalExceptionHandler())
            .build();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void notFoundMapsTo404ProblemJson() throws Exception {
        mockMvc.perform(get("/probe/not-found"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.title", is("Transaction not found")))
                .andExpect(jsonPath("$.status", is(404)));
    }

    @Test
    void idempotencyConflictMapsTo409() throws Exception {
        mockMvc.perform(get("/probe/idempotency-conflict"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title", is("Idempotency conflict")));
    }

    @Test
    void invalidStateTransitionMapsTo409() throws Exception {
        mockMvc.perform(get("/probe/invalid-transition"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title", is("Invalid state transition")));
    }

    @Test
    void reversalNotAllowedMapsTo409() throws Exception {
        mockMvc.perform(get("/probe/reversal-not-allowed"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title", is("Reversal not allowed")));
    }

    @Test
    void invalidRequestMapsTo400() throws Exception {
        mockMvc.perform(get("/probe/invalid-request"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title", is("Invalid transaction request")));
    }

    @Test
    void correlationIdFromMdcIsEchoedIntoTheProblem() throws Exception {
        MDC.put(MdcKeys.CORRELATION_ID, "corr-42");
        mockMvc.perform(get("/probe/not-found"))
                .andExpect(jsonPath("$.correlationId", is("corr-42")));
    }

    @Test
    void bodyValidationFailureMapsTo400WithFieldErrors() throws Exception {
        mockMvc.perform(post("/probe/validate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title", is("Validation failed")))
                .andExpect(jsonPath("$.errors").exists());
    }

    @Test
    void malformedJsonMapsTo400() throws Exception {
        mockMvc.perform(post("/probe/validate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{ not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title", is("Malformed request body")));
    }

    @Test
    void typeMismatchMapsTo400() throws Exception {
        mockMvc.perform(get("/probe/uuid/not-a-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title", is("Invalid parameter")));
    }

    @Test
    void unexpectedExceptionMapsToOpaque500() throws Exception {
        mockMvc.perform(get("/probe/boom"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.title", is("Internal error")))
                .andExpect(jsonPath("$.detail", is("An unexpected error occurred")));
    }

    @RestController
    static class ProbeController {

        @org.springframework.web.bind.annotation.GetMapping("/probe/not-found")
        void notFound() {
            throw new TransactionNotFoundException(UUID.randomUUID());
        }

        @org.springframework.web.bind.annotation.GetMapping("/probe/idempotency-conflict")
        void idempotency() {
            throw new IdempotencyConflictException("biz-1");
        }

        @org.springframework.web.bind.annotation.GetMapping("/probe/invalid-transition")
        void invalidTransition() {
            throw new InvalidStateTransitionException(TransactionStatus.COMPLETED, TransactionStatus.DISPATCHED);
        }

        @org.springframework.web.bind.annotation.GetMapping("/probe/reversal-not-allowed")
        void reversal() {
            throw new ReversalNotAllowedException("original is not COMPLETED");
        }

        @org.springframework.web.bind.annotation.GetMapping("/probe/invalid-request")
        void invalidRequest() {
            throw new InvalidTransactionRequestException("sourceAccount must differ from targetAccount");
        }

        @org.springframework.web.bind.annotation.GetMapping("/probe/uuid/{id}")
        void uuid(@PathVariable UUID id) {
            // reached only with a valid UUID; a bad value triggers MethodArgumentTypeMismatchException
        }

        @org.springframework.web.bind.annotation.GetMapping("/probe/boom")
        void boom() {
            throw new IllegalStateException("some internal detail that must not leak");
        }

        @PostMapping("/probe/validate")
        void validate(@RequestBody @Valid Payload payload) {
        }
    }

    record Payload(@NotBlank String name) {
    }
}
