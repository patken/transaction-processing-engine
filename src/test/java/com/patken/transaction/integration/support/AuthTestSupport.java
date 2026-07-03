package com.patken.transaction.integration.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Reusable across every integration suite (Phase 4.5 DoD) — fetches a real token from
 * the actual {@code /dev/token} endpoint rather than injecting a fake pre-authenticated
 * security context, so integration tests exercise the whole JWT pipeline (encode via
 * {@code JwtEncoder}, decode via {@code JwtDecoder}) end to end, consistent with this
 * project's "real infra, no fakes" testing policy (no H2, real Testcontainers).
 */
public final class AuthTestSupport {

    private AuthTestSupport() {
    }

    public static String fetchTestToken(MockMvc mockMvc) throws Exception {
        String body = mockMvc.perform(post("/dev/token"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return new ObjectMapper().readTree(body).get("access_token").asText();
    }

    public static String bearer(String token) {
        return "Bearer " + token;
    }
}
