package com.patken.transaction.api;

import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * Issues a locally-signed test JWT (ADR-007) — the one open endpoint, since there's no
 * external IdP a reviewer could get a token from otherwise. Not part of the versioned
 * business API (not under {@code /api/v1}, not in {@code oas3.yaml}): this exists
 * purely so {@code docker-compose up} + curl is enough to exercise the whole API,
 * demo-grade only. See the README "Try it locally" section and ADR-007's
 * consequences: no scopes/roles, a single "authenticated" claim, one-hour expiry.
 */
@RestController
@RequestMapping("/dev")
public class DevTokenController {

    private final JwtEncoder jwtEncoder;

    public DevTokenController(JwtEncoder jwtEncoder) {
        this.jwtEncoder = jwtEncoder;
    }

    @PostMapping("/token")
    public Map<String, Object> issueToken() {
        Instant now = Instant.now();
        Instant expiresAt = now.plus(1, ChronoUnit.HOURS);

        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer("transaction-processing-engine")
                .issuedAt(now)
                .expiresAt(expiresAt)
                .subject("demo-user")
                .build();
        JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).build();

        Jwt jwt = jwtEncoder.encode(JwtEncoderParameters.from(header, claims));

        return Map.of(
                "access_token", jwt.getTokenValue(),
                "token_type", "Bearer",
                "expires_in", ChronoUnit.SECONDS.between(now, expiresAt)
        );
    }
}
