package com.patken.transaction.observability;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Establishes the correlationId for every request: reuses an inbound
 * {@code X-Correlation-Id} header if the caller supplied one (continuing an existing
 * trace), otherwise mints one. It goes into the MDC (so every log line during the
 * request carries it) and onto the response header (so the caller can correlate too).
 * The transaction created downstream copies it into its {@code correlation_id} column,
 * from where the producer forwards it as a Kafka header — closing the API → Kafka →
 * consumer trace the spec calls for.
 *
 * <p>Ordered first so the correlationId is in the MDC before anything else logs.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String correlationId = request.getHeader(MdcKeys.CORRELATION_ID_HEADER);
        if (!StringUtils.hasText(correlationId)) {
            correlationId = UUID.randomUUID().toString();
        }

        MDC.put(MdcKeys.CORRELATION_ID, correlationId);
        response.setHeader(MdcKeys.CORRELATION_ID_HEADER, correlationId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(MdcKeys.CORRELATION_ID);
        }
    }
}
