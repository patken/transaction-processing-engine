package com.patken.transaction.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

/**
 * Enables {@code @Retryable} (Spring Retry) for transient DB-failure retries with
 * exponential backoff — see {@link com.patken.transaction.persistence.TransactionGateway#persistIdempotent}.
 * This is distinct from the consumer's processing retry loop (that one is persistent —
 * it moves the transaction through RETRY/retry_count/audit); this is an in-memory,
 * same-call retry for blips like a lock-acquisition timeout or a dropped connection.
 */
@EnableRetry
@Configuration
public class RetryConfig {
}
