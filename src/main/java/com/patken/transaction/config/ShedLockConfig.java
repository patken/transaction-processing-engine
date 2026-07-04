package com.patken.transaction.config;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;

/**
 * Enables scheduling and distributed locking for the recovery schedulers (ADR-006). The
 * lock lives in the {@code shedlock} DB table (V4 migration), so exactly one instance
 * runs each named job per cycle in a multi-node deployment.
 *
 * <p>{@code usingDbTime()} makes ShedLock compare lock timestamps using the database
 * server's clock rather than each app node's — no cross-node clock-skew hazard.
 * {@code defaultLockAtMostFor} is the safety ceiling: if a node dies holding the lock,
 * it's force-released after this long so recovery isn't stalled forever.
 */
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT10M")
public class ShedLockConfig {

    @Bean
    public LockProvider lockProvider(DataSource dataSource) {
        return new JdbcTemplateLockProvider(
                JdbcTemplateLockProvider.Configuration.builder()
                        .withJdbcTemplate(new JdbcTemplate(dataSource))
                        .usingDbTime()
                        .build());
    }
}
