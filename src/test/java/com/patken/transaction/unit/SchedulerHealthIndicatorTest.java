package com.patken.transaction.unit;

import com.patken.transaction.observability.SchedulerHealthIndicator;
import com.patken.transaction.observability.SchedulerHeartbeat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerHealthIndicatorTest {

    private static final long POLL_INTERVAL_MS = 1_000;

    private SchedulerHeartbeat heartbeat;
    private SchedulerHealthIndicator indicator;

    @BeforeEach
    void setUp() {
        heartbeat = new SchedulerHeartbeat();
        indicator = new SchedulerHealthIndicator(heartbeat, POLL_INTERVAL_MS);
    }

    @Test
    void upWhenSchedulersHaveNotRunYet() {
        // Just-started (or, on a multi-node deploy, a non-lock-holding node) — not a failure.
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void upWhenSchedulersRanRecently() {
        heartbeat.recordRun(SchedulerHealthIndicator.STUCK_JOB);
        heartbeat.recordRun(SchedulerHealthIndicator.UNPUBLISHED_JOB);

        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void downWhenAScheduler_ranBefore_butHasGoneStale() {
        // A run far enough in the past that it exceeds 3× the poll interval (the stale threshold).
        SchedulerHeartbeat staleHeartbeat = new SchedulerHeartbeat() {
            @Override
            public java.util.Optional<java.time.Instant> lastRun(String schedulerName) {
                return java.util.Optional.of(java.time.Instant.now().minusSeconds(3600));
            }
        };
        SchedulerHealthIndicator staleIndicator = new SchedulerHealthIndicator(staleHeartbeat, POLL_INTERVAL_MS);

        Health health = staleIndicator.health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }
}
