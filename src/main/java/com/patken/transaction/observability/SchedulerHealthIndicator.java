package com.patken.transaction.observability;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Recovery-scheduler liveness for {@code /actuator/health} (ADR-006). A scheduler that
 * has run and then gone quiet for far longer than its interval is treated as dead → the
 * component reports DOWN, surfacing a silently-stalled scheduler as an alert.
 *
 * <p>A scheduler that hasn't run yet (null last-run) is UP, not DOWN — that's the normal
 * just-started state, and on a multi-node deployment it's also the steady state for every
 * node that isn't currently the ShedLock holder, so it must not be a false negative.
 * Reported as the {@code schedulers} health component.
 */
@Component("schedulers")
public class SchedulerHealthIndicator implements HealthIndicator {

    public static final String STUCK_JOB = "stuckTransactionRecovery";
    public static final String UNPUBLISHED_JOB = "unpublishedTransactionRecovery";

    private final SchedulerHeartbeat heartbeat;
    private final Duration staleThreshold;

    public SchedulerHealthIndicator(SchedulerHeartbeat heartbeat,
                                    @Value("${transaction-engine.recovery.poll-interval-ms:300000}") long pollIntervalMs) {
        this.heartbeat = heartbeat;
        // Allow a couple of missed cycles before calling a scheduler dead.
        this.staleThreshold = Duration.ofMillis(pollIntervalMs * 3);
    }

    @Override
    public Health health() {
        Health.Builder builder = Health.up();
        boolean anyStale = false;

        for (String job : List.of(STUCK_JOB, UNPUBLISHED_JOB)) {
            Optional<Instant> lastRun = heartbeat.lastRun(job);
            if (lastRun.isEmpty()) {
                builder.withDetail(job, "not yet run");
                continue;
            }
            Duration since = Duration.between(lastRun.get(), Instant.now());
            builder.withDetail(job, "last run " + lastRun.get() + " (" + since.toSeconds() + "s ago)");
            if (since.compareTo(staleThreshold) > 0) {
                anyStale = true;
            }
        }

        return anyStale ? builder.down().build() : builder.build();
    }
}
