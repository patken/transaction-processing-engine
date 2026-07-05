package com.patken.transaction.observability;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records the last time each recovery scheduler ran, so {@link SchedulerHealthIndicator}
 * can tell a live-but-idle scheduler apart from a silently-dead one (ADR-006). Decouples
 * the schedulers from the health check — they just call {@link #recordRun}.
 */
@Component
public class SchedulerHeartbeat {

    private final Map<String, Instant> lastRun = new ConcurrentHashMap<>();

    public void recordRun(String schedulerName) {
        lastRun.put(schedulerName, Instant.now());
    }

    public Optional<Instant> lastRun(String schedulerName) {
        return Optional.ofNullable(lastRun.get(schedulerName));
    }
}
