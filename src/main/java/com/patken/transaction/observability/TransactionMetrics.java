package com.patken.transaction.observability;

import com.patken.transaction.domain.TransactionType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Micrometer instruments for the transaction pipeline (spec §Observability), exposed on
 * {@code /actuator/prometheus}. A thin facade over {@link MeterRegistry} so the call
 * sites read as domain intent ({@code recordCompleted(...)}) rather than metric plumbing,
 * and so the metric names live in one place.
 *
 * <p>Counters are named without a {@code .total} suffix — the Prometheus registry appends
 * {@code _total} itself, yielding e.g. {@code transactions_created_total}.
 */
@Component
public class TransactionMetrics {

    private final MeterRegistry registry;
    private final Counter completed;
    private final Counter failed;
    private final Counter deadLettered;
    private final Counter stuckRecovered;
    private final Counter publishFailures;
    private final Timer processingDuration;

    public TransactionMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.completed = registry.counter("transactions.completed");
        this.failed = registry.counter("transactions.failed");
        this.deadLettered = registry.counter("transactions.dead_lettered");
        this.stuckRecovered = registry.counter("transactions.stuck.recovered");
        this.publishFailures = registry.counter("kafka.publish.failures");
        this.processingDuration = Timer.builder("transactions.processing.duration")
                .description("Time from RECEIVED to COMPLETED")
                .register(registry);
    }

    /**
     * Tagged by type so CREDIT/DEBIT/REVERSAL volumes can be told apart. Named
     * {@code transactions.creations} rather than {@code transactions.created}: a metric
     * ending in "created" collides with OpenMetrics' reserved {@code _created} timestamp
     * suffix, and the Prometheus renderer strips it — leaving a misleading
     * {@code transactions_total}. "creations" renders cleanly as
     * {@code transactions_creations_total}.
     */
    public void recordCreated(TransactionType type) {
        registry.counter("transactions.creations", "type", type.name()).increment();
    }

    public void recordCompleted(Duration processingTime) {
        completed.increment();
        processingDuration.record(processingTime);
    }

    public void recordFailed() {
        failed.increment();
    }

    public void recordDeadLettered() {
        deadLettered.increment();
    }

    public void recordStuckRecovered() {
        stuckRecovered.increment();
    }

    public void recordPublishFailure() {
        publishFailures.increment();
    }
}
