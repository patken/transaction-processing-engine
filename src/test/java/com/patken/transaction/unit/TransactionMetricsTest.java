package com.patken.transaction.unit;

import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.observability.TransactionMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionMetricsTest {

    private SimpleMeterRegistry registry;
    private TransactionMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new TransactionMetrics(registry);
    }

    @Test
    void createdIsCountedPerType() {
        metrics.recordCreated(TransactionType.CREDIT);
        metrics.recordCreated(TransactionType.CREDIT);
        metrics.recordCreated(TransactionType.REVERSAL);

        assertThat(registry.get("transactions.creations").tag("type", "CREDIT").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("transactions.creations").tag("type", "REVERSAL").counter().count()).isEqualTo(1.0);
    }

    @Test
    void completedRecordsBothTheCounterAndTheTimer() {
        metrics.recordCompleted(Duration.ofMillis(250));

        assertThat(registry.get("transactions.completed").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("transactions.processing.duration").timer().count()).isEqualTo(1);
        assertThat(registry.get("transactions.processing.duration").timer().totalTime(java.util.concurrent.TimeUnit.MILLISECONDS))
                .isGreaterThanOrEqualTo(250.0);
    }

    @Test
    void failureDeadLetterRecoveryAndPublishFailureCounters() {
        metrics.recordFailed();
        metrics.recordDeadLettered();
        metrics.recordStuckRecovered();
        metrics.recordPublishFailure();

        assertThat(registry.get("transactions.failed").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("transactions.dead_lettered").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("transactions.stuck.recovered").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("kafka.publish.failures").counter().count()).isEqualTo(1.0);
    }
}
