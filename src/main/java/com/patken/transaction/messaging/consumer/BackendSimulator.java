package com.patken.transaction.messaging.consumer;

import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.exception.TransientProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Stands in for the external backend that would actually move funds. Delays make the
 * async pipeline visibly asynchronous in a demo; {@code process}'s configurable failure
 * rate (Phase 5) lets the retry → DLQ path be exercised without a real flaky backend.
 *
 * <p>{@code dispatch} and {@code process} are deliberately separate calls, matching the
 * DISPATCHED and PROCESSING statuses: dispatch stands in for handing the request off
 * to the backend (network round trip), process stands in for the backend actually
 * doing the work — two different failure modes in a real system (a dispatch failure
 * means the backend never saw the request; a processing failure means it did).
 *
 * <p>Failure injection is on {@code process} only (not {@code dispatch}) to keep the
 * demo's single knob easy to reason about; integration tests replace this bean with a
 * mock for deterministic control rather than relying on the probabilistic rate.
 */
@Component
public class BackendSimulator {

    private static final Logger log = LoggerFactory.getLogger(BackendSimulator.class);

    private final long dispatchDelayMs;
    private final long processingDelayMs;
    private final double failureRate;

    public BackendSimulator(
            @Value("${transaction-engine.backend-simulator.dispatch-delay-ms:20}") long dispatchDelayMs,
            @Value("${transaction-engine.backend-simulator.processing-delay-ms:100}") long processingDelayMs,
            @Value("${transaction-engine.backend-simulator.failure-rate:0.0}") double failureRate) {
        this.dispatchDelayMs = dispatchDelayMs;
        this.processingDelayMs = processingDelayMs;
        this.failureRate = failureRate;
    }

    public void dispatch(Transaction transaction) {
        log.debug("Dispatching transaction {} to backend", transaction.getId());
        sleep(transaction, dispatchDelayMs);
    }

    public void process(Transaction transaction) {
        sleep(transaction, processingDelayMs);
        if (failureRate > 0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new TransientProcessingException(
                    "Simulated backend processing failure for transaction " + transaction.getId());
        }
    }

    private void sleep(Transaction transaction, long delayMs) {
        if (delayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Backend simulator interrupted while handling " + transaction.getId(), e);
        }
    }
}
