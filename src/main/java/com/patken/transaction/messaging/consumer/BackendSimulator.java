package com.patken.transaction.messaging.consumer;

import com.patken.transaction.domain.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Stands in for the external backend that would actually move funds. Happy-path only
 * for now (Phase 4) — delays are configurable to make the async pipeline visibly
 * asynchronous in a demo; configurable failure injection for resilience testing is
 * added in Phase 5.
 *
 * <p>{@code dispatch} and {@code process} are deliberately separate calls, matching the
 * DISPATCHED and PROCESSING statuses: dispatch stands in for handing the request off
 * to the backend (network round trip), process stands in for the backend actually
 * doing the work — two different failure modes in a real system (a dispatch failure
 * means the backend never saw the request; a processing failure means it did).
 */
@Component
public class BackendSimulator {

    private static final Logger log = LoggerFactory.getLogger(BackendSimulator.class);

    private final long dispatchDelayMs;
    private final long processingDelayMs;

    public BackendSimulator(
            @Value("${transaction-engine.backend-simulator.dispatch-delay-ms:20}") long dispatchDelayMs,
            @Value("${transaction-engine.backend-simulator.processing-delay-ms:100}") long processingDelayMs) {
        this.dispatchDelayMs = dispatchDelayMs;
        this.processingDelayMs = processingDelayMs;
    }

    public void dispatch(Transaction transaction) {
        log.debug("Dispatching transaction {} to backend", transaction.getId());
        sleep(transaction, dispatchDelayMs);
    }

    public void process(Transaction transaction) {
        sleep(transaction, processingDelayMs);
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
