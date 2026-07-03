package com.patken.transaction.domain;

/**
 * Which failure path an audit row belongs to (ADR-004). {@code PROCESSING} is a consumer
 * failure while driving a transaction toward COMPLETED; {@code PUBLISH} is a failure to
 * publish to Kafka (recorded by the Phase 6 unpublished-transaction recovery scheduler).
 */
public enum FailureType {
    PROCESSING,
    PUBLISH
}
