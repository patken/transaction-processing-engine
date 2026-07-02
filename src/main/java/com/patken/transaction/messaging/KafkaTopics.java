package com.patken.transaction.messaging;

/**
 * Fixed topic names shared by {@code KafkaConfig} (topic provisioning), the producer,
 * and {@code @KafkaListener} annotations (which require compile-time constants).
 */
public final class KafkaTopics {

    public static final String COMMANDS = "transaction.commands";
    public static final String EVENTS = "transaction.events";
    public static final String DLQ = "transaction.dlq";

    private KafkaTopics() {
    }
}
