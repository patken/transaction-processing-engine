package com.patken.transaction.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.patken.transaction.messaging.KafkaTopics;
import com.patken.transaction.messaging.TransactionCommandMessage;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer/consumer
 */
@EnableKafka
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic transactionCommandsTopic() {
        return new NewTopic(KafkaTopics.COMMANDS, 3, (short) 1);
    }

    @Bean
    public NewTopic transactionEventsTopic() {
        return new NewTopic(KafkaTopics.EVENTS, 3, (short) 1);
    }

    @Bean
    public NewTopic transactionDlqTopic() {
        return new NewTopic(KafkaTopics.DLQ, 3, (short) 1);
    }

    /**
     * Deliberately NOT a {@code @Bean}: a second unqualified {@code ObjectMapper} bean
     * in the context breaks Spring Boot's autowiring of its own autoconfigured one into
     * the MVC message converter — it silently falls back to a bare ObjectMapper with
     * none of Boot's customizations (found the hard way: JsonNullableModule, registered
     * via JacksonConfig, stopped applying to API responses the moment this was a
     * second {@code @Bean ObjectMapper}). This mapper is only ever needed inside this
     * class, so a private factory method sidesteps the collision entirely.
     */
    private static ObjectMapper kafkaObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(config);
        JsonSerializer<Object> serializer = new JsonSerializer<>(kafkaObjectMapper());
        serializer.setAddTypeInfo(false); // wire format matches the documented message shape, no Java class metadata
        factory.setValueSerializer(serializer);
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // Typed to TransactionCommandMessage rather than Object: this codebase only ever
    // consumes one message shape from transaction.commands
    @Bean
    public ConsumerFactory<String, TransactionCommandMessage> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId) {
        Map<String, Object> config = new HashMap<>();
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<TransactionCommandMessage> deserializer =
                new JsonDeserializer<>(TransactionCommandMessage.class, kafkaObjectMapper());
        deserializer.setUseTypeHeaders(false);
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TransactionCommandMessage> kafkaListenerContainerFactory(
            ConsumerFactory<String, TransactionCommandMessage> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, TransactionCommandMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
