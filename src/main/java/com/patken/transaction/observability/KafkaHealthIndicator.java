package com.patken.transaction.observability;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Kafka broker connectivity for {@code /actuator/health} (spec §Observability). Spring
 * Boot auto-configures DB health but not Kafka, so this asks an {@link AdminClient} to
 * describe the cluster with a short timeout: reachable brokers → UP (with node count and
 * cluster id), otherwise DOWN with the error. Reported as the {@code kafka} health
 * component.
 */
@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final AdminClient adminClient;

    public KafkaHealthIndicator(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.adminClient = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis()));
    }

    @Override
    public Health health() {
        try {
            DescribeClusterResult cluster = adminClient.describeCluster();
            int nodeCount = cluster.nodes().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).size();
            String clusterId = cluster.clusterId().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return Health.up().withDetail("clusterId", clusterId).withDetail("nodes", nodeCount).build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down(e).build();
        } catch (Exception e) {
            return Health.down(e).build();
        }
    }

    @PreDestroy
    void close() {
        adminClient.close(Duration.ofSeconds(1));
    }
}
