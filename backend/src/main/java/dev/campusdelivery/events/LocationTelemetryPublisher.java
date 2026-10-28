package dev.campusdelivery.events;

import dev.campusdelivery.common.Json;
import dev.campusdelivery.config.AppProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Courier pings are high-volume, loss-tolerant telemetry, so they bypass the outbox and go
 * straight to Kafka (fire-and-forget). Business state changes use the outbox instead.
 */
@Component
public class LocationTelemetryPublisher {

    private static final Logger log = LoggerFactory.getLogger(LocationTelemetryPublisher.class);

    public record Ping(int schemaVersion, java.util.UUID courierId, double lat, double lng, Instant recordedAt,
                       Instant receivedAt, Float speedMps, Float headingDeg, Float accuracyM,
                       java.util.UUID orderId) {}

    private final KafkaTemplate<String, String> kafka;
    private final Json json;
    private final AppProperties props;
    private volatile Instant lastWarning = Instant.EPOCH;

    public LocationTelemetryPublisher(ObjectProvider<KafkaTemplate<String, String>> kafka, Json json,
                                      AppProperties props) {
        this.kafka = props.kafka().enabled() ? kafka.getIfAvailable() : null;
        this.json = json;
        this.props = props;
    }

    public void publish(List<Ping> pings) {
        if (kafka == null || pings.isEmpty()) {
            return;
        }
        String topic = props.kafka().courierLocationsTopic();
        for (Ping p : pings) {
            try {
                kafka.send(topic, p.courierId().toString(), json.write(p)).whenComplete((r, ex) -> {
                    if (ex != null) {
                        warn(ex);
                    }
                });
            } catch (RuntimeException ex) {
                warn(ex);
            }
        }
    }

    private void warn(Throwable ex) {
        Instant now = Instant.now();
        if (Duration.between(lastWarning, now).getSeconds() >= 30) {
            lastWarning = now;
            log.warn("Location telemetry publish failed (dropping pings): {}", ex.getMessage());
        }
    }
}
