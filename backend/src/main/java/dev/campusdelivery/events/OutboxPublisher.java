package dev.campusdelivery.events;

import dev.campusdelivery.common.Json;
import dev.campusdelivery.config.AppProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Transactional outbox: domain events are written to the events table in the same transaction as
 * the state change, then relayed to Kafka here. At-least-once: a crash between send and
 * markPublished re-sends, consumers dedupe on eventId.
 */
@Component
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true")
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH = 200;

    private final EventRepository events;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final Json json;
    private final AppProperties props;
    private volatile Instant lastWarning = Instant.EPOCH;

    public OutboxPublisher(EventRepository events, KafkaTemplate<String, String> kafka,
                           TransactionTemplate tx, Json json, AppProperties props) {
        this.events = events;
        this.kafka = kafka;
        this.tx = tx;
        this.json = json;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${app.kafka.outbox-tick-ms}", initialDelayString = "${app.kafka.outbox-tick-ms}")
    public void relay() {
        try {
            Integer sent;
            do {
                sent = tx.execute(status -> {
                    List<EventRecord> batch = events.lockUnpublished(BATCH);
                    if (batch.isEmpty()) {
                        return 0;
                    }
                    List<Long> done = new ArrayList<>(batch.size());
                    for (EventRecord e : batch) {
                        publishBlocking(e);
                        done.add(e.id());
                    }
                    events.markPublished(done);
                    return done.size();
                });
            } while (sent != null && sent == BATCH);
        } catch (RuntimeException ex) {
            Instant now = Instant.now();
            if (Duration.between(lastWarning, now).getSeconds() >= 30) {
                lastWarning = now;
                log.warn("Outbox relay to Kafka failed, will retry: {}", ex.getMessage());
            }
        }
    }

    private void publishBlocking(EventRecord e) {
        String topic = EventRecord.AGGREGATE_COURIER.equals(e.aggregateType())
                ? props.kafka().courierEventsTopic()
                : props.kafka().orderEventsTopic();
        String key = e.aggregateId().toString();
        var record = new ProducerRecord<>(topic, key, json.write(envelope(e)));
        record.headers().add("event-type", e.type().getBytes());
        record.headers().add("schema-version", "1".getBytes());
        try {
            kafka.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("Kafka send failed for event " + e.id(), ex);
        }
    }

    /** Wire format: contracts/order-event.v1.schema.json and contracts/courier-event.v1.schema.json. */
    private Map<String, Object> envelope(EventRecord e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("schemaVersion", 1);
        m.put("eventId", e.id());
        m.put("type", e.type());
        m.put("occurredAt", e.createdAt());
        m.put("aggregateType", e.aggregateType());
        m.put("aggregateId", e.aggregateId());
        m.put("orderId", e.orderId());
        m.put("courierId", e.courierId());
        m.put("customerId", e.customerId());
        m.put("payload", e.payload());
        return m;
    }
}
