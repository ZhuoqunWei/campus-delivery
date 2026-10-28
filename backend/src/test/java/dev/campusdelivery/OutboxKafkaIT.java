package dev.campusdelivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import dev.campusdelivery.support.AbstractIntegrationTest;
import dev.campusdelivery.support.Fixtures;
import dev.campusdelivery.support.Fixtures.Actor;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

/** Order events reach Kafka through the outbox, keyed by order id, with the versioned envelope. */
class OutboxKafkaIT extends AbstractIntegrationTest {

    @Test
    void orderLifecycleIsPublishedInOrder_andLocationPingsAreStreamed() throws Exception {
        Actor customer = fx.customer();
        Actor courier = fx.courier();
        fx.goOnline(courier);
        UUID merchant = fx.merchant(37.8719, -122.2585);

        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of("order-events.v1", "courier-locations.v1"));
            consumer.poll(Duration.ofSeconds(2)); // join the group before producing

            UUID orderId = UUID.fromString(fx.createOrder(customer, merchant, 37.87, -122.26).get("id").asText());
            Fixtures.expect(fx.post(courier.token(), "/api/courier/orders/" + orderId + "/accept", null), 200);
            fx.ping(courier, 37.871, -122.259, Instant.now());

            List<ConsumerRecord<String, String>> orderRecords = new ArrayList<>();
            List<ConsumerRecord<String, String>> pingRecords = new ArrayList<>();
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (r.topic().equals("order-events.v1") && orderId.toString().equals(r.key())) {
                        orderRecords.add(r);
                    } else if (r.topic().equals("courier-locations.v1") && courier.id().toString().equals(r.key())) {
                        pingRecords.add(r);
                    }
                }
                assertThat(orderRecords).hasSizeGreaterThanOrEqualTo(2);
                assertThat(pingRecords).isNotEmpty();
            });

            List<String> types = new ArrayList<>();
            for (ConsumerRecord<String, String> r : orderRecords) {
                JsonNode env = fx.mapper().readTree(r.value());
                assertThat(env.get("schemaVersion").asInt()).isEqualTo(1);
                assertThat(env.get("orderId").asText()).isEqualTo(orderId.toString());
                assertThat(env.at("/payload/order/id").asText()).isEqualTo(orderId.toString());
                assertThat(new String(r.headers().lastHeader("event-type").value())).isEqualTo(env.get("type").asText());
                types.add(env.get("type").asText());
            }
            assertThat(types).startsWith("ORDER_CREATED", "ORDER_ACCEPTED");

            JsonNode ping = fx.mapper().readTree(pingRecords.get(0).value());
            assertThat(ping.get("schemaVersion").asInt()).isEqualTo(1);
            assertThat(ping.get("orderId").asText()).isEqualTo(orderId.toString());
            assertThat(ping.get("lat").asDouble()).isEqualTo(37.871);

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                long unpublished = jdbc.sql("SELECT count(*) FROM events WHERE order_id = :o AND published_at IS NULL")
                        .param("o", orderId).query(Long.class).single();
                assertThat(unpublished).isZero();
            });
        }
    }

    private KafkaConsumer<String, String> consumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(p);
    }
}
