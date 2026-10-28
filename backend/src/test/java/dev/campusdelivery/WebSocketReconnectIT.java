package dev.campusdelivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import dev.campusdelivery.support.AbstractIntegrationTest;
import dev.campusdelivery.support.Fixtures;
import dev.campusdelivery.support.Fixtures.Actor;
import dev.campusdelivery.support.WsTestClient;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Hard property #4: reconnect resumes from the last event id and replays what was missed. */
class WebSocketReconnectIT extends AbstractIntegrationTest {

    @Test
    void reconnectWithLastEventId_replaysMissedEventsInOrder_thenGoesLive() {
        // Customer scope: only this customer's orders show up, and the merchant is far from every
        // courier position other tests create, so the assignment loop never adds offer events.
        Actor customer = fx.customer();
        Actor courier = fx.courier();
        fx.goOnline(courier);
        UUID merchant = fx.merchant(35.6762, 139.6503);

        WsTestClient ws = WsTestClient.connect(wsUrl(customer.token()));
        assertThat(ws.nextOfType("hello", Duration.ofSeconds(5)).get("role").asText()).isEqualTo("CUSTOMER");
        ws.subscribe(null);
        JsonNode snapshot = ws.nextOfType("snapshot", Duration.ofSeconds(5));
        assertThat(snapshot.get("orders")).isEmpty();

        UUID orderA = UUID.fromString(fx.createOrder(customer, merchant, 35.68, 139.65).get("id").asText());
        JsonNode created = ws.nextOfType("event", Duration.ofSeconds(5));
        assertThat(created.get("eventType").asText()).isEqualTo("ORDER_CREATED");
        assertThat(created.get("orderId").asText()).isEqualTo(orderA.toString());
        long lastSeen = created.get("id").asLong();

        // Network drops.
        ws.abort();

        // Things keep happening while we are away.
        UUID orderB = UUID.fromString(fx.createOrder(customer, merchant, 35.68, 139.65).get("id").asText());
        UUID orderC = UUID.fromString(fx.createOrder(customer, merchant, 35.68, 139.65).get("id").asText());
        Fixtures.expect(fx.post(courier.token(), "/api/courier/orders/" + orderB + "/accept", null), 200);

        // Reconnect and resume from the last id we saw.
        WsTestClient ws2 = WsTestClient.connect(wsUrl(customer.token()));
        ws2.nextOfType("hello", Duration.ofSeconds(5));
        ws2.subscribe(lastSeen);
        List<JsonNode> frames = ws2.collectUntil("resumed", Duration.ofSeconds(5));
        List<JsonNode> events = frames.subList(0, frames.size() - 1);
        JsonNode resumed = frames.get(frames.size() - 1);

        assertThat(events).extracting(e -> e.get("type").asText()).containsOnly("event");
        assertThat(events).extracting(e -> e.get("eventType").asText())
                .containsExactly("ORDER_CREATED", "ORDER_CREATED", "ORDER_ACCEPTED");
        assertThat(events).extracting(e -> e.get("orderId").asText())
                .containsExactly(orderB.toString(), orderC.toString(), orderB.toString());
        List<Long> ids = events.stream().map(e -> e.get("id").asLong()).toList();
        assertThat(ids).isSorted();
        assertThat(ids.get(0)).isGreaterThan(lastSeen);
        assertThat(resumed.get("replayed").asInt()).isEqualTo(3);
        assertThat(resumed.get("lastEventId").asLong()).isGreaterThanOrEqualTo(ids.get(2));

        // And we are live again.
        UUID orderD = UUID.fromString(fx.createOrder(customer, merchant, 35.68, 139.65).get("id").asText());
        JsonNode live = ws2.nextOfType("event", Duration.ofSeconds(5));
        assertThat(live.get("orderId").asText()).isEqualTo(orderD.toString());
        assertThat(live.get("id").asLong()).isGreaterThan(resumed.get("lastEventId").asLong());
        assertThat(live.at("/payload/order/status").asText()).isEqualTo("CREATED");
        ws2.close();
    }

    @Test
    void gapLargerThanReplayLimit_fallsBackToSnapshot() {
        Actor dispatcher = fx.dispatcher();
        Actor customer = fx.customer();
        UUID merchant = fx.merchant(37.8719, -122.2585);

        WsTestClient ws = WsTestClient.connect(wsUrl(dispatcher.token()));
        ws.nextOfType("hello", Duration.ofSeconds(5));
        ws.subscribe(null);
        long lastSeen = ws.nextOfType("snapshot", Duration.ofSeconds(5)).get("lastEventId").asLong();
        ws.abort();

        for (int i = 0; i < 25; i++) { // replay-limit is 20 in tests
            fx.createOrder(customer, merchant, 37.87, -122.26);
        }

        WsTestClient ws2 = WsTestClient.connect(wsUrl(dispatcher.token()));
        ws2.nextOfType("hello", Duration.ofSeconds(5));
        ws2.subscribe(lastSeen);
        JsonNode first = ws2.next(Duration.ofSeconds(5));
        assertThat(first.get("type").asText()).isEqualTo("snapshot");
        assertThat(first.get("lastEventId").asLong()).isGreaterThanOrEqualTo(lastSeen + 25);
        ws2.close();
    }

    @Test
    void customersOnlySeeTheirOwnOrders() {
        Actor alice = fx.customer();
        Actor bob = fx.customer();
        UUID merchant = fx.merchant(37.8719, -122.2585);

        WsTestClient ws = WsTestClient.connect(wsUrl(alice.token()));
        ws.nextOfType("hello", Duration.ofSeconds(5));
        ws.subscribe(null);
        ws.nextOfType("snapshot", Duration.ofSeconds(5));

        fx.createOrder(bob, merchant, 37.87, -122.26);
        UUID mine = UUID.fromString(fx.createOrder(alice, merchant, 37.87, -122.26).get("id").asText());

        JsonNode ev = ws.nextOfType("event", Duration.ofSeconds(5));
        assertThat(ev.get("orderId").asText()).isEqualTo(mine.toString());
        assertThat(ws.next(Duration.ofMillis(500))).as("nothing about Bob's order").isNull();
        ws.close();
    }

    @Test
    void handshakeWithoutValidToken_isRejected() {
        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> WsTestClient.connect("ws://localhost:" + port + "/ws?token=not-a-jwt"));
    }
}
