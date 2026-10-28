package dev.campusdelivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.fasterxml.jackson.databind.JsonNode;
import dev.campusdelivery.support.AbstractIntegrationTest;
import dev.campusdelivery.support.Fixtures;
import dev.campusdelivery.support.Fixtures.Actor;
import dev.campusdelivery.support.WsTestClient;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Hard property #3: batched, out-of-order location updates with a staleness indicator. */
class LocationUpdatesIT extends AbstractIntegrationTest {

    @Test
    void outOfOrderBatch_lastKnownIsNewestByTimestamp_andOlderBatchesNeverMoveItBack() {
        Actor courier = fx.courier();
        fx.goOnline(courier);
        Instant base = Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        Instant t1 = base.plusSeconds(1);
        Instant t2 = base.plusSeconds(2);
        Instant t3 = base.plusSeconds(3);

        // Arrives as [t3, t1, t2]: the newest point must win regardless of array order.
        JsonNode r1 = Fixtures.expect(fx.post(courier.token(), "/api/courier/locations", Map.of("points", List.of(
                Fixtures.point(37.8730, -122.2570, t3),
                Fixtures.point(37.8710, -122.2590, t1),
                Fixtures.point(37.8720, -122.2580, t2)))), 200);
        assertThat(r1.get("accepted").asInt()).isEqualTo(3);
        assertThat(r1.get("advanced").asBoolean()).isTrue();
        assertThat(r1.at("/courier/location/lat").asDouble()).isCloseTo(37.8730, within(1e-6));
        assertThat(Instant.parse(r1.at("/courier/locationAt").asText())).isEqualTo(t3);
        assertThat(r1.at("/courier/stale").asBoolean()).isFalse();

        // A delayed batch with older points: recorded in history, but the marker does not move.
        JsonNode r2 = Fixtures.expect(fx.post(courier.token(), "/api/courier/locations", Map.of("points", List.of(
                Fixtures.point(37.8600, -122.2700, base)))), 200);
        assertThat(r2.get("accepted").asInt()).isEqualTo(1);
        assertThat(r2.get("advanced").asBoolean()).isFalse();
        assertThat(r2.at("/courier/location/lat").asDouble()).isCloseTo(37.8730, within(1e-6));
        assertThat(Instant.parse(r2.at("/courier/locationAt").asText())).isEqualTo(t3);

        long history = jdbc.sql("SELECT count(*) FROM courier_locations WHERE courier_id = :c").param("c", courier.id())
                .query(Long.class).single();
        assertThat(history).isEqualTo(4);
    }

    @Test
    void pointsFromTheFuture_areRejected() {
        Actor courier = fx.courier();
        fx.goOnline(courier);
        JsonNode r = Fixtures.expect(fx.post(courier.token(), "/api/courier/locations", Map.of("points", List.of(
                Fixtures.point(37.87, -122.26, Instant.now().plus(Duration.ofMinutes(10))),
                Fixtures.point(95.0, -122.26, Instant.now())))), 200);
        assertThat(r.get("accepted").asInt()).isEqualTo(0);
        assertThat(r.get("rejected").asInt()).isEqualTo(2);
        assertThat(r.get("advanced").asBoolean()).isFalse();
    }

    @Test
    void stalenessFollowsServerReceiptTime() {
        Actor courier = fx.courier();
        Actor dispatcher = fx.dispatcher();
        fx.goOnline(courier);
        fx.ping(courier, 37.87, -122.26, Instant.now());

        JsonNode fresh = find(Fixtures.expect(fx.get(dispatcher.token(), "/api/dispatch/couriers"), 200), courier);
        assertThat(fresh.get("stale").asBoolean()).isFalse();

        // Pretend we have not heard from the device for two minutes.
        jdbc.sql("UPDATE couriers SET location_received_at = now() - interval '2 minutes' WHERE user_id = :c")
                .param("c", courier.id()).update();
        JsonNode stale = find(Fixtures.expect(fx.get(dispatcher.token(), "/api/dispatch/couriers"), 200), courier);
        assertThat(stale.get("stale").asBoolean()).isTrue();
        assertThat(stale.at("/location/lat").asDouble()).as("last-known position is still shown").isCloseTo(37.87, within(1e-6));

        // Even a ping with an OLD device timestamp proves the device is alive: not stale, position unchanged.
        JsonNode r = fx.ping(courier, 37.80, -122.30, Instant.now().minus(Duration.ofMinutes(30)));
        assertThat(r.get("advanced").asBoolean()).isFalse();
        assertThat(r.at("/courier/stale").asBoolean()).isFalse();
        assertThat(r.at("/courier/location/lat").asDouble()).isCloseTo(37.87, within(1e-6));
    }

    @Test
    void dispatcherReceivesLivePositions_onlyWhenTheMarkerAdvances() {
        Actor courier = fx.courier();
        Actor dispatcher = fx.dispatcher();
        fx.goOnline(courier);

        WsTestClient ws = WsTestClient.connect(wsUrl(dispatcher.token()));
        ws.nextOfType("hello", Duration.ofSeconds(5));
        ws.subscribe(null);
        ws.nextOfType("snapshot", Duration.ofSeconds(5));

        // Far from every merchant other tests create, so the assignment loop never offers this courier anything.
        Instant now = Instant.now();
        fx.ping(courier, -33.8688, 151.2093, now);
        JsonNode live = ws.nextOfType("location", Duration.ofSeconds(5));
        assertThat(live.get("courierId").asText()).isEqualTo(courier.id().toString());
        assertThat(live.get("lat").asDouble()).isCloseTo(-33.8688, within(1e-6));

        // Older ping: nothing should be broadcast (dispatcher still gets unrelated order events, which we ignore).
        fx.ping(courier, -33.9, 151.1, now.minusSeconds(30));
        JsonNode nothing = ws.nextOfTypeOrNull("location", Duration.ofMillis(700));
        assertThat(nothing).as("no location frame for a stale ping").isNull();
        ws.close();
    }

    private static JsonNode find(JsonNode couriers, Actor courier) {
        for (JsonNode c : couriers) {
            if (c.get("id").asText().equals(courier.id().toString())) {
                return c;
            }
        }
        throw new AssertionError("courier not in dispatcher list");
    }
}
