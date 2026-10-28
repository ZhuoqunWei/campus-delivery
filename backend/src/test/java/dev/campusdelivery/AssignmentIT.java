package dev.campusdelivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import dev.campusdelivery.support.AbstractIntegrationTest;
import dev.campusdelivery.support.Fixtures;
import dev.campusdelivery.support.Fixtures.Actor;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Nearest online courier gets the offer; offers time out and re-offer to the next candidate. */
class AssignmentIT extends AbstractIntegrationTest {

    // Merchant far from the other tests' data so their couriers cannot interfere.
    private static final double LAT = 40.4406;
    private static final double LNG = -79.9959;

    @Test
    void nearestCourierIsOffered_expiryReoffersToNext_declineAlsoReoffers() {
        Actor customer = fx.customer();
        Actor dispatcher = fx.dispatcher();
        UUID merchant = fx.merchant(LAT, LNG);

        Actor near = fx.courier();     // ~220 m
        Actor far = fx.courier();      // ~1.1 km
        Actor outside = fx.courier();  // ~11 km, outside the 3 km radius
        Actor offline = fx.courier();  // right next door but OFFLINE
        for (Actor c : new Actor[] {near, far, outside}) {
            fx.goOnline(c);
        }
        Instant now = Instant.now();
        fx.ping(near, LAT + 0.002, LNG, now);
        fx.ping(far, LAT + 0.010, LNG, now);
        fx.ping(outside, LAT + 0.100, LNG, now);
        fx.ping(offline, LAT + 0.0005, LNG, now);

        UUID orderId = UUID.fromString(fx.createOrder(customer, merchant, LAT, LNG).get("id").asText());

        // 1. nearest gets the offer
        JsonNode offered = awaitOrder(dispatcher, orderId, o -> o.get("status").asText().equals("OFFERED"));
        assertThat(offered.at("/offer/courierId").asText()).isEqualTo(near.id().toString());
        assertThat(fx.get(near.token(), "/api/courier/orders").getBody()).hasSize(1);

        // 2. near ignores it; after the TTL (2s in tests) it goes to the next nearest
        JsonNode reoffered = awaitOrder(dispatcher, orderId,
                o -> o.get("status").asText().equals("OFFERED") && o.at("/offer/courierId").asText().equals(far.id().toString()));
        assertThat(reoffered.at("/offer/courierId").asText()).isEqualTo(far.id().toString());
        long expired = jdbc.sql("SELECT count(*) FROM order_offers WHERE order_id = :o AND status = 'EXPIRED'")
                .param("o", orderId).query(Long.class).single();
        assertThat(expired).isEqualTo(1);

        // 3. far accepts and delivers
        Fixtures.expect(fx.post(far.token(), "/api/courier/orders/" + orderId + "/accept", null), 200);
        Fixtures.expect(fx.post(far.token(), "/api/courier/orders/" + orderId + "/pickup", null), 200);
        Fixtures.expect(fx.post(far.token(), "/api/courier/orders/" + orderId + "/deliver", null), 200);

        // 4. new order: near is offered (cooldown is per order), declines, far gets it next
        UUID second = UUID.fromString(fx.createOrder(customer, merchant, LAT, LNG).get("id").asText());
        JsonNode offered2 = awaitOrder(dispatcher, second, o -> o.at("/offer/courierId").asText().equals(near.id().toString()));
        UUID offerId = UUID.fromString(offered2.at("/offer/id").asText());
        JsonNode declined = Fixtures.expect(fx.post(near.token(), "/api/courier/offers/" + offerId + "/decline", null), 200);
        assertThat(declined.get("status").asText()).isEqualTo("CREATED");
        awaitOrder(dispatcher, second, o -> o.at("/offer/courierId").asText().equals(far.id().toString()));

        // The out-of-radius and offline couriers never saw anything.
        long outsideOffers = jdbc.sql("SELECT count(*) FROM order_offers WHERE courier_id IN (:a, :b)")
                .param("a", outside.id()).param("b", offline.id()).query(Long.class).single();
        assertThat(outsideOffers).isZero();
    }

    @Test
    void dispatcherCanReassign_andStuckOrdersAreVisible() {
        Actor customer = fx.customer();
        Actor dispatcher = fx.dispatcher();
        Actor a = fx.courier();
        Actor b = fx.courier();
        fx.goOnline(a);
        fx.goOnline(b);
        UUID merchant = fx.merchant(51.5074, -0.1278); // nobody near here -> stays CREATED

        UUID orderId = UUID.fromString(fx.createOrder(customer, merchant, 51.5, -0.12).get("id").asText());
        Fixtures.expect(fx.post(a.token(), "/api/courier/orders/" + orderId + "/accept", null), 200);

        JsonNode reassigned = Fixtures.expect(fx.post(dispatcher.token(), "/api/dispatch/orders/" + orderId + "/reassign",
                Map.of("courierId", b.id().toString())), 200);
        assertThat(reassigned.get("status").asText()).isEqualTo("ACCEPTED");
        assertThat(reassigned.get("courierId").asText()).isEqualTo(b.id().toString());
        // a is free again and can take something else
        assertThat(fx.get(a.token(), "/api/courier/orders").getBody()).isEmpty();

        // Unassign back to the pool; nobody is nearby so it stays CREATED and eventually counts as stuck.
        JsonNode pooled = Fixtures.expect(fx.post(dispatcher.token(), "/api/dispatch/orders/" + orderId + "/reassign", null), 200);
        assertThat(pooled.get("status").asText()).isEqualTo("CREATED");
        jdbc.sql("UPDATE orders SET status_changed_at = now() - interval '10 minutes' WHERE id = :id").param("id", orderId).update();
        JsonNode stuck = Fixtures.expect(fx.get(dispatcher.token(), "/api/dispatch/orders/stuck?olderThanSeconds=300"), 200);
        assertThat(stuck).extracting(o -> o.get("id").asText()).contains(orderId.toString());
    }

    private JsonNode awaitOrder(Actor dispatcher, UUID orderId, java.util.function.Predicate<JsonNode> condition) {
        return await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(150))
                .until(() -> Fixtures.expect(fx.get(dispatcher.token(), "/api/dispatch/orders/" + orderId), 200), condition::test);
    }
}
