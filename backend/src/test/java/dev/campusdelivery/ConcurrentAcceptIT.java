package dev.campusdelivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import dev.campusdelivery.common.PgErrors;
import dev.campusdelivery.support.AbstractIntegrationTest;
import dev.campusdelivery.support.Fixtures;
import dev.campusdelivery.support.Fixtures.Actor;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;

/** Hard property #1: exactly one courier per order. */
class ConcurrentAcceptIT extends AbstractIntegrationTest {

    private static final int COURIERS = 20;

    @Test
    void twentyConcurrentAccepts_exactlyOneWins() throws Exception {
        Actor customer = fx.customer();
        UUID merchant = fx.merchant(37.8719, -122.2585);
        List<Actor> couriers = new ArrayList<>();
        for (int i = 0; i < COURIERS; i++) {
            Actor c = fx.courier();
            fx.goOnline(c);
            couriers.add(c);
        }
        UUID orderId = UUID.fromString(fx.createOrder(customer, merchant, 37.8700, -122.2600).get("id").asText());

        ExecutorService pool = Executors.newFixedThreadPool(COURIERS);
        CountDownLatch ready = new CountDownLatch(COURIERS);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<ResponseEntity<JsonNode>>> futures = new ArrayList<>();
        for (Actor c : couriers) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await(10, TimeUnit.SECONDS);
                return fx.post(c.token(), "/api/courier/orders/" + orderId + "/accept", null);
            }));
        }
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        go.countDown();

        int wins = 0;
        int conflicts = 0;
        UUID winner = null;
        for (int i = 0; i < futures.size(); i++) {
            ResponseEntity<JsonNode> r = futures.get(i).get(30, TimeUnit.SECONDS);
            if (r.getStatusCode().value() == 200) {
                wins++;
                winner = couriers.get(i).id();
                assertThat(r.getBody().get("status").asText()).isEqualTo("ACCEPTED");
                assertThat(r.getBody().get("courierId").asText()).isEqualTo(winner.toString());
            } else {
                conflicts++;
                assertThat(r.getStatusCode().value()).as("loser response: " + r.getBody()).isEqualTo(409);
                assertThat(r.getBody().get("code").asText()).isEqualTo("order_unavailable");
            }
        }
        pool.shutdownNow();

        assertThat(wins).isEqualTo(1);
        assertThat(conflicts).isEqualTo(COURIERS - 1);

        // Database agrees: one courier, status ACCEPTED, version bumped exactly once past creation.
        JsonNode row = jdbc.sql("SELECT status::text AS status, courier_id, version FROM orders WHERE id = :id")
                .param("id", orderId)
                .query((rs, i) -> fx.mapper().createObjectNode()
                        .put("status", rs.getString("status"))
                        .put("courierId", rs.getString("courier_id"))
                        .put("version", rs.getLong("version")))
                .single();
        assertThat(row.get("status").asText()).isEqualTo("ACCEPTED");
        assertThat(row.get("courierId").asText()).isEqualTo(winner.toString());
        assertThat(row.get("version").asLong()).isEqualTo(1);
    }

    @Test
    void courierWithActiveOrder_cannotAcceptASecondOne() {
        Actor customer = fx.customer();
        Actor courier = fx.courier();
        fx.goOnline(courier);
        UUID merchant = fx.merchant(37.8719, -122.2585);
        UUID first = UUID.fromString(fx.createOrder(customer, merchant, 37.87, -122.26).get("id").asText());
        UUID second = UUID.fromString(fx.createOrder(customer, merchant, 37.87, -122.26).get("id").asText());

        Fixtures.expect(fx.post(courier.token(), "/api/courier/orders/" + first + "/accept", null), 200);
        ResponseEntity<JsonNode> busy = fx.post(courier.token(), "/api/courier/orders/" + second + "/accept", null);
        assertThat(busy.getStatusCode().value()).isEqualTo(409);
        assertThat(busy.getBody().get("code").asText()).isEqualTo("courier_busy");

        // And even if application code were bypassed, the partial unique index refuses a second active row.
        assertThatThrownBy(() -> jdbc.sql("UPDATE orders SET status = 'ACCEPTED', courier_id = :c WHERE id = :id")
                .param("c", courier.id()).param("id", second).update())
                .isInstanceOf(DataAccessException.class)
                .satisfies(ex -> assertThat(PgErrors.sqlState(ex)).isEqualTo(PgErrors.UNIQUE_VIOLATION));
    }

    @Test
    void offeredOrder_isExclusiveToTheOfferedCourier() {
        Actor customer = fx.customer();
        Actor offered = fx.courier();
        Actor other = fx.courier();
        fx.goOnline(offered);
        fx.goOnline(other);
        UUID merchant = fx.merchant(37.8719, -122.2585);
        UUID orderId = UUID.fromString(fx.createOrder(customer, merchant, 37.87, -122.26).get("id").asText());
        // Simulate the assignment loop having offered it to `offered`.
        jdbc.sql("INSERT INTO order_offers (order_id, courier_id, expires_at) VALUES (:o, :c, now() + interval '1 minute')")
                .param("o", orderId).param("c", offered.id()).update();
        jdbc.sql("UPDATE orders SET status = 'OFFERED' WHERE id = :id").param("id", orderId).update();

        ResponseEntity<JsonNode> denied = fx.post(other.token(), "/api/courier/orders/" + orderId + "/accept", null);
        assertThat(denied.getStatusCode().value()).isEqualTo(409);
        assertThat(denied.getBody().get("code").asText()).isEqualTo("offered_to_other_courier");

        JsonNode accepted = Fixtures.expect(fx.post(offered.token(), "/api/courier/orders/" + orderId + "/accept", null), 200);
        assertThat(accepted.get("status").asText()).isEqualTo("ACCEPTED");
        assertThat(accepted.get("courierId").asText()).isEqualTo(offered.id().toString());
    }
}
