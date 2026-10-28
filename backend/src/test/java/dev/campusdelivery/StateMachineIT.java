package dev.campusdelivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import dev.campusdelivery.common.PgErrors;
import dev.campusdelivery.support.AbstractIntegrationTest;
import dev.campusdelivery.support.Fixtures;
import dev.campusdelivery.support.Fixtures.Actor;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;

/** Illegal transitions are rejected by the database trigger, not just by application code. */
class StateMachineIT extends AbstractIntegrationTest {

    @Test
    void databaseRejectsIllegalTransitions() {
        Actor customer = fx.customer();
        UUID merchant = fx.merchant(37.8719, -122.2585);
        UUID orderId = UUID.fromString(fx.createOrder(customer, merchant, 37.87, -122.26).get("id").asText());

        assertIllegal(orderId, "DELIVERED");   // CREATED -> DELIVERED
        assertIllegal(orderId, "PICKED_UP");   // CREATED -> PICKED_UP

        // Legal path, each step bumping version and status_changed_at.
        Actor courier = fx.courier();
        fx.goOnline(courier);
        Fixtures.expect(fx.post(courier.token(), "/api/courier/orders/" + orderId + "/accept", null), 200);
        assertIllegal(orderId, "OFFERED");     // ACCEPTED -> OFFERED
        assertIllegal(orderId, "DELIVERED");   // ACCEPTED -> DELIVERED

        JsonNode picked = Fixtures.expect(fx.post(courier.token(), "/api/courier/orders/" + orderId + "/pickup", null), 200);
        assertThat(picked.get("status").asText()).isEqualTo("PICKED_UP");
        assertIllegal(orderId, "ACCEPTED");    // PICKED_UP -> ACCEPTED
        assertIllegal(orderId, "CREATED");     // PICKED_UP -> CREATED

        JsonNode delivered = Fixtures.expect(fx.post(courier.token(), "/api/courier/orders/" + orderId + "/deliver", null), 200);
        assertThat(delivered.get("status").asText()).isEqualTo("DELIVERED");
        assertThat(delivered.get("version").asLong()).isEqualTo(3);
        assertIllegal(orderId, "CANCELLED");   // terminal
        assertIllegal(orderId, "CREATED");
    }

    @Test
    void apiRefusesOutOfOrderCourierActions() {
        Actor customer = fx.customer();
        Actor courier = fx.courier();
        fx.goOnline(courier);
        UUID merchant = fx.merchant(37.8719, -122.2585);
        UUID orderId = UUID.fromString(fx.createOrder(customer, merchant, 37.87, -122.26).get("id").asText());

        ResponseEntity<JsonNode> notMine = fx.post(courier.token(), "/api/courier/orders/" + orderId + "/pickup", null);
        assertThat(notMine.getStatusCode().value()).isEqualTo(409);

        Fixtures.expect(fx.post(courier.token(), "/api/courier/orders/" + orderId + "/accept", null), 200);
        ResponseEntity<JsonNode> deliverBeforePickup = fx.post(courier.token(), "/api/courier/orders/" + orderId + "/deliver", null);
        assertThat(deliverBeforePickup.getStatusCode().value()).isEqualTo(409);
        assertThat(deliverBeforePickup.getBody().get("code").asText()).isEqualTo("illegal_transition");
    }

    @Test
    void customerCannotCancelAfterPickup_butDispatcherCan() {
        Actor customer = fx.customer();
        Actor courier = fx.courier();
        Actor dispatcher = fx.dispatcher();
        fx.goOnline(courier);
        UUID merchant = fx.merchant(37.8719, -122.2585);
        UUID orderId = UUID.fromString(fx.createOrder(customer, merchant, 37.87, -122.26).get("id").asText());
        Fixtures.expect(fx.post(courier.token(), "/api/courier/orders/" + orderId + "/accept", null), 200);
        Fixtures.expect(fx.post(courier.token(), "/api/courier/orders/" + orderId + "/pickup", null), 200);

        assertThat(fx.post(customer.token(), "/api/customer/orders/" + orderId + "/cancel", null).getStatusCode().value()).isEqualTo(409);

        JsonNode current = Fixtures.expect(fx.get(dispatcher.token(), "/api/dispatch/orders/" + orderId), 200);
        ResponseEntity<JsonNode> staleVersion = fx.post(dispatcher.token(), "/api/dispatch/orders/" + orderId + "/cancel",
                java.util.Map.of("reason", "test", "expectedVersion", current.get("version").asLong() - 1));
        assertThat(staleVersion.getStatusCode().value()).isEqualTo(409);
        assertThat(staleVersion.getBody().get("code").asText()).isEqualTo("version_mismatch");

        JsonNode cancelled = Fixtures.expect(fx.post(dispatcher.token(), "/api/dispatch/orders/" + orderId + "/cancel",
                java.util.Map.of("reason", "customer unreachable", "expectedVersion", current.get("version").asLong())), 200);
        assertThat(cancelled.get("status").asText()).isEqualTo("CANCELLED");
        assertThat(cancelled.get("cancelReason").asText()).isEqualTo("customer unreachable");
        // courier is free again
        Fixtures.expect(fx.post(courier.token(), "/api/courier/status", java.util.Map.of("status", "OFFLINE")), 200);
    }

    private void assertIllegal(UUID orderId, String target) {
        assertThatThrownBy(() -> jdbc.sql("UPDATE orders SET status = CAST(:s AS order_status) WHERE id = :id")
                .param("s", target).param("id", orderId).update())
                .isInstanceOf(DataAccessException.class)
                .satisfies(ex -> assertThat(PgErrors.sqlState(ex)).as("sqlstate for -> " + target).isEqualTo(PgErrors.ILLEGAL_TRANSITION));
    }
}
