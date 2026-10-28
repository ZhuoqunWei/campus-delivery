package dev.campusdelivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import dev.campusdelivery.support.AbstractIntegrationTest;
import dev.campusdelivery.support.Fixtures.Actor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

/** Hard property #2: idempotent order creation. */
class IdempotentOrderCreationIT extends AbstractIntegrationTest {

    @Test
    void retryWithSameKey_returnsSameOrder() {
        Actor customer = fx.customer();
        UUID merchant = fx.merchant(37.8719, -122.2585);
        Map<String, Object> body = fx.orderBody(merchant, 37.87, -122.26);
        String key = UUID.randomUUID().toString();

        ResponseEntity<JsonNode> first = fx.post(customer.token(), "/api/customer/orders", body, Map.of("Idempotency-Key", key));
        ResponseEntity<JsonNode> retry = fx.post(customer.token(), "/api/customer/orders", body, Map.of("Idempotency-Key", key));

        assertThat(first.getStatusCode().value()).isEqualTo(201);
        assertThat(retry.getStatusCode().value()).isEqualTo(200);
        assertThat(retry.getHeaders().getFirst("Idempotency-Replayed")).isEqualTo("true");
        assertThat(retry.getBody().get("id").asText()).isEqualTo(first.getBody().get("id").asText());

        long count = jdbc.sql("SELECT count(*) FROM orders WHERE customer_id = :c").param("c", customer.id())
                .query(Long.class).single();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void sameKeyDifferentBody_isRejected() {
        Actor customer = fx.customer();
        UUID merchant = fx.merchant(37.8719, -122.2585);
        String key = UUID.randomUUID().toString();
        fx.post(customer.token(), "/api/customer/orders", fx.orderBody(merchant, 37.87, -122.26), Map.of("Idempotency-Key", key));

        ResponseEntity<JsonNode> different = fx.post(customer.token(), "/api/customer/orders",
                fx.orderBody(merchant, 37.88, -122.27), Map.of("Idempotency-Key", key));
        assertThat(different.getStatusCode().value()).isEqualTo(422);
        assertThat(different.getBody().get("code").asText()).isEqualTo("idempotency_key_reused");
    }

    @Test
    void missingKey_isABadRequest() {
        Actor customer = fx.customer();
        UUID merchant = fx.merchant(37.8719, -122.2585);
        ResponseEntity<JsonNode> r = fx.post(customer.token(), "/api/customer/orders", fx.orderBody(merchant, 37.87, -122.26));
        assertThat(r.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void keysAreScopedPerCustomer() {
        Actor a = fx.customer();
        Actor b = fx.customer();
        UUID merchant = fx.merchant(37.8719, -122.2585);
        String key = "shared-key-" + UUID.randomUUID();
        JsonNode oa = fx.post(a.token(), "/api/customer/orders", fx.orderBody(merchant, 37.87, -122.26), Map.of("Idempotency-Key", key)).getBody();
        JsonNode ob = fx.post(b.token(), "/api/customer/orders", fx.orderBody(merchant, 37.87, -122.26), Map.of("Idempotency-Key", key)).getBody();
        assertThat(oa.get("id").asText()).isNotEqualTo(ob.get("id").asText());
    }

    @Test
    void tenConcurrentRetries_createExactlyOneOrder() throws Exception {
        Actor customer = fx.customer();
        UUID merchant = fx.merchant(37.8719, -122.2585);
        Map<String, Object> body = fx.orderBody(merchant, 37.87, -122.26);
        String key = UUID.randomUUID().toString();
        int n = 10;

        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<ResponseEntity<JsonNode>>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(pool.submit(() -> {
                go.await(10, TimeUnit.SECONDS);
                return fx.post(customer.token(), "/api/customer/orders", body, Map.of("Idempotency-Key", key));
            }));
        }
        go.countDown();

        Set<String> ids = new HashSet<>();
        int created = 0;
        for (Future<ResponseEntity<JsonNode>> f : futures) {
            ResponseEntity<JsonNode> r = f.get(30, TimeUnit.SECONDS);
            assertThat(r.getStatusCode().value()).as("response: " + r.getBody()).isIn(200, 201);
            if (r.getStatusCode().value() == 201) {
                created++;
            }
            ids.add(r.getBody().get("id").asText());
        }
        pool.shutdownNow();

        assertThat(ids).hasSize(1);
        assertThat(created).isEqualTo(1);
        long count = jdbc.sql("SELECT count(*) FROM orders WHERE customer_id = :c").param("c", customer.id())
                .query(Long.class).single();
        assertThat(count).isEqualTo(1);
        long events = jdbc.sql("SELECT count(*) FROM events WHERE order_id = CAST(:id AS uuid) AND type = 'ORDER_CREATED'")
                .param("id", ids.iterator().next()).query(Long.class).single();
        assertThat(events).as("exactly one ORDER_CREATED event").isEqualTo(1);
    }
}
