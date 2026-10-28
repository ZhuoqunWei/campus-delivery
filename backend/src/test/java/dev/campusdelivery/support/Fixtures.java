package dev.campusdelivery.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.campusdelivery.merchants.MerchantRepository;
import dev.campusdelivery.common.GeoPoint;
import dev.campusdelivery.users.UserRepository;
import dev.campusdelivery.users.UserRole;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;

/** Creates users/merchants directly and talks to the API like a client would. */
public class Fixtures {

    public static final String PASSWORD = "pw-test";

    public record Actor(UUID id, String email, UserRole role, String token) {}

    private static final AtomicInteger SEQ = new AtomicInteger();

    private final UserRepository users;
    private final MerchantRepository merchants;
    private final TestRestTemplate rest;
    private final ObjectMapper mapper;
    private final String passwordHash;

    public Fixtures(UserRepository users, MerchantRepository merchants, PasswordEncoder encoder,
                    TestRestTemplate rest, ObjectMapper mapper) {
        this.users = users;
        this.merchants = merchants;
        this.rest = rest;
        this.mapper = mapper;
        this.passwordHash = encoder.encode(PASSWORD);
    }

    // ---- actors ----

    public Actor customer() {
        return actor(UserRole.CUSTOMER);
    }

    public Actor courier() {
        return actor(UserRole.COURIER);
    }

    public Actor dispatcher() {
        return actor(UserRole.DISPATCHER);
    }

    public Actor actor(UserRole role) {
        String email = role.name().toLowerCase() + "-" + UUID.randomUUID().toString().substring(0, 8) + "@test.local";
        UUID id = users.upsert(email, passwordHash, role.name().charAt(0) + role.name().substring(1).toLowerCase()
                + " #" + SEQ.incrementAndGet(), role);
        ResponseEntity<JsonNode> login = post(null, "/api/auth/login",
                Map.of("email", email, "password", PASSWORD));
        if (!login.getStatusCode().is2xxSuccessful()) {
            throw new IllegalStateException("login failed: " + login);
        }
        return new Actor(id, email, role, login.getBody().get("token").asText());
    }

    public UUID merchant(double lat, double lng) {
        return merchants.upsert("Merchant " + UUID.randomUUID().toString().substring(0, 8), new GeoPoint(lat, lng));
    }

    // ---- courier helpers ----

    public JsonNode goOnline(Actor courier) {
        return expect(post(courier.token(), "/api/courier/status", Map.of("status", "ONLINE")), 200);
    }

    public JsonNode ping(Actor courier, double lat, double lng, Instant at) {
        return expect(post(courier.token(), "/api/courier/locations",
                Map.of("points", List.of(point(lat, lng, at)))), 200);
    }

    public static Map<String, Object> point(double lat, double lng, Instant at) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("lat", lat);
        m.put("lng", lng);
        m.put("recordedAt", at.toString());
        return m;
    }

    // ---- orders ----

    public Map<String, Object> orderBody(UUID merchantId, double lat, double lng) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("merchantId", merchantId.toString());
        m.put("dropoff", Map.of("lat", lat, "lng", lng));
        m.put("note", "Leave at the door");
        m.put("items", List.of(Map.of("name", "Burrito", "quantity", 1)));
        return m;
    }

    public JsonNode createOrder(Actor customer, UUID merchantId, double lat, double lng) {
        return expect(post(customer.token(), "/api/customer/orders", orderBody(merchantId, lat, lng),
                Map.of("Idempotency-Key", UUID.randomUUID().toString())), 201);
    }

    // ---- HTTP ----

    public ResponseEntity<JsonNode> post(String token, String path, Object body) {
        return post(token, path, body, Map.of());
    }

    public ResponseEntity<JsonNode> post(String token, String path, Object body, Map<String, String> extraHeaders) {
        HttpHeaders headers = headers(token);
        extraHeaders.forEach(headers::set);
        return rest.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), JsonNode.class);
    }

    public ResponseEntity<JsonNode> get(String token, String path) {
        return rest.exchange(path, HttpMethod.GET, new HttpEntity<>(headers(token)), JsonNode.class);
    }

    public static JsonNode expect(ResponseEntity<JsonNode> response, int status) {
        if (response.getStatusCode().value() != status) {
            throw new AssertionError("Expected HTTP " + status + " but got " + response.getStatusCode().value()
                    + ": " + response.getBody());
        }
        return response.getBody();
    }

    private static HttpHeaders headers(String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (token != null) {
            headers.setBearerAuth(token);
        }
        return headers;
    }

    public ObjectMapper mapper() {
        return mapper;
    }
}
