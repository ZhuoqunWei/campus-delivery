package dev.campusdelivery.events;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/** One row of the append-only event log. */
public record EventRecord(
        long id,
        String type,
        String aggregateType,
        UUID aggregateId,
        UUID orderId,
        UUID courierId,
        UUID customerId,
        JsonNode payload,
        Instant createdAt) {

    public static final String AGGREGATE_ORDER = "ORDER";
    public static final String AGGREGATE_COURIER = "COURIER";
}
