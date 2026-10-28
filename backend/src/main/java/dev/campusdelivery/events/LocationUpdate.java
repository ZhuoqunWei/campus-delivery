package dev.campusdelivery.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Ephemeral live message: a courier's last-known position advanced. Not persisted to the event log
 * (positions are re-sent as a snapshot on reconnect), so it carries no event id.
 */
public record LocationUpdate(
        UUID courierId,
        double lat,
        double lng,
        Instant recordedAt,
        Instant receivedAt,
        UUID orderId,
        UUID customerId) {}
