package dev.campusdelivery.couriers;

import dev.campusdelivery.common.GeoPoint;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Last-known position plus a staleness indicator. `lastSeenAt` is server receipt time (immune to
 * device clock skew); `locationAt` is the device timestamp of the position itself.
 */
public record CourierView(
        UUID id,
        String name,
        CourierStatus status,
        GeoPoint location,
        Instant locationAt,
        Instant lastSeenAt,
        boolean stale,
        UUID activeOrderId) {

    public static boolean isStale(Instant lastSeenAt, Instant now, Duration staleAfter) {
        return lastSeenAt == null || lastSeenAt.plus(staleAfter).isBefore(now);
    }
}
