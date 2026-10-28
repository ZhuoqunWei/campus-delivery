package dev.campusdelivery.orders;

import java.time.Instant;
import java.util.UUID;

/** The pending offer attached to an OFFERED order. */
public record OfferView(UUID id, UUID courierId, String courierName, Instant offeredAt, Instant expiresAt) {}
