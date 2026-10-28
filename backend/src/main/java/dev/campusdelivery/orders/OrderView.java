package dev.campusdelivery.orders;

import com.fasterxml.jackson.databind.JsonNode;
import dev.campusdelivery.common.GeoPoint;
import dev.campusdelivery.couriers.CourierView;
import dev.campusdelivery.merchants.MerchantView;
import java.time.Instant;
import java.util.UUID;

/** Read model returned by every order endpoint and embedded in every ORDER_* event. */
public record OrderView(
        UUID id,
        OrderStatus status,
        long version,
        UUID customerId,
        String customerName,
        MerchantView merchant,
        GeoPoint dropoff,
        String note,
        JsonNode items,
        UUID courierId,
        CourierView courier,
        OfferView offer,
        Instant createdAt,
        Instant updatedAt,
        Instant statusChangedAt,
        Instant acceptedAt,
        Instant pickedUpAt,
        Instant deliveredAt,
        Instant cancelledAt,
        String cancelReason) {}
