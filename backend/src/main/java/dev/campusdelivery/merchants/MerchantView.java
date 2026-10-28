package dev.campusdelivery.merchants;

import dev.campusdelivery.common.GeoPoint;
import java.util.UUID;

public record MerchantView(UUID id, String name, GeoPoint location) {}
