package dev.campusdelivery.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public record GeoPoint(
        @Min(-90) @Max(90) double lat,
        @Min(-180) @Max(180) double lng) {

    @JsonIgnore
    public boolean isValid() {
        return lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180
                && !Double.isNaN(lat) && !Double.isNaN(lng);
    }
}
