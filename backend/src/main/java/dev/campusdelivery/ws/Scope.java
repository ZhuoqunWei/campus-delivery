package dev.campusdelivery.ws;

import dev.campusdelivery.events.EventRecord;
import dev.campusdelivery.events.LocationUpdate;
import dev.campusdelivery.users.UserRole;
import java.util.UUID;

/** What one connection is allowed to see, derived from its JWT. */
public record Scope(UserRole role, UUID userId) {

    public boolean matches(EventRecord e) {
        return switch (role) {
            case DISPATCHER -> true;
            case CUSTOMER -> userId.equals(e.customerId());
            case COURIER -> userId.equals(e.courierId());
        };
    }

    public boolean matches(LocationUpdate u) {
        return switch (role) {
            case DISPATCHER -> true;
            case CUSTOMER -> userId.equals(u.customerId());
            case COURIER -> userId.equals(u.courierId());
        };
    }
}
