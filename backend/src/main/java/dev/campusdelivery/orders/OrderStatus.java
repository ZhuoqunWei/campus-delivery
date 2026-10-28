package dev.campusdelivery.orders;

import java.util.EnumSet;
import java.util.Set;

public enum OrderStatus {
    CREATED, OFFERED, ACCEPTED, PICKED_UP, DELIVERED, CANCELLED;

    public static final Set<OrderStatus> TERMINAL = EnumSet.of(DELIVERED, CANCELLED);
    public static final Set<OrderStatus> UNASSIGNED = EnumSet.of(CREATED, OFFERED);
    public static final Set<OrderStatus> ACTIVE_FOR_COURIER = EnumSet.of(ACCEPTED, PICKED_UP);

    public boolean terminal() {
        return TERMINAL.contains(this);
    }
}
