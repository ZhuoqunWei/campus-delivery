package dev.campusdelivery.events;

public final class EventTypes {

    private EventTypes() {}

    public static final String ORDER_CREATED = "ORDER_CREATED";
    public static final String ORDER_OFFERED = "ORDER_OFFERED";
    public static final String OFFER_EXPIRED = "OFFER_EXPIRED";
    public static final String OFFER_DECLINED = "OFFER_DECLINED";
    public static final String OFFER_WITHDRAWN = "OFFER_WITHDRAWN";
    public static final String ORDER_ACCEPTED = "ORDER_ACCEPTED";
    public static final String ORDER_PICKED_UP = "ORDER_PICKED_UP";
    public static final String ORDER_DELIVERED = "ORDER_DELIVERED";
    public static final String ORDER_CANCELLED = "ORDER_CANCELLED";
    public static final String ORDER_UNASSIGNED = "ORDER_UNASSIGNED";
    public static final String COURIER_STATUS_CHANGED = "COURIER_STATUS_CHANGED";
}
