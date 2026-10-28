package dev.campusdelivery.events;

public interface LiveListener {

    void onEvent(EventRecord event);

    void onLocation(LocationUpdate update);
}
