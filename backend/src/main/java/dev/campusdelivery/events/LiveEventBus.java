package dev.campusdelivery.events;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** In-process fan-out of committed events and live positions to WebSocket sessions. */
@Component
public class LiveEventBus {

    private static final Logger log = LoggerFactory.getLogger(LiveEventBus.class);

    private final List<LiveListener> listeners = new CopyOnWriteArrayList<>();

    public void register(LiveListener listener) {
        listeners.add(listener);
    }

    public void publish(EventRecord event) {
        for (LiveListener l : listeners) {
            try {
                l.onEvent(event);
            } catch (RuntimeException e) {
                log.warn("Live listener failed for event {}", event.id(), e);
            }
        }
    }

    public void publish(LocationUpdate update) {
        for (LiveListener l : listeners) {
            try {
                l.onLocation(update);
            } catch (RuntimeException e) {
                log.warn("Live listener failed for location update", e);
            }
        }
    }
}
