package dev.campusdelivery.ws;

import dev.campusdelivery.config.AppProperties;
import dev.campusdelivery.couriers.CourierRepository;
import dev.campusdelivery.couriers.CourierView;
import dev.campusdelivery.events.EventRecord;
import dev.campusdelivery.events.EventRepository;
import dev.campusdelivery.orders.OrderRepository;
import dev.campusdelivery.orders.OrderView;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Builds what a client receives right after `subscribe`: either a replay of the events it missed
 * (small gap) or a full snapshot (first connect, or gap too large to replay).
 */
@Service
public class LiveFeedService {

    public record Sync(List<Map<String, Object>> messages, long lastEventId) {}

    private final EventRepository events;
    private final OrderRepository orders;
    private final CourierRepository couriers;
    private final AppProperties props;
    private final Clock clock;

    public LiveFeedService(EventRepository events, OrderRepository orders, CourierRepository couriers,
                           AppProperties props, Clock clock) {
        this.events = events;
        this.orders = orders;
        this.couriers = couriers;
        this.props = props;
        this.clock = clock;
    }

    @Transactional(readOnly = true)
    public Sync resume(Scope scope, Long lastEventId) {
        // Read the high-water mark BEFORE the snapshot/replay queries. Anything committed after this
        // point is re-sent live (and deduplicated client-side by id / order version).
        long maxId = events.maxId();
        int limit = props.ws().replayLimit();

        if (lastEventId == null || lastEventId > maxId || maxId - lastEventId > limit) {
            return new Sync(List.of(snapshot(scope, maxId)), maxId);
        }
        List<EventRecord> missed = switch (scope.role()) {
            case DISPATCHER -> events.findAfter(lastEventId, limit + 1);
            case CUSTOMER -> events.findAfterForCustomer(lastEventId, scope.userId(), limit + 1);
            case COURIER -> events.findAfterForCourier(lastEventId, scope.userId(), limit + 1);
        };
        if (missed.size() > limit) {
            return new Sync(List.of(snapshot(scope, maxId)), maxId);
        }
        List<Map<String, Object>> out = new ArrayList<>(missed.size() + 1);
        for (EventRecord e : missed) {
            out.add(LiveFeedHandler.eventMessage(e));
        }
        Map<String, Object> resumed = new LinkedHashMap<>();
        resumed.put("type", "resumed");
        resumed.put("lastEventId", maxId);
        resumed.put("replayed", missed.size());
        resumed.put("couriers", couriersFor(scope, ordersFor(scope)));
        out.add(resumed);
        return new Sync(out, maxId);
    }

    private Map<String, Object> snapshot(Scope scope, long maxId) {
        List<OrderView> orderViews = ordersFor(scope);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "snapshot");
        m.put("lastEventId", maxId);
        m.put("orders", orderViews);
        m.put("couriers", couriersFor(scope, orderViews));
        return m;
    }

    private List<OrderView> ordersFor(Scope scope) {
        return switch (scope.role()) {
            case DISPATCHER -> {
                List<OrderView> all = new ArrayList<>(orders.activeViews());
                all.addAll(orders.recentTerminalViews(50));
                yield all;
            }
            case CUSTOMER -> orders.viewsForCustomer(scope.userId(), 100);
            case COURIER -> orders.activeViewsForCourier(scope.userId());
        };
    }

    private List<CourierView> couriersFor(Scope scope, List<OrderView> orderViews) {
        return switch (scope.role()) {
            case DISPATCHER -> couriers.viewsVisible(clock.instant().minus(Duration.ofMinutes(10)));
            case CUSTOMER -> {
                Set<UUID> ids = new LinkedHashSet<>();
                for (OrderView o : orderViews) {
                    if (o.courierId() != null && !o.status().terminal()) {
                        ids.add(o.courierId());
                    }
                }
                yield couriers.viewsByIds(ids);
            }
            case COURIER -> couriers.view(scope.userId()).map(List::of).orElse(List.of());
        };
    }
}
