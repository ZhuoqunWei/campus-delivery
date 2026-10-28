package dev.campusdelivery.orders;

import dev.campusdelivery.config.AppProperties;
import dev.campusdelivery.couriers.CourierRepository;
import dev.campusdelivery.couriers.CourierView;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/dispatch")
public class DispatchController {

    private final OrderService service;
    private final OrderRepository orders;
    private final CourierRepository couriers;
    private final AppProperties props;

    public DispatchController(OrderService service, OrderRepository orders, CourierRepository couriers, AppProperties props) {
        this.service = service;
        this.orders = orders;
        this.couriers = couriers;
        this.props = props;
    }

    public record CancelRequest(String reason, Long expectedVersion) {}

    public record ReassignRequest(UUID courierId, Long expectedVersion) {}

    /** Active orders first, then the most recent terminal ones. */
    @GetMapping("/orders")
    public List<OrderView> orders(@RequestParam(defaultValue = "50") int recentTerminal) {
        List<OrderView> all = new ArrayList<>(orders.activeViews());
        all.addAll(orders.recentTerminalViews(recentTerminal));
        return all;
    }

    @GetMapping("/orders/stuck")
    public List<OrderView> stuck(@RequestParam(required = false) Long olderThanSeconds) {
        Duration threshold = olderThanSeconds == null ? props.dispatch().stuckAfter() : Duration.ofSeconds(olderThanSeconds);
        return orders.stuckViews(threshold);
    }

    @GetMapping("/orders/{id}")
    public OrderView one(@PathVariable UUID id) {
        return orders.view(id);
    }

    @GetMapping("/couriers")
    public List<CourierView> couriers() {
        return couriers.viewsAll();
    }

    @PostMapping("/orders/{id}/cancel")
    public OrderView cancel(@PathVariable UUID id, @RequestBody(required = false) CancelRequest req) {
        return service.cancelByDispatcher(id, req == null ? null : req.reason(), req == null ? null : req.expectedVersion());
    }

    @PostMapping("/orders/{id}/reassign")
    public OrderView reassign(@PathVariable UUID id, @RequestBody(required = false) ReassignRequest req) {
        return service.reassign(id, req == null ? null : req.courierId(), req == null ? null : req.expectedVersion());
    }
}
