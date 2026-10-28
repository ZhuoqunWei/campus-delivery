package dev.campusdelivery.orders;

import dev.campusdelivery.auth.Principals;
import dev.campusdelivery.config.AppProperties;
import java.util.List;
import java.util.UUID;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/courier")
public class CourierOrderController {

    private final OrderService service;
    private final OrderRepository orders;
    private final AppProperties props;

    public CourierOrderController(OrderService service, OrderRepository orders, AppProperties props) {
        this.service = service;
        this.orders = orders;
        this.props = props;
    }

    /** Orders offered to me or already mine. */
    @GetMapping("/orders")
    public List<OrderView> active(@AuthenticationPrincipal Jwt jwt) {
        return orders.activeViewsForCourier(Principals.userId(jwt));
    }

    /** Unoffered orders near my last-known position that I may accept directly. */
    @GetMapping("/orders/open")
    public List<OrderView> open(@AuthenticationPrincipal Jwt jwt) {
        return orders.openNearCourier(Principals.userId(jwt), props.assignment().radiusMeters(), 50);
    }

    @GetMapping("/orders/history")
    public List<OrderView> history(@AuthenticationPrincipal Jwt jwt) {
        return orders.historyForCourier(Principals.userId(jwt), 50);
    }

    @PostMapping("/orders/{id}/accept")
    public OrderView accept(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return service.accept(id, Principals.userId(jwt));
    }

    @PostMapping("/offers/{offerId}/decline")
    public OrderView decline(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID offerId) {
        return service.decline(offerId, Principals.userId(jwt));
    }

    @PostMapping("/orders/{id}/pickup")
    public OrderView pickUp(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return service.pickUp(id, Principals.userId(jwt));
    }

    @PostMapping("/orders/{id}/deliver")
    public OrderView deliver(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return service.deliver(id, Principals.userId(jwt));
    }
}
