package dev.campusdelivery.orders;

import dev.campusdelivery.auth.Principals;
import dev.campusdelivery.orders.OrderService.CreateOrderRequest;
import dev.campusdelivery.orders.OrderService.CreateResult;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/customer/orders")
public class CustomerOrderController {

    private final OrderService service;
    private final OrderRepository orders;

    public CustomerOrderController(OrderService service, OrderRepository orders) {
        this.service = service;
        this.orders = orders;
    }

    public record CancelRequest(String reason) {}

    /** 201 on first creation, 200 with the same body on any retry carrying the same Idempotency-Key. */
    @PostMapping
    public ResponseEntity<OrderView> create(@AuthenticationPrincipal Jwt jwt,
                                            @RequestHeader("Idempotency-Key") String idempotencyKey,
                                            @RequestBody CreateOrderRequest req) {
        CreateResult result = service.create(Principals.userId(jwt), idempotencyKey, req);
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK)
                .header("Idempotency-Replayed", String.valueOf(!result.created()))
                .body(result.order());
    }

    @GetMapping
    public List<OrderView> mine(@AuthenticationPrincipal Jwt jwt) {
        return orders.viewsForCustomer(Principals.userId(jwt), 100);
    }

    @GetMapping("/{id}")
    public OrderView one(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        OrderView view = orders.view(id);
        if (!view.customerId().equals(Principals.userId(jwt))) {
            throw dev.campusdelivery.common.ApiException.notFound("Order");
        }
        return view;
    }

    @PostMapping("/{id}/cancel")
    public OrderView cancel(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id,
                            @RequestBody(required = false) CancelRequest req) {
        return service.cancelByCustomer(Principals.userId(jwt), id, req == null ? null : req.reason());
    }
}
