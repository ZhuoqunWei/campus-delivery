package dev.campusdelivery.couriers;

import dev.campusdelivery.auth.Principals;
import dev.campusdelivery.common.ApiException;
import dev.campusdelivery.couriers.CourierService.LocationBatch;
import dev.campusdelivery.couriers.CourierService.LocationBatchResult;
import dev.campusdelivery.orders.OrderRepository;
import dev.campusdelivery.orders.OrderView;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/courier")
public class CourierController {

    private final CourierService service;
    private final CourierRepository couriers;
    private final OrderRepository orders;

    public CourierController(CourierService service, CourierRepository couriers, OrderRepository orders) {
        this.service = service;
        this.couriers = couriers;
        this.orders = orders;
    }

    public record StatusRequest(@NotNull CourierStatus status) {}

    public record MeResponse(CourierView courier, List<OrderView> orders) {}

    @GetMapping("/me")
    public MeResponse me(@AuthenticationPrincipal Jwt jwt) {
        UUID id = Principals.userId(jwt);
        CourierView view = couriers.view(id).orElseThrow(() -> ApiException.notFound("Courier"));
        return new MeResponse(view, orders.activeViewsForCourier(id));
    }

    @PostMapping("/status")
    public CourierView setStatus(@AuthenticationPrincipal Jwt jwt, @Valid @RequestBody StatusRequest req) {
        return service.setStatus(Principals.userId(jwt), req.status());
    }

    @PostMapping("/locations")
    public LocationBatchResult locations(@AuthenticationPrincipal Jwt jwt, @Valid @RequestBody LocationBatch batch) {
        return service.recordLocations(Principals.userId(jwt), batch);
    }
}
