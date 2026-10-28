package dev.campusdelivery.couriers;

import dev.campusdelivery.assignment.OfferRepository;
import dev.campusdelivery.common.ApiException;
import dev.campusdelivery.common.GeoPoint;
import dev.campusdelivery.config.AppProperties;
import dev.campusdelivery.events.EventRecord;
import dev.campusdelivery.events.EventRepository;
import dev.campusdelivery.events.EventTypes;
import dev.campusdelivery.events.LiveEventBus;
import dev.campusdelivery.events.LocationTelemetryPublisher;
import dev.campusdelivery.events.LocationUpdate;
import dev.campusdelivery.orders.OrderRepository;
import dev.campusdelivery.orders.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class CourierService {

    public record LocationPoint(double lat, double lng, @NotNull Instant recordedAt,
                                Float speedMps, Float headingDeg, Float accuracyM) {}

    public record LocationBatch(@NotEmpty List<@Valid LocationPoint> points) {}

    public record LocationBatchResult(int accepted, int rejected, boolean advanced, CourierView courier) {}

    private final CourierRepository couriers;
    private final OrderRepository orders;
    private final OfferRepository offers;
    private final OrderService orderService;
    private final EventRepository events;
    private final LiveEventBus bus;
    private final LocationTelemetryPublisher telemetry;
    private final AppProperties props;
    private final Clock clock;

    public CourierService(CourierRepository couriers, OrderRepository orders, OfferRepository offers,
                          OrderService orderService, EventRepository events, LiveEventBus bus,
                          LocationTelemetryPublisher telemetry, AppProperties props, Clock clock) {
        this.couriers = couriers;
        this.orders = orders;
        this.offers = offers;
        this.orderService = orderService;
        this.events = events;
        this.bus = bus;
        this.telemetry = telemetry;
        this.props = props;
        this.clock = clock;
    }

    @Transactional
    public CourierView setStatus(UUID courierId, CourierStatus status) {
        CourierRepository.CourierRecord current = couriers.find(courierId)
                .orElseThrow(() -> ApiException.notFound("Courier"));
        if (status == CourierStatus.OFFLINE) {
            if (orders.activeOrderForCourier(courierId).isPresent()) {
                throw ApiException.conflict("courier_busy", "Finish or hand off your active order before going offline");
            }
            // Going offline withdraws any pending offer so the order returns to the pool right away.
            Optional<OfferRepository.Offer> pending = offers.pendingForCourier(courierId);
            pending.ifPresent(p -> orderService.releaseOffer(p.id(), OfferRepository.Status.WITHDRAWN, EventTypes.OFFER_WITHDRAWN));
        }
        if (current.status() != status) {
            couriers.setStatus(courierId, status);
            CourierView view = couriers.view(courierId).orElseThrow();
            events.append(EventTypes.COURIER_STATUS_CHANGED, EventRecord.AGGREGATE_COURIER, courierId,
                    null, courierId, null, Map.of("courier", view));
            return view;
        }
        return couriers.view(courierId).orElseThrow();
    }

    /**
     * Batched, out-of-order tolerant. Every valid ping lands in history and Kafka; only the newest
     * (by device timestamp) can move the last-known marker, and only if it is newer than what the
     * database already holds.
     */
    @Transactional
    public LocationBatchResult recordLocations(UUID courierId, LocationBatch batch) {
        if (batch.points().size() > props.location().maxBatch()) {
            throw ApiException.badRequest("batch_too_large", "At most " + props.location().maxBatch() + " points per batch");
        }
        Instant now = clock.instant();
        Instant maxFuture = now.plus(props.location().maxFutureSkew());
        List<CourierRepository.Ping> valid = new ArrayList<>();
        int rejected = 0;
        for (LocationPoint p : batch.points()) {
            boolean ok = new GeoPoint(p.lat(), p.lng()).isValid()
                    && p.recordedAt() != null && !p.recordedAt().isAfter(maxFuture);
            if (ok) {
                valid.add(new CourierRepository.Ping(p.lat(), p.lng(), p.recordedAt(), p.speedMps(), p.headingDeg(), p.accuracyM()));
            } else {
                rejected++;
            }
        }
        if (valid.isEmpty()) {
            CourierView view = couriers.view(courierId).orElseThrow(() -> ApiException.notFound("Courier"));
            return new LocationBatchResult(0, rejected, false, view);
        }

        couriers.insertPings(courierId, valid, now);
        CourierRepository.Ping newest = valid.stream().max(Comparator.comparing(CourierRepository.Ping::recordedAt)).orElseThrow();
        boolean advanced = couriers.updateLastKnown(courierId, newest, now)
                .orElseThrow(() -> ApiException.notFound("Courier"));

        Optional<OrderRepository.ActiveOrder> active = orders.activeOrderForCourier(courierId);
        UUID orderId = active.map(OrderRepository.ActiveOrder::id).orElse(null);
        UUID customerId = active.map(OrderRepository.ActiveOrder::customerId).orElse(null);

        List<LocationTelemetryPublisher.Ping> pings = valid.stream()
                .map(p -> new LocationTelemetryPublisher.Ping(1, courierId, p.lat(), p.lng(), p.recordedAt(), now,
                        p.speedMps(), p.headingDeg(), p.accuracyM(), orderId))
                .toList();
        LocationUpdate live = advanced
                ? new LocationUpdate(courierId, newest.lat(), newest.lng(), newest.recordedAt(), now, orderId, customerId)
                : null;
        afterCommit(() -> {
            telemetry.publish(pings);
            if (live != null) {
                bus.publish(live);
            }
        });

        CourierView view = couriers.view(courierId).orElseThrow();
        return new LocationBatchResult(valid.size(), rejected, advanced, view);
    }

    private static void afterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            });
        } else {
            action.run();
        }
    }
}
