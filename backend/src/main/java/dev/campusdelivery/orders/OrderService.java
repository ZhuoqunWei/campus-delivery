package dev.campusdelivery.orders;

import dev.campusdelivery.assignment.OfferRepository;
import dev.campusdelivery.assignment.OfferRepository.Offer;
import dev.campusdelivery.common.ApiException;
import dev.campusdelivery.common.GeoPoint;
import dev.campusdelivery.common.Json;
import dev.campusdelivery.couriers.CourierRepository;
import dev.campusdelivery.couriers.CourierStatus;
import dev.campusdelivery.events.EventRecord;
import dev.campusdelivery.events.EventRepository;
import dev.campusdelivery.events.EventTypes;
import dev.campusdelivery.merchants.MerchantRepository;
import dev.campusdelivery.orders.OrderRepository.LockedOrder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * All order state changes. Each public method is one transaction that (1) takes the order row lock,
 * (2) validates the business rule, (3) updates (the DB trigger re-validates the transition), and
 * (4) appends the event that clients and Kafka will see after commit.
 */
@Service
public class OrderService {

    public record CreateOrderRequest(UUID merchantId, GeoPoint dropoff, String note, List<Item> items) {
        public record Item(String name, int quantity) {}
    }

    public record CreateResult(OrderView order, boolean created) {}

    private final OrderRepository orders;
    private final OfferRepository offers;
    private final CourierRepository couriers;
    private final MerchantRepository merchants;
    private final EventRepository events;
    private final Json json;

    public OrderService(OrderRepository orders, OfferRepository offers, CourierRepository couriers,
                        MerchantRepository merchants, EventRepository events, Json json) {
        this.orders = orders;
        this.offers = offers;
        this.couriers = couriers;
        this.merchants = merchants;
        this.events = events;
        this.json = json;
    }

    // ---------------------------------------------------------------- customer

    /**
     * Idempotent create. The (customer, key) unique constraint plus INSERT ... ON CONFLICT DO NOTHING
     * makes concurrent retries collapse onto one row without aborting the transaction; a retry with
     * a different body for the same key is rejected instead of silently returning the old order.
     */
    @Transactional
    public CreateResult create(UUID customerId, String idempotencyKey, CreateOrderRequest req) {
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200) {
            throw ApiException.badRequest("invalid_idempotency_key", "Idempotency-Key header is required (max 200 chars)");
        }
        if (req.dropoff() == null || !req.dropoff().isValid()) {
            throw ApiException.badRequest("invalid_dropoff", "dropoff must be a valid lat/lng");
        }
        merchants.findById(req.merchantId()).orElseThrow(() -> ApiException.notFound("Merchant"));

        String hash = json.sha256(req);
        List<CreateOrderRequest.Item> items = req.items() == null ? List.of() : req.items();
        Optional<UUID> inserted = orders.insertIfAbsent(customerId, req.merchantId(), idempotencyKey, hash,
                req.dropoff(), req.note(), json.write(items));

        if (inserted.isPresent()) {
            OrderView view = orders.view(inserted.get());
            emit(EventTypes.ORDER_CREATED, view, null, Map.of());
            return new CreateResult(view, true);
        }
        OrderRepository.IdempotencyMatch existing = orders.findByIdempotencyKey(customerId, idempotencyKey)
                .orElseThrow(() -> new IllegalStateException("idempotency key vanished"));
        if (!existing.requestHash().equals(hash)) {
            throw ApiException.unprocessable("idempotency_key_reused",
                    "Idempotency-Key was already used with a different request body");
        }
        return new CreateResult(orders.view(existing.id()), false);
    }

    @Transactional
    public OrderView cancelByCustomer(UUID customerId, UUID orderId, String reason) {
        LockedOrder o = lock(orderId);
        if (!o.customerId().equals(customerId)) {
            throw ApiException.notFound("Order");
        }
        if (o.status() == OrderStatus.PICKED_UP) {
            throw ApiException.conflict("too_late_to_cancel", "Order is already picked up; contact a dispatcher");
        }
        return cancel(o, reason == null ? "Cancelled by customer" : reason, "CUSTOMER");
    }

    // ---------------------------------------------------------------- courier

    /**
     * Exactly one courier per order. The FOR UPDATE lock serializes racing accepts; the loser sees
     * status ACCEPTED and gets 409. The partial unique index on (courier_id) WHERE active is the
     * database-level backstop for "one active order per courier".
     */
    @Transactional
    public OrderView accept(UUID orderId, UUID courierId) {
        LockedOrder o = lock(orderId);
        if (!OrderStatus.UNASSIGNED.contains(o.status())) {
            throw ApiException.conflict("order_unavailable",
                    "Order is no longer available (status " + o.status() + ")");
        }
        Optional<Offer> pending = offers.pendingForOrder(orderId);
        if (o.status() == OrderStatus.OFFERED && pending.isPresent()
                && !pending.get().courierId().equals(courierId)) {
            throw ApiException.conflict("offered_to_other_courier", "Order is currently offered to another courier");
        }
        requireOnlineAndFree(courierId);

        pending.ifPresent(p -> offers.resolve(p.id(), OfferRepository.Status.ACCEPTED));
        orders.markAccepted(orderId, courierId);
        OrderView view = orders.view(orderId);
        emit(EventTypes.ORDER_ACCEPTED, view, courierId, Map.of());
        return view;
    }

    @Transactional
    public OrderView decline(UUID offerId, UUID courierId) {
        Offer offer = offers.findById(offerId).orElseThrow(() -> ApiException.notFound("Offer"));
        if (!offer.courierId().equals(courierId)) {
            throw ApiException.notFound("Offer");
        }
        LockedOrder o = lock(offer.orderId());
        if (offers.resolve(offerId, OfferRepository.Status.DECLINED) == 0) {
            throw ApiException.conflict("offer_not_pending", "Offer is no longer pending");
        }
        if (o.status() == OrderStatus.OFFERED) {
            orders.markUnassigned(o.id());
        }
        OrderView view = orders.view(o.id());
        emit(EventTypes.OFFER_DECLINED, view, courierId, Map.of("offerId", offerId));
        return view;
    }

    @Transactional
    public OrderView pickUp(UUID orderId, UUID courierId) {
        LockedOrder o = lockOwned(orderId, courierId);
        if (o.status() != OrderStatus.ACCEPTED) {
            throw ApiException.conflict("illegal_transition", "Order must be ACCEPTED to pick up (is " + o.status() + ")");
        }
        orders.markPickedUp(orderId);
        OrderView view = orders.view(orderId);
        emit(EventTypes.ORDER_PICKED_UP, view, courierId, Map.of());
        return view;
    }

    @Transactional
    public OrderView deliver(UUID orderId, UUID courierId) {
        LockedOrder o = lockOwned(orderId, courierId);
        if (o.status() != OrderStatus.PICKED_UP) {
            throw ApiException.conflict("illegal_transition", "Order must be PICKED_UP to deliver (is " + o.status() + ")");
        }
        orders.markDelivered(orderId);
        OrderView view = orders.view(orderId);
        emit(EventTypes.ORDER_DELIVERED, view, courierId, Map.of());
        return view;
    }

    // ---------------------------------------------------------------- dispatcher

    @Transactional
    public OrderView cancelByDispatcher(UUID orderId, String reason, Long expectedVersion) {
        LockedOrder o = lock(orderId);
        checkVersion(o, expectedVersion);
        return cancel(o, reason == null ? "Cancelled by dispatcher" : reason, "DISPATCHER");
    }

    /**
     * Pull the order off its current courier/offer. With a target courier, assign it to them
     * directly (dispatcher override); otherwise it returns to the pool and the assignment loop
     * re-offers it.
     */
    @Transactional
    public OrderView reassign(UUID orderId, UUID newCourierId, Long expectedVersion) {
        LockedOrder o = lock(orderId);
        checkVersion(o, expectedVersion);
        if (o.status().terminal()) {
            throw ApiException.conflict("order_terminal", "Order is already " + o.status());
        }
        if (o.status() == OrderStatus.PICKED_UP) {
            throw ApiException.conflict("already_picked_up", "Order is already picked up; cancel it instead");
        }
        if (newCourierId != null && newCourierId.equals(o.courierId())) {
            throw ApiException.conflict("same_courier", "Order is already assigned to that courier");
        }

        Optional<Offer> pending = offers.pendingForOrder(orderId);
        pending.ifPresent(p -> offers.resolve(p.id(), OfferRepository.Status.WITHDRAWN));
        UUID previous = o.courierId();
        if (o.status() != OrderStatus.CREATED) {
            orders.markUnassigned(orderId);
        }
        if (previous != null) {
            offers.recordRevoked(orderId, previous);
        }

        OrderView unassigned = orders.view(orderId);
        if (previous != null) {
            emit(EventTypes.ORDER_UNASSIGNED, unassigned, previous, Map.of("previousCourierId", previous));
        } else if (pending.isPresent()) {
            emit(EventTypes.OFFER_WITHDRAWN, unassigned, pending.get().courierId(), Map.of("offerId", pending.get().id()));
        }
        if (newCourierId == null) {
            if (previous == null && pending.isEmpty()) {
                emit(EventTypes.ORDER_UNASSIGNED, unassigned, null, Map.of());
            }
            return unassigned;
        }

        requireOnlineAndFree(newCourierId);
        orders.markAccepted(orderId, newCourierId);
        OrderView assigned = orders.view(orderId);
        emit(EventTypes.ORDER_ACCEPTED, assigned, newCourierId, Map.of("assignedBy", "DISPATCHER"));
        return assigned;
    }

    // ---------------------------------------------------------------- assignment loop hooks

    /** CREATED -> OFFERED to a specific courier. Caller holds the order lock. */
    @Transactional(propagation = org.springframework.transaction.annotation.Propagation.MANDATORY)
    public OrderView offer(UUID orderId, UUID courierId, double distanceMeters, java.time.Instant expiresAt) {
        UUID offerId = offers.create(orderId, courierId, distanceMeters, expiresAt);
        orders.markOffered(orderId);
        OrderView view = orders.view(orderId);
        emit(EventTypes.ORDER_OFFERED, view, courierId, Map.of("offerId", offerId, "distanceMeters", distanceMeters));
        return view;
    }

    /** OFFERED -> CREATED because the offer expired or the courier left. */
    @Transactional
    public Optional<OrderView> releaseOffer(UUID offerId, OfferRepository.Status resolution, String eventType) {
        Offer offer = offers.findById(offerId).orElse(null);
        if (offer == null) {
            return Optional.empty();
        }
        LockedOrder o = lock(offer.orderId());
        if (offers.resolve(offerId, resolution) == 0) {
            return Optional.empty(); // resolved concurrently (accepted, declined, ...)
        }
        if (o.status() == OrderStatus.OFFERED) {
            orders.markUnassigned(o.id());
        }
        OrderView view = orders.view(o.id());
        emit(eventType, view, offer.courierId(), Map.of("offerId", offerId));
        return Optional.of(view);
    }

    // ---------------------------------------------------------------- helpers

    private OrderView cancel(LockedOrder o, String reason, String by) {
        if (o.status().terminal()) {
            throw ApiException.conflict("order_terminal", "Order is already " + o.status());
        }
        Optional<Offer> pending = offers.pendingForOrder(o.id());
        pending.ifPresent(p -> offers.resolve(p.id(), OfferRepository.Status.WITHDRAWN));
        orders.markCancelled(o.id(), reason);
        OrderView view = orders.view(o.id());
        UUID courierToNotify = o.courierId() != null ? o.courierId() : pending.map(Offer::courierId).orElse(null);
        emit(EventTypes.ORDER_CANCELLED, view, courierToNotify, Map.of("reason", reason, "cancelledBy", by));
        return view;
    }

    private LockedOrder lock(UUID orderId) {
        return orders.lock(orderId).orElseThrow(() -> ApiException.notFound("Order"));
    }

    private LockedOrder lockOwned(UUID orderId, UUID courierId) {
        LockedOrder o = lock(orderId);
        if (!courierId.equals(o.courierId())) {
            throw ApiException.conflict("not_your_order", "Order is not assigned to you");
        }
        return o;
    }

    private void requireOnlineAndFree(UUID courierId) {
        CourierRepository.CourierRecord c = couriers.find(courierId).orElseThrow(() -> ApiException.notFound("Courier"));
        if (c.status() != CourierStatus.ONLINE) {
            throw ApiException.conflict("courier_offline", "Courier is not online");
        }
        if (orders.activeOrderForCourier(courierId).isPresent()) {
            throw ApiException.conflict("courier_busy", "Courier already has an active order");
        }
    }

    private static void checkVersion(LockedOrder o, Long expectedVersion) {
        if (expectedVersion != null && expectedVersion != o.version()) {
            throw ApiException.conflict("version_mismatch",
                    "Order changed since you loaded it (version " + o.version() + ", expected " + expectedVersion + ")");
        }
    }

    private void emit(String type, OrderView view, UUID courierId, Map<String, ?> extra) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("order", view);
        payload.putAll(extra);
        events.append(type, EventRecord.AGGREGATE_ORDER, view.id(), view.id(), courierId, view.customerId(), payload);
    }
}
