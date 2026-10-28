package dev.campusdelivery.assignment;

import dev.campusdelivery.config.AppProperties;
import dev.campusdelivery.events.EventTypes;
import dev.campusdelivery.orders.OrderRepository;
import dev.campusdelivery.orders.OrderService;
import dev.campusdelivery.orders.OrderStatus;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Assignment loop. Each tick: (1) expire timed-out offers so their orders go back to the pool,
 * (2) offer every CREATED order to the nearest eligible courier. Every order is handled in its own
 * transaction under a SKIP LOCKED row lock, so several backend instances can run this safely.
 */
@Service
public class AssignmentService {

    private static final Logger log = LoggerFactory.getLogger(AssignmentService.class);
    private static final int BATCH = 50;

    private final OrderRepository orders;
    private final OfferRepository offers;
    private final OrderService orderService;
    private final TransactionTemplate tx;
    private final AppProperties props;
    private final Clock clock;

    public AssignmentService(OrderRepository orders, OfferRepository offers, OrderService orderService,
                             TransactionTemplate tx, AppProperties props, Clock clock) {
        this.orders = orders;
        this.offers = offers;
        this.orderService = orderService;
        this.tx = tx;
        this.props = props;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${app.assignment.tick-ms}", initialDelayString = "${app.assignment.tick-ms}")
    public void tick() {
        try {
            expireOffers();
            offerOpenOrders();
        } catch (RuntimeException e) {
            log.warn("Assignment tick failed: {}", e.getMessage());
        }
    }

    public int expireOffers() {
        Instant now = clock.instant();
        int released = 0;
        for (OfferRepository.Offer offer : offers.expired(now, BATCH)) {
            try {
                Optional<?> result = orderService.releaseOffer(offer.id(), OfferRepository.Status.EXPIRED, EventTypes.OFFER_EXPIRED);
                if (result.isPresent()) {
                    released++;
                    log.info("Offer {} for order {} expired (courier {})", offer.id(), offer.orderId(), offer.courierId());
                }
            } catch (DataAccessException e) {
                log.warn("Failed to expire offer {}: {}", offer.id(), e.getMessage());
            }
        }
        return released;
    }

    public int offerOpenOrders() {
        List<UUID> candidates = tx.execute(s -> orders.lockCreatedSkipLocked(BATCH));
        int offered = 0;
        if (candidates == null) {
            return 0;
        }
        for (UUID orderId : candidates) {
            try {
                Boolean ok = tx.execute(s -> offerOne(orderId));
                if (Boolean.TRUE.equals(ok)) {
                    offered++;
                }
            } catch (DataAccessException e) {
                // e.g. two ticks picked the same courier: the partial unique index rejects the second offer.
                log.debug("Offer for order {} skipped: {}", orderId, e.getMessage());
            }
        }
        return offered;
    }

    private boolean offerOne(UUID orderId) {
        Optional<OrderRepository.LockedOrder> locked = orders.lock(orderId);
        if (locked.isEmpty() || locked.get().status() != OrderStatus.CREATED) {
            return false;
        }
        Instant now = clock.instant();
        AppProperties.Assignment cfg = props.assignment();
        Optional<OfferRepository.Candidate> nearest = offers.nearestCandidate(
                orderId, locked.get().merchantId(), cfg.radiusMeters(),
                now.minus(cfg.courierFreshness()), now.minus(cfg.retryCooldown()));
        if (nearest.isEmpty()) {
            return false;
        }
        OfferRepository.Candidate c = nearest.get();
        orderService.offer(orderId, c.courierId(), c.distanceMeters(), now.plus(cfg.offerTtl()));
        log.info("Offered order {} to courier {} ({} m away)", orderId, c.courierId(), Math.round(c.distanceMeters()));
        return true;
    }
}
