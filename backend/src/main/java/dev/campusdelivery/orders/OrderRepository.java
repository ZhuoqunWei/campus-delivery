package dev.campusdelivery.orders;

import dev.campusdelivery.common.ApiException;
import dev.campusdelivery.common.GeoPoint;
import dev.campusdelivery.common.Json;
import dev.campusdelivery.common.Timestamps;
import dev.campusdelivery.config.AppProperties;
import dev.campusdelivery.couriers.CourierStatus;
import dev.campusdelivery.couriers.CourierView;
import dev.campusdelivery.merchants.MerchantView;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OrderRepository {

    /** Minimal row read under FOR UPDATE before a state change. */
    public record LockedOrder(UUID id, OrderStatus status, long version, UUID customerId, UUID courierId, UUID merchantId) {}

    public record IdempotencyMatch(UUID id, String requestHash) {}

    public record ActiveOrder(UUID id, UUID customerId) {}

    private static final String TERMINAL = "('DELIVERED', 'CANCELLED')";

    private static final String VIEW_SELECT = """
            SELECT o.id, o.status::text AS status, o.version, o.customer_id, cu.display_name AS customer_name,
                   m.id AS merchant_id, m.name AS merchant_name,
                   ST_Y(m.location::geometry) AS m_lat, ST_X(m.location::geometry) AS m_lng,
                   ST_Y(o.dropoff::geometry) AS d_lat, ST_X(o.dropoff::geometry) AS d_lng,
                   o.dropoff_note, o.items::text AS items,
                   o.courier_id, cr.display_name AS courier_name, c.status::text AS courier_status,
                   ST_Y(c.location::geometry) AS c_lat, ST_X(c.location::geometry) AS c_lng,
                   c.location_at AS c_location_at, c.location_received_at AS c_last_seen,
                   f.id AS offer_id, f.courier_id AS offer_courier_id, fu.display_name AS offer_courier_name,
                   f.offered_at, f.expires_at,
                   o.created_at, o.updated_at, o.status_changed_at, o.accepted_at, o.picked_up_at,
                   o.delivered_at, o.cancelled_at, o.cancel_reason
            FROM orders o
            JOIN users cu ON cu.id = o.customer_id
            JOIN merchants m ON m.id = o.merchant_id
            LEFT JOIN couriers c ON c.user_id = o.courier_id
            LEFT JOIN users cr ON cr.id = o.courier_id
            LEFT JOIN order_offers f ON f.order_id = o.id AND f.status = 'PENDING'
            LEFT JOIN users fu ON fu.id = f.courier_id
            """;

    private final JdbcClient jdbc;
    private final Clock clock;
    private final AppProperties props;
    private final RowMapper<OrderView> viewMapper;

    public OrderRepository(JdbcClient jdbc, Json json, Clock clock, AppProperties props) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.props = props;
        this.viewMapper = (rs, i) -> {
            UUID courierId = rs.getObject("courier_id", UUID.class);
            CourierView courier = null;
            if (courierId != null) {
                Instant lastSeen = Timestamps.instant(rs, "c_last_seen");
                boolean hasLocation = rs.getObject("c_lat") != null;
                courier = new CourierView(
                        courierId,
                        rs.getString("courier_name"),
                        CourierStatus.valueOf(rs.getString("courier_status")),
                        hasLocation ? new GeoPoint(rs.getDouble("c_lat"), rs.getDouble("c_lng")) : null,
                        Timestamps.instant(rs, "c_location_at"),
                        lastSeen,
                        CourierView.isStale(lastSeen, clock.instant(), props.location().staleAfter()),
                        rs.getObject("id", UUID.class));
            }
            UUID offerId = rs.getObject("offer_id", UUID.class);
            OfferView offer = offerId == null ? null : new OfferView(
                    offerId,
                    rs.getObject("offer_courier_id", UUID.class),
                    rs.getString("offer_courier_name"),
                    Timestamps.instant(rs, "offered_at"),
                    Timestamps.instant(rs, "expires_at"));
            return new OrderView(
                    rs.getObject("id", UUID.class),
                    OrderStatus.valueOf(rs.getString("status")),
                    rs.getLong("version"),
                    rs.getObject("customer_id", UUID.class),
                    rs.getString("customer_name"),
                    new MerchantView(rs.getObject("merchant_id", UUID.class), rs.getString("merchant_name"),
                            new GeoPoint(rs.getDouble("m_lat"), rs.getDouble("m_lng"))),
                    new GeoPoint(rs.getDouble("d_lat"), rs.getDouble("d_lng")),
                    rs.getString("dropoff_note"),
                    json.read(rs.getString("items")),
                    courierId,
                    courier,
                    offer,
                    Timestamps.instant(rs, "created_at"),
                    Timestamps.instant(rs, "updated_at"),
                    Timestamps.instant(rs, "status_changed_at"),
                    Timestamps.instant(rs, "accepted_at"),
                    Timestamps.instant(rs, "picked_up_at"),
                    Timestamps.instant(rs, "delivered_at"),
                    Timestamps.instant(rs, "cancelled_at"),
                    rs.getString("cancel_reason"));
        };
    }

    // ---- locking & lookups ----

    /** Row lock. Every state change goes through this so concurrent writers queue up. */
    public Optional<LockedOrder> lock(UUID id) {
        return jdbc.sql("SELECT id, status::text AS status, version, customer_id, courier_id, merchant_id FROM orders WHERE id = :id FOR UPDATE")
                .param("id", id)
                .query((rs, i) -> new LockedOrder(
                        rs.getObject("id", UUID.class),
                        OrderStatus.valueOf(rs.getString("status")),
                        rs.getLong("version"),
                        rs.getObject("customer_id", UUID.class),
                        rs.getObject("courier_id", UUID.class),
                        rs.getObject("merchant_id", UUID.class)))
                .optional();
    }

    /** Like lock(), but skips rows another transaction already holds. Used by the assignment loop. */
    public List<UUID> lockCreatedSkipLocked(int limit) {
        return jdbc.sql("SELECT id FROM orders WHERE status = 'CREATED' ORDER BY created_at LIMIT :limit FOR UPDATE SKIP LOCKED")
                .param("limit", limit)
                .query(UUID.class)
                .list();
    }

    public Optional<IdempotencyMatch> findByIdempotencyKey(UUID customerId, String key) {
        return jdbc.sql("SELECT id, request_hash FROM orders WHERE customer_id = :c AND idempotency_key = :k")
                .param("c", customerId)
                .param("k", key)
                .query((rs, i) -> new IdempotencyMatch(rs.getObject("id", UUID.class), rs.getString("request_hash")))
                .optional();
    }

    /** Inserts unless (customer, key) exists. Returns the new id, or empty when the key was already used. */
    public Optional<UUID> insertIfAbsent(UUID customerId, UUID merchantId, String key, String requestHash,
                                         GeoPoint dropoff, String note, String itemsJson) {
        return jdbc.sql("""
                INSERT INTO orders (customer_id, merchant_id, idempotency_key, request_hash, dropoff, dropoff_note, items)
                VALUES (:c, :m, :k, :h, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography, :note, CAST(:items AS jsonb))
                ON CONFLICT (customer_id, idempotency_key) DO NOTHING
                RETURNING id
                """)
                .param("c", customerId)
                .param("m", merchantId)
                .param("k", key)
                .param("h", requestHash)
                .param("lat", dropoff.lat())
                .param("lng", dropoff.lng())
                .param("note", note)
                .param("items", itemsJson)
                .query(UUID.class)
                .optional();
    }

    public Optional<ActiveOrder> activeOrderForCourier(UUID courierId) {
        return jdbc.sql("SELECT id, customer_id FROM orders WHERE courier_id = :c AND status IN ('ACCEPTED', 'PICKED_UP') LIMIT 1")
                .param("c", courierId)
                .query((rs, i) -> new ActiveOrder(rs.getObject("id", UUID.class), rs.getObject("customer_id", UUID.class)))
                .optional();
    }

    // ---- read model ----

    public OrderView view(UUID id) {
        return findView(id).orElseThrow(() -> ApiException.notFound("Order"));
    }

    public Optional<OrderView> findView(UUID id) {
        return jdbc.sql(VIEW_SELECT + " WHERE o.id = :id").param("id", id).query(viewMapper).optional();
    }

    public List<OrderView> viewsForCustomer(UUID customerId, int limit) {
        return jdbc.sql(VIEW_SELECT + " WHERE o.customer_id = :c ORDER BY o.created_at DESC LIMIT :limit")
                .param("c", customerId).param("limit", limit).query(viewMapper).list();
    }

    public List<OrderView> activeViews() {
        return jdbc.sql(VIEW_SELECT + " WHERE o.status NOT IN " + TERMINAL + " ORDER BY o.created_at")
                .query(viewMapper).list();
    }

    public List<OrderView> recentTerminalViews(int limit) {
        return jdbc.sql(VIEW_SELECT + " WHERE o.status IN " + TERMINAL + " ORDER BY o.status_changed_at DESC LIMIT :limit")
                .param("limit", limit).query(viewMapper).list();
    }

    /** Orders a courier is working on, plus the one currently offered to them. */
    public List<OrderView> activeViewsForCourier(UUID courierId) {
        return jdbc.sql(VIEW_SELECT + """
                 WHERE (o.courier_id = :c AND o.status IN ('ACCEPTED', 'PICKED_UP'))
                    OR (f.courier_id = :c AND o.status = 'OFFERED')
                 ORDER BY o.created_at
                """)
                .param("c", courierId).query(viewMapper).list();
    }

    public List<OrderView> historyForCourier(UUID courierId, int limit) {
        return jdbc.sql(VIEW_SELECT + " WHERE o.courier_id = :c AND o.status IN " + TERMINAL
                        + " ORDER BY o.status_changed_at DESC LIMIT :limit")
                .param("c", courierId).param("limit", limit).query(viewMapper).list();
    }

    /** Unoffered orders whose merchant is within radius of the courier's last-known position. */
    public List<OrderView> openNearCourier(UUID courierId, double radiusMeters, int limit) {
        return jdbc.sql(VIEW_SELECT + """
                 WHERE o.status = 'CREATED'
                   AND ST_DWithin(m.location, (SELECT location FROM couriers WHERE user_id = :c), :radius)
                 ORDER BY o.created_at
                 LIMIT :limit
                """)
                .param("c", courierId).param("radius", radiusMeters).param("limit", limit)
                .query(viewMapper).list();
    }

    /** Non-terminal orders whose status has not changed for longer than the threshold. */
    public List<OrderView> stuckViews(Duration threshold) {
        return jdbc.sql(VIEW_SELECT + " WHERE o.status NOT IN " + TERMINAL
                        + " AND o.status_changed_at < :before ORDER BY o.status_changed_at")
                .param("before", Timestamps.db(clock.instant().minus(threshold)))
                .query(viewMapper).list();
    }

    // ---- state changes (the trigger validates every transition) ----

    public int markOffered(UUID id) {
        return jdbc.sql("UPDATE orders SET status = 'OFFERED' WHERE id = :id").param("id", id).update();
    }

    public int markAccepted(UUID id, UUID courierId) {
        return jdbc.sql("UPDATE orders SET status = 'ACCEPTED', courier_id = :c, accepted_at = now() WHERE id = :id")
                .param("id", id).param("c", courierId).update();
    }

    public int markPickedUp(UUID id) {
        return jdbc.sql("UPDATE orders SET status = 'PICKED_UP', picked_up_at = now() WHERE id = :id")
                .param("id", id).update();
    }

    public int markDelivered(UUID id) {
        return jdbc.sql("UPDATE orders SET status = 'DELIVERED', delivered_at = now() WHERE id = :id")
                .param("id", id).update();
    }

    public int markCancelled(UUID id, String reason) {
        return jdbc.sql("UPDATE orders SET status = 'CANCELLED', cancelled_at = now(), cancel_reason = :r WHERE id = :id")
                .param("id", id).param("r", reason).update();
    }

    /** Back to the pool: OFFERED/ACCEPTED -> CREATED with no courier. */
    public int markUnassigned(UUID id) {
        return jdbc.sql("UPDATE orders SET status = 'CREATED', courier_id = NULL, accepted_at = NULL WHERE id = :id")
                .param("id", id).update();
    }
}
