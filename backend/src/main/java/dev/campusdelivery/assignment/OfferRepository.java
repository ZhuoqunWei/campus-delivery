package dev.campusdelivery.assignment;

import dev.campusdelivery.common.Timestamps;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OfferRepository {

    public enum Status { PENDING, ACCEPTED, EXPIRED, DECLINED, WITHDRAWN, REVOKED }

    public record Offer(UUID id, UUID orderId, UUID courierId, Status status, Instant offeredAt, Instant expiresAt) {}

    public record Candidate(UUID courierId, String name, double distanceMeters) {}

    private static final RowMapper<Offer> MAPPER = (rs, i) -> new Offer(
            rs.getObject("id", UUID.class),
            rs.getObject("order_id", UUID.class),
            rs.getObject("courier_id", UUID.class),
            Status.valueOf(rs.getString("status")),
            Timestamps.instant(rs, "offered_at"),
            Timestamps.instant(rs, "expires_at"));

    private static final String SELECT = "SELECT id, order_id, courier_id, status::text AS status, offered_at, expires_at FROM order_offers ";

    private final JdbcClient jdbc;

    public OfferRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public UUID create(UUID orderId, UUID courierId, double distanceMeters, Instant expiresAt) {
        return jdbc.sql("""
                INSERT INTO order_offers (order_id, courier_id, distance_m, expires_at)
                VALUES (:o, :c, :d, :exp)
                RETURNING id
                """)
                .param("o", orderId).param("c", courierId).param("d", distanceMeters)
                .param("exp", Timestamps.db(expiresAt))
                .query(UUID.class).single();
    }

    public Optional<Offer> pendingForOrder(UUID orderId) {
        return jdbc.sql(SELECT + "WHERE order_id = :o AND status = 'PENDING'").param("o", orderId).query(MAPPER).optional();
    }

    public Optional<Offer> pendingForCourier(UUID courierId) {
        return jdbc.sql(SELECT + "WHERE courier_id = :c AND status = 'PENDING'").param("c", courierId).query(MAPPER).optional();
    }

    public Optional<Offer> findById(UUID id) {
        return jdbc.sql(SELECT + "WHERE id = :id").param("id", id).query(MAPPER).optional();
    }

    /** Offers whose TTL passed. Read without locks; callers lock the order first (lock order: order -> offer). */
    public List<Offer> expired(Instant now, int limit) {
        return jdbc.sql(SELECT + "WHERE status = 'PENDING' AND expires_at <= :now ORDER BY expires_at LIMIT :limit")
                .param("now", Timestamps.db(now)).param("limit", limit).query(MAPPER).list();
    }

    /** Transitions a PENDING offer; returns 0 if someone else resolved it first. */
    public int resolve(UUID offerId, Status to) {
        return jdbc.sql("UPDATE order_offers SET status = CAST(:s AS offer_status), resolved_at = now() WHERE id = :id AND status = 'PENDING'")
                .param("s", to.name()).param("id", offerId).update();
    }

    /** Audit row so the assignment loop does not immediately re-offer to a courier a dispatcher just pulled off. */
    public void recordRevoked(UUID orderId, UUID courierId) {
        jdbc.sql("""
                INSERT INTO order_offers (order_id, courier_id, status, expires_at, resolved_at)
                VALUES (:o, :c, 'REVOKED', now(), now())
                """)
                .param("o", orderId).param("c", courierId).update();
    }

    /**
     * Nearest online, free courier to the merchant. ST_DWithin on geography works in meters and uses
     * the GiST index; couriers we have not heard from recently, or that turned this order down
     * within the cooldown, are skipped.
     */
    public Optional<Candidate> nearestCandidate(UUID orderId, UUID merchantId, double radiusMeters,
                                                Instant heardSince, Instant cooldownSince) {
        return jdbc.sql("""
                SELECT c.user_id, u.display_name, ST_Distance(c.location, m.location) AS distance_m
                FROM couriers c
                JOIN users u ON u.id = c.user_id
                JOIN merchants m ON m.id = :merchant
                WHERE c.status = 'ONLINE'
                  AND c.location IS NOT NULL
                  AND c.location_received_at > :heardSince
                  AND ST_DWithin(c.location, m.location, :radius)
                  AND NOT EXISTS (SELECT 1 FROM orders a
                                  WHERE a.courier_id = c.user_id AND a.status IN ('ACCEPTED', 'PICKED_UP'))
                  AND NOT EXISTS (SELECT 1 FROM order_offers p
                                  WHERE p.courier_id = c.user_id AND p.status = 'PENDING')
                  AND NOT EXISTS (SELECT 1 FROM order_offers x
                                  WHERE x.order_id = :order AND x.courier_id = c.user_id
                                    AND x.status IN ('EXPIRED', 'DECLINED', 'REVOKED')
                                    AND x.resolved_at > :cooldownSince)
                ORDER BY distance_m
                LIMIT 1
                """)
                .param("merchant", merchantId)
                .param("order", orderId)
                .param("radius", radiusMeters)
                .param("heardSince", Timestamps.db(heardSince))
                .param("cooldownSince", Timestamps.db(cooldownSince))
                .query((rs, i) -> new Candidate(rs.getObject("user_id", UUID.class), rs.getString("display_name"),
                        rs.getDouble("distance_m")))
                .optional();
    }
}
