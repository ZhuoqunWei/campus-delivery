package dev.campusdelivery.couriers;

import dev.campusdelivery.common.GeoPoint;
import dev.campusdelivery.common.Timestamps;
import dev.campusdelivery.config.AppProperties;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class CourierRepository {

    public record CourierRecord(UUID id, CourierStatus status, Instant locationAt, Instant lastSeenAt) {}

    public record Ping(double lat, double lng, Instant recordedAt, Float speedMps, Float headingDeg, Float accuracyM) {}

    private static final String VIEW_SELECT = """
            SELECT c.user_id AS id, u.display_name AS name, c.status::text AS status,
                   ST_Y(c.location::geometry) AS lat, ST_X(c.location::geometry) AS lng,
                   c.location_at, c.location_received_at,
                   (SELECT o.id FROM orders o WHERE o.courier_id = c.user_id
                      AND o.status IN ('ACCEPTED', 'PICKED_UP') LIMIT 1) AS active_order_id
            FROM couriers c
            JOIN users u ON u.id = c.user_id
            """;

    private final JdbcClient jdbc;
    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;
    private final AppProperties props;
    private final RowMapper<CourierView> viewMapper;

    public CourierRepository(JdbcClient jdbc, JdbcTemplate jdbcTemplate, Clock clock, AppProperties props) {
        this.jdbc = jdbc;
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
        this.props = props;
        this.viewMapper = (rs, i) -> {
            Instant lastSeen = Timestamps.instant(rs, "location_received_at");
            boolean hasLocation = rs.getObject("lat") != null;
            return new CourierView(
                    rs.getObject("id", UUID.class),
                    rs.getString("name"),
                    CourierStatus.valueOf(rs.getString("status")),
                    hasLocation ? new GeoPoint(rs.getDouble("lat"), rs.getDouble("lng")) : null,
                    Timestamps.instant(rs, "location_at"),
                    lastSeen,
                    CourierView.isStale(lastSeen, clock.instant(), props.location().staleAfter()),
                    rs.getObject("active_order_id", UUID.class));
        };
    }

    public Optional<CourierRecord> find(UUID id) {
        return jdbc.sql("SELECT user_id, status::text AS status, location_at, location_received_at FROM couriers WHERE user_id = :id")
                .param("id", id)
                .query((rs, i) -> new CourierRecord(
                        rs.getObject("user_id", UUID.class),
                        CourierStatus.valueOf(rs.getString("status")),
                        Timestamps.instant(rs, "location_at"),
                        Timestamps.instant(rs, "location_received_at")))
                .optional();
    }

    public Optional<CourierView> view(UUID id) {
        return jdbc.sql(VIEW_SELECT + " WHERE c.user_id = :id").param("id", id).query(viewMapper).optional();
    }

    public List<CourierView> viewsAll() {
        return jdbc.sql(VIEW_SELECT + " ORDER BY u.display_name").query(viewMapper).list();
    }

    /** Couriers a dispatcher map cares about: online, or heard from recently. */
    public List<CourierView> viewsVisible(Instant heardSince) {
        return jdbc.sql(VIEW_SELECT + " WHERE c.status = 'ONLINE' OR c.location_received_at > :since ORDER BY u.display_name")
                .param("since", Timestamps.db(heardSince))
                .query(viewMapper).list();
    }

    public List<CourierView> viewsByIds(Collection<UUID> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        return jdbc.sql(VIEW_SELECT + " WHERE c.user_id IN (:ids)").param("ids", ids).query(viewMapper).list();
    }

    public int setStatus(UUID id, CourierStatus status) {
        return jdbc.sql("UPDATE couriers SET status = CAST(:s AS courier_status), updated_at = now() WHERE user_id = :id")
                .param("s", status.name())
                .param("id", id)
                .update();
    }

    /**
     * Records that we heard from the courier now, and moves the last-known position only if this
     * ping is newer than what we already have. Out-of-order batches therefore cannot move the
     * marker backwards. Returns whether the position advanced (empty if the courier is unknown).
     */
    public Optional<Boolean> updateLastKnown(UUID id, Ping newest, Instant receivedAt) {
        return jdbc.sql("""
                WITH prev AS (
                    SELECT location_at AS prev_at FROM couriers WHERE user_id = :id FOR UPDATE
                )
                UPDATE couriers c SET
                    location_received_at = :received,
                    location    = CASE WHEN prev.prev_at IS NULL OR prev.prev_at < :t
                                       THEN ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography
                                       ELSE c.location END,
                    location_at = CASE WHEN prev.prev_at IS NULL OR prev.prev_at < :t THEN :t ELSE c.location_at END,
                    updated_at  = now()
                FROM prev
                WHERE c.user_id = :id
                RETURNING (prev.prev_at IS NULL OR prev.prev_at < :t) AS advanced
                """)
                .param("id", id)
                .param("received", Timestamps.db(receivedAt))
                .param("t", Timestamps.db(newest.recordedAt()))
                .param("lat", newest.lat())
                .param("lng", newest.lng())
                .query(Boolean.class)
                .optional();
    }

    public void insertPings(UUID courierId, List<Ping> pings, Instant receivedAt) {
        jdbcTemplate.batchUpdate("""
                INSERT INTO courier_locations (courier_id, location, recorded_at, received_at, speed_mps, heading_deg, accuracy_m)
                VALUES (?, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography, ?, ?, ?, ?, ?)
                """, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Ping p = pings.get(i);
                ps.setObject(1, courierId);
                ps.setDouble(2, p.lng());
                ps.setDouble(3, p.lat());
                ps.setObject(4, Timestamps.db(p.recordedAt()));
                ps.setObject(5, Timestamps.db(receivedAt));
                setFloat(ps, 6, p.speedMps());
                setFloat(ps, 7, p.headingDeg());
                setFloat(ps, 8, p.accuracyM());
            }

            @Override
            public int getBatchSize() {
                return pings.size();
            }
        });
    }

    private static void setFloat(PreparedStatement ps, int idx, Float value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.REAL);
        } else {
            ps.setFloat(idx, value);
        }
    }
}
