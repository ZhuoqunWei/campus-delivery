package dev.campusdelivery.events;

import dev.campusdelivery.common.Json;
import dev.campusdelivery.common.Timestamps;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Repository
public class EventRepository {

    /**
     * Every event-producing transaction takes this lock right before it takes an id and holds it
     * until commit. That makes event ids commit in order, so a client that resumes from
     * "last id I saw" cannot miss an event that got a smaller id but committed later.
     * Cost: the tail of those transactions is serialized; at campus scale that is cheap.
     */
    private static final long EVENT_ORDER_LOCK = 4242;

    private static final String SELECT = """
            SELECT id, type, aggregate_type, aggregate_id, order_id, courier_id, customer_id,
                   payload::text AS payload, created_at
            FROM events
            """;

    private final JdbcClient jdbc;
    private final Json json;
    private final LiveEventBus bus;
    private final RowMapper<EventRecord> mapper;

    public EventRepository(JdbcClient jdbc, Json json, LiveEventBus bus) {
        this.jdbc = jdbc;
        this.json = json;
        this.bus = bus;
        this.mapper = (rs, i) -> new EventRecord(
                rs.getLong("id"),
                rs.getString("type"),
                rs.getString("aggregate_type"),
                rs.getObject("aggregate_id", UUID.class),
                rs.getObject("order_id", UUID.class),
                rs.getObject("courier_id", UUID.class),
                rs.getObject("customer_id", UUID.class),
                json.read(rs.getString("payload")),
                Timestamps.instant(rs, "created_at"));
    }

    /** Appends inside the caller's transaction and schedules live fan-out for after commit. */
    public EventRecord append(String type, String aggregateType, UUID aggregateId,
                              UUID orderId, UUID courierId, UUID customerId, Object payload) {
        jdbc.sql("SELECT pg_advisory_xact_lock(:k)").param("k", EVENT_ORDER_LOCK).query().singleValue();
        EventRecord saved = jdbc.sql("""
                INSERT INTO events (type, aggregate_type, aggregate_id, order_id, courier_id, customer_id, payload)
                VALUES (:type, :aggType, :aggId, :orderId, :courierId, :customerId, CAST(:payload AS jsonb))
                RETURNING id, type, aggregate_type, aggregate_id, order_id, courier_id, customer_id,
                          payload::text AS payload, created_at
                """)
                .param("type", type)
                .param("aggType", aggregateType)
                .param("aggId", aggregateId)
                .param("orderId", orderId)
                .param("courierId", courierId)
                .param("customerId", customerId)
                .param("payload", json.write(payload))
                .query(mapper)
                .single();

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    bus.publish(saved);
                }
            });
        } else {
            bus.publish(saved);
        }
        return saved;
    }

    public long maxId() {
        Long max = jdbc.sql("SELECT max(id) FROM events").query(Long.class).optional().orElse(null);
        return max == null ? 0L : max;
    }

    public List<EventRecord> findAfterForCustomer(long afterId, UUID customerId, int limit) {
        return jdbc.sql(SELECT + " WHERE id > :after AND customer_id = :who ORDER BY id LIMIT :limit")
                .param("after", afterId).param("who", customerId).param("limit", limit)
                .query(mapper).list();
    }

    public List<EventRecord> findAfterForCourier(long afterId, UUID courierId, int limit) {
        return jdbc.sql(SELECT + " WHERE id > :after AND courier_id = :who ORDER BY id LIMIT :limit")
                .param("after", afterId).param("who", courierId).param("limit", limit)
                .query(mapper).list();
    }

    public List<EventRecord> findAfter(long afterId, int limit) {
        return jdbc.sql(SELECT + " WHERE id > :after ORDER BY id LIMIT :limit")
                .param("after", afterId).param("limit", limit)
                .query(mapper).list();
    }

    // --- outbox ---

    public List<EventRecord> lockUnpublished(int limit) {
        return jdbc.sql(SELECT + " WHERE published_at IS NULL ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED")
                .param("limit", limit)
                .query(mapper).list();
    }

    public void markPublished(List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.sql("UPDATE events SET published_at = now() WHERE id IN (:ids)")
                .param("ids", ids)
                .update();
    }
}
