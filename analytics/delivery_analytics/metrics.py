"""Pure DataFrame transformations. Every function works on both static and streaming DataFrames so
the same code is unit-tested in batch and run in Structured Streaming.

Metric definitions
------------------
order_minute_metrics      one row per 1-minute event-time window: counts of each order event type,
                          net_open_delta (= created - delivered - cancelled; cumulative sum gives the
                          near-real-time open-order count) and time-to-accept stats for orders
                          accepted in that minute (accepted_at - created_at, both from the payload).
active_orders_minutely    exact number of orders active in each minute. Known only when an order
                          ends, so it is computed from terminal events by exploding the
                          [created_at, ended_at] interval into minutes. The watermark on this query is
                          therefore sized to the longest order we are willing to account for.
courier_utilization       per courier per minute: share of location pings that carried an order id
                          (couriers ping every few seconds, so this is time-on-task within the minute).
order_lifecycle           one row per finished order with every timestamp and derived durations;
                          rows with a negative duration are quarantined instead of dropped.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schemas import COURIER_LOCATION, ORDER_EVENT, ORDER_EVENT_TYPES, TERMINAL_TYPES


def parse_ts(col):
    """ISO-8601 string -> timestamp. Trims fractional seconds beyond microseconds first."""
    trimmed = F.regexp_replace(col, r"(\.\d{6})\d+", r"$1")
    return F.to_timestamp(trimmed)


@dataclass
class Parsed:
    good: DataFrame
    quarantine: DataFrame


def _quarantine(df: DataFrame, reason_col, topic: str) -> DataFrame:
    return df.select(
        F.lit(topic).alias("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("value").cast("string").alias("raw"),
        reason_col.alias("reason"),
        F.current_timestamp().alias("processed_at"),
        F.to_date(F.col("timestamp")).alias("date"),
    )


def parse_order_events(raw: DataFrame) -> Parsed:
    """raw = Kafka source rows (key, value, topic, partition, offset, timestamp)."""
    parsed = raw.withColumn("json", F.from_json(F.col("value").cast("string"), ORDER_EVENT))
    known = F.array([F.lit(t) for t in sorted(ORDER_EVENT_TYPES)])
    # from_json yields an all-null struct (not a null) for input that is not a JSON object
    unparseable = F.col("json").isNull() | (
        F.col("json.schemaVersion").isNull() & F.col("json.eventId").isNull()
        & F.col("json.type").isNull() & F.col("json.occurredAt").isNull()
    )
    reason = (
        F.when(unparseable, "unparseable")
        .when(F.coalesce(F.col("json.schemaVersion"), F.lit(-1)) != 1, "unsupported_schema_version")
        .when(F.col("json.eventId").isNull() | F.col("json.orderId").isNull(), "missing_identity")
        .when(F.col("json.type").isNull() | ~F.array_contains(known, F.col("json.type")), "unknown_event_type")
        .when(parse_ts(F.col("json.occurredAt")).isNull(), "bad_timestamp")
    )
    with_reason = parsed.withColumn("reason", reason)
    good = with_reason.filter(F.col("reason").isNull()).select(
        F.col("json.eventId").alias("event_id"),
        F.col("json.type").alias("type"),
        parse_ts(F.col("json.occurredAt")).alias("occurred_at"),
        F.col("json.orderId").alias("order_id"),
        F.col("json.courierId").alias("courier_id"),
        F.col("json.customerId").alias("customer_id"),
        F.col("json.payload.order.status").alias("status"),
        F.col("json.payload.order.merchant.id").alias("merchant_id"),
        F.col("json.payload.order.merchant.name").alias("merchant_name"),
        parse_ts(F.col("json.payload.order.createdAt")).alias("created_at"),
        parse_ts(F.col("json.payload.order.acceptedAt")).alias("accepted_at"),
        parse_ts(F.col("json.payload.order.pickedUpAt")).alias("picked_up_at"),
        parse_ts(F.col("json.payload.order.deliveredAt")).alias("delivered_at"),
        parse_ts(F.col("json.payload.order.cancelledAt")).alias("cancelled_at"),
        F.col("json.payload.order.cancelReason").alias("cancel_reason"),
        F.col("json.payload.distanceMeters").alias("offer_distance_m"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    bad = _quarantine(with_reason.filter(F.col("reason").isNotNull()), F.col("reason"), "order-events.v1")
    return Parsed(good, bad)


def parse_pings(raw: DataFrame) -> Parsed:
    parsed = raw.withColumn("json", F.from_json(F.col("value").cast("string"), COURIER_LOCATION))
    unparseable = F.col("json").isNull() | (
        F.col("json.schemaVersion").isNull() & F.col("json.courierId").isNull()
        & F.col("json.recordedAt").isNull() & F.col("json.lat").isNull()
    )
    reason = (
        F.when(unparseable, "unparseable")
        .when(F.coalesce(F.col("json.schemaVersion"), F.lit(-1)) != 1, "unsupported_schema_version")
        .when(F.col("json.courierId").isNull(), "missing_identity")
        .when(parse_ts(F.col("json.recordedAt")).isNull(), "bad_timestamp")
        .when(F.col("json.lat").isNull() | F.col("json.lng").isNull()
              | (F.abs(F.col("json.lat")) > 90) | (F.abs(F.col("json.lng")) > 180), "bad_coordinates")
    )
    with_reason = parsed.withColumn("reason", reason)
    good = with_reason.filter(F.col("reason").isNull()).select(
        F.col("json.courierId").alias("courier_id"),
        F.col("json.lat").alias("lat"),
        F.col("json.lng").alias("lng"),
        parse_ts(F.col("json.recordedAt")).alias("recorded_at"),
        parse_ts(F.col("json.receivedAt")).alias("received_at"),
        F.col("json.speedMps").alias("speed_mps"),
        F.col("json.orderId").alias("order_id"),
        F.col("timestamp").alias("kafka_ts"),
    )
    bad = _quarantine(with_reason.filter(F.col("reason").isNotNull()), F.col("reason"), "courier-locations.v1")
    return Parsed(good, bad)


def _dedupe(df: DataFrame, cols: list[str]) -> DataFrame:
    """The outbox is at-least-once; drop redelivered events. Bounded state in streaming."""
    return df.dropDuplicatesWithinWatermark(cols) if df.isStreaming else df.dropDuplicates(cols)


def _count_type(t: str):
    return F.count(F.when(F.col("type") == t, True)).alias(t.lower().replace("order_", "").replace("offer_", "offers_"))


def order_minute_metrics(events: DataFrame, watermark: str = "2 minutes") -> DataFrame:
    e = _dedupe(events.withWatermark("occurred_at", watermark), ["event_id"])
    tta = F.when(F.col("type") == "ORDER_ACCEPTED",
                 F.unix_timestamp("accepted_at") - F.unix_timestamp("created_at"))
    agg = e.groupBy(F.window("occurred_at", "1 minute")).agg(
        _count_type("ORDER_CREATED"),
        _count_type("ORDER_OFFERED"),
        _count_type("OFFER_EXPIRED"),
        _count_type("OFFER_DECLINED"),
        _count_type("ORDER_ACCEPTED"),
        _count_type("ORDER_PICKED_UP"),
        _count_type("ORDER_DELIVERED"),
        _count_type("ORDER_CANCELLED"),
        F.avg(tta).alias("time_to_accept_avg_s"),
        F.max(tta).alias("time_to_accept_max_s"),
        F.min(tta).alias("time_to_accept_min_s"),
        F.count("*").alias("events"),
        F.max("occurred_at").alias("last_event_at"),
    )
    return agg.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "created", "offered", "offers_expired", "offers_declined", "accepted", "picked_up", "delivered", "cancelled",
        (F.col("created") - F.col("delivered") - F.col("cancelled")).alias("net_open_delta"),
        "time_to_accept_avg_s", "time_to_accept_max_s", "time_to_accept_min_s", "events", "last_event_at",
        F.current_timestamp().alias("processed_at"),
        F.to_date("window.start").alias("date"),
    )


def order_lifecycle(events: DataFrame, watermark: str = "2 minutes") -> Parsed:
    e = _dedupe(events.withWatermark("occurred_at", watermark), ["event_id"])
    terminal = e.filter(F.col("type").isin(*TERMINAL_TYPES))
    ended_at = F.coalesce("delivered_at", "cancelled_at", "occurred_at")
    rows = terminal.select(
        "event_id", "order_id", "status", "courier_id", "customer_id", "merchant_id", "merchant_name",
        "created_at", "accepted_at", "picked_up_at", ended_at.alias("ended_at"), "cancel_reason",
        (F.unix_timestamp("accepted_at") - F.unix_timestamp("created_at")).alias("seconds_to_accept"),
        (F.unix_timestamp("picked_up_at") - F.unix_timestamp("accepted_at")).alias("seconds_to_pickup"),
        (F.unix_timestamp(ended_at) - F.unix_timestamp("created_at")).alias("seconds_to_end"),
        "occurred_at", "kafka_ts", "kafka_partition", "kafka_offset",
        F.current_timestamp().alias("processed_at"),
        F.to_date(ended_at).alias("date"),
    )
    negative = (
        (F.col("seconds_to_accept") < 0) | (F.col("seconds_to_pickup") < 0) | (F.col("seconds_to_end") < 0)
    )
    good = rows.filter(~F.coalesce(negative, F.lit(False)))
    bad = rows.filter(F.coalesce(negative, F.lit(False))).select(
        F.lit("order-events.v1").alias("topic"),
        F.col("kafka_partition").alias("partition"),
        F.col("kafka_offset").alias("offset"),
        F.col("kafka_ts"),
        F.to_json(F.struct("event_id", "order_id", "status", "created_at", "accepted_at", "picked_up_at", "ended_at")).alias("raw"),
        F.lit("negative_duration").alias("reason"),
        F.col("processed_at"),
        F.to_date("kafka_ts").alias("date"),
    )
    return Parsed(good, bad)


def active_orders_minutely(events: DataFrame, max_order_duration: str = "30 minutes") -> DataFrame:
    """Exact active-orders-per-minute from finished orders. Each terminal event is exploded into the
    minutes its order was open; the single watermark is on that minute column and is sized to the
    longest order we are willing to account for. A minute is final once the watermark (driven by
    order end times) passes it; orders longer than `max_order_duration` lose their earliest
    minutes and are reconciled by the daily batch job."""
    terminal = events.filter(F.col("type").isin(*TERMINAL_TYPES)).filter(F.col("created_at").isNotNull())
    ended_at = F.coalesce("delivered_at", "cancelled_at", "occurred_at")
    minutes = terminal.select(
        "order_id", "event_id",
        F.explode(F.sequence(
            F.date_trunc("minute", F.col("created_at")),
            F.date_trunc("minute", ended_at),
            F.expr("interval 1 minute"),
        )).alias("minute"),
    )
    # One watermark only (Spark refuses a second one); dedupe on (event, minute) so a redelivered
    # terminal event does not double-count while every minute of a genuine event survives.
    minutes = _dedupe(minutes.withWatermark("minute", max_order_duration), ["event_id", "minute"])
    agg = minutes.groupBy(F.window("minute", "1 minute")).agg(F.count("*").alias("active_orders"))
    return agg.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "active_orders",
        F.current_timestamp().alias("processed_at"),
        F.to_date("window.start").alias("date"),
    )


def courier_utilization(pings: DataFrame, watermark: str = "2 minutes") -> DataFrame:
    p = pings.withWatermark("recorded_at", watermark)
    agg = p.groupBy(F.window("recorded_at", "1 minute"), "courier_id").agg(
        F.count("*").alias("pings"),
        F.count("order_id").alias("busy_pings"),
        F.avg("speed_mps").alias("avg_speed_mps"),
        F.max_by("lat", "recorded_at").alias("last_lat"),
        F.max_by("lng", "recorded_at").alias("last_lng"),
        F.max("recorded_at").alias("last_ping_at"),
        F.max(F.unix_timestamp("received_at") - F.unix_timestamp("recorded_at")).alias("max_receipt_lag_s"),
    )
    return agg.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "courier_id", "pings", "busy_pings",
        (F.col("busy_pings") / F.col("pings")).alias("utilization"),
        "avg_speed_mps", "last_lat", "last_lng", "last_ping_at", "max_receipt_lag_s",
        F.current_timestamp().alias("processed_at"),
        F.to_date("window.start").alias("date"),
    )
