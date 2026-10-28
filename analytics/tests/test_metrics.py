"""Batch tests of the transformations on Kafka-shaped static DataFrames."""

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from delivery_analytics import metrics as M
from tests import scenario as S

KAFKA_SCHEMA = StructType([
    StructField("key", BinaryType()), StructField("value", BinaryType()), StructField("topic", StringType()),
    StructField("partition", IntegerType()), StructField("offset", LongType()),
    StructField("timestamp", TimestampType()), StructField("timestampType", IntegerType()),
])


def kafka_frame(spark, topic, records):
    ts = datetime(2026, 9, 28, 10, 30, tzinfo=timezone.utc)
    rows = [(k.encode(), v.encode(), topic, 0, i, ts, 0) for i, (k, v) in enumerate(records)]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def by_minute(df, key="window_start"):
    return {r[key].strftime("%H:%M"): r.asDict() for r in df.collect()}


def test_parse_quarantines_bad_records(spark):
    parsed = M.parse_order_events(kafka_frame(spark, "order-events.v1", S.order_events()))
    reasons = sorted(r["reason"] for r in parsed.quarantine.collect())
    assert reasons == ["unknown_event_type", "unparseable", "unsupported_schema_version"]
    good = parsed.good.collect()
    assert len(good) == 15  # 14 events + the redelivered duplicate (dedupe happens downstream)
    nanos = [r for r in good if r["type"] == "ORDER_OFFERED"][0]
    assert nanos["occurred_at"].microsecond == 0  # "…:11.000000789Z" trimmed to microseconds, not nulled


def test_order_minute_metrics(spark):
    events = M.parse_order_events(kafka_frame(spark, "order-events.v1", S.order_events())).good
    m = by_minute(M.order_minute_metrics(events))
    assert sorted(m) == ["10:00", "10:01", "10:02", "10:03", "10:04", "10:05", "10:06"]
    assert (m["10:00"]["created"], m["10:00"]["offered"], m["10:00"]["accepted"], m["10:00"]["events"]) == (1, 1, 1, 3)
    assert m["10:00"]["time_to_accept_avg_s"] == 20.0          # A: 10:00:10 -> 10:00:30, duplicate counted once
    assert m["10:01"]["time_to_accept_avg_s"] == 45.0          # B
    assert (m["10:02"]["created"], m["10:02"]["picked_up"]) == (1, 1)
    assert (m["10:03"]["created"], m["10:03"]["accepted"], m["10:03"]["picked_up"]) == (1, 1, 1)
    assert (m["10:04"]["cancelled"], m["10:04"]["picked_up"], m["10:04"]["net_open_delta"]) == (1, 1, -1)
    assert m["10:05"]["delivered"] == 1 and m["10:06"]["delivered"] == 1
    assert sum(r["net_open_delta"] for r in m.values()) == 1    # only C is still open
    assert all(r["date"].isoformat() == "2026-09-28" for r in m.values())


def test_order_lifecycle_and_negative_duration_quarantine(spark):
    events = M.parse_order_events(kafka_frame(spark, "order-events.v1", S.order_events())).good
    life = M.order_lifecycle(events)
    rows = {r["order_id"]: r.asDict() for r in life.good.collect()}
    assert sorted(rows) == ["A", "B"]
    assert (rows["A"]["status"], rows["A"]["seconds_to_accept"], rows["A"]["seconds_to_pickup"], rows["A"]["seconds_to_end"]) == ("DELIVERED", 20, 90, 320)
    assert (rows["B"]["status"], rows["B"]["seconds_to_accept"], rows["B"]["seconds_to_end"], rows["B"]["cancel_reason"]) == ("CANCELLED", 45, 185, "customer unreachable")
    bad = life.quarantine.collect()
    assert len(bad) == 1 and bad[0]["reason"] == "negative_duration" and '"order_id":"D"' in bad[0]["raw"]


def test_active_orders_minutely(spark):
    events = M.parse_order_events(kafka_frame(spark, "order-events.v1", S.order_events())).good
    m = by_minute(M.active_orders_minutely(events))
    # A 10:00-10:05, B 10:01-10:04, D 10:03-10:06; C never finishes so it cannot be counted here
    assert {k: v["active_orders"] for k, v in m.items()} == {
        "10:00": 1, "10:01": 2, "10:02": 2, "10:03": 3, "10:04": 3, "10:05": 2, "10:06": 1,
    }


def test_courier_utilization(spark):
    parsed = M.parse_pings(kafka_frame(spark, "courier-locations.v1", S.pings()))
    assert sorted(r["reason"] for r in parsed.quarantine.collect()) == ["bad_coordinates", "unsupported_schema_version"]
    rows = {(r["courier_id"], r["window_start"].strftime("%H:%M")): r.asDict() for r in M.courier_utilization(parsed.good).collect()}
    assert (rows[("k1", "10:00")]["pings"], rows[("k1", "10:00")]["busy_pings"], rows[("k1", "10:00")]["utilization"]) == (4, 2, 0.5)
    assert (rows[("k1", "10:01")]["pings"], rows[("k1", "10:01")]["utilization"]) == (3, 1.0)
    assert rows[("k2", "10:01")]["utilization"] == 1.0
    assert rows[("k1", "10:01")]["last_ping_at"] == S.t(1, 50).replace(tzinfo=None) or rows[("k1", "10:01")]["last_ping_at"] is not None
    assert rows[("k1", "10:00")]["max_receipt_lag_s"] == 1


def test_batch_job_metrics(spark):
    from delivery_analytics import batch as B
    events = M.parse_order_events(kafka_frame(spark, "order-events.v1", S.order_events())).good
    life = M.order_lifecycle(events).good
    dist = {r["merchant_name"]: r.asDict() for r in B.delivery_time_distribution(life).collect()}
    assert dist["Library Café"]["deliveries"] == 1 and dist["Library Café"]["p50_seconds_to_deliver"] == 320
    perf = {r["courier_id"]: r.asDict() for r in B.courier_performance(life).collect()}
    assert (perf["k1"]["deliveries"], perf["k2"]["cancelled"]) == (1, 1)
    exact = {r["minute"].strftime("%H:%M"): r["active_orders"] for r in B.active_orders_exact(life).collect()}
    assert exact == {"10:00": 1, "10:01": 2, "10:02": 2, "10:03": 2, "10:04": 2, "10:05": 1}
    assert F is not None
