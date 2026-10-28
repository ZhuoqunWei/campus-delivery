"""Streaming tests against a real Kafka broker (Testcontainers). Needs Docker.

Phases (one broker, one scenario):
  1. first run        windows flush once the watermark passes them; late data is dropped
  2. restart          resuming from the same checkpoint writes nothing twice
  3. fresh replay x2  two independent runs over the same offsets produce identical Parquet content
  4. offset replay    starting from a per-partition offset yields exactly the suffix of run 3
"""

from __future__ import annotations

import time

import pytest
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from testcontainers.kafka import KafkaContainer

from delivery_analytics import COURIER_LOCATIONS_TOPIC, ORDER_EVENTS_TOPIC
from delivery_analytics.stream import StreamConfig, start_queries
from tests import scenario as S

pytestmark = pytest.mark.streaming

TABLES = [
    "order_minute_metrics", "active_orders_minutely", "courier_utilization", "order_lifecycle",
    "quarantine_order_events", "quarantine_order_lifecycle", "quarantine_pings",
]
ORDER_TABLES = ["order_minute_metrics", "active_orders_minutely", "order_lifecycle", "quarantine_order_events", "quarantine_order_lifecycle"]


@pytest.fixture(scope="module")
def kafka():
    with KafkaContainer(image="confluentinc/cp-kafka:7.6.0") as k:
        bootstrap = k.get_bootstrap_server()
        admin = KafkaAdminClient(bootstrap_servers=bootstrap)
        admin.create_topics([NewTopic(ORDER_EVENTS_TOPIC, 1, 1), NewTopic(COURIER_LOCATIONS_TOPIC, 1, 1)])
        admin.close()
        yield bootstrap


@pytest.fixture(scope="module")
def producer(kafka):
    p = KafkaProducer(bootstrap_servers=kafka, key_serializer=str.encode, value_serializer=str.encode, acks="all")
    yield p
    p.close()


def produce(producer, topic, records):
    for k, v in records:
        producer.send(topic, key=k, value=v)
    producer.flush()


def cfg_for(kafka, root, starting="earliest"):
    return StreamConfig(bootstrap=kafka, output_dir=f"{root}/out", checkpoint_dir=f"{root}/ckpt",
                        starting_offsets=starting, trigger_seconds=1,
                        order_watermark="2 minutes", ping_watermark="2 minutes", max_order_duration="30 minutes")


def read(spark, cfg, name):
    try:
        return spark.read.parquet(f"{cfg.output_dir}/{name}")
    except Exception:  # noqa: BLE001 - not written yet
        return None


def counts(spark, cfg):
    out = {}
    for n in TABLES:
        df = read(spark, cfg, n)
        out[n] = df.count() if df is not None else 0
    return out


def run_until_stable(spark, cfg, minimum: dict[str, int], settle_s: float = 6, timeout_s: float = 150):
    """Run the streaming queries until every table has at least `minimum` rows and nothing has
    changed for `settle_s` seconds, then stop them and return the final counts."""
    queries = start_queries(spark, cfg)
    try:
        deadline = time.time() + timeout_s
        last, stable_since = None, None
        while time.time() < deadline:
            for q in queries:
                if q.exception() is not None:
                    raise RuntimeError(f"query {q.name} failed: {q.exception()}")
            c = counts(spark, cfg)
            if c != last:
                last, stable_since = c, time.time()
            elif all(c[n] >= v for n, v in minimum.items()) and time.time() - stable_since >= settle_s:
                return c
            time.sleep(1)
        raise AssertionError(f"timed out; wanted at least {minimum}, got {last}")
    finally:
        for q in queries:
            q.stop()


def rows(df, drop=("processed_at",)):
    cols = [c for c in df.columns if c not in drop]
    return sorted(tuple(str(v) for v in r) for r in df.select(*cols).collect())


def by_minute(df):
    return {r["window_start"].strftime("%H:%M"): r.asDict() for r in df.collect()}


def test_streaming_first_run_and_restart(spark, kafka, producer, tmp_path_factory):
    cfg = cfg_for(kafka, tmp_path_factory.mktemp("run1"))

    # ---- 1. first run: scenario + a sentinel far in the future so every window can flush
    produce(producer, ORDER_EVENTS_TOPIC, S.order_events() + S.sentinel_terminal_order(60))
    produce(producer, COURIER_LOCATIONS_TOPIC, S.pings() + [S.sentinel_ping(60)])
    c = run_until_stable(spark, cfg, {
        "order_minute_metrics": 7, "active_orders_minutely": 7, "courier_utilization": 3,
        "order_lifecycle": 3, "quarantine_order_events": 3, "quarantine_order_lifecycle": 1, "quarantine_pings": 2,
    })
    m = by_minute(read(spark, cfg, "order_minute_metrics"))
    assert sorted(m) == ["10:00", "10:01", "10:02", "10:03", "10:04", "10:05", "10:06"]  # sentinel window (11:00) still open
    assert (m["10:00"]["created"], m["10:00"]["accepted"], m["10:00"]["time_to_accept_avg_s"], m["10:00"]["events"]) == (1, 1, 20.0, 3)
    assert sum(r["net_open_delta"] for r in m.values()) == 1
    active = {k: v["active_orders"] for k, v in by_minute(read(spark, cfg, "active_orders_minutely")).items()}
    assert active == {"10:00": 1, "10:01": 2, "10:02": 2, "10:03": 3, "10:04": 3, "10:05": 2, "10:06": 1}
    life = {r["order_id"]: r for r in read(spark, cfg, "order_lifecycle").collect()}
    assert sorted(life) == ["A", "B", "S60"] and life["A"]["seconds_to_end"] == 320
    util = {(r["courier_id"], r["window_start"].strftime("%H:%M")): r["utilization"] for r in read(spark, cfg, "courier_utilization").collect()}
    assert util[("k1", "10:00")] == 0.5 and util[("k1", "10:01")] == 1.0
    assert c["quarantine_order_events"] == 3 and c["quarantine_order_lifecycle"] == 1 and c["quarantine_pings"] == 2

    # ---- 1b. late data: watermark is ~10:58 now. An event at 10:00:15 is dropped; one at 10:59:30
    #          is on time and flushes together with the 11:00 sentinel window once 11:05 arrives.
    late = S.Envelopes(); late.next_id = 8001
    late.event("ORDER_CREATED", S.t(0, 15), S.order_view("LATE", "CREATED", S.t(0, 15)))
    ontime = S.Envelopes(); ontime.next_id = 8002
    ontime.event("ORDER_CREATED", S.t(59, 30), S.order_view("ONTIME", "CREATED", S.t(59, 30)))
    produce(producer, ORDER_EVENTS_TOPIC, late.records + ontime.records + [S.sentinel_order(65)])
    c2 = run_until_stable(spark, cfg, {"order_minute_metrics": 9})
    m2 = by_minute(read(spark, cfg, "order_minute_metrics"))
    assert sorted(m2) == sorted(m) + ["10:59", "11:00"]
    assert m2["10:00"]["created"] == 1 and m2["10:00"]["events"] == 3      # LATE was dropped, window not rewritten
    assert read(spark, cfg, "order_minute_metrics").filter("window_start = timestamp '2026-09-28 10:00:00'").count() == 1
    assert m2["10:59"]["created"] == 1                                      # ONTIME counted
    assert c2["order_lifecycle"] == 3

    # ---- 2. restart from the same checkpoint: no new data, nothing written twice
    c3 = run_until_stable(spark, cfg, {}, settle_s=8, timeout_s=60)
    assert c3 == c2
    ids = [r["event_id"] for r in read(spark, cfg, "order_lifecycle").collect()]
    assert len(ids) == len(set(ids))


def test_fresh_replays_are_identical_and_offset_replay_is_a_suffix(spark, kafka, tmp_path_factory):
    # Everything the previous test produced is still in Kafka. Two fresh runs must agree byte for
    # byte on every metric row (processed_at aside).
    a = cfg_for(kafka, tmp_path_factory.mktemp("replayA"))
    b = cfg_for(kafka, tmp_path_factory.mktemp("replayB"))
    minimum = {"order_minute_metrics": 9, "active_orders_minutely": 7, "courier_utilization": 3, "order_lifecycle": 3}
    ca = run_until_stable(spark, a, minimum)
    cb = run_until_stable(spark, b, minimum)
    assert ca == cb
    for name in TABLES:
        assert rows(read(spark, a, name)) == rows(read(spark, b, name)), name

    # Replayed as one batch, LATE is no longer late: the 10:00 window now counts it. Late-data
    # handling depends on arrival order, which is why the first run and a replay can differ here.
    ma = by_minute(read(spark, a, "order_minute_metrics"))
    assert ma["10:00"]["created"] == 2 and ma["10:00"]["events"] == 4

    # Replay from a specific offset: order A's six records (offsets 0-5) are skipped.
    k = 6
    c = cfg_for(kafka, tmp_path_factory.mktemp("replayC"),
                starting=f'{{"{ORDER_EVENTS_TOPIC}":{{"0":{k}}},"{COURIER_LOCATIONS_TOPIC}":{{"0":0}}}}')
    run_until_stable(spark, c, {"order_lifecycle": 2, "courier_utilization": 3})
    full = read(spark, a, "order_lifecycle").filter(f"kafka_offset >= {k}")
    assert rows(read(spark, c, "order_lifecycle")) == rows(full)
    assert sorted(r["order_id"] for r in read(spark, c, "order_lifecycle").collect()) == ["B", "S60"]
    assert rows(read(spark, c, "courier_utilization")) == rows(read(spark, a, "courier_utilization"))
    for name in ORDER_TABLES:
        df = read(spark, c, name)
        if df is not None and "kafka_offset" in df.columns:
            assert df.filter(f"kafka_offset < {k}").count() == 0, name
