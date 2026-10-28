# Delivery analytics (Spark Structured Streaming)

Consumes Campus Delivery's Kafka topics and writes per-minute metrics as Parquet partitioned by
date. PySpark 4.1, one process, four streaming queries, each with its own checkpoint.

```
order-events.v1 ─┬─▶ order_minute_metrics     counts per event type, net_open_delta, time-to-accept   (watermark 2 min)
                 ├─▶ active_orders_minutely   exact active orders per minute from finished orders    (watermark 30 min)
                 └─▶ order_lifecycle          one row per finished order + durations; negatives quarantined
courier-locations.v1 ─▶ courier_utilization   per courier per minute: busy pings / pings            (watermark 2 min)
                                              + quarantine_* tables for anything that failed parsing
```

## Run

```bash
# against the compose stack (Kafka EXTERNAL listener on :29092)
cd analytics && python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-test.txt
JAVA_HOME=$(/usr/libexec/java_home -v 21) .venv/bin/python -m delivery_analytics.stream \
    --bootstrap localhost:29092 --output data/output --checkpoint data/checkpoints --starting-offsets latest

# or as a compose service (writes to analytics/data/compose/output)
docker compose up analytics

# latency report and the daily batch job over what the stream wrote
.venv/bin/python -m delivery_analytics.latency --input data/output
.venv/bin/python -m delivery_analytics.batch --input data/output --output data/daily --date 2026-09-29
```

Tests (`pytest`): batch tests of every transformation on Kafka-shaped DataFrames, plus a streaming
test that starts a Kafka container, produces a known day, and checks watermark flushing, late-data
dropping, checkpoint restart, deterministic replay, and replay from a per-partition offset.

## Metric definitions

| Table | Grain | Definition |
|---|---|---|
| `order_minute_metrics` | 1-minute event-time window | Count of each event type in the window; `net_open_delta = created − delivered − cancelled` (its running sum is the near-real-time open-order count); `time_to_accept_*` over orders accepted in the window, from `acceptedAt − createdAt` in the event payload. |
| `active_orders_minutely` | 1-minute window | Exact number of orders that were open during the minute. Known only when an order finishes, so it is computed from terminal events by exploding `[created_at, ended_at]` into minutes. Emitted 30 minutes after the minute; orders longer than 30 minutes lose their earliest minutes and are reconciled by the batch job. |
| `courier_utilization` | courier × 1-minute window | `busy_pings / pings`: couriers ping every few seconds and each ping carries the active order id, so this is time-on-task within the minute. Also last position and max device-to-server receipt lag. |
| `order_lifecycle` | one finished order | Every timestamp plus `seconds_to_accept`, `seconds_to_pickup`, `seconds_to_end`; three clocks (`occurred_at`, `kafka_ts`, `processed_at`) for latency. |
| `quarantine_*` | one bad record | Unparseable JSON, unsupported `schemaVersion`, unknown event type, bad timestamp or coordinates, negative durations. Kept with the raw value and a reason, never dropped. |

Daily batch (`batch.py`): delivery-time distribution per merchant (p50/p90/p95), courier
performance, and an exact `active_orders_exact` series that covers orders of any length.

## Correctness

- **Watermarks.** Windows are emitted once the event-time watermark passes them (append mode), so
  a row is written once and never updated. Events older than the watermark are dropped at the
  aggregation, which the streaming test proves: a record at 10:00:15 arriving when the watermark is
  10:58 leaves the already-written 10:00 row untouched.
- **At-least-once input.** The outbox may redeliver an event; `dropDuplicatesWithinWatermark`
  on `eventId` removes it with bounded state. The test's redelivered `ORDER_ACCEPTED` is counted once.
- **Exactly-once output.** Kafka offsets are committed to the checkpoint per micro-batch and the
  Parquet sink keeps a per-batch metadata log, so a restart re-runs an unfinished batch from the
  same offsets and the sink ignores files already committed. Readers use that log, so they never
  see a half-written batch.
- **Replay.** Two fresh runs over the same offsets produce byte-identical metric rows, and a run
  from `{"order-events.v1":{"0":6}}` produces exactly the suffix of the full run. One honest caveat:
  late-data handling depends on arrival order, so a record that was dropped as late in real time is
  counted when the same offsets are replayed as one batch.
- **Clocks.** Everything is UTC: the JVM, the SQL session, and the Python driver. Device
  timestamps (`recordedAt`) drive the utilization window; server receipt time is kept alongside.

## Measured

Seven-minute run on 2026-09-29 against the compose stack (10 simulated couriers, ~2 orders/min,
5-second trigger, `--starting-offsets latest`), Apple M-series laptop, local[*] Spark, Kafka in
Docker. `python -m delivery_analytics.latency --input data/live/output`:

| Measure | n | p50 | p95 | max |
|---|---|---|---|---|
| Outbox lag (Postgres commit → Kafka) | 17 | 0.24 s | 0.49 s | 0.49 s |
| Spark lag (Kafka → Parquet row visible) | 17 | 2.62 s | 5.00 s | 5.00 s |
| **End-to-end (order finished in DB → `order_lifecycle` row)** | 17 | **2.69 s** | **5.24 s** | 5.24 s |
| `order_minute_metrics` emitted after window end | 4 | 130.0 s | 135.0 s | 135.0 s |
| `courier_utilization` emitted after window end | 50 | 130.0 s | 130.0 s | 130.0 s |

Reading the numbers: per-row latency is bounded by the trigger interval (5 s) plus one
micro-batch; the outbox relay itself adds a quarter of a second. Windowed rows land two minutes
after the window closes because the watermark is two minutes: that delay is a chosen trade-off
against late pings, not processing time. `active_orders_minutely` (30-minute watermark) did not
emit within a seven-minute run, as designed. No record was quarantined on real traffic; the
quarantine paths are exercised by the tests.

**Replay throughput.** `--starting-offsets earliest --available-now` over the whole week of history
in Kafka (116,334 location pings + 4,130 order events; the four queries read ~128k records in total)
finished in 20 s wall clock including JVM start-up, on the same laptop with the compose stack running:
roughly 6,000 records/s end to end, with zero quarantined records. Live traffic is a few events per
second, so the streaming job idles most of each trigger.
