"""Structured Streaming job: Kafka -> per-minute metrics -> Parquet partitioned by date.

Exactly-once for the sinks comes from two pieces working together: the Kafka source records the
offsets of every micro-batch in the checkpoint, and the file sink keeps a metadata log of which
files each batch wrote. A restart re-executes an unfinished batch from the same offsets and the sink
ignores files already committed for that batch id, so no metric row is written twice.

Usage:
  python -m delivery_analytics.stream --bootstrap localhost:29092 --output data/output --checkpoint data/checkpoints
  python -m delivery_analytics.stream ... --available-now              # drain what is there, then stop
  python -m delivery_analytics.stream ... --starting-offsets '{"order-events.v1":{"0":120}}'
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from . import COURIER_LOCATIONS_TOPIC, ORDER_EVENTS_TOPIC
from . import metrics as M
from .session import build_session


@dataclass
class StreamConfig:
    bootstrap: str
    output_dir: str
    checkpoint_dir: str
    starting_offsets: str = "latest"          # latest | earliest | JSON per-partition offsets
    trigger_seconds: int | None = 10          # None => availableNow
    max_offsets_per_trigger: int | None = None
    order_watermark: str = "2 minutes"
    ping_watermark: str = "2 minutes"
    max_order_duration: str = "30 minutes"
    run_seconds: int | None = None            # stop after this long (demos, measurements)
    queries: list[str] = field(default_factory=lambda: [
        "order_minute_metrics", "active_orders_minutely", "courier_utilization", "order_lifecycle",
    ])


def read_topic(spark: SparkSession, cfg: StreamConfig, topic: str) -> DataFrame:
    starting = cfg.starting_offsets
    if starting not in ("latest", "earliest"):
        # per-partition JSON may mention several topics; keep only this one
        offsets = json.loads(starting)
        starting = json.dumps({topic: offsets[topic]}) if topic in offsets else "earliest"
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .option("failOnDataLoss", "false")
        .option("includeHeaders", "false")
    )
    if cfg.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(cfg.max_offsets_per_trigger))
    return reader.load()


def build_outputs(spark: SparkSession, cfg: StreamConfig) -> dict[str, DataFrame]:
    """Named DataFrames, each becoming one Parquet sink. Each sink has its own Kafka source so a
    slow or failed query never holds the others back."""
    out: dict[str, DataFrame] = {}
    if "order_minute_metrics" in cfg.queries:
        parsed = M.parse_order_events(read_topic(spark, cfg, ORDER_EVENTS_TOPIC))
        out["order_minute_metrics"] = M.order_minute_metrics(parsed.good, cfg.order_watermark)
        out["quarantine_order_events"] = parsed.quarantine
    if "active_orders_minutely" in cfg.queries:
        parsed = M.parse_order_events(read_topic(spark, cfg, ORDER_EVENTS_TOPIC))
        out["active_orders_minutely"] = M.active_orders_minutely(parsed.good, cfg.max_order_duration)
    if "order_lifecycle" in cfg.queries:
        parsed = M.parse_order_events(read_topic(spark, cfg, ORDER_EVENTS_TOPIC))
        lifecycle = M.order_lifecycle(parsed.good, cfg.order_watermark)
        out["order_lifecycle"] = lifecycle.good
        out["quarantine_order_lifecycle"] = lifecycle.quarantine
    if "courier_utilization" in cfg.queries:
        parsed = M.parse_pings(read_topic(spark, cfg, COURIER_LOCATIONS_TOPIC))
        out["courier_utilization"] = M.courier_utilization(parsed.good, cfg.ping_watermark)
        out["quarantine_pings"] = parsed.quarantine
    return out


def start_queries(spark: SparkSession, cfg: StreamConfig) -> list[StreamingQuery]:
    queries = []
    for name, df in build_outputs(spark, cfg).items():
        writer = (
            df.writeStream.queryName(name)
            .format("parquet")
            .option("path", f"{cfg.output_dir}/{name}")
            .option("checkpointLocation", f"{cfg.checkpoint_dir}/{name}")
            .partitionBy("date")
            .outputMode("append")
        )
        writer = writer.trigger(availableNow=True) if cfg.trigger_seconds is None \
            else writer.trigger(processingTime=f"{cfg.trigger_seconds} seconds")
        queries.append(writer.start())
    return queries


def run(cfg: StreamConfig, spark: SparkSession | None = None) -> None:
    spark = spark or build_session("delivery-analytics-stream")
    queries = start_queries(spark, cfg)
    if cfg.trigger_seconds is None:
        for q in queries:
            q.awaitTermination()
    elif cfg.run_seconds:
        spark.streams.awaitAnyTermination(timeout=cfg.run_seconds)
        for q in queries:
            q.stop()
    else:
        spark.streams.awaitAnyTermination()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--output", default="data/output")
    p.add_argument("--checkpoint", default="data/checkpoints")
    p.add_argument("--starting-offsets", default="latest")
    p.add_argument("--trigger-seconds", type=int, default=10)
    p.add_argument("--available-now", action="store_true", help="process everything available, then exit")
    p.add_argument("--max-offsets-per-trigger", type=int, default=None)
    p.add_argument("--order-watermark", default="2 minutes")
    p.add_argument("--ping-watermark", default="2 minutes")
    p.add_argument("--max-order-duration", default="30 minutes")
    p.add_argument("--run-seconds", type=int, default=None, help="stop after N seconds")
    a = p.parse_args(argv)
    cfg = StreamConfig(
        bootstrap=a.bootstrap, output_dir=a.output, checkpoint_dir=a.checkpoint,
        starting_offsets=a.starting_offsets,
        trigger_seconds=None if a.available_now else a.trigger_seconds,
        max_offsets_per_trigger=a.max_offsets_per_trigger,
        order_watermark=a.order_watermark, ping_watermark=a.ping_watermark,
        max_order_duration=a.max_order_duration, run_seconds=a.run_seconds,
    )
    print(f"streaming from {cfg.bootstrap} -> {cfg.output_dir} (checkpoint {cfg.checkpoint_dir}, "
          f"offsets={cfg.starting_offsets}, trigger={cfg.trigger_seconds or 'availableNow'})", file=sys.stderr)
    run(cfg)


if __name__ == "__main__":
    main()
