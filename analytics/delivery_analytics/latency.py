"""End-to-end latency report from the Parquet outputs.

  order_lifecycle rows carry three clocks:
    occurred_at   when the order changed in Postgres (event row committed)
    kafka_ts      when the outbox relay produced it to Kafka
    processed_at  when the Spark micro-batch that wrote the row ran
  so   outbox lag  = kafka_ts - occurred_at
       spark lag   = processed_at - kafka_ts
       end-to-end  = processed_at - occurred_at
  Windowed metrics additionally report processed_at - window_end, which by design includes the
  watermark delay (a window can only be emitted once the watermark has passed it).

  python -m delivery_analytics.latency --input data/output
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .session import build_session


def _stats(df: DataFrame, col: str) -> dict:
    row = df.select(
        F.count(col).alias("n"),
        F.percentile_approx(col, 0.5).alias("p50"),
        F.percentile_approx(col, 0.95).alias("p95"),
        F.max(col).alias("max"),
        F.avg(col).alias("avg"),
    ).first()
    return {"n": row["n"], "p50_s": row["p50"], "p95_s": row["p95"], "max_s": row["max"], "avg_s": row["avg"]}


def report(spark: SparkSession, input_dir: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    life = spark.read.parquet(f"{input_dir}/order_lifecycle")
    secs = lambda a, b: (F.unix_micros(F.col(a)) - F.unix_micros(F.col(b))) / 1e6  # noqa: E731
    life = life.select(
        secs("kafka_ts", "occurred_at").alias("outbox_lag"),
        secs("processed_at", "kafka_ts").alias("spark_lag"),
        secs("processed_at", "occurred_at").alias("end_to_end"),
    )
    out["order_lifecycle.outbox_lag"] = _stats(life, "outbox_lag")
    out["order_lifecycle.spark_lag"] = _stats(life, "spark_lag")
    out["order_lifecycle.end_to_end"] = _stats(life, "end_to_end")
    for name in ("order_minute_metrics", "courier_utilization", "active_orders_minutely"):
        try:
            df = spark.read.parquet(f"{input_dir}/{name}")
        except Exception:  # noqa: BLE001 - table may not exist yet
            continue
        df = df.select(secs("processed_at", "window_end").alias("emit_after_window_end"))
        out[f"{name}.emit_after_window_end"] = _stats(df, "emit_after_window_end")
    return out


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/output")
    a = p.parse_args(argv)
    spark = build_session("delivery-analytics-latency")
    for name, s in report(spark, a.input).items():
        print(f"{name:45s} n={s['n']:6d}  p50={s['p50_s']:7.2f}s  p95={s['p95_s']:7.2f}s  max={s['max_s']:7.2f}s", file=sys.stdout)


if __name__ == "__main__":
    main()
