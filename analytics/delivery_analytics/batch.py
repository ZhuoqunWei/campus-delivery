"""Daily batch job over the streaming outputs: delivery-time distributions, courier performance and
an exact active-orders-per-minute series for one date. Idempotent: it overwrites only the
partition for the requested date.

  python -m delivery_analytics.batch --input data/output --output data/daily --date 2026-09-28
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .session import build_session


def delivery_time_distribution(lifecycle: DataFrame) -> DataFrame:
    delivered = lifecycle.filter(F.col("status") == "DELIVERED")
    return delivered.groupBy("date", "merchant_name").agg(
        F.count("*").alias("deliveries"),
        F.percentile_approx("seconds_to_end", 0.5).alias("p50_seconds_to_deliver"),
        F.percentile_approx("seconds_to_end", 0.9).alias("p90_seconds_to_deliver"),
        F.percentile_approx("seconds_to_end", 0.95).alias("p95_seconds_to_deliver"),
        F.avg("seconds_to_accept").alias("avg_seconds_to_accept"),
        F.percentile_approx("seconds_to_accept", 0.95).alias("p95_seconds_to_accept"),
    )


def courier_performance(lifecycle: DataFrame) -> DataFrame:
    return lifecycle.filter(F.col("courier_id").isNotNull()).groupBy("date", "courier_id").agg(
        F.count(F.when(F.col("status") == "DELIVERED", True)).alias("deliveries"),
        F.count(F.when(F.col("status") == "CANCELLED", True)).alias("cancelled"),
        F.avg(F.when(F.col("status") == "DELIVERED", F.col("seconds_to_end"))).alias("avg_seconds_to_deliver"),
        F.avg("seconds_to_pickup").alias("avg_seconds_to_pickup"),
    )


def active_orders_exact(lifecycle: DataFrame) -> DataFrame:
    """Same interval-explode as the streaming query, but over the whole day, so orders longer than
    the streaming watermark are counted in full."""
    minutes = lifecycle.select(
        "order_id",
        F.explode(F.sequence(F.date_trunc("minute", "created_at"), F.date_trunc("minute", "ended_at"),
                             F.expr("interval 1 minute"))).alias("minute"),
    )
    return minutes.groupBy("minute").agg(F.count("*").alias("active_orders")) \
        .withColumn("date", F.to_date("minute"))


def run(spark: SparkSession, input_dir: str, output_dir: str, date: str) -> dict[str, int]:
    lifecycle = spark.read.parquet(f"{input_dir}/order_lifecycle").filter(F.col("date") == F.lit(date))
    results = {
        "delivery_time_distribution": delivery_time_distribution(lifecycle),
        "courier_performance": courier_performance(lifecycle),
        "active_orders_exact": active_orders_exact(lifecycle).filter(F.col("date") == F.lit(date)),
    }
    counts = {}
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    for name, df in results.items():
        df.write.mode("overwrite").partitionBy("date").parquet(f"{output_dir}/{name}")
        counts[name] = df.count()
    return counts


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/output")
    p.add_argument("--output", default="data/daily")
    p.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    a = p.parse_args(argv)
    spark = build_session("delivery-analytics-batch")
    counts = run(spark, a.input, a.output, a.date)
    print(f"batch for {a.date}: {counts}", file=sys.stderr)


if __name__ == "__main__":
    main()
