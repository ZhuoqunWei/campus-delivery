from __future__ import annotations

import os
import sys
import time

from pyspark.sql import SparkSession

from . import KAFKA_PACKAGE


def build_session(app_name: str = "delivery-analytics", local: bool = True, extra: dict | None = None) -> SparkSession:
    # Workers must run the same interpreter as the driver (a venv is not on the workers' PATH).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # Every clock in this pipeline is UTC: the JVM (launched as a child process, inherits TZ), the
    # SQL session, and the Python driver that materialises collected timestamps as naive datetimes.
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    b = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.ui.enabled", os.environ.get("SPARK_UI", "false"))
    )
    if local:
        b = b.master(os.environ.get("SPARK_MASTER", "local[*]"))
    for k, v in (extra or {}).items():
        b = b.config(k, v)
    return b.getOrCreate()
