import os

import pytest

from delivery_analytics.session import build_session


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    s = build_session("delivery-analytics-tests", extra={"spark.sql.shuffle.partitions": "2"})
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()
