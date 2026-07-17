"""Spark session for the Spark-based DQ tests (the pure gate tests don't use it)."""

from __future__ import annotations

import pytest

try:
    from pyspark.sql import SparkSession
    _HAVE_SPARK = True
except ImportError:
    _HAVE_SPARK = False


@pytest.fixture(scope="session")
def spark():
    if not _HAVE_SPARK:
        pytest.skip("pyspark not installed")
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("dq-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
