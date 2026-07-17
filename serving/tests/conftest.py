"""Spark session for the serving tests (Cosmos document builder)."""

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
        .appName("serving-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
