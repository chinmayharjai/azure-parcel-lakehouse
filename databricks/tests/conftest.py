"""Local Spark session for the transform tests.

Pure-function transforms need only a plain SparkSession — no Delta extension, no
storage — which keeps the suite fast enough to run on every PR. The Delta I/O lives
in each job's main(), which these tests deliberately do not exercise (there's nothing
to learn from testing that Spark can write a file).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# The job modules live one level up and import each other by bare name (databricks/
# is a notebook-style flat dir, not a package), so put it on the path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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
        .appName("parcel-transform-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
