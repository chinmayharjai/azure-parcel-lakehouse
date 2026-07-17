"""Bronze tests: the raw layer must not drop anything.

The one property that matters: a malformed line is preserved in the rescue column,
not discarded. Everything else in bronze is faithful passthrough; the rescue
behaviour is the only real logic, and it's the thing that, if broken, loses data
silently at the very first step.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_DBX = Path(__file__).resolve().parents[1]


def _load(fname: str, mod_name: str):
    """Load a job module whose filename starts with a digit (not importable by name)."""
    spec = importlib.util.spec_from_file_location(mod_name, _DBX / fname)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


bronze = _load("01_bronze_ingest.py", "bronze_ingest")
from schema import RESCUE_COLUMN  # noqa: E402


GOOD = (
    '{"scan_id":"PCL-1-S01","parcel_id":"PCL-1","lane_id":"LANE-1","seller_id":"SLR-1",'
    '"hub_id":"HUB-001","scan_type":"PICKUP","event_time":"2026-06-10T08:00:00+00:00",'
    '"sync_time":"2026-06-10T08:05:00+00:00","customer_phone":"+919876543210","promised_hours":48}'
)


def test_good_rows_parse_into_typed_columns(spark):
    df = bronze.to_bronze(spark.createDataFrame([(GOOD,)], ["value"]))
    row = df.collect()[0]
    assert row["scan_id"] == "PCL-1-S01"
    assert row["parcel_id"] == "PCL-1"
    assert row["promised_hours"] == 48
    assert row[RESCUE_COLUMN] is None
    # partition columns derived from sync_time (arrival), not event_time
    assert row["dt"] == "2026-06-10"
    assert row["hour"] == "08"


def test_malformed_line_is_rescued_not_dropped(spark):
    """The whole contract of a raw layer: nothing vanishes. A broken line must appear
    in the output with its raw text in the rescue column."""
    rows = [(GOOD,), ("{this is not valid json",), (GOOD.replace("PCL-1", "PCL-2"),)]
    df = bronze.to_bronze(spark.createDataFrame(rows, ["value"]))

    assert df.count() == 3, "a row was dropped — the raw layer lost data"
    rescued = df.filter(df[RESCUE_COLUMN].isNotNull()).collect()
    assert len(rescued) == 1
    assert "not valid json" in rescued[0][RESCUE_COLUMN]
    # rescued rows still land somewhere — under the _rescued partition sentinel
    assert rescued[0]["dt"] == "_rescued"


def test_partition_is_by_arrival_time(spark):
    """A scan that happened at 08:00 but synced at 14:30 must partition under hour=14
    — bronze inherits landing's arrival-time partitioning."""
    late = GOOD.replace('"sync_time":"2026-06-10T08:05:00+00:00"',
                        '"sync_time":"2026-06-10T14:30:00+00:00"')
    df = bronze.to_bronze(spark.createDataFrame([(late,)], ["value"]))
    row = df.collect()[0]
    assert row["hour"] == "14"
    assert row["event_time"].hour == 8  # event time preserved, just not the partition


def test_bronze_keeps_ingest_timestamp(spark):
    df = bronze.to_bronze(spark.createDataFrame([(GOOD,)], ["value"]))
    assert "_bronze_ingest_time" in df.columns
    assert df.collect()[0]["_bronze_ingest_time"] is not None
