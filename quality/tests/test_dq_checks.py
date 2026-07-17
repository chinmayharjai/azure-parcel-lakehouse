"""DQ tests: the gates, and the divergence experiment that is the meaningful observation.

The divergence test is the one that matters most — it constructs the exact defect the
platform exists to catch (a delivery whose scan arrived out of order) and proves the
naive method undercounts while the control total would block on it.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql.types import (BooleanType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

_ROOT = Path(__file__).resolve().parents[2]


def _load(relpath, mod):
    spec = importlib.util.spec_from_file_location(mod, _ROOT / relpath)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


dq = _load("quality/dq_checks.py", "dq_checks")
gold_common = _load("databricks/gold_common.py", "gold_common")

_SILVER = StructType([
    StructField("parcel_id", StringType()),
    StructField("scan_id", StringType()),
    StructField("lane_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("hub_id", StringType()),
    StructField("scan_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("sync_time", TimestampType()),
    StructField("event_seq", IntegerType()),
    StructField("is_out_of_order", BooleanType()),
    StructField("promised_hours", IntegerType()),
])

_BASE = datetime(2026, 6, 1, 0, 0)


def _row(pid, seq, stype, ev_h, sync_h, lane="LANE-1", seller="SLR-1", hub="HUB-001"):
    return (pid, f"{pid}-{seq}", lane, seller, hub, stype,
            _BASE + timedelta(hours=ev_h), _BASE + timedelta(hours=sync_h),
            seq, False, 24)


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=_SILVER)


# --- Pure count-based gates (no Spark needed, but grouped here) ----------------

def test_row_reconciliation_balances():
    ok = dq.check_row_reconciliation(bronze_rows=1000, silver_rows=940,
                                     quarantined_rows=20, dedup_removed=40)
    assert ok["passed"] is True
    bad = dq.check_row_reconciliation(bronze_rows=1000, silver_rows=940,
                                      quarantined_rows=20, dedup_removed=10)
    assert bad["passed"] is False
    assert bad["severity"] == dq.ERROR


def test_control_total_requires_exact_equality():
    assert dq.control_total_delivered(50000, 50000)["passed"] is True
    off = dq.control_total_delivered(50000, 49999)
    assert off["passed"] is False
    assert off["severity"] == dq.ERROR
    assert off["metric_value"] == 1.0            # the one-parcel gap is reported


def test_blocking_failures_selects_only_unpassed_errors():
    incidents = [
        dq.control_total_delivered(10, 10),        # pass
        dq.control_total_delivered(10, 9),         # fail, error -> blocks
        dq._incident("some_warn", passed=False, severity=dq.WARN),  # fail but warn
    ]
    blocking = dq.blocking_failures(incidents)
    assert len(blocking) == 1
    assert blocking[0]["check_name"] == "control_total_delivered"


# --- Spark-based validity checks ---------------------------------------------

def test_null_check_catches_bad_row(spark):
    rows = [_row("P1", 1, "PICKUP", 1, 1), (None, "x", "LANE-1", "SLR-1", "HUB-001",
            "HUB_IN", _BASE, _BASE, 2, False, 24)]
    incidents = dq.check_mandatory_nulls(_df(spark, rows), ["scan_id", "parcel_id"])
    by = {i["check_name"]: i for i in incidents}
    assert by["null_check_parcel_id"]["passed"] is False
    assert by["null_check_parcel_id"]["metric_value"] == 1.0


def test_referential_integrity_flags_orphans(spark):
    silver = _df(spark, [
        _row("P1", 1, "PICKUP", 1, 1, lane="LANE-1", seller="SLR-1"),
        _row("P2", 1, "PICKUP", 1, 1, lane="LANE-999", seller="SLR-404"),  # both orphan
    ])
    lanes = spark.createDataFrame([("LANE-1",)], ["lane_id"])
    sellers = spark.createDataFrame([("SLR-1",)], ["seller_id"])
    by = {i["check_name"]: i for i in dq.check_referential_integrity(silver, lanes, sellers)}
    assert by["ri_scan_has_known_lane"]["passed"] is False
    assert by["ri_scan_has_known_lane"]["severity"] == dq.ERROR     # orphan lane blocks
    assert by["ri_scan_has_known_seller"]["passed"] is False
    assert by["ri_scan_has_known_seller"]["severity"] == dq.WARN    # orphan seller warns


# --- THE divergence experiment -----------------------------------------------

def test_naive_arrival_order_undercounts_delivered(spark):
    """The meaningful observation, in miniature.

    P1 is genuinely delivered: PICKUP (event 1), DELIVERED (event 3). But a HUB_IN
    (event 2) ARRIVED last (sync 9) — an out-of-order arrival. The correct,
    event-time method counts P1 delivered; the naive method, taking the last-RECEIVED
    scan (the HUB_IN) as current state, sees 'in transit' and misses it. That's the
    undercount the control total would catch and block on."""
    rows = [
        _row("P1", 1, "PICKUP", ev_h=1, sync_h=1),
        _row("P1", 2, "HUB_IN", ev_h=2, sync_h=9),      # arrived LAST though it happened 2nd
        _row("P1", 3, "DELIVERED", ev_h=3, sync_h=3),   # happened last, arrived 3rd
        # P2: clean delivery, no inversion
        _row("P2", 1, "PICKUP", ev_h=1, sync_h=1),
        _row("P2", 2, "DELIVERED", ev_h=2, sync_h=2),
    ]
    silver = _df(spark, rows)
    lifecycle = gold_common.parcel_lifecycle(silver)

    d = dq.delivery_method_divergence(silver, lifecycle)
    assert d["correct_delivered"] == 2      # both really delivered
    assert d["naive_delivered"] == 1        # naive misses P1 (last-arrived scan is HUB_IN)
    assert d["divergence"] == 1
    assert d["divergence_pct"] == 50.0      # 1 of 2 in this tiny sample


def test_no_divergence_when_everything_arrives_in_order(spark):
    rows = [
        _row("P1", 1, "PICKUP", ev_h=1, sync_h=1),
        _row("P1", 2, "DELIVERED", ev_h=2, sync_h=2),
    ]
    silver = _df(spark, rows)
    lifecycle = gold_common.parcel_lifecycle(silver)
    d = dq.delivery_method_divergence(silver, lifecycle)
    assert d["divergence"] == 0
    assert d["correct_delivered"] == d["naive_delivered"] == 1
