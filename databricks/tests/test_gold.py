"""Gold tests: parcel lifecycle (event-time correctness), ops aggregates, SLA star.

The lifecycle tests are the important ones — they pin that delivery is computed on
EVENT time, which is the correctness claim the whole platform is built to make good
on and the thing M6 will measure.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql.types import (BooleanType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

_DBX = Path(__file__).resolve().parents[1]


def _load(fname, mod):
    spec = importlib.util.spec_from_file_location(mod, _DBX / fname)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


gold_common = _load("gold_common.py", "gold_common")
ops = _load("03_gold_ops_aggregates.py", "gold_ops")
sla = _load("04_gold_sla_mart.py", "gold_sla")

_SILVER = StructType([
    StructField("parcel_id", StringType()),
    StructField("scan_id", StringType()),
    StructField("lane_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("hub_id", StringType()),
    StructField("scan_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("sync_time", TimestampType()),
    StructField("promised_hours", IntegerType()),
    StructField("is_out_of_order", BooleanType()),
    StructField("is_late_arrival", BooleanType()),
])


def _t(day, h):
    # Naive UTC, deliberately: the session timezone is UTC (see conftest), and PySpark
    # returns TimestampType to the driver as naive Python datetimes in that zone. A
    # tz-aware expected value would never equal the naive value Spark hands back, even
    # though both mean the same instant. Building naive keeps input and expected in the
    # same representation Spark uses.
    return datetime(2026, 6, day, h, 0)


def _scan(pid, stype, ev_day, ev_h, promised=24, hub="HUB-001", lane="LANE-1",
          sync_day=None, sync_h=None, ooo=False, late=False, seller="SLR-1"):
    st = _t(sync_day or ev_day, sync_h if sync_h is not None else ev_h)
    return (pid, f"{pid}-{stype}-{ev_day}{ev_h}", lane, seller, hub, stype,
            _t(ev_day, ev_h), st, promised, ooo, late)


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=_SILVER)


# --- parcel_lifecycle: delivery is on EVENT time ------------------------------

def test_delivered_parcel_lifecycle(spark):
    rows = [
        _scan("P1", "PICKUP", 1, 8, promised=24),
        _scan("P1", "HUB_IN", 1, 12),
        _scan("P1", "DELIVERED", 1, 20),   # 12h after pickup, promised 24 -> on time
    ]
    lc = gold_common.parcel_lifecycle(_df(spark, rows)).collect()[0]
    assert lc["pickup_time"] == _t(1, 8)
    assert lc["delivered_time"] == _t(1, 20)
    assert lc["is_delivered"] is True
    assert lc["actual_hours"] == 12.0
    assert lc["is_breach"] is False
    assert lc["exception_reason"] is None


def test_delivery_uses_event_time_not_arrival(spark):
    """The crux. The DELIVERED scan happened at 20:00 (event) but ARRIVED at 09:00
    next day (sync), and an early hub scan arrived even later. Lifecycle must read
    delivered_time from EVENT time = 20:00, giving 12h — a naive arrival-ordered
    computation would get a different, wrong answer. This is what M6 quantifies."""
    rows = [
        _scan("P1", "PICKUP", 1, 8, sync_day=1, sync_h=8),
        _scan("P1", "HUB_IN", 1, 12, sync_day=2, sync_h=10, ooo=True),   # arrived very late
        _scan("P1", "DELIVERED", 1, 20, sync_day=2, sync_h=9, ooo=True), # delivered-scan arrived before hub_in
    ]
    lc = gold_common.parcel_lifecycle(_df(spark, rows)).collect()[0]
    assert lc["delivered_time"] == _t(1, 20)   # event time, not the 2nd-day arrival
    assert lc["actual_hours"] == 12.0
    assert lc["is_delivered"] is True


def test_breach_is_flagged(spark):
    rows = [
        _scan("P1", "PICKUP", 1, 8, promised=24),
        _scan("P1", "DELIVERED", 2, 20),   # 36h later, promised 24 -> breach
    ]
    lc = gold_common.parcel_lifecycle(_df(spark, rows)).collect()[0]
    assert lc["actual_hours"] == 36.0
    assert lc["is_breach"] is True
    assert lc["exception_reason"] == "sla_breach"


def test_rto_parcel(spark):
    rows = [
        _scan("P1", "PICKUP", 1, 8),
        _scan("P1", "FAILED_ATTEMPT", 1, 18),
        _scan("P1", "RTO_INITIATED", 2, 6),
        _scan("P1", "RTO_DELIVERED", 3, 10),
    ]
    lc = gold_common.parcel_lifecycle(_df(spark, rows)).collect()[0]
    assert lc["is_delivered"] is False       # not delivered to the customer
    assert lc["exception_reason"] == "rto"


def test_in_transit_parcel(spark):
    rows = [
        _scan("P1", "PICKUP", 1, 8),
        _scan("P1", "HUB_IN", 1, 14),        # never delivered
    ]
    lc = gold_common.parcel_lifecycle(_df(spark, rows)).collect()[0]
    assert lc["is_delivered"] is False
    assert lc["actual_hours"] is None
    assert lc["exception_reason"] == "in_transit"


def test_delivered_after_failed_attempts(spark):
    rows = [
        _scan("P1", "PICKUP", 1, 8, promised=72),
        _scan("P1", "FAILED_ATTEMPT", 1, 18),
        _scan("P1", "DELIVERED", 2, 9),      # within 72h, but had a failed attempt
    ]
    lc = gold_common.parcel_lifecycle(_df(spark, rows)).collect()[0]
    assert lc["is_delivered"] is True
    assert lc["is_breach"] is False
    assert lc["exception_reason"] == "delivered_after_failed_attempts"


# --- ops aggregates -----------------------------------------------------------

def test_ops_failed_delivery_rate_and_zero_safety(spark):
    rows = [
        _scan("P1", "PICKUP", 1, 8, hub="HUB-001"),
        _scan("P2", "PICKUP", 1, 8, hub="HUB-001"),
        _scan("P1", "FAILED_ATTEMPT", 1, 8, hub="HUB-001"),
    ]
    df = _df(spark, rows)
    lc = gold_common.parcel_lifecycle(df)
    agg = ops.hub_hourly_ops(df, lc)
    row = [r for r in agg.collect() if r["hub_id"] == "HUB-001" and r["event_hour"] == 8][0]
    assert row["distinct_parcels"] == 2
    assert row["failed_attempt_count"] == 1
    assert row["failed_delivery_rate"] == 0.5


def test_ops_flags_breach_risk_parcel(spark):
    """A parcel at 90% of its 24h window, not yet delivered, must count as breach-risk
    at the scan where it crossed 80%."""
    rows = [
        _scan("P1", "PICKUP", 1, 0, promised=24, hub="HUB-001"),
        _scan("P1", "OFD", 1, 22, promised=24, hub="HUB-009"),   # 22h in = 91% of 24, undelivered
    ]
    df = _df(spark, rows)
    lc = gold_common.parcel_lifecycle(df)
    agg = ops.hub_hourly_ops(df, lc)
    risk = {r["hub_id"]: r["breach_risk_parcels"] for r in agg.collect()}
    assert risk.get("HUB-009", 0) == 1     # flagged at the OFD hub
    assert risk.get("HUB-001", 0) == 0     # at pickup it was 0% in, not at risk


def test_ops_counts_late_arrival_scans(spark):
    rows = [
        _scan("P1", "PICKUP", 1, 8, hub="HUB-007", late=True),
        _scan("P1", "HUB_IN", 1, 9, hub="HUB-007", late=True),
        _scan("P2", "PICKUP", 1, 8, hub="HUB-001", late=False),
    ]
    df = _df(spark, rows)
    lc = gold_common.parcel_lifecycle(df)
    agg = ops.hub_hourly_ops(df, lc)
    # The two HUB-007 scans are in different hours (8 and 9), so they land in two
    # (hub, hour) rows — sum across them per hub, don't keep only the last.
    late: dict[str, int] = {}
    for r in agg.collect():
        late[r["hub_id"]] = late.get(r["hub_id"], 0) + r["late_arrival_scans"]
    assert late.get("HUB-007", 0) == 2     # the late-sync hub shows as the hole it is
    assert late.get("HUB-001", 0) == 0


# --- SLA star schema ----------------------------------------------------------

def _lanes(spark):
    return spark.createDataFrame(
        [("LANE-1", "HUB-001", "HUB-009", 24)],
        ["lane_id", "origin_hub", "dest_hub", "promised_hours"],
    )


def test_fct_sla_has_parcel_grain_and_lane_keys(spark):
    rows = [
        _scan("P1", "PICKUP", 1, 8, promised=24),
        _scan("P1", "DELIVERED", 1, 20, promised=24),
        _scan("P2", "PICKUP", 1, 8, promised=24),
    ]
    lc = gold_common.parcel_lifecycle(_df(spark, rows))
    dim_lane = sla.build_dim_lane(_lanes(spark))
    fct = sla.build_fct_sla(lc, dim_lane).collect()

    assert len(fct) == 2                       # one row per parcel
    p1 = [r for r in fct if r["parcel_id"] == "P1"][0]
    assert p1["origin_hub_id"] == "HUB-001"
    assert p1["dest_hub_id"] == "HUB-009"
    assert p1["is_delivered"] is True
    assert p1["delivery_date_key"] == 20260601


def test_fct_sla_in_transit_has_null_delivery_key(spark):
    rows = [_scan("P1", "PICKUP", 1, 8), _scan("P1", "HUB_IN", 1, 14)]
    lc = gold_common.parcel_lifecycle(_df(spark, rows))
    fct = sla.build_fct_sla(lc, sla.build_dim_lane(_lanes(spark))).collect()[0]
    assert fct["is_delivered"] is False
    assert fct["delivery_date_key"] is None    # meaningful null: no delivery yet


def test_dim_date_covers_exactly_the_delivery_dates(spark):
    rows = [
        _scan("P1", "PICKUP", 1, 8), _scan("P1", "DELIVERED", 1, 20),
        _scan("P2", "PICKUP", 1, 8), _scan("P2", "DELIVERED", 3, 10),
    ]
    lc = gold_common.parcel_lifecycle(_df(spark, rows))
    fct = sla.build_fct_sla(lc, sla.build_dim_lane(_lanes(spark)))
    dd = sla.build_dim_date(fct).collect()
    keys = {r["date_key"] for r in dd}
    assert keys == {20260601, 20260603}        # only dates the fact references, no gaps-as-error
    jun1 = [r for r in dd if r["date_key"] == 20260601][0]
    assert jun1["year"] == 2026 and jun1["month"] == 6 and jun1["day"] == 1
