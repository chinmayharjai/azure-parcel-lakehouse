"""Silver tests: dedup rule, ordering repair, late flags, PII masking, quarantine.

These assert the decisions that the rest of the platform depends on being exactly
so — especially the dedup tie-break (earliest sync) and the ordering annotation,
because the M6 control total is built on both.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType,
                               TimestampType)

_DBX = Path(__file__).resolve().parents[1]


def _load(fname: str, mod_name: str):
    spec = importlib.util.spec_from_file_location(mod_name, _DBX / fname)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


silver = _load("02_silver_clean.py", "silver_clean")
from schema import RESCUE_COLUMN  # noqa: E402

# A bronze-shaped schema for building test rows directly (skip the JSON round-trip).
_BRONZE = StructType([
    StructField("scan_id", StringType()),
    StructField("parcel_id", StringType()),
    StructField("lane_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("hub_id", StringType()),
    StructField("scan_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("sync_time", TimestampType()),
    StructField("customer_phone", StringType()),
    StructField("promised_hours", IntegerType()),
    StructField(RESCUE_COLUMN, StringType()),
    StructField("_raw", StringType()),
    StructField("dt", StringType()),
])


def _t(h, m=0):
    return datetime(2026, 6, 10, h, m, tzinfo=timezone.utc)


def _row(scan_id, parcel_id, scan_type, event_h, sync_h, phone="+919876543210",
         rescue=None, hub="HUB-001"):
    return (scan_id, parcel_id, "LANE-1", "SLR-1", hub, scan_type,
            _t(event_h), _t(sync_h), phone, 48, rescue, "{}", "2026-06-10")


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=_BRONZE)


# --- Quarantine: bad rows routed out, not into the windows --------------------

def test_rescued_and_null_key_rows_are_quarantined(spark):
    rows = [
        _row("S1", "P1", "PICKUP", 8, 8),
        _row("S2", "P1", "HUB_IN", 9, 9, rescue="{garbage"),   # rescued
        _row(None, "P1", "HUB_OUT", 10, 10),                    # null scan_id
    ]
    res = silver.to_silver(_df(spark, rows), salt="s")
    assert res.silver.count() == 1
    q = {r["quarantine_reason"] for r in res.quarantine.collect()}
    assert q == {"rescued_schema_drift_or_parse_error", "null_scan_id"}


# --- Dedup: earliest sync wins (the manifest's rule) --------------------------

def test_duplicate_scan_id_collapses_to_earliest_sync(spark):
    """Two rows, same scan_id, different sync — silver keeps the earliest arrival.
    This MUST match the M1 manifest tie-break or the M6 control total disagrees for a
    convention, not a defect."""
    rows = [
        _row("S1", "P1", "PICKUP", 8, 8),    # earliest sync (08:00)
        _row("S1", "P1", "PICKUP", 8, 11),   # re-upload, sync 11:00
    ]
    res = silver.to_silver(_df(spark, rows), salt="s")
    kept = res.silver.collect()
    assert len(kept) == 1
    assert kept[0]["sync_lag_minutes"] == 0.0   # 08:00 event vs 08:00 sync -> kept the early one


def test_dedup_happens_before_ordering(spark):
    """A duplicate must not occupy a sequence slot. Three distinct scans + one dup of
    the first => event_seq runs 1,2,3, not 1,2,3,4."""
    rows = [
        _row("S1", "P1", "PICKUP", 8, 8),
        _row("S1", "P1", "PICKUP", 8, 12),   # duplicate
        _row("S2", "P1", "HUB_IN", 9, 9),
        _row("S3", "P1", "HUB_OUT", 10, 10),
    ]
    res = silver.to_silver(_df(spark, rows), salt="s")
    seqs = sorted(r["event_seq"] for r in res.silver.collect())
    assert seqs == [1, 2, 3]


# --- Ordering repair: event order vs arrival order ----------------------------

def test_out_of_order_arrival_is_flagged_not_reordered_away(spark):
    """S1 happened first (event 08) but arrived last (sync 13); S2 happened later
    (event 09) but arrived first (sync 09). event_seq must reflect TRUE order; the
    disagreement must be flagged, not hidden."""
    rows = [
        _row("S1", "P1", "PICKUP", 8, 13),   # first event, last arrival
        _row("S2", "P1", "HUB_IN", 9, 9),    # second event, first arrival
    ]
    res = silver.to_silver(_df(spark, rows), salt="s")
    by_id = {r["scan_id"]: r for r in res.silver.collect()}

    assert by_id["S1"]["event_seq"] == 1     # true movement order
    assert by_id["S1"]["arrival_seq"] == 2   # but arrived second
    assert by_id["S1"]["is_out_of_order"] is True
    assert by_id["S2"]["is_out_of_order"] is True


def test_in_order_parcel_is_not_flagged(spark):
    rows = [
        _row("S1", "P1", "PICKUP", 8, 8),
        _row("S2", "P1", "HUB_IN", 9, 9),
    ]
    res = silver.to_silver(_df(spark, rows), salt="s")
    assert all(r["is_out_of_order"] is False for r in res.silver.collect())


# --- Late-arrival flag --------------------------------------------------------

def test_late_sync_beyond_threshold_is_flagged(spark):
    rows = [
        _row("S1", "P1", "PICKUP", 8, 8),      # 0 min lag
        _row("S2", "P1", "HUB_IN", 9, 14),     # 5h lag -> late
    ]
    res = silver.to_silver(_df(spark, rows), salt="s")
    by_id = {r["scan_id"]: r for r in res.silver.collect()}
    assert by_id["S1"]["is_late_arrival"] is False
    assert by_id["S2"]["is_late_arrival"] is True
    assert by_id["S2"]["sync_lag_minutes"] == 300.0


# --- PII masking --------------------------------------------------------------

def test_phone_is_hashed_and_raw_is_gone(spark):
    rows = [_row("S1", "P1", "PICKUP", 8, 8, phone="+919876543210")]
    res = silver.to_silver(_df(spark, rows), salt="pepper")
    cols = res.silver.columns
    assert "customer_phone" not in cols          # raw PII removed from silver
    assert "customer_phone_hash" in cols
    assert res.silver.collect()[0]["customer_phone_hash"] is not None


def test_same_phone_same_hash_different_salt_changes_it(spark):
    rows = [_row("S1", "P1", "PICKUP", 8, 8, phone="+919876543210")]
    h1 = silver.to_silver(_df(spark, rows), salt="A").silver.collect()[0]["customer_phone_hash"]
    h2 = silver.to_silver(_df(spark, rows), salt="A").silver.collect()[0]["customer_phone_hash"]
    h3 = silver.to_silver(_df(spark, rows), salt="B").silver.collect()[0]["customer_phone_hash"]
    assert h1 == h2      # deterministic under a fixed salt (joins are stable)
    assert h1 != h3      # salt actually participates (not a bare reversible sha256)


def test_pii_mapping_roundtrips_and_is_the_only_place_raw_survives(spark):
    rows = [
        _row("S1", "P1", "PICKUP", 8, 8, phone="+919876543210"),
        _row("S2", "P1", "HUB_IN", 9, 9, phone="+919876543210"),   # same person
        _row("S3", "P2", "PICKUP", 8, 8, phone="+918888888888"),
    ]
    res = silver.to_silver(_df(spark, rows), salt="pepper")
    mapping = {r["customer_phone_hash"]: r["customer_phone"] for r in res.pii_mapping.collect()}
    # two distinct people -> two mapping rows (deduped), raw phone present only here
    assert len(mapping) == 2
    assert set(mapping.values()) == {"+919876543210", "+918888888888"}

    # every hash in silver is re-identifiable via the mapping and nowhere else
    silver_hashes = {r["customer_phone_hash"] for r in res.silver.collect()}
    assert silver_hashes <= set(mapping)


def test_null_phone_produces_null_hash_no_mapping_row(spark):
    rows = [_row("S1", "P1", "PICKUP", 8, 8, phone=None)]
    res = silver.to_silver(_df(spark, rows), salt="pepper")
    assert res.silver.collect()[0]["customer_phone_hash"] is None
    assert res.pii_mapping.count() == 0


# --- Unknown scan type flag ---------------------------------------------------

def test_unknown_scan_type_is_flagged(spark):
    rows = [
        _row("S1", "P1", "PICKUP", 8, 8),
        _row("S2", "P1", "TELEPORTED", 9, 9),   # not a known scan type
    ]
    res = silver.to_silver(_df(spark, rows), salt="s")
    by_id = {r["scan_id"]: r for r in res.silver.collect()}
    assert by_id["S1"]["is_unknown_scan_type"] is False
    assert by_id["S2"]["is_unknown_scan_type"] is True
