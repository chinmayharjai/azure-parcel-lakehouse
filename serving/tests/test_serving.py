"""Serving tests: Cosmos document shape, the copy-integrity gate, DDL sanity.

The Cosmos builder is the piece with real logic (current state on event order, history
capping); the SQL side is mostly DDL and a JDBC write, so what's testable there is the
row-count gate's arithmetic and that the DDL declares the keys/indexes finance depends
on.
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


cosmos = _load("serving/cosmos/upsert_state.py", "upsert_state")
load_mart = _load("serving/azure_sql/load_mart.py", "load_mart")

_SILVER = StructType([
    StructField("parcel_id", StringType()),
    StructField("scan_id", StringType()),
    StructField("lane_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("hub_id", StringType()),
    StructField("scan_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("event_seq", IntegerType()),
    StructField("is_out_of_order", BooleanType()),
    StructField("promised_hours", IntegerType()),
])


def _row(pid, seq, stype, hub="HUB-001", ooo=False):
    # event_time monotonic in seq via a timedelta, so it stays valid for large seq
    # (the capping test runs well past 24 events).
    return (pid, f"{pid}-{seq}", "LANE-1", "SLR-1", hub, stype,
            datetime(2026, 6, 1) + timedelta(hours=seq), seq, ooo, 24)


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=_SILVER)


# --- Cosmos document shape ----------------------------------------------------

def test_one_document_per_parcel_keyed_by_parcel_id(spark):
    rows = [_row("P1", 1, "PICKUP"), _row("P1", 2, "DELIVERED"),
            _row("P2", 1, "PICKUP")]
    docs = {d["id"]: d for d in cosmos.build_parcel_state_documents(_df(spark, rows)).collect()}
    assert set(docs) == {"P1", "P2"}
    # id and partition key are the same value, by design (point-read on parcel_id)
    assert docs["P1"]["parcel_id"] == "P1"


def test_current_status_is_latest_by_event_order(spark):
    """The customer must see the true current status — the last scan by EVENT order,
    even if a later-happening scan arrived earlier. Here seq 3 (DELIVERED) is current
    though we deliberately feed the rows out of order."""
    rows = [_row("P1", 2, "HUB_IN"), _row("P1", 3, "DELIVERED"), _row("P1", 1, "PICKUP")]
    doc = cosmos.build_parcel_state_documents(_df(spark, rows)).collect()[0]
    assert doc["current_status"] == "DELIVERED"
    assert doc["is_delivered"] is True
    assert doc["is_terminal"] is True


def test_in_transit_parcel_is_not_delivered(spark):
    rows = [_row("P1", 1, "PICKUP"), _row("P1", 2, "OFD")]
    doc = cosmos.build_parcel_state_documents(_df(spark, rows)).collect()[0]
    assert doc["current_status"] == "OFD"
    assert doc["is_delivered"] is False
    assert doc["is_terminal"] is False


def test_scan_history_is_event_ordered(spark):
    rows = [_row("P1", 3, "OFD"), _row("P1", 1, "PICKUP"), _row("P1", 2, "HUB_IN")]
    doc = cosmos.build_parcel_state_documents(_df(spark, rows)).collect()[0]
    seqs = [h["seq"] for h in doc["scan_history"]]
    assert seqs == [1, 2, 3]
    assert [h["status"] for h in doc["scan_history"]] == ["PICKUP", "HUB_IN", "OFD"]


def test_history_is_capped_to_most_recent_events(spark):
    """A mini history: the document keeps only the most recent MAX_HISTORY_EVENTS, and
    keeps the LATEST ones (the tail), not the earliest."""
    n = cosmos.MAX_HISTORY_EVENTS + 5
    rows = [_row("P1", s, "HUB_IN") for s in range(1, n + 1)]
    doc = cosmos.build_parcel_state_documents(_df(spark, rows)).collect()[0]
    seqs = [h["seq"] for h in doc["scan_history"]]
    assert len(seqs) == cosmos.MAX_HISTORY_EVENTS
    assert seqs[-1] == n                 # newest kept
    assert seqs[0] == n - cosmos.MAX_HISTORY_EVENTS + 1   # oldest kept is the tail start


# --- Copy-integrity gate ------------------------------------------------------

def test_row_count_gate_passes_on_exact_match():
    load_mart.validate_row_counts("fct_sla", 1000, 1000)   # no raise


def test_row_count_gate_fails_on_any_mismatch():
    with pytest.raises(load_mart.RowCountMismatch):
        load_mart.validate_row_counts("fct_sla", 1000, 999)   # one missing row is a broken copy


# --- DDL sanity ---------------------------------------------------------------

def test_ddl_declares_the_star_and_finance_indexes():
    ddl = (_ROOT / "serving/azure_sql/ddl.sql").read_text(encoding="utf-8").lower()
    for tbl in ["dbo.fct_sla", "dbo.dim_lane", "dbo.dim_hub", "dbo.dim_date"]:
        assert tbl in ddl, f"{tbl} not defined"
    assert "primary key" in ddl
    # the penalty query's covering index and the lane-daily view finance reads
    assert "ix_fct_date_lane" in ddl
    assert "vw_lane_daily_sla" in ddl
    # foreign keys make the star explicit
    assert "foreign key" in ddl
