"""Tests for the parcel scan simulator.

The simulator's job is to inject *known* defects in *counted* quantities, so the
downstream milestones can prove they caught them. These tests assert the two
properties that makes that trustworthy:

  1. Determinism — same seed, same bytes. A manifest is only evidence if the run
     it describes is reproducible.
  2. The defects are actually present, at roughly the injected rate, and the
     manifest's counts match what's really in the files.

Run:  pytest simulator/tests -v
"""

from __future__ import annotations

import gzip
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pytest

SIM = Path(__file__).resolve().parents[1] / "scan_events.py"


def _run(tmp: Path, parcels: int = 2000, seed: int = 42) -> Path:
    out = tmp / "landing"
    subprocess.run(
        [sys.executable, str(SIM), "--parcels", str(parcels),
         "--seed", str(seed), "--out", str(out)],
        check=True, capture_output=True, text=True,
    )
    return out


def _load_scans(landing: Path) -> list[dict]:
    scans = []
    for path in landing.rglob("*.json.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                scans.append(json.loads(line))
    return scans


def _manifest(landing: Path) -> dict:
    return json.loads((landing.parent / "_manifest.json").read_text())


def _dedup_earliest_sync(scans: list[dict]) -> dict[str, dict]:
    """Collapse duplicate re-emissions to one scan per scan_id, keeping the earliest
    sync_time. Must match the manifest's rule exactly (and silver's dedup) — a
    duplicate re-upload lands later, so 'first arrival' is the earliest sync. Using
    a plain {id: s} comprehension would keep the LAST occurrence (the duplicate) and
    silently disagree, which is the discrepancy this helper exists to prevent."""
    uniq: dict[str, dict] = {}
    for s in scans:
        prev = uniq.get(s["scan_id"])
        if prev is None or s["sync_time"] < prev["sync_time"]:
            uniq[s["scan_id"]] = s
    return uniq


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("sim"))


# --- Determinism --------------------------------------------------------------

def test_same_seed_same_bytes(tmp_path):
    """The evidence claim: a manifest describes a reproducible run."""
    a = _run(tmp_path / "a", parcels=800, seed=7)
    b = _run(tmp_path / "b", parcels=800, seed=7)

    def digest(landing):
        blobs = {}
        for p in sorted(landing.rglob("*.json.gz")):
            rel = p.relative_to(landing).as_posix()
            blobs[rel] = gzip.open(p, "rt", encoding="utf-8").read()
        return blobs

    assert digest(a) == digest(b)


def test_different_seed_differs(tmp_path):
    a = _load_scans(_run(tmp_path / "a", parcels=800, seed=1))
    b = _load_scans(_run(tmp_path / "b", parcels=800, seed=2))
    assert len(a) != len(b) or a[0]["parcel_id"] != b[0]["parcel_id"]


# --- Partitioning is by ARRIVAL, the central design choice --------------------

def test_files_are_partitioned_by_sync_time_not_event_time(landing):
    """A scan must live in the dt=/hour= partition of its sync_time (arrival), not
    its event_time. This is the whole premise — arrival order != event order."""
    for path in landing.rglob("*.json.gz"):
        # path: .../dt=YYYY-MM-DD/hour=HH/batch-*.json.gz
        dt = path.parent.parent.name.split("=")[1]
        hour = int(path.parent.name.split("=")[1])
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                s = json.loads(line)
                assert s["sync_time"][:10] == dt
                assert int(s["sync_time"][11:13]) == hour


def test_at_least_some_scans_arrive_out_of_event_order(landing):
    """If arrival order always equalled event order, there'd be nothing to fix."""
    per_parcel = defaultdict(list)
    for s in _load_scans(landing):
        per_parcel[s["parcel_id"]].append(s)

    inverted_parcels = 0
    for scans in per_parcel.values():
        uniq = _dedup_earliest_sync(scans)
        by_event = sorted(uniq.values(), key=lambda s: s["event_time"])
        if any(a["sync_time"] > b["sync_time"] for a, b in zip(by_event, by_event[1:])):
            inverted_parcels += 1
    assert inverted_parcels > 0


# --- Injected defects are present AND match the manifest ----------------------

def test_manifest_inversion_count_matches_files(landing):
    """The manifest is measured on the files, not the injection counter. Recompute
    independently and require exact agreement — this is the property the whole
    'measured, not claimed' discipline rests on."""
    m = _manifest(landing)
    per_parcel = defaultdict(list)
    for s in _load_scans(landing):
        per_parcel[s["parcel_id"]].append(s)

    inv, affected = 0, 0
    for scans in per_parcel.values():
        uniq = _dedup_earliest_sync(scans)
        by_event = sorted(uniq.values(), key=lambda s: s["event_time"])
        p_inv = sum(1 for a, b in zip(by_event, by_event[1:])
                    if a["sync_time"] > b["sync_time"])
        if p_inv:
            inv += p_inv
            affected += 1

    assert m["injected_problems"]["arrival_inversions"]["count"] == inv
    assert m["injected_problems"]["arrival_inversions"]["parcels_affected"] == affected


def test_duplicates_share_scan_id_with_a_later_sync(landing):
    """A duplicate is the same scan_id re-emitted with a later sync_time — dedup
    must key on scan_id, so the duplicate cannot be a fresh id."""
    by_id = defaultdict(list)
    for s in _load_scans(landing):
        by_id[s["scan_id"]].append(s)

    dupes = {sid: rows for sid, rows in by_id.items() if len(rows) > 1}
    assert dupes, "no duplicate scan_ids found"

    for rows in dupes.values():
        # all copies share event_time; sync_times differ
        assert len({r["event_time"] for r in rows}) == 1
        assert len({r["sync_time"] for r in rows}) == len(rows)

    m = _manifest(landing)
    emitted_dupes = sum(len(rows) - 1 for rows in dupes.values())
    assert emitted_dupes == m["injected_problems"]["duplicate_scans"]["count"]


def test_late_sync_hub_is_systematically_late(landing):
    """Every scan at the late hub must sync hours after its event; a normal hub
    syncs within minutes. The gap is what makes the hub a visible hole."""
    from datetime import datetime

    m = _manifest(landing)
    late_hub = m["network"]["late_sync_hub"]

    def lag_minutes(s):
        e = datetime.fromisoformat(s["event_time"])
        y = datetime.fromisoformat(s["sync_time"])
        return (y - e).total_seconds() / 60

    late_lags, normal_lags = [], []
    for s in _load_scans(landing):
        (late_lags if s["hub_id"] == late_hub else normal_lags).append(lag_minutes(s))

    assert late_lags, "no scans at the late hub"
    # The late hub's *median* lag is in hours; normal hubs in minutes. (Inversions
    # add tail lag to some normal scans, so compare medians not maxima.)
    late_lags.sort(); normal_lags.sort()
    assert late_lags[len(late_lags) // 2] > 120       # > 2h
    assert normal_lags[len(normal_lags) // 2] < 60    # < 1h


def test_every_event_carries_pii_to_mask(landing):
    """PII masking in silver can only be tested if the PII is really here."""
    for s in _load_scans(landing):
        assert s["customer_phone"].startswith("+91")
        assert len(s["customer_phone"]) == 13


def test_terminal_states_cover_the_real_outcomes(landing):
    """Delivered, failed-then-delivered, and RTO must all occur, or the SLA mart
    has nothing interesting to compute."""
    m = _manifest(landing)
    terminals = m["terminal_state_distribution"]
    assert terminals.get("DELIVERED", 0) > 0
    assert "RTO_DELIVERED" in terminals or "RTO_INITIATED" in terminals
    # Delivered should dominate (~85% path), but not be everything.
    total = sum(terminals.values())
    assert 0.5 < terminals["DELIVERED"] / total < 0.98


def test_seller_master_is_a_csv_extract(landing):
    """The 'on-prem SQL' stand-in must be a flat CSV with the join key."""
    import csv
    path = landing / "seller_master.csv"
    assert path.exists()
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 5000
    assert {"seller_id", "seller_name", "pickup_hub", "is_active"} <= set(rows[0])

    # Every seller_id referenced by a scan must exist in the master (referential
    # integrity the M6 checks will enforce downstream).
    seller_ids = {r["seller_id"] for r in rows}
    scan_sellers = {s["seller_id"] for s in _load_scans(landing)}
    assert scan_sellers <= seller_ids


def test_reference_network_is_emitted(landing):
    """hubs.json and lanes.json feed the transforms; lanes must carry the SLA
    promise finance computes against."""
    ref = landing.parent / "reference"
    hubs = json.loads((ref / "hubs.json").read_text())
    lanes = json.loads((ref / "lanes.json").read_text())
    assert len(hubs) == 40
    assert len(lanes) == 300
    assert all("promised_hours" in ln for ln in lanes)
    assert {ln["promised_hours"] for ln in lanes} <= {24, 48, 72}
