"""Measure the arrival-vs-event divergence on the real generated data, no Spark.

This is the evidence behind the headline number in the README. It replicates, in pure
Python, the exact logic the Spark DQ check applies — the same earliest-sync dedup rule
as silver, the same correct-vs-naive delivered definitions as
`quality.dq_checks.delivery_method_divergence` — so the number anyone gets by running

    python simulator/scan_events.py
    python quality/measure_divergence.py

is the same number the pipeline would compute, reproducible with no cloud and no cluster.

correct  — a parcel is delivered if a DELIVERED scan exists in its (deduped) trail
           [event-time truth]
naive    — a parcel is delivered if its LAST-ARRIVED scan (max sync_time) is DELIVERED
           [what a consumer trusting arrival order sees]

The gap is the parcels a naive arrival-ordered pipeline would undercount as still in
transit — the concrete, in-money proof that arrival order != event order.
"""

from __future__ import annotations

import argparse
import gzip
import json
from collections import defaultdict
from pathlib import Path

DELIVERED = "DELIVERED"


def load_scans(landing: Path):
    for path in sorted(landing.rglob("*.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", type=Path, default=Path("data/landing"))
    ap.add_argument("--manifest", type=Path, default=Path("data/_manifest.json"))
    args = ap.parse_args()

    if not args.landing.exists():
        print(f"No data at {args.landing}. Run: python simulator/scan_events.py")
        return 1

    # 1. Dedup by scan_id keeping earliest sync_time — silver's rule, exactly.
    earliest: dict[str, dict] = {}
    for s in load_scans(args.landing):
        prev = earliest.get(s["scan_id"])
        if prev is None or s["sync_time"] < prev["sync_time"]:
            earliest[s["scan_id"]] = s

    # 2. Group deduped scans by parcel.
    by_parcel: dict[str, list] = defaultdict(list)
    for s in earliest.values():
        by_parcel[s["parcel_id"]].append(s)

    correct_delivered = 0
    naive_delivered = 0
    breaches = 0
    for scans in by_parcel.values():
        types = {s["scan_type"] for s in scans}
        is_correct = DELIVERED in types
        if is_correct:
            correct_delivered += 1
            # breach: actual (delivered event - pickup event) > promised
            pickup = min(s["event_time"] for s in scans)
            delivered = max(s["event_time"] for s in scans if s["scan_type"] == DELIVERED)
            promised = max(s["promised_hours"] for s in scans)
            # ISO strings compare lexicographically for same-format UTC; parse for hours.
            from datetime import datetime
            hrs = (datetime.fromisoformat(delivered) - datetime.fromisoformat(pickup)).total_seconds() / 3600
            if hrs > promised:
                breaches += 1

        # naive: the last-ARRIVED scan (max sync_time; event_time as tie-break)
        last_arrived = max(scans, key=lambda s: (s["sync_time"], s["event_time"]))
        if last_arrived["scan_type"] == DELIVERED:
            naive_delivered += 1

    parcels = len(by_parcel)
    divergence = correct_delivered - naive_delivered
    pct_of_delivered = (divergence / correct_delivered * 100) if correct_delivered else 0.0
    pct_of_parcels = (divergence / parcels * 100) if parcels else 0.0

    print(f"parcels                 : {parcels:,}")
    print(f"unique scans (deduped)  : {len(earliest):,}")
    print(f"delivered (correct)     : {correct_delivered:,}")
    print(f"delivered (naive)       : {naive_delivered:,}")
    print(f"UNDERCOUNT (divergence) : {divergence:,}")
    print(f"  as % of delivered     : {pct_of_delivered:.3f}%")
    print(f"  as % of all parcels   : {pct_of_parcels:.3f}%")
    print(f"breached parcels        : {breaches:,} "
          f"({breaches / correct_delivered * 100:.2f}% of delivered)" if correct_delivered else "")

    if args.manifest.exists():
        m = json.loads(args.manifest.read_text())
        inv = m["injected_problems"]["arrival_inversions"]
        print(f"\nfor context, the manifest's injected arrival inversions:")
        print(f"  inversions            : {inv['count']:,} across "
              f"{inv['parcels_affected']:,} parcels")
        print("The undercount is the subset of inverted parcels where the inversion "
              "specifically\nhid a completed delivery behind a late-arriving earlier scan.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
