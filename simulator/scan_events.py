"""Generate parcel scan trails as JSON batches, partitioned by ARRIVAL time.

Emits the landing zone an ADF copy activity would pick up:

    data/landing/dt=2026-06-05/hour=14/batch-0001.json.gz   (scan events, NDJSON)
    data/landing/seller_master.csv                          (the "on-prem SQL" extract)

The single most important design decision: batches are partitioned by when a scan
ARRIVED (sync_time), not when it happened (event_time). That is how real scan data
lands — a rural delivery van syncs when it finds signal — and it is what creates the
central data problem this platform exists to solve: a parcel's scans arrive out of
order, so any consumer that trusts arrival order gets wrong parcel states. The
manifest counts exactly how many parcels are affected, and the M6 control-total
experiment measures what naive processing would get wrong because of it.

Injected defects, all counted on the emitted files into data/_manifest.json:

  arrival inversions   ~5% of scans sync late enough to land AFTER a later scan
  late-sync hub        one hub's scanner batch-uploads on a 2-8h delay, systematically
  duplicate scans      ~2% re-emitted verbatim with a later sync_time (same scan_id)
  missing hub scans    ~3% of parcels lose one intermediate scan entirely
  PII                  every event carries the consignee's phone number (to mask in silver)

Usage:
    python simulator/scan_events.py                    # 200K parcels, ~2 min
    python simulator/scan_events.py --parcels 5000     # quick
"""

from __future__ import annotations

import argparse
import gzip
import json
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ANCHOR_END = datetime(2026, 6, 30, 23, 59, 59, tzinfo=timezone.utc)
WINDOW_DAYS = 30

N_HUBS = 40
N_LANES = 300
N_SELLERS = 5000

# --- Injection knobs. Each is asserted against in tests/ and counted in the manifest.
ARRIVAL_INVERSION_RATE = 0.05   # a scan syncs late enough to land after its successor
DUPLICATE_RATE = 0.02           # scanner re-uploads: same scan_id, later sync_time
MISSING_SCAN_RATE = 0.03        # parcels that lose one intermediate hub scan
LATE_SYNC_HUB_INDEX = 7         # this hub batch-uploads 2-8h late, systematically

CITIES = [
    "Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Chennai", "Kolkata", "Pune",
    "Ahmedabad", "Jaipur", "Lucknow", "Surat", "Nagpur", "Indore", "Bhopal",
    "Patna", "Chandigarh", "Guwahati", "Kochi", "Coimbatore", "Visakhapatnam",
]

# SLA promise tiers by lane distance class. Finance computes penalties against
# these, which is why they live on the lane and travel with every parcel.
SLA_TIERS = [(24, 0.25), (48, 0.50), (72, 0.25)]  # (promised_hours, share of lanes)


def build_network(rng: random.Random) -> tuple[list[dict], list[dict]]:
    """40 hubs and 300 directed lanes between them, each lane with an SLA tier."""
    hubs = []
    for i in range(N_HUBS):
        city = CITIES[i % len(CITIES)]
        hubs.append({
            "hub_id": f"HUB-{i:03d}",
            "hub_name": f"{city} {'Mega' if i < len(CITIES) else 'Sort'} Hub {i:02d}",
            "city": city,
        })

    tier_choices = []
    for hours, share in SLA_TIERS:
        tier_choices.extend([hours] * int(share * 100))

    lanes, seen = [], set()
    while len(lanes) < N_LANES:
        a, b = rng.sample(range(N_HUBS), 2)
        if (a, b) in seen:
            continue
        seen.add((a, b))
        lanes.append({
            "lane_id": f"LANE-{len(lanes):04d}",
            "origin_hub": hubs[a]["hub_id"],
            "dest_hub": hubs[b]["hub_id"],
            "promised_hours": rng.choice(tier_choices),
        })
    return hubs, lanes


def build_sellers(rng: random.Random, hubs: list[dict]) -> list[dict]:
    """The 'on-prem SQL' extract: seller master data an ADF self-hosted IR would pull."""
    return [{
        "seller_id": f"SLR-{i:06d}",
        "seller_name": f"Seller {i:06d} {rng.choice(['Retail', 'Traders', 'Enterprises', 'Store', 'Mart'])}",
        "city": rng.choice(CITIES),
        "pickup_hub": rng.choice(hubs)["hub_id"],
        "onboarded_date": str((ANCHOR_END - timedelta(days=rng.randint(30, 900))).date()),
        "is_active": rng.random() > 0.04,
    } for i in range(N_SELLERS)]


def make_phone(rng: random.Random) -> str:
    return f"+91{rng.randint(6000000000, 9999999999)}"


def plan_trail(rng: random.Random, parcel: dict, lane: dict,
               start: datetime) -> list[dict]:
    """The TRUE scan sequence for one parcel, in event-time order.

    Built truthfully first; the mess (inversions, duplicates, drops) is applied at
    emission time. Keeping the truth and the corruption separate is what lets the
    manifest count each defect exactly — the same generate-then-corrupt discipline
    as the other simulators in this portfolio.
    """
    scans = []
    t = start
    seq = 0

    def scan(hub: str, scan_type: str, minutes_ahead: tuple[int, int]) -> None:
        nonlocal t, seq
        t = t + timedelta(minutes=rng.randint(*minutes_ahead))
        seq += 1
        scans.append({
            "scan_id": f"{parcel['parcel_id']}-S{seq:02d}",
            "parcel_id": parcel["parcel_id"],
            "lane_id": lane["lane_id"],
            "seller_id": parcel["seller_id"],
            "hub_id": hub,
            "scan_type": scan_type,
            "event_time": t,
            "customer_phone": parcel["customer_phone"],
            "promised_hours": lane["promised_hours"],
        })

    scan(lane["origin_hub"], "PICKUP", (10, 120))
    scan(lane["origin_hub"], "HUB_IN", (30, 180))
    scan(lane["origin_hub"], "HUB_OUT", (60, 600))

    # 0-2 intermediate hubs on long lanes.
    n_via = 0 if lane["promised_hours"] == 24 else rng.randint(0, 2)
    for _ in range(n_via):
        via = f"HUB-{rng.randrange(N_HUBS):03d}"
        scan(via, "HUB_IN", (180, 900))
        scan(via, "HUB_OUT", (60, 360))

    scan(lane["dest_hub"], "HUB_IN", (180, 900))
    scan(lane["dest_hub"], "OFD", (60, 720))  # out for delivery

    # Terminal: mostly delivered, sometimes after failed attempts, occasionally RTO.
    roll = rng.random()
    if roll < 0.85:
        scan(lane["dest_hub"], "DELIVERED", (30, 480))
    elif roll < 0.95:
        attempts = rng.randint(1, 2)
        for _ in range(attempts):
            scan(lane["dest_hub"], "FAILED_ATTEMPT", (60, 480))
            scan(lane["dest_hub"], "OFD", (600, 1200))
        scan(lane["dest_hub"], "DELIVERED", (30, 480))
    else:
        scan(lane["dest_hub"], "FAILED_ATTEMPT", (60, 480))
        scan(lane["dest_hub"], "RTO_INITIATED", (300, 900))
        scan(lane["origin_hub"], "RTO_DELIVERED", (1200, 4000))

    return scans


def assign_sync_times(rng: random.Random, scans: list[dict],
                      stats: dict) -> list[dict]:
    """Give each scan its ARRIVAL time, injecting the ordering defects.

    Normal: sync_time = event_time + 1-20 minutes (device uploads promptly).
    Inversion (~5%): a scan's sync is pushed past the NEXT scan's sync, so it
      arrives after its successor — the defect that breaks arrival-order consumers.
    Late-sync hub: every scan at HUB-007 syncs 2-8h late. Systematic, so the whole
      hub's traffic arrives in the wrong hour partitions — visible as a hub-shaped
      hole in any arrival-time dashboard, which is exactly how a real hub with a
      broken uplink presents.
    """
    late_hub = f"HUB-{LATE_SYNC_HUB_INDEX:03d}"

    for scan in scans:
        base_lag = timedelta(minutes=rng.randint(1, 20))
        if scan["hub_id"] == late_hub:
            base_lag = timedelta(hours=rng.uniform(2, 8))
            stats["late_sync_hub_scans"] += 1
        scan["sync_time"] = scan["event_time"] + base_lag

    # Inversions: push a scan's sync past its successor's.
    for i in range(len(scans) - 1):
        if rng.random() < ARRIVAL_INVERSION_RATE:
            successor_sync = scans[i + 1]["sync_time"]
            scans[i]["sync_time"] = successor_sync + timedelta(minutes=rng.randint(5, 240))
            stats["arrival_inversions"] += 1

    return scans


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--parcels", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=Path("data/landing"))
    args = ap.parse_args()

    rng = random.Random(args.seed)
    start_window = ANCHOR_END - timedelta(days=WINDOW_DAYS)
    args.out.mkdir(parents=True, exist_ok=True)

    hubs, lanes = build_network(rng)
    sellers = build_sellers(rng, hubs)

    print(f"window  : {start_window.date()} -> {ANCHOR_END.date()}")
    print(f"network : {len(hubs)} hubs, {len(lanes)} lanes, {len(sellers)} sellers")

    stats = defaultdict(int)
    all_scans: list[dict] = []
    terminal_counts: defaultdict[str, int] = defaultdict(int)

    for i in range(args.parcels):
        lane = rng.choice(lanes)
        parcel = {
            "parcel_id": f"PCL-{i:08d}",
            "seller_id": sellers[rng.randrange(N_SELLERS)]["seller_id"],
            "customer_phone": make_phone(rng),
        }
        # Pickup spread across the window, leaving tail room for the trail.
        pickup_at = start_window + timedelta(
            minutes=rng.randint(0, (WINDOW_DAYS - 4) * 24 * 60))
        trail = plan_trail(rng, parcel, lane, pickup_at)

        # Injected: missing intermediate scan. One HUB_IN/HUB_OUT vanishes — the
        # scanner was down, the parcel moved anyway. Never the pickup or terminal,
        # because a trail with no start or end is a different (rarer) failure.
        if rng.random() < MISSING_SCAN_RATE and len(trail) > 4:
            candidates = [j for j in range(1, len(trail) - 1)
                          if trail[j]["scan_type"] in ("HUB_IN", "HUB_OUT")]
            if candidates:
                del trail[rng.choice(candidates)]
                stats["parcels_missing_scan"] += 1

        trail = assign_sync_times(rng, trail, stats)

        # Trim scans whose sync lands beyond the window (parcels near the end may
        # complete after it). The parcel is then legitimately "in transit" at close.
        trail = [s for s in trail if s["sync_time"] <= ANCHOR_END]
        if not trail:
            continue

        terminal = trail[-1]["scan_type"] if trail else "NONE"
        terminal_counts[max((s for s in trail), key=lambda s: s["event_time"])["scan_type"]] += 1

        # Injected: duplicates — the scanner re-uploads a scan verbatim except for
        # a later sync_time. Same scan_id, so dedup must key on it.
        for scan in list(trail):
            if rng.random() < DUPLICATE_RATE:
                dupe = dict(scan)
                dupe["sync_time"] = scan["sync_time"] + timedelta(minutes=rng.randint(10, 300))
                if dupe["sync_time"] <= ANCHOR_END:
                    trail.append(dupe)
                    stats["duplicate_scans"] += 1

        all_scans.extend(trail)
        stats["parcels"] += 1

    print(f"scans   : {len(all_scans):,} (sorting into arrival partitions...)")

    # Partition by ARRIVAL (sync_time). This is the landing zone's physical truth:
    # a scan that synced at 14:07 lands in hour=14 regardless of when it happened.
    by_partition: defaultdict[tuple, list] = defaultdict(list)
    for scan in all_scans:
        st = scan["sync_time"]
        by_partition[(st.date(), st.hour)].append(scan)

    written = 0
    for (d, h), scans in sorted(by_partition.items()):
        pdir = args.out / f"dt={d}" / f"hour={h:02d}"
        pdir.mkdir(parents=True, exist_ok=True)
        scans.sort(key=lambda s: s["sync_time"])
        path = pdir / "batch-0001.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            for s in scans:
                row = dict(s)
                row["event_time"] = row["event_time"].isoformat()
                row["sync_time"] = row["sync_time"].isoformat()
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
        written += len(scans)

    # Seller master as CSV — the stand-in for the on-prem SQL extract that ADF
    # would pull through a self-hosted integration runtime.
    import csv
    seller_path = args.out / "seller_master.csv"
    with open(seller_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(sellers[0].keys()))
        w.writeheader()
        w.writerows(sellers)

    # Reference data for the transforms.
    ref_dir = args.out.parent / "reference"
    ref_dir.mkdir(exist_ok=True)
    (ref_dir / "hubs.json").write_text(json.dumps(hubs, indent=1))
    (ref_dir / "lanes.json").write_text(json.dumps(lanes, indent=1))

    # --- Manifest: measured on what was emitted, per portfolio discipline. -----
    # Recount inversions on the FILES (a scan whose sync_time is later than a
    # successor scan's sync_time within the same parcel), because trimming at the
    # window edge can remove one side of an injected inversion.
    per_parcel: defaultdict[str, list] = defaultdict(list)
    for s in all_scans:
        per_parcel[s["parcel_id"]].append(s)

    measured_inversions = 0
    affected_parcels = 0
    for pid, scans in per_parcel.items():
        # Collapse duplicate re-emissions to one scan per scan_id for ordering.
        # Tie-break EXPLICITLY on earliest sync_time (the first arrival) rather than
        # on list order — a duplicate re-upload lands later, and "when did we first
        # learn of this scan" is the earliest sync. This is the same rule silver's
        # dedup uses, so the manifest's inversion count matches what silver will see
        # after dedup, not some other ordering.
        uniq: dict[str, dict] = {}
        for s in scans:
            prev = uniq.get(s["scan_id"])
            if prev is None or s["sync_time"] < prev["sync_time"]:
                uniq[s["scan_id"]] = s
        ordered_by_event = sorted(uniq.values(), key=lambda s: s["event_time"])
        inv = sum(1 for a, b in zip(ordered_by_event, ordered_by_event[1:])
                  if a["sync_time"] > b["sync_time"])
        if inv:
            measured_inversions += inv
            affected_parcels += 1

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seed": args.seed,
        "window": {"start": str(start_window.date()), "end": str(ANCHOR_END.date()),
                   "days": WINDOW_DAYS},
        "network": {"hubs": len(hubs), "lanes": len(lanes), "sellers": len(sellers),
                    "late_sync_hub": f"HUB-{LATE_SYNC_HUB_INDEX:03d}"},
        "parcels": stats["parcels"],
        "scan_events_emitted": written,
        "terminal_state_distribution": dict(sorted(terminal_counts.items())),
        "note": "Counts measured on emitted files. arrival_inversions is recounted "
                "from the files (event-order neighbours whose sync order is reversed), "
                "not from the injection counter, because window-edge trimming can "
                "remove one side of an injected inversion.",
        "injected_problems": {
            "arrival_inversions": {
                "count": measured_inversions,
                "parcels_affected": affected_parcels,
                "rate_knob": ARRIVAL_INVERSION_RATE,
                "detail": "A scan syncs after its successor, so arrival order != event "
                          "order. Any consumer trusting arrival order derives wrong "
                          "parcel states — measured by the M6 control-total experiment.",
            },
            "duplicate_scans": {
                "count": stats["duplicate_scans"],
                "rate_knob": DUPLICATE_RATE,
                "detail": "Same scan_id re-uploaded with a later sync_time. Dedup must "
                          "key on scan_id keeping first-by-event/latest-by-sync.",
            },
            "parcels_missing_scan": {
                "count": stats["parcels_missing_scan"],
                "rate_knob": MISSING_SCAN_RATE,
                "detail": "One intermediate HUB_IN/HUB_OUT missing; trail has a gap "
                          "but valid endpoints.",
            },
            "late_sync_hub_scans": {
                "count": stats["late_sync_hub_scans"],
                "hub": f"HUB-{LATE_SYNC_HUB_INDEX:03d}",
                "detail": "Every scan at this hub syncs 2-8h late — a systematic "
                          "uplink problem, visible as a hub-shaped hole in arrival "
                          "dashboards.",
            },
            "pii_phone_on_every_event": {
                "count": written,
                "detail": "customer_phone rides on every scan event and must be "
                          "masked in silver with a restricted mapping table.",
            },
        },
    }
    manifest_path = args.out.parent / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    print(f"parcels : {stats['parcels']:,}")
    print(f"scans   : {written:,} written across {len(by_partition)} partitions")
    print(f"  inversions (measured) : {measured_inversions:,} across {affected_parcels:,} parcels")
    print(f"  duplicates            : {stats['duplicate_scans']:,}")
    print(f"  missing-scan parcels  : {stats['parcels_missing_scan']:,}")
    print(f"  late-sync hub scans   : {stats['late_sync_hub_scans']:,}")
    print(f"manifest: {manifest_path}")


if __name__ == "__main__":
    main()
