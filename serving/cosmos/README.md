# Cosmos DB — the "where is my parcel?" store

The customer tracking lookup: millions of point reads by `parcel_id`, each needing to
return in tens of milliseconds. Every design choice here follows from that one access
pattern.

## Partition key = `parcel_id`

The single most important decision. The partition key should be the field the hottest
query filters on — and every tracking lookup is `WHERE parcel_id = ?`. With `parcel_id`
as the partition key **and** the document id, a lookup is a *point read*: Cosmos routes
straight to the one physical partition holding that item and returns it. That costs ~1
RU and single-digit milliseconds, and — critically — it does not get slower as the
container grows to hundreds of millions of parcels, because it never scans.

The alternative keys are all traps:
- **`hub_id`** — a few dozen hubs means a few dozen partitions, each enormous and hot; a
  "hot partition" throttles because one physical partition has a 10 GB / throughput
  ceiling. And a tracking lookup would become a cross-partition fan-out.
- **`seller_id`** — same skew (a few big sellers dominate), same fan-out for tracking.
- **`lane_id`** — same problem, and it isn't even in the lookup predicate.

`parcel_id` is high-cardinality and evenly distributed, so partitions stay small and
balanced, and the one query that matters is a point read. That is the textbook fit, and
it's the fit because we designed the store around the query, not the other way round.

## One document per parcel = latest state + mini history

The document holds the parcel's **current status** plus its recent scan trail, so the
customer screen renders from a *single* read — no join, no fan-out, no assembling a
timeline from N event rows. The current status is the last scan by **event order**
(`event_seq`), not arrival order, so a lookup never shows a stale or out-of-sequence
status because a later-happening scan synced early. History is capped
(`MAX_HISTORY_EVENTS = 12`) so a document can't bloat; the lakehouse remains the full
system of record.

The write is an **upsert on `id`**, and the document is a pure function of the parcel's
scans, so re-running a serving refresh overwrites each doc with an identical one — never
a second copy. That's the same idempotency the lakehouse jobs have, carried into serving.

## RU/s and the free tier

Cosmos DB's free tier grants **1000 RU/s and 25 GB** per account, for free, indefinitely
— and this workload is sized to live inside it:

- **Reads**: a point read is ~1 RU. 1000 RU/s ⇒ ~1000 tracking lookups/second sustained
  before any autoscale, which covers a portfolio-scale demo with headroom.
- **Writes**: an upsert of a small document is ~5–10 RU. The serving refresh writes
  changed parcels in batches, spread over time, well within budget.
- **Storage**: one small doc per parcel; 200K parcels is a few dozen MB, far under 25 GB.

The container is provisioned at `offer_throughput=1000` to sit exactly on the free grant.
Scale up (or switch to autoscale) only when measured read QPS or the write refresh
demands it — starting higher just spends RU budget to no benefit. **Set a budget alert on
day one** regardless: the failure mode of a serverless store is a surprise bill, not an
outage.

## What this store is NOT for

Analytical questions — "breach rate per lane last week", "failed-delivery hotspots" — do
*not* belong here. Those are scans and joins over the whole dataset, which in a point-read
store are expensive cross-partition queries. They go to the lakehouse gold (ops) and the
Azure SQL mart (finance). Forcing Cosmos to answer them would be slow and would burn RU;
that's the whole reason this platform has three stores instead of one. See
[docs/access_pattern_matrix.md](../../docs/access_pattern_matrix.md).
