# Databricks — the medallion transforms

Bronze and silver (M3); gold follows in M4. The rule that shapes every file here:
**each transform is a pure function over a DataFrame**, and the Delta I/O lives only in
`main()`. That's what lets the logic be tested on a handful of hand-built rows in CI,
with real Spark, without a cluster or a storage account — the transform *decisions* are
what's worth testing, not that Spark can write a file.

Files are named `01_`, `02_`… to read as an ordered notebook sequence in the Databricks
workspace; the tests load them by path (a leading digit isn't an importable module name).

## 01 — bronze ingest

Raw scan JSON → Delta bronze, **schema-on-read with a rescue column**. Bronze's only job
is to become a queryable, replayable copy of the raw bytes *without dropping anything*. A
line that fails to parse lands in the rescue column with its original text instead of
being discarded — the whole contract of a raw layer. Partitioned by arrival (`dt`,
`hour` from `sync_time`), overwritten dynamically per partition so an hour can be replayed
without touching the rest.

> One subtlety worth the comment it gets in the code: the rescue column must be part of
> the `from_json` parse schema, or PERMISSIVE mode silently drops corrupt records instead
> of capturing them. That's the line between "quarantined with evidence" and "vanished."

## 02 — silver clean

Bronze's faithful-but-messy copy → the trustworthy layer, **without ever silently
discarding a row**. Five steps, composed in an order that matters:

1. **Quarantine** rescued / null-key rows out, each with a *specific* reason (not
   "invalid") — the M6 `dq_incidents` table and the runbooks need to know which kind of bad.
2. **Dedup** by `scan_id`, keeping the **earliest sync** (first arrival). This is the same
   tie-break the M1 manifest uses — stated in both places, because the M6 control total
   compares them and a dedup convention masquerading as a defect would be a nasty bug.
   Dedup runs *before* ordering so a duplicate can't occupy a sequence slot.
3. **Repair ordering**: annotate every scan with `event_seq` (true movement order, by
   event time) and `arrival_seq` (the order we learned it), and flag `is_out_of_order`.
   "Repair" means *surface*, not reorder-and-hide — the disorder is real signal the
   control total depends on.
4. **Flag late arrivals** (`sync_lag_minutes` > 120) — trips on the late-sync hub, not on
   a rural van that took an hour. Kept as data so the hub shows up as the hole it is.
5. **Mask PII**: salted SHA-256 of the phone, raw dropped from silver, reverse mapping
   written to a restricted table. The salt defeats a rainbow table of the ~10⁹ phone
   space; re-identification requires the mapping table's ACL — a single auditable grant.

Idempotent by `MERGE` on `scan_id`: replay updates in place, and because the dedup rule is
deterministic, the merged result is identical however many times a batch is replayed.

## gold_common — the shared lifecycle

Both gold jobs collapse a parcel's scan trail into one lifecycle row (pickup, delivery,
SLA outcome), so that computation lives in `gold_common.parcel_lifecycle` once. The
correctness crux, and the reason the whole project exists:

> **delivery is determined by EVENT time, never arrival time.**

A parcel's DELIVERED scan can *arrive* before an earlier hub scan (the injected
inversion). A naive consumer ordering by arrival — or taking the last-arrived scan as
"current state" — marks such a parcel delivered at the wrong time, or before its hub-in
even landed. Because silver repaired ordering and this aggregates on `event_time`, the
lifecycle is computed on the true sequence. M6 measures exactly what the naive version
gets wrong.

## 03 — gold ops aggregates

Hourly hub-level health at `(hub_id, event_date, event_hour)`: total scans, distinct
parcels, failed-attempt count and **rate** (zero-denominator-safe, as its own column),
**breach-risk parcels** (point-in-time: past 80% of the promised window at this scan and
not yet delivered), and — the metric this platform exists to expose — **late-arrival and
out-of-order scan counts**, which make the late-sync hub show up as the hub-shaped hole
it is. Z-ORDER by `hub_id` because the ops dashboard filters by hub.

## 04 — gold SLA mart

A finance-grade star at parcel grain: `fct_sla` (promised vs actual hours, breach flag,
exception reason) + `dim_lane`, `dim_hub`, `dim_date`. Lane-daily rollups are a `GROUP
BY`, not a re-derivation. `dim_date` is built *from the data* so it never has a gap the
fact needs or a date no fact references. The `is_delivered` count here is the number the
**M6 control total** reconciles against the lakehouse — if the star and the lakehouse
disagree on how many parcels were delivered, nothing publishes.

### Time travel

The SLA mart is overwritten each run, and Delta keeps the prior versions — so "what did
finance's numbers say before today's backfill corrected them?" is a query, not a restore:

```sql
-- yesterday's breach count vs today's, for the same lane
SELECT 'today' AS v, count(*) FROM delta.`/gold/sla/fct_sla` WHERE is_breach
UNION ALL
SELECT 'prior', count(*) FROM delta.`/gold/sla/fct_sla` VERSION AS OF 0 WHERE is_breach;

-- or read the fact as of a wall-clock time before the correction
SELECT * FROM delta.`/gold/sla/fct_sla` TIMESTAMP AS OF '2026-06-30 02:00:00';
```

The practical use: an SLA penalty dispute is answerable — "the number we billed on was
this version, computed from this data, at this time."

## Tests (CI, real Spark on JDK 17)

`databricks/tests/` — bronze (nothing dropped, rescue works, arrival-time partitioning),
silver (quarantine reasons, the earliest-sync dedup rule, out-of-order flagged not hidden,
late-arrival threshold, PII hashed + salted + mapping round-trips + raw gone, unknown scan
type flagged), and gold (lifecycle on event time incl. the inversion crux, breach/RTO/
in-transit outcomes, ops rate zero-safety + breach-risk + late-scan counts, the SLA star's
parcel grain + lane keys + meaningful-null delivery key + gap-free dim_date). There's no
local JDK in this build, so CI is the source of truth for the Spark layer.
