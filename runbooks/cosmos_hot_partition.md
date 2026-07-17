# Runbook — Cosmos hot partition / RU throttling

**Symptom:** customer tracking lookups are slow or failing — Cosmos is returning **429
(Too Many Requests)**, the RU-throttling alarm paged, or the p99 on "where is my parcel"
climbed past its budget.

> A 429 has two causes that look identical from the alarm and need opposite fixes.
> **Genuine load**: total demand exceeds provisioned RU — the fix is more RU. **A hot
> partition**: one physical partition is saturated while the account as a whole has RU to
> spare — and more RU *does not fix it*, because the ceiling is per-partition, not per-
> account. Telling these apart is the entire job, and getting it wrong means either
> overpaying for RU that won't help or ignoring a real capacity problem.

## 0. Load or hot partition? — the one diagnostic that matters

Look at **normalized RU consumption per physical partition** (Cosmos metrics → "Normalized
RU Consumption", split by PartitionKeyRangeId):

| Pattern | Cause | Section |
|---|---|---|
| All partitions near 100% | **Genuine load** — the whole account is at its ceiling | §1 |
| One (or few) partition at 100%, others low | **Hot partition** — skew, RU won't help | §2 |

This single chart is the fork. Do not touch throughput until you've read it — raising RU to
"fix" a hot partition spends money and changes nothing, because the hot partition's ceiling
doesn't move.

## 1. Genuine load — the account is at its ceiling

Every partition is hot; demand simply exceeds supply.

- **Immediate:** raise provisioned RU, or switch the container to **autoscale** (scales
  between 10% and 100% of a max you set, so a tracking-traffic spike is absorbed without a
  manual change). This workload's reads are ~1 RU each, so the RU number is essentially
  "peak lookups per second" — size to the measured peak with headroom.
- **Confirm it worked:** 429 rate drops to zero, normalized RU comes off the ceiling.
- **Then:** if load is genuinely growing, this is capacity planning, not an incident —
  set autoscale max appropriately and move the budget alarm up knowingly (don't let the
  cost alarm fight the capacity fix silently).

Genuine sustained load is the *good* problem — it means the product is being used. The only
mistake here is not having the budget alarm keep pace so the RU increase isn't a surprise
on the invoice.

## 2. Hot partition — skew, and RU won't fix it

One partition saturated, the rest idle. The partition key isn't distributing load evenly.
**This should be rare here by design** — the container is keyed on `parcel_id`, which is
high-cardinality and evenly distributed (that was the M5 decision, see
[serving/cosmos/README.md](../serving/cosmos/README.md)). So a hot partition means one of a
few specific things went wrong:

| Cause | How to confirm | Fix |
|---|---|---|
| A single parcel getting pathological read volume (a bot, a stuck client polling one id) | The hot PartitionKeyRange maps to one/few parcel_ids with huge request counts | Rate-limit or cache at the app tier; this is a client problem, not a Cosmos one |
| The document for one parcel grew huge | A parcel with a runaway scan history | The history cap (`MAX_HISTORY_EVENTS = 12`) exists to prevent exactly this — confirm the serving job is applying it; a doc that skipped the cap is the bug |
| Someone changed the container's partition key to something skewed (hub_id, seller_id) | Container settings show a non-`parcel_id` key | This is the classic mistake the M5 README warns about — a low-cardinality key. The partition key **cannot be changed in place**; fixing it means creating a new container keyed on `parcel_id` and re-populating from the lakehouse (which is fine — Cosmos is a serving copy, not the system of record) |

**Do not raise RU to chase a hot partition.** It's the tempting move because it's the fast
one, and it will appear to help for minutes as autoscale throws RU at the account, but the
hot partition's per-partition ceiling is unchanged — you'll be back, poorer.

## 3. If it's a partition-key design error

The only real remediation is re-keying, and because Cosmos is a *serving* store here (not
the source of truth), that's a routine operation, not a data-loss event:

1. Create a new container keyed on `/parcel_id`.
2. Re-run the serving job (`serving/cosmos/upsert_state.py`) to populate it from the
   lakehouse silver — the documents are a pure function of the scans, so they rebuild
   identically.
3. Cut the app's read endpoint over to the new container.
4. Delete the old one.

Because the lakehouse holds the truth and the documents are deterministic, this is safe to
do any time — the worst case is a brief window where tracking reads the old container.

## What NOT to do

- **Don't raise RU before reading the per-partition chart.** It's the reflex and it's wrong
  half the time (the hot-partition half).
- **Don't try to change the partition key in place** — Cosmos doesn't allow it; you'll waste
  time looking for a setting that doesn't exist. Re-key via a new container.
- **Don't disable the RU-throttling alarm** to stop the paging during a known load spike —
  you'll miss the next *real* one. Silence the incident, not the alarm.
- **Don't cache tracking responses for long TTLs** to reduce RU — a stale "delivered" or
  "out for delivery" is a worse customer experience than a slightly slower fresh one. If
  you cache, keep it to seconds.
