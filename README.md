# Parcel Intelligence Lakehouse on Azure

> India ships ~10 million e-commerce parcels a day. Every parcel leaves a trail of scan
> events, and three very different consumers need that trail: a customer refreshing
> "where is my order?" (<50ms point lookup), an ops manager (hourly hub dashboards),
> and a finance team computing SLA penalties (exact, reconciled, historical).
> No single database serves all three well. **Same events, three access patterns,
> three stores** — orchestrated by ADF, transformed once in the lakehouse, served everywhere.

**Azure Data Factory · ADLS Gen2 · Delta Lake · Databricks · Azure SQL Database · Azure Cosmos DB**

Built milestone by milestone, one PR each, in the same measured-not-claimed discipline as
the rest of the portfolio: every number in this README is produced by a script anyone can
run, and every injected data defect is counted on the emitted files, not asserted.

| # | Milestone | Status |
|---|-----------|--------|
| M1 | Parcel scan simulator (arrival-time partitioned) | ✅ |
| M2 | ADF ingestion pipelines | ✅ |
| M3 | Lakehouse bronze + silver | ⬜ |
| M4 | Lakehouse gold (ops + SLA mart) | ⬜ |
| M5 | Serving: Azure SQL + Cosmos DB | ⬜ |
| M6 | Data quality + control totals | ⬜ |
| M7 | Monitoring + runbooks | ⬜ |
| M8 | Final README + results | ⬜ |

## The one idea the whole platform turns on

Scan batches are partitioned by when a scan **arrived** (`sync_time`), not when it
**happened** (`event_time`). That is how real scan data lands — a rural delivery van syncs
when it finds signal — and it manufactures the central problem this platform exists to
solve: a parcel's scans arrive out of order, so any consumer that trusts arrival order
derives the wrong parcel state. The M6 control-total check between the lakehouse and the
Azure SQL mart is designed to make that disagreement *impossible to publish*.

## Run the simulator

```bash
pip install -r requirements.txt
python simulator/scan_events.py --parcels 5000     # quick; 200_000 for full scale
pytest simulator/tests -q
```

Outputs a landing zone an ADF copy activity would pick up
(`data/landing/dt=YYYY-MM-DD/hour=HH/…`), a `seller_master.csv` on-prem-SQL stand-in, and
`data/_manifest.json` with every injected defect counted on the emitted files.
