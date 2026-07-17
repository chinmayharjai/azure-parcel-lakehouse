# Access-pattern matrix — which store answers which question, and why

The architectural heart of the platform: **one pipeline, three fit-for-purpose serving
stores.** No single database serves all three consumers well, so each consumer's query
goes to the store built for its shape. Forcing one store to do all three means overpaying
on latency, cost, or both, everywhere.

| Consumer | The question | Store | Query shape | Latency target | Why this store |
|---|---|---|---|---|---|
| **Customer** | "Where is my parcel PCL-00042?" | **Cosmos DB** | Point read by `parcel_id` | **< 50 ms** p99 | Partition key = `parcel_id` ⇒ single-partition point read, ~1 RU, flat as the container grows. Millions of these per day. |
| **Ops manager** | "Backlog / breach-risk / failed-delivery hotspots per hub, this hour" | **Lakehouse gold (Delta)** | Scan + aggregate over recent partitions, Z-ordered by `hub_id` | seconds (interactive) | Columnar Delta with partition pruning + Z-order is built for hub-hour aggregations over the full history; refreshed each pipeline run. |
| **Finance / SLA** | "Breaches per lane per day; penalty computation" | **Azure SQL Database** | Joins across star dims, filtered by date/lane, BI concurrency | seconds, **exact** | Relational engine with real indexes + optimizer for analytical joins; exact, reconciled numbers penalties are computed from. |

## The same data, three shapes

All three are derived from the **same** silver/gold lakehouse — transformed once, served
three ways. That's what makes them agree: there is one definition of "delivered" (event
-time, in `gold_common.parcel_lifecycle`), and the M6 control total enforces that the
Azure SQL mart and the lakehouse still agree on it before finance can publish.

## Why not one store?

- **Everything in Cosmos?** Analytical joins ("breach rate per lane") become
  cross-partition fan-out queries — slow and RU-expensive. Point-read stores are bad at
  aggregation.
- **Everything in Azure SQL?** A relational DB can serve point lookups, but at millions of
  tracking QPS you're paying for a big SQL tier to do what a point-read store does for ~1
  RU — and you've coupled the customer-facing read path to the finance database's
  availability and load.
- **Everything in the lakehouse?** Delta/Spark is superb for batch analytics and hopeless
  at a 50 ms single-row lookup — query planning alone blows the latency budget.

Each store is doing the one thing it's best at. The cost of running three is far less than
the cost of forcing one to do all three badly.

## Latency expectations, stated honestly

The **< 50 ms** Cosmos target is the store's documented point-read characteristic (~1 RU,
single-digit ms server-side) plus network overhead — not a figure this portfolio
load-tested against a provisioned account. The ops/finance "seconds" targets are the
expected shape (interactive aggregation over this data volume), likewise not benchmarked
here. Measuring them for real means provisioning the Azure resources and running the
queries under load, which the M8 README flags as the remaining step. The *architecture* —
each query on the store built for it — is the defensible claim; the exact milliseconds are
a measurement, and the repo says which is which.
