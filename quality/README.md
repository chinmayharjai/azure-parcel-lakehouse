# Data quality — validity gates + the control total

Runs as a Databricks step **between silver and gold**, invoked by ADF. A blocking
failure fails the step, which fails the ADF run, which halts the gold→Azure SQL copy —
so finance never computes penalties on data that didn't reconcile. Every incident (pass
or fail) is written to a `dq_incidents` Delta table, so a clean run is as auditable as a
dirty one.

## Two kinds of check, on purpose

**Validity** — catches a broken *pipeline*:
- **Row reconciliation** bronze → silver: every bronze row became a silver row, was
  quarantined, or was a duplicate collapsed by dedup. If the four numbers don't balance,
  a row leaked or was double-counted — invisible in any single record.
- **Mandatory nulls**: no null `scan_id` / `parcel_id` / `event_time` in silver (the
  check that guards the quarantine guard).
- **Referential integrity**: every scan references a known lane (ERROR — the SLA promise
  lives on the lane) and a known seller (WARN — attribution degrades, SLA math survives).
  The severity split is a blast-radius judgement, not a formality.

**The control total** — catches a broken *number* even when every row is valid:
> the delivered-parcel count in the lakehouse gold and in the Azure SQL mart must be
> **exactly** equal. Not close — equal. A one-parcel disagreement means the two stores
> computed "delivered" differently, and a penalty on either is suspect until they agree.
> Mismatch blocks the copy.

This is finance's oldest trick: if the sum downstream ≠ the sum upstream, nothing ships.
Cheap to compute, catastrophic to skip.

## The meaningful observation, measured — not asserted

`delivery_method_divergence` counts delivered parcels two ways and reports the gap:

- **correct** — by *event* time (a DELIVERED scan exists in the parcel's true sequence)
- **naive** — by *arrival*: take each parcel's last-*received* scan (max `sync_time`) as
  its current state, the way a consumer trusting arrival order would

A parcel whose DELIVERED scan happened last but whose non-terminal scan *arrived* later
is seen by the naive method as still in transit — an **undercount**. This function
measures exactly how many parcels naive processing misclassifies. It is the concrete
proof, in counted parcels, that "arrival order ≠ event order" costs money — and it's the
number the top-level README quotes (measured by running the pipeline, not asserted).

`test_naive_arrival_order_undercounts_delivered` builds that exact defect in miniature
and pins the behaviour: correct counts it delivered, naive misses it, the control total
would block.

## Tests

`quality/tests/` runs on the Spark CI job (the DataFrame checks need a session). Pure
count-based gates (reconciliation, control total, blocking-set selection) plus the
Spark-based validity checks and the divergence experiment.
