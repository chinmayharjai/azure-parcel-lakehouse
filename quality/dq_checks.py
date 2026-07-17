"""Data-quality gates, run as a Databricks step between silver and gold.

Two kinds of check live here, and the distinction is the whole point:

  * VALIDITY checks — nulls in mandatory fields, referential integrity, row-count
    reconciliation across layers. These catch a broken pipeline.
  * The CONTROL TOTAL — the delivered-parcel count in the SLA mart must EXACTLY match
    the lakehouse gold count. This is finance's oldest trick: if the sum downstream
    doesn't equal the sum upstream, nothing ships. It catches a broken *number* even
    when every row is individually valid.

Every check returns a structured incident; `blocking_failures` picks the error-severity
ones that must halt publication. In the pipeline, a non-empty blocking set fails the
Databricks step, which fails the ADF run, which blocks the gold→Azure SQL copy — so
finance never computes penalties on data that didn't reconcile. Incidents land in a
`dq_incidents` Delta table either way, so a passing run is as auditable as a failing one.

The functions are pure (DataFrames/counts in, incidents out) so the gate logic is tested
without a cluster.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ERROR = "error"   # blocks publication
WARN = "warn"     # recorded, does not block

DELIVERED = "DELIVERED"


def _incident(check_name: str, passed: bool, severity: str,
              metric_value: float | None = None, detail: str = "") -> dict:
    return {
        "check_name": check_name,
        "passed": bool(passed),
        "severity": severity,
        "metric_value": None if metric_value is None else float(metric_value),
        "detail": detail,
    }


def check_row_reconciliation(bronze_rows: int, silver_rows: int,
                             quarantined_rows: int, dedup_removed: int) -> dict:
    """Every bronze row must be accounted for: it became a silver row, or was
    quarantined, or was a duplicate collapsed by dedup. If the four numbers don't
    balance, a row leaked or was double-counted somewhere between the layers — a
    class of bug that's invisible in any single record."""
    accounted = silver_rows + quarantined_rows + dedup_removed
    passed = accounted == bronze_rows
    return _incident(
        "row_reconciliation_bronze_to_silver", passed, ERROR,
        metric_value=bronze_rows - accounted,
        detail=(f"bronze={bronze_rows:,}; silver={silver_rows:,} + "
                f"quarantined={quarantined_rows:,} + dedup_removed={dedup_removed:,} "
                f"= {accounted:,}"),
    )


def check_mandatory_nulls(silver: DataFrame, mandatory_cols: list[str]) -> list[dict]:
    """No null in a field the platform depends on. A null scan_id or parcel_id in
    silver means a bad row got past quarantine — the check that guards the guard."""
    incidents = []
    for col in mandatory_cols:
        n = silver.filter(F.col(col).isNull()).count()
        incidents.append(_incident(
            f"null_check_{col}", n == 0, ERROR, metric_value=n,
            detail=f"{n:,} rows with null {col} in silver",
        ))
    return incidents


def check_referential_integrity(silver: DataFrame, lanes: DataFrame,
                                sellers: DataFrame) -> list[dict]:
    """Every scan references a known lane and a known seller. An orphan lane is an
    ERROR (the SLA promise lives on the lane; no lane means no computable promise); an
    orphan seller is a WARN (reporting attribution degrades, but SLA math still works).
    The severity split is a judgement about blast radius, not a formality."""
    orphan_lanes = (silver.join(lanes, on="lane_id", how="left_anti")
                    .select("lane_id").distinct().count())
    orphan_sellers = (silver.join(sellers, on="seller_id", how="left_anti")
                      .select("seller_id").distinct().count())
    return [
        _incident("ri_scan_has_known_lane", orphan_lanes == 0, ERROR,
                  metric_value=orphan_lanes,
                  detail=f"{orphan_lanes:,} distinct lane_ids in silver not in dim_lane"),
        _incident("ri_scan_has_known_seller", orphan_sellers == 0, WARN,
                  metric_value=orphan_sellers,
                  detail=f"{orphan_sellers:,} distinct seller_ids in silver not in seller_master"),
    ]


def control_total_delivered(gold_delivered: int, sql_delivered: int) -> dict:
    """THE finance-grade gate. The delivered-parcel count in the lakehouse gold and in
    the Azure SQL mart must be exactly equal. Not close — equal. A single-parcel
    disagreement means the two stores computed 'delivered' differently, and a penalty
    computed on either is suspect until they agree. Mismatch blocks the copy."""
    passed = gold_delivered == sql_delivered
    return _incident(
        "control_total_delivered", passed, ERROR,
        metric_value=gold_delivered - sql_delivered,
        detail=(f"lakehouse gold delivered={gold_delivered:,}, "
                f"Azure SQL mart delivered={sql_delivered:,}"),
    )


def delivery_method_divergence(silver: DataFrame, lifecycle: DataFrame) -> dict:
    """The meaningful observation, measured. Count delivered parcels two ways:

      correct — by EVENT time (the lifecycle: a DELIVERED scan exists)
      naive   — by ARRIVAL: take each parcel's LAST-RECEIVED scan (max sync_time) as
                its current state, the way a consumer that trusts arrival order would

    A parcel whose DELIVERED scan happened last but whose non-terminal scan ARRIVED
    later is seen by the naive method as still in transit — an undercount. This
    function measures exactly how many parcels naive processing would misclassify, and
    it is the number the README quotes. It's the concrete proof that arrival order !=
    event order matters in money, not just in principle."""
    correct = lifecycle.filter(F.col("is_delivered")).count()

    w = Window.partitionBy("parcel_id").orderBy(F.col("sync_time").desc(),
                                                F.col("event_seq").desc())
    naive = (silver.withColumn("_rn", F.row_number().over(w))
             .filter(F.col("_rn") == 1)
             .filter(F.col("scan_type") == DELIVERED)
             .count())

    divergence = correct - naive
    pct = (divergence / correct * 100) if correct else 0.0
    return {
        "correct_delivered": correct,
        "naive_delivered": naive,
        "divergence": divergence,
        "divergence_pct": round(pct, 3),
    }


def blocking_failures(incidents: list[dict]) -> list[dict]:
    """The error-severity incidents that must halt publication."""
    return [i for i in incidents if i["severity"] == ERROR and not i["passed"]]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-path", required=True)
    ap.add_argument("--gold-sla-path", required=True)
    ap.add_argument("--lanes-ref", required=True)
    ap.add_argument("--sellers-path", required=True)
    ap.add_argument("--dq-incidents-path", required=True)
    ap.add_argument("--bronze-rows", type=int, required=True)
    ap.add_argument("--quarantined-rows", type=int, required=True)
    ap.add_argument("--dedup-removed", type=int, required=True)
    ap.add_argument("--sql-delivered", type=int, required=True,
                    help="delivered count read back from the Azure SQL mart")
    args = ap.parse_args()

    import sys
    from datetime import datetime, timezone

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("dq_checks")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # gold_common lives in databricks/; add it to the path for the lifecycle import.
    sys.path.insert(0, "databricks")
    from gold_common import parcel_lifecycle

    silver = spark.read.format("delta").load(args.silver_path)
    lifecycle = parcel_lifecycle(silver)
    lanes = spark.read.option("multiline", "true").json(args.lanes_ref)
    sellers = spark.read.option("header", "true").csv(args.sellers_path)

    gold_delivered = lifecycle.filter(F.col("is_delivered")).count()

    incidents = [
        check_row_reconciliation(args.bronze_rows, silver.count(),
                                 args.quarantined_rows, args.dedup_removed),
        control_total_delivered(gold_delivered, args.sql_delivered),
    ]
    incidents += check_mandatory_nulls(silver, ["scan_id", "parcel_id", "event_time"])
    incidents += check_referential_integrity(silver, lanes, sellers)

    divergence = delivery_method_divergence(silver, lifecycle)
    print(f"delivery-method divergence: correct={divergence['correct_delivered']:,} "
          f"naive={divergence['naive_delivered']:,} "
          f"undercount={divergence['divergence']:,} ({divergence['divergence_pct']}%)")

    run_id = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = [{**i, "run_id": run_id} for i in incidents]
    (spark.createDataFrame(rows).write.format("delta").mode("append")
        .save(args.dq_incidents_path))

    blocking = blocking_failures(incidents)
    for i in incidents:
        print(f"[{'PASS' if i['passed'] else 'FAIL'}] {i['check_name']}: {i['detail']}")

    if blocking:
        print(f"\n{len(blocking)} BLOCKING failure(s) — gold->Azure SQL copy halted.")
        spark.stop()
        sys.exit(1)   # fails the Databricks step -> ADF run -> blocks the serve copy

    print("\nall gates passed — publication may proceed.")
    spark.stop()


if __name__ == "__main__":
    main()
