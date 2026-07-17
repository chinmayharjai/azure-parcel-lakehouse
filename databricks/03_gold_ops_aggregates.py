"""Gold ops aggregates: hourly hub-level operational health.

The ops manager's question is "what is happening at my hubs right now?" — backlog,
things aging toward a breach, failed deliveries, and (the one this platform makes
visible) hubs whose scans are arriving systematically late. Grain is
(hub_id, event_date, event_hour).

Pure function `hub_hourly_ops(silver, lifecycle)`; `main()` does the Delta write with
OPTIMIZE + Z-ORDER.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gold_common import BREACH_RISK_FRACTION, FAILED_ATTEMPT, parcel_lifecycle


def hub_hourly_ops(silver: DataFrame, lifecycle: DataFrame) -> DataFrame:
    """Aggregate silver scans to hub-hour operational metrics.

    Breach-risk is computed per SCAN, point-in-time: at the moment of a scan, has the
    parcel passed 80% of its promised window without being delivered yet? That is the
    honest 'at risk right now' signal — a parcel delivered an hour later was still at
    risk at this scan, and a parcel already delivered is not at risk regardless of age.
    """
    # Join each scan to its parcel's lifecycle for the pickup anchor and the delivery
    # time. NOT promised_hours — every silver scan already carries it (it's constant
    # per parcel), and pulling it from both sides makes the reference ambiguous. Use
    # silver's own.
    j = silver.join(
        lifecycle.select("parcel_id", "pickup_time", "delivered_time"),
        on="parcel_id", how="left",
    )

    age_hours = (F.col("event_time").cast("long") - F.col("pickup_time").cast("long")) / 3600.0
    not_yet_delivered = (
        F.col("delivered_time").isNull() | (F.col("event_time") < F.col("delivered_time"))
    )
    is_breach_risk = (
        not_yet_delivered
        & (age_hours > (BREACH_RISK_FRACTION * F.col("promised_hours")))
    )

    annotated = (j
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("_is_failed", F.when(F.col("scan_type") == FAILED_ATTEMPT, 1).otherwise(0))
        .withColumn("_is_breach_risk", F.when(is_breach_risk, 1).otherwise(0))
        .withColumn("_is_late", F.when(F.col("is_late_arrival"), 1).otherwise(0))
        .withColumn("_is_ooo", F.when(F.col("is_out_of_order"), 1).otherwise(0))
    )

    agg = annotated.groupBy("hub_id", "event_date", "event_hour").agg(
        F.count("*").alias("total_scans"),
        F.countDistinct("parcel_id").alias("distinct_parcels"),
        F.sum("_is_failed").alias("failed_attempt_count"),
        F.countDistinct(F.when(F.col("_is_breach_risk") == 1, F.col("parcel_id")))
            .alias("breach_risk_parcels"),
        F.sum("_is_late").alias("late_arrival_scans"),
        F.sum("_is_ooo").alias("out_of_order_scans"),
    )

    # Failed-delivery RATE as its own column, not left to the dashboard to divide — a
    # rate metric with a zero denominator (an hour with no parcels) must be 0, not a
    # divide-by-zero, exactly as in the payments repo's quarantine rate.
    return agg.withColumn(
        "failed_delivery_rate",
        F.when(F.col("distinct_parcels") > 0,
               F.col("failed_attempt_count") / F.col("distinct_parcels"))
        .otherwise(F.lit(0.0)),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-path", required=True)
    ap.add_argument("--gold-ops-path", required=True)
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("gold_ops_aggregates")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    silver = spark.read.format("delta").load(args.silver_path)
    lifecycle = parcel_lifecycle(silver)
    ops = hub_hourly_ops(silver, lifecycle)

    (ops.write.format("delta").mode("overwrite")
        .partitionBy("event_date")
        .save(args.gold_ops_path))

    # Z-ORDER on hub_id: the ops dashboard filters by hub, and Z-ordering co-locates a
    # hub's rows within the day's files so a per-hub query reads far fewer of them.
    # (No-op if the table is tiny, but correct at scale and free to declare.)
    spark.sql(f"OPTIMIZE delta.`{args.gold_ops_path}` ZORDER BY (hub_id)")

    print(f"gold ops rows: {ops.count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
