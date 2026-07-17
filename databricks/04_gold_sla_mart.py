"""Gold SLA mart: a finance-grade star schema for penalty computation.

Finance computes SLA penalties from this, so the numbers must be exact and the model
must be one BI tools and auditors can read: a fact at parcel grain, three conformed
dimensions. Grain is one row per parcel (the finest useful grain — penalties are
per-parcel), from which lane-daily rollups are a GROUP BY, not a re-derivation.

    fct_sla     one row per parcel: promised vs actual hours, breach flag, exception
    dim_lane    lane_id -> origin/dest hub, promised window
    dim_hub     hub_id -> name, city
    dim_date    delivery date -> calendar attributes

Pure builders; `main()` writes the four tables and runs OPTIMIZE/Z-ORDER. The delivered
count in fct_sla is the number the M6 control total reconciles against the lakehouse —
if the star and the lakehouse disagree on how many parcels were delivered, nothing
publishes.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gold_common import parcel_lifecycle


def build_dim_lane(lanes_ref: DataFrame) -> DataFrame:
    """dim_lane straight from the lane reference. promised_hours lives here because it
    is a property of the lane, and finance's penalty is 'actual vs the lane's promise'
    — the promise must travel with the lane, not be re-guessed per parcel."""
    return lanes_ref.select(
        F.col("lane_id"),
        F.col("origin_hub").alias("origin_hub_id"),
        F.col("dest_hub").alias("dest_hub_id"),
        F.col("promised_hours").alias("lane_promised_hours"),
    ).dropDuplicates(["lane_id"])


def build_dim_hub(hubs_ref: DataFrame) -> DataFrame:
    return hubs_ref.select("hub_id", "hub_name", "city").dropDuplicates(["hub_id"])


def build_fct_sla(lifecycle: DataFrame, dim_lane: DataFrame) -> DataFrame:
    """The fact: parcel-grain SLA outcome with foreign keys to lane and (via lane) the
    origin/dest hubs, plus a delivery_date_key into dim_date.

    delivery_date_key is null for a still-in-transit parcel — it belongs in the fact
    (finance needs to see it hasn't been delivered) but has no delivery date yet. That
    null is meaningful, not missing."""
    j = lifecycle.join(dim_lane, on="lane_id", how="left")

    return j.select(
        "parcel_id",
        "lane_id",
        "origin_hub_id",
        "dest_hub_id",
        "seller_id",
        "pickup_time",
        "delivered_time",
        F.when(F.col("delivered_time").isNotNull(),
               F.date_format("delivered_time", "yyyyMMdd").cast("int"))
            .alias("delivery_date_key"),
        F.col("promised_hours"),
        "actual_hours",
        "is_delivered",
        "is_breach",
        "exception_reason",
    )


def build_dim_date(fct_sla: DataFrame) -> DataFrame:
    """dim_date over the delivery dates present in the fact. Built from the data (not a
    pre-seeded calendar) so it never has gaps the fact needs, and never carries dates
    no fact references."""
    dates = (fct_sla
             .filter(F.col("delivery_date_key").isNotNull())
             .select(F.to_date("delivered_time").alias("date"))
             .distinct())
    return dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-path", required=True)
    ap.add_argument("--lanes-ref", required=True, help="reference/lanes.json")
    ap.add_argument("--hubs-ref", required=True, help="reference/hubs.json")
    ap.add_argument("--gold-sla-path", required=True, help="dir; writes fct_sla/ + dims")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("gold_sla_mart")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    silver = spark.read.format("delta").load(args.silver_path)
    lanes_ref = spark.read.option("multiline", "true").json(args.lanes_ref)
    hubs_ref = spark.read.option("multiline", "true").json(args.hubs_ref)

    lifecycle = parcel_lifecycle(silver)
    dim_lane = build_dim_lane(lanes_ref)
    dim_hub = build_dim_hub(hubs_ref)
    fct_sla = build_fct_sla(lifecycle, dim_lane)
    dim_date = build_dim_date(fct_sla)

    base = args.gold_sla_path.rstrip("/")
    fct_sla.write.format("delta").mode("overwrite").save(f"{base}/fct_sla")
    dim_lane.write.format("delta").mode("overwrite").save(f"{base}/dim_lane")
    dim_hub.write.format("delta").mode("overwrite").save(f"{base}/dim_hub")
    dim_date.write.format("delta").mode("overwrite").save(f"{base}/dim_date")

    # Z-ORDER the fact on lane_id: finance's reports group and filter by lane, so
    # co-locating a lane's rows makes the lane-daily rollup read fewer files.
    spark.sql(f"OPTIMIZE delta.`{base}/fct_sla` ZORDER BY (lane_id)")

    delivered = fct_sla.filter(F.col("is_delivered")).count()
    breaches = fct_sla.filter(F.col("is_breach")).count()
    print(f"fct_sla: {fct_sla.count():,}  delivered: {delivered:,}  breaches: {breaches:,}")
    spark.stop()


if __name__ == "__main__":
    main()
