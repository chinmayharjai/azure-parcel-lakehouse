"""Shared gold logic: collapse a parcel's scan trail into one lifecycle row.

Both gold jobs need the same per-parcel summary — when it was picked up, whether and
when it was delivered, whether it breached SLA — so that computation lives here, once.
If ops and the SLA mart each derived "is this parcel delivered" their own way, they
could disagree, and the M6 control total (which reconciles the SLA mart against the
lakehouse) would be reconciling two different definitions of delivered.

The correctness crux, and the whole reason this project exists:

    delivery is determined by EVENT time, never arrival time.

A parcel's DELIVERED scan can *arrive* before an earlier hub scan (the inversion the
simulator injects). A naive consumer that ordered by arrival, or took the
last-arrived scan as "current state", would mark such a parcel delivered at the wrong
time — or mark it delivered before its hub-in even landed. Because silver already
repaired ordering (event_seq) and this function aggregates on event_time, the
lifecycle is computed on the true sequence. The M6 experiment measures exactly what a
naive arrival-ordered version would get wrong.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Terminal scan types split by what they mean for the customer promise.
DELIVERED_TO_CUSTOMER = "DELIVERED"
RTO_DELIVERED = "RTO_DELIVERED"
FAILED_ATTEMPT = "FAILED_ATTEMPT"

# Breach-risk threshold: a parcel whose elapsed time has passed this fraction of its
# promised window, and which is not yet delivered, is "at risk" — the ops floor wants
# these before they breach, not after.
BREACH_RISK_FRACTION = 0.80


def parcel_lifecycle(silver: DataFrame) -> DataFrame:
    """One row per parcel: pickup, delivery, SLA outcome — all on event time.

    pickup_time is the earliest event_time. The simulator never drops the PICKUP scan
    (only intermediate hub scans), so the earliest event is the pickup; using min()
    rather than filtering scan_type == 'PICKUP' is also robust to a genuinely missing
    pickup, where the earliest known event is the best available start.
    """
    delivered_evt = F.when(F.col("scan_type") == DELIVERED_TO_CUSTOMER, F.col("event_time"))
    rto_evt = F.when(F.col("scan_type") == RTO_DELIVERED, F.col("event_time"))
    failed_flag = F.when(F.col("scan_type") == FAILED_ATTEMPT, 1).otherwise(0)

    agg = silver.groupBy("parcel_id").agg(
        F.min("event_time").alias("pickup_time"),
        # max() over a when() ignores nulls, so this is the delivered event_time or
        # null if the parcel was never delivered — on event time, not arrival.
        F.max(delivered_evt).alias("delivered_time"),
        F.max(rto_evt).alias("rto_time"),
        F.sum(failed_flag).alias("failed_attempts"),
        F.max("promised_hours").alias("promised_hours"),
        F.first("lane_id", ignorenulls=True).alias("lane_id"),
        F.first("seller_id", ignorenulls=True).alias("seller_id"),
        F.count("*").alias("scan_count"),
        F.max(F.when(F.col("is_out_of_order"), 1).otherwise(0)).alias("had_out_of_order"),
    )

    actual_hours = (F.col("delivered_time").cast("long") - F.col("pickup_time").cast("long")) / 3600.0

    enriched = (agg
        .withColumn("is_delivered", F.col("delivered_time").isNotNull())
        .withColumn("actual_hours",
                    F.when(F.col("delivered_time").isNotNull(), actual_hours))
        .withColumn("is_breach",
                    F.col("delivered_time").isNotNull()
                    & (actual_hours > F.col("promised_hours")))
    )

    # A single primary exception reason, in priority order. RTO first (the parcel came
    # back — that dominates), then still-in-transit, then a delivered-but-breached
    # parcel, then delivered-but-with-failed-attempts, else a clean on-time delivery.
    exception_reason = (
        F.when(F.col("rto_time").isNotNull(), F.lit("rto"))
        .when(~F.col("is_delivered"), F.lit("in_transit"))
        .when(F.col("is_breach"), F.lit("sla_breach"))
        .when(F.col("failed_attempts") > 0, F.lit("delivered_after_failed_attempts"))
        .otherwise(F.lit(None))
    )
    return enriched.withColumn("exception_reason", exception_reason)
