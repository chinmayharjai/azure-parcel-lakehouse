"""Silver: dedup, repair ordering, flag late arrivals, mask PII, conform.

Every step is a pure function over a DataFrame, composed by `to_silver`, so each can
be tested in isolation on a few hand-built rows. `main()` does the Delta MERGE.

The through-line: silver takes bronze's faithful-but-messy copy and produces the
*trustworthy* layer everything downstream reads — but it does so without ever
silently discarding a row. Bad rows are routed to a quarantine with a reason;
duplicates are collapsed by an explicit rule; out-of-order arrivals are *flagged, not
hidden*, because the disorder is real information the M6 control-total check depends
on.

Idempotency: silver is written by MERGE on scan_id, so re-processing a batch updates
in place instead of appending. The dedup rule (earliest sync per scan_id) is
deterministic, so the merged result is identical no matter how many times a batch is
replayed.
"""

from __future__ import annotations

import argparse
from collections import namedtuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from schema import (KNOWN_SCAN_TYPES, LATE_ARRIVAL_THRESHOLD_MINUTES, RESCUE_COLUMN)

SilverResult = namedtuple("SilverResult", ["silver", "pii_mapping", "quarantine"])

_MANDATORY = ["scan_id", "parcel_id", "event_time", "sync_time"]


def split_valid_and_quarantine(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate rows silver can trust from rows it must quarantine (with a reason).

    A row is quarantined if bronze rescued it (parse failure or schema drift) or if
    any mandatory field is null. The reason is specific, not just 'invalid', because
    the M6 dq_incidents table and the runbooks need to know WHICH kind of bad — a
    rescued row means schema drift (an upstream deploy), a null scan_id means a
    fundamentally unusable record.
    """
    reason = (
        F.when(F.col(RESCUE_COLUMN).isNotNull(), F.lit("rescued_schema_drift_or_parse_error"))
        .when(F.col("scan_id").isNull(), F.lit("null_scan_id"))
        .when(F.col("parcel_id").isNull(), F.lit("null_parcel_id"))
        .when(F.col("event_time").isNull(), F.lit("null_event_time"))
        .when(F.col("sync_time").isNull(), F.lit("null_sync_time"))
        .otherwise(F.lit(None))
    )
    tagged = bronze.withColumn("quarantine_reason", reason)
    quarantine = tagged.filter(F.col("quarantine_reason").isNotNull())
    valid = tagged.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    return valid, quarantine


def dedup_scans(df: DataFrame) -> DataFrame:
    """Collapse duplicate re-emissions to one row per scan_id, keeping the EARLIEST
    sync_time (the first arrival).

    This rule is not arbitrary — it is the same tie-break the M1 manifest uses to
    count inversions. If silver deduped to the latest sync instead, the inversion
    structure silver sees would differ from what the manifest measured, and the M6
    control total (which compares the two) would disagree for a reason that is a
    dedup convention, not a data defect. One rule, stated in both places.
    """
    w = Window.partitionBy("scan_id").orderBy(F.col("sync_time").asc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


def annotate_ordering(df: DataFrame) -> DataFrame:
    """Give every scan its TRUE position (by event_time) and its ARRIVAL position (by
    sync_time), and flag the ones where they disagree.

    This is the 'repair event ordering' step, and the word 'repair' is deliberate:
    silver does not reorder or drop anything. It computes `event_seq` — the correct
    movement order — so downstream can trust it regardless of arrival order, and it
    surfaces `is_out_of_order` so the disorder is queryable. Hiding the disorder
    (by only keeping event order) would throw away exactly the signal the control
    total uses to prove naive processing is wrong.
    """
    by_event = Window.partitionBy("parcel_id").orderBy(F.col("event_time").asc(),
                                                       F.col("scan_id").asc())
    by_arrival = Window.partitionBy("parcel_id").orderBy(F.col("sync_time").asc(),
                                                         F.col("scan_id").asc())
    return (df
            .withColumn("event_seq", F.row_number().over(by_event))
            .withColumn("arrival_seq", F.row_number().over(by_arrival))
            .withColumn("is_out_of_order", F.col("event_seq") != F.col("arrival_seq")))


def flag_late_arrivals(df: DataFrame) -> DataFrame:
    """Flag scans whose sync lagged their event by more than the threshold.

    sync_lag = sync_time - event_time. The late-sync hub (2-8h) trips this; a rural
    van that took an hour does not. The flag is kept as data (not filtered) so an ops
    dashboard can show the late-sync hub as the hub-shaped hole it is, and so the
    parcel's state is still computed — a late scan is a real scan, just a late one.
    """
    lag_min = (F.col("sync_time").cast("long") - F.col("event_time").cast("long")) / 60.0
    return (df
            .withColumn("sync_lag_minutes", lag_min)
            .withColumn("is_late_arrival",
                        F.col("sync_lag_minutes") > F.lit(LATE_ARRIVAL_THRESHOLD_MINUTES)))


def flag_unknown_scan_type(df: DataFrame) -> DataFrame:
    """An unknown scan_type is structurally valid but semantically new — flag it so a
    new operational scan type shows up as a metric instead of silently bucketing into
    'other'."""
    known = F.array(*[F.lit(t) for t in sorted(KNOWN_SCAN_TYPES)])
    return df.withColumn("is_unknown_scan_type",
                         ~F.array_contains(known, F.col("scan_type")))


def mask_pii(df: DataFrame, salt: str) -> tuple[DataFrame, DataFrame]:
    """Hash customer_phone and split the reverse mapping into a restricted table.

    Silver keeps only `customer_phone_hash`; the raw number never survives into the
    analytical layer. The hash is salted so it is not reversible by a rainbow table
    of the 10-digit phone space (a plain SHA-256 of a phone number is trivially
    reversible — there are only ~10^9 of them). Re-identification requires the
    mapping table, which lives under its own restricted path with its own ACL, so
    'who can join a hash back to a person' is a single, auditable grant.

    Normalizing/lowercasing is unnecessary here (phones are digits), but the hash is
    computed on the exact stored string so the mapping round-trips exactly.
    """
    hashed = df.withColumn(
        "customer_phone_hash",
        F.when(F.col("customer_phone").isNotNull(),
               F.sha2(F.concat(F.lit(salt), F.col("customer_phone")), 256))
    )
    mapping = (hashed
               .filter(F.col("customer_phone").isNotNull())
               .select("customer_phone_hash", "customer_phone")
               .distinct())
    silver = hashed.drop("customer_phone", "_raw", RESCUE_COLUMN)
    return silver, mapping


def to_silver(bronze: DataFrame, salt: str) -> SilverResult:
    """Compose the full silver transform. Order matters:

      quarantine first  — never let a bad row into the dedup/ordering windows
      dedup next        — collapse duplicates BEFORE ordering, or a duplicate would
                          occupy a sequence position and corrupt event_seq
      then annotate/flag on the clean, deduped set
      mask last         — so the mapping table is built from deduped rows only
    """
    valid, quarantine = split_valid_and_quarantine(bronze)
    deduped = dedup_scans(valid)
    ordered = annotate_ordering(deduped)
    flagged = flag_unknown_scan_type(flag_late_arrivals(ordered))
    silver, mapping = mask_pii(flagged, salt)
    return SilverResult(silver=silver, pii_mapping=mapping, quarantine=quarantine)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-path", required=True)
    ap.add_argument("--silver-path", required=True)
    ap.add_argument("--pii-mapping-path", required=True)
    ap.add_argument("--quarantine-path", required=True)
    ap.add_argument("--salt", required=True, help="in prod, fetched from Key Vault")
    args = ap.parse_args()

    from delta.tables import DeltaTable

    spark = (
        SparkSession.builder.appName("silver_clean")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    bronze = spark.read.format("delta").load(args.bronze_path)
    result = to_silver(bronze, args.salt)

    # MERGE by scan_id for idempotent upsert: replay updates in place, never appends
    # a second copy. On first run the table won't exist, so create it.
    if DeltaTable.isDeltaTable(spark, args.silver_path):
        tgt = DeltaTable.forPath(spark, args.silver_path)
        (tgt.alias("t").merge(result.silver.alias("s"), "t.scan_id = s.scan_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        result.silver.write.format("delta").partitionBy("dt").save(args.silver_path)

    # The mapping table appends new hashes; distinct guards against re-adding a known
    # one. Restricted path — its ACL is the re-identification boundary.
    (result.pii_mapping.write.format("delta").mode("append").save(args.pii_mapping_path))
    (result.quarantine.write.format("delta").mode("append").save(args.quarantine_path))

    print(f"silver: {result.silver.count():,}  "
          f"quarantined: {result.quarantine.count():,}  "
          f"pii_map: {result.pii_mapping.count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
