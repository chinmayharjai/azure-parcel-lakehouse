"""Bronze ingest: raw scan JSON -> Delta bronze, schema-on-read with a rescue column.

Bronze's one job is to become a queryable, replayable copy of the raw bytes WITHOUT
losing anything. That means schema-on-read (parse against the known schema) but with
a rescue column that catches every field the schema didn't expect and every line that
didn't parse — so schema drift and malformed records are preserved for silver to
quarantine, never dropped at the door.

The transform is a pure function `to_bronze(raw_lines)` so it can be tested on a
handful of JSON strings — including a deliberately broken one — without a cluster or
a storage account. `main()` does the Delta I/O.

Idempotency: bronze is overwritten by (dt, hour) partition, the same partitioning as
landing. Re-running an hour replaces that partition's files rather than appending, so
a ret[rigger or a manual replay can't double-write. Deterministic partition path in,
deterministic partition path out.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from schema import RESCUE_COLUMN, SCAN_EVENT_SCHEMA, bronze_parse_schema


def to_bronze(raw_lines: DataFrame) -> DataFrame:
    """Parse one batch of raw NDJSON lines into typed bronze columns + a rescue column.

    `raw_lines` has a single string column `value`, one JSON object per row — exactly
    what `spark.read.text` yields, and what Auto Loader hands a foreachBatch. Parsing
    here (not in ADF) is deliberate: ADF moved the bytes verbatim; bronze is the first
    place we interpret them, and it does so without the right to discard.

    A line that fails to parse, or carries fields outside the schema, lands non-null
    in the rescue column with its original text. Nothing is dropped — that is the
    whole contract of a raw layer.
    """
    parsed = raw_lines.select(
        F.col("value").alias("_raw"),
        F.from_json(
            F.col("value"),
            # The parse schema INCLUDES the rescue column — required for PERMISSIVE
            # mode to populate it. A bad line yields all-null typed fields and the raw
            # text in the rescue column, instead of failing the whole batch.
            bronze_parse_schema(),
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": RESCUE_COLUMN},
        ).alias("j"),
    )

    typed_cols = [F.col(f"j.{f.name}").alias(f.name)
                  for f in SCAN_EVENT_SCHEMA.fields if f.name != RESCUE_COLUMN]

    bronze = parsed.select(
        *typed_cols,
        F.col(f"j.{RESCUE_COLUMN}").alias(RESCUE_COLUMN),
        F.col("_raw"),
        # Ingest metadata: when bronze saw the row, so a late replay is distinguishable
        # from the original load, and a per-partition file audit has a timestamp.
        F.current_timestamp().alias("_bronze_ingest_time"),
    )

    # Derive the arrival partition columns from sync_time (bronze is partitioned the
    # same way landing is). A rescued row has null sync_time; it still needs to land
    # somewhere, so it partitions under a sentinel rather than vanishing.
    return bronze.withColumn(
        "dt",
        F.coalesce(F.date_format("sync_time", "yyyy-MM-dd"), F.lit("_rescued")),
    ).withColumn(
        "hour",
        F.coalesce(F.date_format("sync_time", "HH"), F.lit("_rescued")),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing-path", required=True)
    ap.add_argument("--bronze-path", required=True)
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("bronze_ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    raw_lines = spark.read.text(args.landing_path)
    bronze = to_bronze(raw_lines)

    # Overwrite by partition (dynamic): only the (dt, hour) partitions present in this
    # batch are replaced, so a single-hour replay doesn't touch the rest of the table.
    (bronze.write.format("delta").mode("overwrite")
        .partitionBy("dt", "hour")
        .option("overwriteSchema", "false")
        .save(args.bronze_path))

    rescued = bronze.filter(F.col(RESCUE_COLUMN).isNotNull()).count()
    print(f"bronze rows: {bronze.count():,}  rescued: {rescued:,}")
    spark.stop()


if __name__ == "__main__":
    main()
