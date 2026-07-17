"""Load the gold SLA star into Azure SQL, with a row-count validation gate.

ADF orchestrates this as a Databricks activity (see adf/pipelines/pl_serve_sla_to_sql).
The write is a JDBC overwrite per table; the gate is the point — the copy is only
"done" if every row that left gold arrived in SQL. A silent short-write (a truncated
batch, a swallowed constraint violation) is exactly the kind of thing finance must
never compute penalties on top of, so a count mismatch fails the job, which fails the
ADF run, which blocks anything downstream.

This is the COPY-INTEGRITY check (did the bytes make it). It is deliberately distinct
from the M6 control total, which is the SEMANTIC check (do the two stores agree on how
many parcels were delivered). Both must pass to publish; they catch different failures.
"""

from __future__ import annotations

import argparse


class RowCountMismatch(Exception):
    """Raised when the source and destination row counts disagree after a load."""


def validate_row_counts(table: str, expected: int, actual: int) -> None:
    """The gate, as a pure function so its logic is tested without a database.

    Exact equality — not 'close enough'. A copy that dropped even one row is a broken
    copy; tolerance here would be tolerance for silent data loss in a finance table.
    """
    if expected != actual:
        raise RowCountMismatch(
            f"{table}: gold had {expected:,} rows, Azure SQL received {actual:,} "
            f"({expected - actual:+,}). Load failed the copy-integrity gate; the "
            f"pipeline is blocked so finance does not compute on a short-written table."
        )


def _jdbc_url(server: str, database: str) -> str:
    return (f"jdbc:sqlserver://{server}.database.windows.net:1433;"
            f"database={database};encrypt=true;trustServerCertificate=false;"
            f"loginTimeout=30;")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold-sla-path", required=True, help="dir with fct_sla/ + dims")
    ap.add_argument("--sql-server", required=True)
    ap.add_argument("--sql-database", required=True)
    ap.add_argument("--sql-user", required=True)
    ap.add_argument("--sql-password", required=True, help="in prod, from Key Vault via ADF")
    args = ap.parse_args()

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("serve_sla_to_sql")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    url = _jdbc_url(args.sql_server, args.sql_database)
    props = {"user": args.sql_user, "password": args.sql_password,
             "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    base = args.gold_sla_path.rstrip("/")

    # Load dims first, fact last: the fact has FKs to the dims, so the dims must exist
    # before the fact rows that reference them, or the constraint load fails.
    for table in ["dim_lane", "dim_hub", "dim_date", "fct_sla"]:
        src = spark.read.format("delta").load(f"{base}/{table}")
        expected = src.count()

        (src.write.format("jdbc").mode("overwrite")
            .option("url", url).option("dbtable", f"dbo.{table}")
            .options(**props).save())

        actual = (spark.read.format("jdbc")
                  .option("url", url).option("dbtable", f"dbo.{table}")
                  .options(**props).load().count())

        validate_row_counts(table, expected, actual)
        print(f"{table}: {actual:,} rows loaded and verified")

    print("all tables loaded and row-count verified")
    spark.stop()


if __name__ == "__main__":
    main()
