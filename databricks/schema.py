"""The conformed scan-event schema — declared once, used by bronze and silver.

Keeping the schema in one importable module (rather than inline in each notebook) is
what lets the tests assert against the same contract the jobs enforce. If bronze and
silver each declared their own StructType, they could drift, and the drift would
surface as a silent cast-to-null in production.
"""

from __future__ import annotations

from pyspark.sql.types import (IntegerType, StringType, StructField, StructType,
                               TimestampType)

# The fields the platform depends on. A source that stops sending one of these has
# broken its contract, and the rescue column (bronze) plus the not-null conformance
# (silver) are the two places that becomes visible instead of silent.
SCAN_EVENT_SCHEMA = StructType([
    StructField("scan_id", StringType(), nullable=False),
    StructField("parcel_id", StringType(), nullable=False),
    StructField("lane_id", StringType(), nullable=True),
    StructField("seller_id", StringType(), nullable=True),
    StructField("hub_id", StringType(), nullable=True),
    StructField("scan_type", StringType(), nullable=True),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("sync_time", TimestampType(), nullable=False),
    StructField("customer_phone", StringType(), nullable=True),   # PII — masked in silver
    StructField("promised_hours", IntegerType(), nullable=True),
])

# The set of scan types the pipeline understands. An unknown scan_type is not a
# parse error (the row is structurally fine) but IS a conformance flag — a new scan
# type means the operations side shipped something the data platform wasn't told
# about, which is worth surfacing rather than silently bucketing.
KNOWN_SCAN_TYPES = {
    "PICKUP", "HUB_IN", "HUB_OUT", "OFD", "DELIVERED",
    "FAILED_ATTEMPT", "RTO_INITIATED", "RTO_DELIVERED",
}

# Terminal scan types — the ones that end a parcel's journey. Gold uses these; silver
# just needs to know which scans are terminal to validate a trail has exactly one.
TERMINAL_SCAN_TYPES = {"DELIVERED", "RTO_DELIVERED"}

# A scan whose sync lag exceeds this is flagged late-arriving. 120 minutes is chosen
# to sit above normal device upload lag (1-20 min) and below the late-sync hub's
# 2-8h, so the flag catches the systematic-late hub without firing on every rural
# van that took an hour to find signal.
LATE_ARRIVAL_THRESHOLD_MINUTES = 120

# The name of the rescue column. In Databricks this is the Auto Loader
# rescuedDataColumn, which captures BOTH unparseable records AND additive schema
# drift (fields the schema didn't declare). In open-source Spark (and the tests) it
# is the permissive-mode corrupt-record column, which captures unparseable records
# only — from_json silently ignores unknown extra fields rather than rescuing them.
# The job is the same in spirit: a malformed record is quarantined with its raw text
# instead of dropped. The difference (drift capture) is a Databricks-runtime feature,
# stated here so the claim isn't oversold by the OSS behaviour the tests can prove.
RESCUE_COLUMN = "_rescued_data"


def bronze_parse_schema() -> StructType:
    """The schema handed to from_json: the event schema plus the rescue column.

    The rescue column MUST be in the parse schema for from_json's PERMISSIVE mode to
    populate it — a corrupt-record column that isn't in the schema is silently
    dropped, and malformed rows would then become all-null structs with no raw text
    kept. This is the subtlety that makes the difference between 'quarantined with
    evidence' and 'vanished'.
    """
    return StructType(list(SCAN_EVENT_SCHEMA.fields)
                      + [StructField(RESCUE_COLUMN, StringType(), nullable=True)])
