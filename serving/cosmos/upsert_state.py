"""Cosmos DB: one document per parcel = its latest state + a mini scan history.

This is the store behind "where is my order?" — millions of point reads by parcel_id,
each needing to return in tens of milliseconds. The whole design follows from that one
access pattern:

  * ONE document per parcel, id = parcel_id, partition key = parcel_id. Every tracking
    lookup is then a single-partition point read — the cheapest and fastest thing
    Cosmos does (~1 RU, single-digit ms), never a cross-partition query.
  * The document carries the CURRENT state plus a short history, so the customer screen
    renders from one read with no join and no fan-out.

`build_parcel_state_documents` is a pure Spark transform (chispa-tested); `upsert_documents`
does the write and imports the Cosmos SDK lazily so the tests need no account.

Idempotency: the write is an UPSERT keyed on id=parcel_id, and the document is a pure
function of the parcel's scans, so re-running a batch overwrites each parcel's doc with
an identical one. Reprocessing never creates a second document for a parcel.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Cap the embedded history so a pathological parcel can't bloat its document (and the
# customer screen only shows the recent trail anyway). Full history lives in the
# lakehouse; Cosmos holds the tracking view, not the system of record.
MAX_HISTORY_EVENTS = 12

DELIVERED_TO_CUSTOMER = "DELIVERED"
TERMINAL_TYPES = {"DELIVERED", "RTO_DELIVERED"}


def build_parcel_state_documents(silver: DataFrame) -> DataFrame:
    """Collapse each parcel's scans into one tracking document.

    Current state is the LAST scan by event order (event_seq) — event order, not
    arrival order, so a tracking lookup never shows a stale or out-of-sequence status
    because a later-happening scan arrived earlier. This is the same event-time
    correctness the gold layer enforces, applied to the customer-facing view.
    """
    # Sort each parcel's scans into an event-ordered array of history entries. Sorting
    # by a struct whose FIRST field is event_seq gives event order deterministically
    # (collect_list alone does not guarantee order).
    history_entry = F.struct(
        F.col("event_seq").alias("seq"),
        F.col("scan_type").alias("status"),
        F.col("hub_id").alias("hub_id"),
        F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
        F.col("is_out_of_order").alias("was_out_of_order"),
    )

    grouped = silver.groupBy("parcel_id").agg(
        F.sort_array(F.collect_list(history_entry)).alias("_full_history"),
        F.max("promised_hours").alias("promised_hours"),
        F.first("lane_id", ignorenulls=True).alias("lane_id"),
        F.first("seller_id", ignorenulls=True).alias("seller_id"),
    )

    # The current state is the last element of the event-ordered history.
    last = F.element_at(F.col("_full_history"), -1)

    docs = grouped.select(
        F.col("parcel_id").alias("id"),          # Cosmos item id
        F.col("parcel_id"),                       # partition key (same value, by design)
        last.getField("status").alias("current_status"),
        last.getField("hub_id").alias("current_hub_id"),
        last.getField("event_time").alias("last_event_time"),
        F.array_contains(
            F.array(*[F.lit(t) for t in sorted(TERMINAL_TYPES)]),
            last.getField("status"),
        ).alias("is_terminal"),
        (last.getField("status") == F.lit(DELIVERED_TO_CUSTOMER)).alias("is_delivered"),
        F.col("promised_hours"),
        F.col("lane_id"),
        F.col("seller_id"),
        # Keep only the most recent MAX_HISTORY_EVENTS entries — the tail of the array.
        F.slice(F.col("_full_history"),
                F.greatest(F.lit(1), F.size("_full_history") - MAX_HISTORY_EVENTS + 1),
                F.lit(MAX_HISTORY_EVENTS)).alias("scan_history"),
        F.current_timestamp().alias("_doc_updated_at"),
    )
    return docs


def upsert_documents(docs: DataFrame, endpoint: str, database: str, container: str,
                     key: str) -> int:
    """Upsert the documents into Cosmos. Imports the SDK lazily so tests don't need it.

    In production this runs as the Databricks Cosmos OLTP connector
    (`cosmos.oltp` format) for throughput; this SDK path is the readable reference and
    the one used for small serving refreshes. Either way it's an UPSERT on id, so the
    write is idempotent.
    """
    from azure.cosmos import CosmosClient, PartitionKey  # noqa: F401

    client = CosmosClient(endpoint, credential=key)
    db = client.create_database_if_not_exists(database)
    cont = db.create_container_if_not_exists(
        id=container,
        partition_key=PartitionKey(path="/parcel_id"),
        # 1000 RU/s sits inside the Cosmos free-tier grant (see README) and comfortably
        # serves point reads; scale up only if the write refresh or read QPS demands it.
        offer_throughput=1000,
    )

    count = 0
    for row in docs.toLocalIterator():
        cont.upsert_item(row.asDict(recursive=True))
        count += 1
    return count
