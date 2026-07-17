# ADF — ingestion orchestration

The Azure Data Factory layer: move bytes and orchestrate, nothing else. No business
logic lives here — ADF copies and schedules, Databricks transforms (M3+). That split
is deliberate and is the first thing to defend in an interview: *copy activities for
movement, notebook activities for logic.* Business rules written as ADF data flows are
the thing you cannot unit-test and cannot read in a diff.

Everything here is import-ready ADF resource JSON, parameterized, with **zero secrets**
(a CI test enforces that — see `adf/tests/`). Import via the ADF UI (Manage → ARM
template) or `az datafactory pipeline create`.

## What's here

| Resource | Type | Job |
|---|---|---|
| `ls_keyvault` | Linked service | The only place a secret is named; everything else fetches through it |
| `ls_adls_gen2` | Linked service | The lake, via the factory's managed identity — no keys |
| `ls_onprem_sql` | Linked service | On-prem SQL Server, reached through the SHIR |
| `shir_onprem` | Integration runtime | Self-hosted agent bridging ADF to on-prem |
| `ds_landing_scan_binary` / `ds_bronze_scan_binary` | Datasets | Raw scan bytes, landing and bronze |
| `ds_onprem_seller_master` / `ds_bronze_seller_master` | Datasets | Seller extract, source and bronze snapshot |
| `pl_ingest_scans_landing_to_bronze` | Pipeline | Hourly/event-driven binary copy |
| `pl_ingest_seller_master` | Pipeline | Daily on-prem pull through the SHIR |
| `tr_hourly_schedule` / `tr_storage_event` | Triggers | Completeness net + low-latency path |

## The decisions worth explaining

**Binary copy, not parsed copy.** The scan pipeline moves gzipped bytes verbatim into
bronze. ADF does *not* parse the JSON. A parsed (Json/DelimitedText) dataset would make
ADF interpret each file and drop a malformed line before bronze ever saw it — defeating
the purpose of a raw, replayable landing zone. Parsing, schema enforcement, and the
rescue column all happen in the Databricks silver step, where a bad record can be
quarantined with a reason instead of silently discarded.

**Two triggers, on purpose.** The storage-event trigger fires when a batch lands and
processes exactly that hour — this is what keeps customer tracking fresh within minutes.
The hourly schedule is the completeness net: it reprocesses the previous hour to catch
anything the event trigger missed (a dropped Event Grid notification, an ADF blip).
Events for latency, schedule for completeness. Because both invoke the *same*
parameterized pipeline and the target bronze path is a pure function of `(dt, hour)`, the
schedule re-running an hour the event already processed simply overwrites the same
partition — no duplication. (That idempotency is why the belt-and-suspenders design is
safe rather than double-counting.)

**Retry is fixed-interval, and that's an honest statement about ADF.** The requirement is
"3 attempts, exponential backoff." ADF's *activity-level* retry (`retry: 3`,
`retryIntervalInSeconds: 60`) is **fixed-interval** — the platform does not do
exponential backoff at the activity level, and claiming it does would be wrong. True
exponential backoff in ADF requires an `Until` loop wrapping the copy with a `Wait` whose
duration doubles each pass. For transient storage/SHIR blips a fixed 60s × 3 is the right
tool (simple, readable, enough); the Until-loop pattern is documented here as the
escalation for a genuinely flaky source, not used by default because it trades
readability for backoff math this workload doesn't need.

**Failure handling is two independent signals.** On a copy failure the pipeline (a) POSTs
a compact alert to an ops webhook and (b) runs a `Fail` activity so the run status becomes
Failed. The webhook is the fast push; the Failed status is what the ADF metric alarm
(M7) watches. Two paths because either one can be down exactly when it's needed — a
webhook endpoint outage must not also mean the metric alarm never fires.

**The SHIR is the whole on-prem story.** `ls_onprem_sql` reaches the seller database only
through `shir_onprem`, a self-hosted agent inside the corporate network that dials *out*
to ADF over 443. No inbound firewall port is opened; the cloud never connects to the
database. For a laptop demo, swap `connectVia` to `AutoResolveIntegrationRuntime` and
point the linked service at an Azure SQL stand-in — one line changes between "reaches
on-prem" and "reaches cloud," which is exactly the abstraction the SHIR provides.

## Parameters (supplied per environment, no secrets)

`storageAccountName`, `keyVaultName`, and `alertWebhookUrl` are global/pipeline
parameters set per environment (dev/prod) at import time. Secrets — the on-prem
connection string, the real webhook URL — live in Key Vault and are fetched at runtime;
they are never in these files or in the ARM parameter file.

## What CI verifies (`adf/tests/`)

Because there's no Azure in CI, the tests verify everything checkable statically: valid
JSON, `name` matches filename (ADF keys on it), every `referenceName` resolves to a
resource in this repo (the #1 cause of import failure), no inline secrets, secrets
actually route through Key Vault, Copy activities retry ≥ 3, each ingest pipeline both
alerts and fails on failure, and both triggers invoke the scan pipeline. Eight tests —
enough that a broken import or a pasted credential fails the PR, not the deploy.
