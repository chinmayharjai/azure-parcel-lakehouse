# Runbook — pipeline failure

**Symptom:** an ADF pipeline run shows Failed — the metric alarm paged, or the webhook
posted to the ops channel.

> The instinct is to hit "rerun" and hope. Don't — not until you know *which layer*
> failed, because the four layers fail for completely different reasons and a blind rerun
> fixes only one of them (a transient blip) while wasting time on the other three. Triage
> top-down through the stack: **trigger → integration runtime → notebook → copy.** Each
> has a distinct signature; the first one that matches is your answer.

## 0. Which pipeline, and is it idempotent? (30 seconds)

Every pipeline here is idempotent — copies overwrite by `(dt, hour)` partition, silver
MERGEs by `scan_id`, the serve load truncates its target, Cosmos upserts by id. **So a
rerun is always safe**; the question is never "will rerun corrupt something" but "will
rerun *work*, or just fail again the same way." That's what the triage below decides.

Open the failed run in ADF Monitor → the activity that shows Failed is your entry point.

## 1. Trigger layer — did the right thing even run?

Symptoms: no run at all when you expected one, or a run with the *wrong* parameters
(processing the wrong hour).

| Check | Finding | Fix |
|---|---|---|
| Did `tr_storage_event` fire? | No run for a batch that landed | Event Grid resource provider unregistered, or the event was dropped. The **hourly schedule is the safety net** — it will reprocess that hour. Confirm `tr_hourly_schedule` picks it up; if EventGrid is the culprit, re-register `Microsoft.EventGrid` on the subscription |
| Run fired with wrong `dt`/`hour` | Parameters look off | The trigger's date expression — check it processed the *previous* hour, not the current one. A wrong window means the right pipeline ran on the wrong data |

**Do not** "fix" a missed event trigger by manually running the hour *and* leaving the
schedule to also run it — they'd both process it. They can (idempotency), but confirm you
aren't chasing a duplicate that isn't there. One is enough.

## 2. Integration Runtime — can ADF reach the source?

Only relevant for `pl_ingest_seller_master` (the on-prem pull). Symptom: the copy fails
with a connection or timeout error, *not* a data error.

| Check | Finding | Fix |
|---|---|---|
| SHIR node status (ADF → Manage → Integration runtimes) | Offline / unavailable | The self-hosted agent's host is down or lost its outbound connection. Restart the SHIR service on the host; a **two-node SHIR** would have ridden through this, which is why prod runs two |
| SHIR CPU / queue | Maxed | The agent host is undersized for concurrent copies — stagger schedules or scale the host |
| Key Vault fetch of the connection string | Access denied | The factory's managed identity lost its Key Vault grant (a policy change?) — re-grant "Key Vault Secrets User" |

The SHIR is the on-prem-only failure. If the failing pipeline is the scan ingest (which
uses ADLS via managed identity, no SHIR), skip this section entirely — it's not your
problem.

## 3. Notebook layer — did the transform or a DQ gate fail?

Symptom: a Databricks activity failed. **The critical fork:** was it an *infrastructure*
failure or a *data-quality gate* doing its job?

| Finding | Meaning | Response |
|---|---|---|
| DQ job failed with a blocking incident (control total, null key, orphan lane) | **The gate worked.** Bad data was caught before it reached finance | This is NOT a pipeline bug. Go to the `dq_incidents` table, read the failing check, and fix the *data* (or the upstream that produced it). Rerunning without fixing the data just trips the same gate |
| Cluster failed to start / lost | Infra | Restart; check the cluster pool and quota. A rerun likely succeeds |
| Notebook threw (schema, path, OOM) | Code or data shape | Read the traceback. An OOM on silver is usually watermark/state or small-file growth — see the transform's notes. Fix, then rerun |

The control-total failure is the one people misread most: a red pipeline there is the
system **succeeding** at protecting finance. The fix lives in the data, never in a retry.

## 4. Copy layer — did the bytes move?

Symptom: a Copy activity failed (landing→bronze, or the serve JDBC write).

| Finding | Fix |
|---|---|
| Copy failed after 3 retries with a storage 503/throttle | Transient — the retries already tried; rerun once. If it recurs, the storage account is throttling (check its metrics) |
| Serve copy failed the **row-count gate** | A short write — see the count in the error. This is the copy-integrity gate (M5) working; investigate why gold and SQL disagree before rerunning |
| Landing folder missing | The `CheckLandingExists` step failed fast — the hour genuinely received no data, or the path is wrong. Confirm the batch actually landed |

## Escalation

| Situation | Action |
|---|---|
| Transient (storage/cluster blip), rerun succeeds | Note it; no escalation |
| DQ gate blocking | **Data incident**, not infra — engage the upstream data owner; do not override the gate to publish |
| SHIR down and no HA node | Infra — the on-prem bridge is a single point of failure until the second node is restored |
| Same pipeline fails 3× on rerun | Stop rerunning. The failure is deterministic — it's data or code, and the traceback (not another rerun) has the answer |
