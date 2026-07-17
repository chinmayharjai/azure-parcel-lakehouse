# Monitoring & alerting

What to watch, what fires a page, and — the part most monitoring setups skip — what each
alert should make the responder *do*. An alert nobody knows how to act on is noise that
trains people to ignore the console.

The layers each have their own failure signature, so they're monitored separately: ADF
(orchestration), Databricks (transforms), Cosmos (the read path), and the subscription
(cost). All alert rules are declared as code (Azure Monitor / Bicep) alongside the
Terraform-style infra so they deploy with the platform, not after an incident.

## ADF — orchestration

**Pipeline-failed (page).** Azure Monitor alert on the `PipelineFailedRuns` metric,
scoped to each pipeline, threshold ≥ 1 over 5 minutes. This is the backstop behind the
in-pipeline webhook alerts (M2/M5): the pipelines *push* a webhook on failure, but a push
can be lost, so the metric alarm fires independently on the run status. Two independent
signals for the same failure, because either path can be down when it's needed.

**Why both, concretely:** the webhook tells you *fast and with context* (which partition,
which run); the metric alarm guarantees you hear about it *at all* even if the webhook
endpoint is down. Route the webhook to the ops channel, the metric alarm to the on-call
pager.

**Long-running pipeline (warn).** Alert on pipeline duration > 2× the rolling median. A
copy that normally takes 4 minutes running for 20 is usually a stuck Self-Hosted IR or a
storage throttle — worth a look before it becomes a missed SLA.

**Trigger-not-firing (warn).** The storage-event trigger going silent doesn't raise an
error — nothing fails, batches just quietly stop being processed on the low-latency path.
Alert on a drop in `TriggerSuccessfulRuns` for `tr_storage_event` to zero over an hour
while landing blobs are still arriving. This is the "suspiciously quiet" alarm; silence is
the most dangerous failure mode because nothing else reports it.

## Databricks — transforms

**Job-failed (page).** Alert on the Databricks job run result = FAILED for the bronze,
silver, gold, and DQ jobs. The DQ job failing is special: it means a **blocking data-
quality gate tripped** (a control-total mismatch, a null key), and that is a *correct*
failure doing its job — the runbook response is "find the bad data," not "restart the
job." Tag this alert so the responder knows a DQ failure is data, not infra.

**Job-duration drift (warn).** A silver job whose runtime is climbing week over week is
usually state or small-file growth; catch it before it's an OOM.

## Cosmos DB — the read path

**RU throttling / 429s (page).** The signal that customer tracking lookups are being
rate-limited. Alert on `TotalRequestUnits` sustained near the provisioned ceiling, and on
any `TotalRequests` with status 429. A 429 means a customer got a slow or failed "where is
my parcel" — the most visible failure in the whole system. See
[runbooks/cosmos_hot_partition.md](../runbooks/cosmos_hot_partition.md): a 429 is *either*
genuine load (raise RU / autoscale) *or* a hot partition (a design problem RU won't fix),
and telling them apart is the first triage step.

**Normalized RU consumption per partition (warn).** The early-warning signal for a hot
partition — one physical partition running hot while others idle — before it becomes 429s.

## Cost — the budget alarm

**Set on day one, before any other resource.** The failure mode of a cloud data platform
is not usually an outage; it's a surprise bill. An Azure Budget on the resource group with
alerts at 50 / 80 / 100 % of the monthly cap, plus an **action group that emails and can
trigger an automation runbook** to stop non-critical compute at 100 %. The Cosmos free
tier and a paused Databricks cluster keep the idle cost near zero, but a left-running
cluster or an accidental large Cosmos throughput change is exactly what the budget alarm
exists to catch before the invoice does.

## What each alert must include

Every alert routes with: the resource, the specific metric and its value, a **direct link
to the relevant runbook**, and the environment (dev/prod). An alert that just says
"pipeline failed" makes the responder start from zero at 2am; an alert that says "`pl_ingest_scans`
failed on dt=2026-06-30/hour=14, 3 retries exhausted, runbook: pipeline_failure.md"
makes them productive in the first minute.
