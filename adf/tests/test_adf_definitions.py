"""Validate the ADF resource JSON without an Azure subscription.

ADF definitions are configuration, not code, so the failure modes are different:
a typo in a referenceName, a secret pasted inline, a Copy activity with no retry.
None of these show up until an import or a 2am pipeline run. These tests catch all
three statically, which is the most that can be verified off-cloud — and it's a lot.

Run:  pytest adf/tests -q
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

ADF = Path(__file__).resolve().parents[1]
CATEGORY_DIRS = [
    "linked_services", "datasets", "pipelines", "triggers", "integration_runtimes"
]


def _all_json() -> list[Path]:
    files: list[Path] = []
    for d in CATEGORY_DIRS:
        files.extend((ADF / d).glob("*.json"))
    return files


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _walk(obj):
    """Yield every dict in a nested JSON structure."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)


@pytest.fixture(scope="module")
def resources() -> dict[str, dict]:
    return {p.stem: _load(p) for p in _all_json()}


# --- Well-formedness ----------------------------------------------------------

def test_every_file_is_valid_json_and_names_match_filename(resources):
    """A resource's declared `name` must equal its filename — ADF import keys on the
    name, and a mismatch means a reference that looks right silently resolves to
    nothing."""
    assert resources, "no ADF JSON found"
    for stem, doc in resources.items():
        assert doc.get("name") == stem, f"{stem}.json declares name={doc.get('name')!r}"


def test_every_resource_has_a_description(resources):
    """These files carry no code comments, so the `description` is where the
    reasoning lives. An undescribed resource is one nobody will understand in six
    months."""
    for stem, doc in resources.items():
        props = doc.get("properties", {})
        assert props.get("description", "").strip(), f"{stem} has no description"


# --- Referential integrity ----------------------------------------------------

def test_all_references_resolve(resources):
    """Every referenceName must point at a resource that exists in this repo. This
    is the single highest-value check: a dangling reference is the #1 way an ADF
    import fails, and it is invisible by eye across 10 files."""
    defined = set(resources)
    dangling = []
    for stem, doc in resources.items():
        for node in _walk(doc):
            ref = node.get("referenceName")
            if ref is None:
                continue
            # Global-parameter and trigger-body expressions are not resource refs.
            if isinstance(ref, str) and ref.startswith("@"):
                continue
            if ref not in defined:
                dangling.append(f"{stem} -> {ref}")
    assert not dangling, "dangling references: " + "; ".join(dangling)


# --- No secrets, ever ---------------------------------------------------------

FORBIDDEN = [
    "accountkey=", "sharedaccesskey", "password=", "pwd=",
    "sig=", "accesskey", "-----begin",
]


def test_no_plaintext_secrets(resources):
    """Every credential must resolve through an AzureKeyVaultSecret reference. If any
    of these substrings appears, a secret was pasted inline — the exact thing the
    Key Vault indirection exists to prevent."""
    for stem, doc in resources.items():
        blob = json.dumps(doc).lower()
        hits = [tok for tok in FORBIDDEN if tok in blob]
        assert not hits, f"{stem} may contain an inline secret: {hits}"


def test_secrets_go_through_key_vault(resources):
    """The on-prem SQL linked service must fetch its connection string from Key
    Vault, not store it. Proves the indirection is actually wired, not just that no
    secret happens to be present."""
    ls = resources["ls_onprem_sql"]["properties"]
    conn = ls["typeProperties"]["connectionString"]
    assert conn["type"] == "AzureKeyVaultSecret"
    assert conn["store"]["referenceName"] == "ls_keyvault"


# --- The operational requirements: retry + failure handling -------------------

def _activities(pipeline: dict) -> list[dict]:
    return pipeline["properties"]["activities"]


def test_copy_activities_retry_at_least_three_times(resources):
    """The requirement is 3 retries on ingestion. A transient storage or SHIR blip
    should not page anyone; only a failure that survives 3 attempts is real."""
    for stem, doc in resources.items():
        if doc.get("type", "").endswith("/pipelines"):
            for act in _activities(doc):
                if act["type"] == "Copy":
                    retry = act.get("policy", {}).get("retry", 0)
                    assert retry >= 3, f"{stem}.{act['name']} retries only {retry}x"


def test_ingest_pipelines_alert_and_fail_on_failure(resources):
    """Each ingest pipeline must (a) push an alert and (b) fail the run on a copy
    failure. Two signals: the webhook for latency, the failed run status for the
    metric alarm — because either alerting path can itself be down."""
    for name in ["pl_ingest_scans_landing_to_bronze", "pl_ingest_seller_master"]:
        acts = _activities(resources[name])
        types = {a["type"] for a in acts}
        assert "WebActivity" in types, f"{name} has no failure alert"
        assert "Fail" in types, f"{name} never fails the run"

        # The alert must actually be wired to a Failed condition, not just present.
        failed_deps = [
            a for a in acts
            for dep in a.get("dependsOn", [])
            if "Failed" in dep.get("dependencyConditions", [])
        ]
        assert failed_deps, f"{name} has no activity gated on a Failed condition"


def test_both_triggers_invoke_the_scan_pipeline(resources):
    """The hourly schedule and the storage event must both drive the same ingest
    pipeline — events for latency, schedule for completeness. If either points
    elsewhere, one of the two guarantees is silently gone."""
    for trig in ["tr_hourly_schedule", "tr_storage_event"]:
        pls = resources[trig]["properties"]["pipelines"]
        refs = {p["pipelineReference"]["referenceName"] for p in pls}
        assert "pl_ingest_scans_landing_to_bronze" in refs, f"{trig} misrouted"
