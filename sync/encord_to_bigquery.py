"""
Encord → BigQuery Sync Pipeline
=================================
Based on the verified reference ingestion script from Encord SDK docs.

Architecture:
    list_projects(include_org_access=True)
        → incremental check via last_edited_at
        → for changed projects only:
            - task_snapshot    (from workflow.stages.get_tasks())
            - time_spent       (from project.list_time_spent())
            - task_actions     (from project.get_task_actions())
        → write to BigQuery (5 tables)

BigQuery Tables:
    encord_dashboard.projects           - one row per project (upserted)
    encord_dashboard.workflow_stages    - stages per project (upserted)
    encord_dashboard.tasks_snapshot     - current task state (replaced per project)
    encord_dashboard.time_spent         - time entries (append, deduped by MERGE)
    encord_dashboard.task_actions       - action log (append, deduped by MERGE)

Usage:
    python sync/encord_to_bigquery.py

Environment variables (set via GitHub Actions secrets):
    ENCORD_SSH_KEY_PATH         Path to SSH private key file
    ENCORD_DOMAIN               https://api.encord.com (or US endpoint)
    GOOGLE_APPLICATION_CREDENTIALS  Path to GCP service account JSON
    GCP_PROJECT                 GCP project ID (e.g. autonex-488609)
    BQ_DATASET                  BigQuery dataset (e.g. encord_dashboard)
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import re
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from encord import EncordUserClient, Project
from encord.workflow import (
    AnnotationStage,
    ConsensusAnnotationStage,
    ConsensusReviewStage,
    FinalStage,
    ReviewStage,
)

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("encord_bq")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

SSH_KEY_PATH     = os.environ["ENCORD_SSH_KEY_PATH"]
ENCORD_DOMAIN    = os.environ.get("ENCORD_DOMAIN", "https://api.encord.com")

# Optional US endpoint (different SSH key + domain)
# File is always written by CI but may be empty if secret is not set — check content
_us_key_path_raw = os.environ.get("ENCORD_US_SSH_KEY_PATH", "")
def _key_file_has_content(path: str) -> bool:
    try:
        return bool(path and os.path.exists(path) and open(path).read().strip())
    except Exception:
        return False
US_SSH_KEY_PATH  = _us_key_path_raw if _key_file_has_content(_us_key_path_raw) else ""
US_DOMAIN        = os.environ.get("ENCORD_US_DOMAIN", "https://api.us.encord.com")

GCP_PROJECT      = os.environ["GCP_PROJECT"]
BQ_DATASET       = os.environ["BQ_DATASET"]
MAX_WORKERS      = int(os.environ.get("MAX_WORKERS", "5"))
PROJECT_TIMEOUT  = int(os.environ.get("PROJECT_TIMEOUT_SEC", "120"))  # per-project SDK timeout
BACKFILL_FROM    = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)  # floor for history

# Optional: filter to specific clients only (comma-separated, case-insensitive)
# e.g. "1X,ACME Co.,Aigen" — if empty, processes ALL clients
_raw_filter = os.environ.get("CLIENT_FILTER", "").strip()
CLIENT_FILTER = {c.strip().lower() for c in _raw_filter.split(",") if c.strip()} if _raw_filter else set()


class ProjectTimeoutError(Exception):
    pass


def _timeout_handler(signum, frame):
    raise ProjectTimeoutError("SDK call timed out")

# ─────────────────────────────────────────────────────────────────────────────
# CLIENT WORKSPACE MAPPING
# Derive client name from creator_email using guest+<name>@encord.com pattern.
# Falls back to "unknown" if pattern doesn't match.
# ─────────────────────────────────────────────────────────────────────────────

_GUEST_PATTERN = re.compile(r"guest\+([^@]+)@encord\.com", re.IGNORECASE)


def derive_client_workspace(creator_email: Optional[str]) -> str:
    """Derive client/workspace name from creator_email."""
    if not creator_email:
        return "unknown"
    m = _GUEST_PATTERN.match(creator_email.strip())
    if m:
        # guest+client_name@encord.com → "Client Name"
        raw = m.group(1).replace("_", " ").replace("-", " ")
        return raw.title()
    # Non-guest email → use domain as workspace name
    domain = creator_email.split("@")[-1].split(".")[0]
    return domain.title()


# ─────────────────────────────────────────────────────────────────────────────
# BIGQUERY SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_PROJECTS = [
    SchemaField("project_hash",       "STRING",    mode="REQUIRED"),
    SchemaField("title",              "STRING"),
    SchemaField("description",        "STRING"),
    SchemaField("creator_email",      "STRING"),
    SchemaField("client_workspace",   "STRING"),
    SchemaField("created_at",         "TIMESTAMP"),
    SchemaField("last_edited_at",     "TIMESTAMP"),
    SchemaField("project_type",       "STRING"),
    SchemaField("status",             "STRING"),
    SchemaField("ontology_hash",      "STRING"),
    SchemaField("ingested_at",        "TIMESTAMP", mode="REQUIRED"),
]

SCHEMA_WORKFLOW_STAGES = [
    SchemaField("project_hash",  "STRING",    mode="REQUIRED"),
    SchemaField("stage_uuid",    "STRING",    mode="REQUIRED"),
    SchemaField("stage_title",   "STRING"),
    SchemaField("stage_type",    "STRING"),
    SchemaField("ingested_at",   "TIMESTAMP", mode="REQUIRED"),
]

SCHEMA_TASKS_SNAPSHOT = [
    SchemaField("project_hash",      "STRING",    mode="REQUIRED"),
    SchemaField("client_workspace",  "STRING"),
    SchemaField("project_title",     "STRING"),
    SchemaField("task_uuid",         "STRING",    mode="REQUIRED"),
    SchemaField("data_hash",         "STRING"),
    SchemaField("data_title",        "STRING"),
    SchemaField("stage_uuid",        "STRING"),
    SchemaField("stage_title",       "STRING"),
    SchemaField("stage_type",        "STRING"),
    SchemaField("task_status",       "STRING"),
    SchemaField("assignee_email",    "STRING"),
    SchemaField("is_complete",       "BOOL"),
    SchemaField("snapshot_at",       "TIMESTAMP", mode="REQUIRED"),
]

SCHEMA_TIME_SPENT = [
    SchemaField("project_hash",        "STRING",    mode="REQUIRED"),
    SchemaField("client_workspace",    "STRING"),
    SchemaField("project_title",       "STRING"),
    SchemaField("user_email",          "STRING",    mode="REQUIRED"),
    SchemaField("project_user_role",   "STRING"),
    SchemaField("data_uuid",           "STRING"),
    SchemaField("data_title",          "STRING"),
    SchemaField("dataset_uuid",        "STRING"),
    SchemaField("dataset_title",       "STRING"),
    SchemaField("workflow_task_uuid",  "STRING"),
    SchemaField("stage_uuid",          "STRING"),
    SchemaField("stage_title",         "STRING"),
    SchemaField("stage_type",          "STRING"),
    SchemaField("period_start_time",   "TIMESTAMP", mode="REQUIRED"),
    SchemaField("period_end_time",     "TIMESTAMP"),
    SchemaField("time_spent_seconds",  "INT64"),
    SchemaField("ingested_at",         "TIMESTAMP", mode="REQUIRED"),
]

SCHEMA_TASK_ACTIONS = [
    SchemaField("project_hash",         "STRING",    mode="REQUIRED"),
    SchemaField("client_workspace",     "STRING"),
    SchemaField("project_title",        "STRING"),
    SchemaField("task_uuid",            "STRING",    mode="REQUIRED"),
    SchemaField("data_unit_uuid",       "STRING"),
    SchemaField("workflow_stage_uuid",  "STRING"),
    SchemaField("actor_email",          "STRING"),
    SchemaField("action_type",          "STRING"),
    SchemaField("event_timestamp",      "TIMESTAMP", mode="REQUIRED"),
    SchemaField("ingested_at",          "TIMESTAMP", mode="REQUIRED"),
]

# Incremental sync state (tracks last_edited_at per project)
SCHEMA_SYNC_STATE = [
    SchemaField("project_hash",      "STRING",    mode="REQUIRED"),
    SchemaField("client_workspace",  "STRING"),
    SchemaField("project_title",     "STRING"),
    SchemaField("last_edited_at",    "TIMESTAMP"),
    SchemaField("last_synced_at",    "TIMESTAMP"),
    SchemaField("created_at",        "TIMESTAMP"),
]


# ─────────────────────────────────────────────────────────────────────────────
# BIGQUERY SETUP
# ─────────────────────────────────────────────────────────────────────────────

def ensure_dataset(bq: bigquery.Client) -> None:
    """Create dataset if it doesn't exist."""
    ds_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    ds_ref.location = "US"
    bq.create_dataset(ds_ref, exists_ok=True)
    log.info("Dataset ready: %s.%s", GCP_PROJECT, BQ_DATASET)


def ensure_table(bq: bigquery.Client, table_id: str, schema: list,
                 partition_field: Optional[str] = None,
                 cluster_fields: Optional[list] = None) -> bigquery.Table:
    """Create table if it doesn't exist."""
    full_id = f"{GCP_PROJECT}.{BQ_DATASET}.{table_id}"
    table = bigquery.Table(full_id, schema=schema)

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
    if cluster_fields:
        table.clustering_fields = cluster_fields

    bq.create_table(table, exists_ok=True)
    return bq.get_table(full_id)


def setup_tables(bq: bigquery.Client) -> None:
    ensure_dataset(bq)
    ensure_table(bq, "projects",        SCHEMA_PROJECTS)
    ensure_table(bq, "workflow_stages", SCHEMA_WORKFLOW_STAGES)
    ensure_table(bq, "tasks_snapshot",  SCHEMA_TASKS_SNAPSHOT,
                 partition_field="snapshot_at",
                 cluster_fields=["project_hash", "client_workspace"])
    ensure_table(bq, "time_spent",      SCHEMA_TIME_SPENT,
                 partition_field="period_start_time",
                 cluster_fields=["project_hash", "user_email"])
    ensure_table(bq, "task_actions",    SCHEMA_TASK_ACTIONS,
                 partition_field="event_timestamp",
                 cluster_fields=["project_hash", "actor_email", "action_type"])
    ensure_table(bq, "sync_state",      SCHEMA_SYNC_STATE)
    log.info("All tables ready.")


# ─────────────────────────────────────────────────────────────────────────────
# LOAD SYNC STATE (last_edited_at per project)
# ─────────────────────────────────────────────────────────────────────────────

def load_sync_state(bq: bigquery.Client) -> dict[str, dt.datetime]:
    """Returns {project_hash: last_edited_at} from sync_state table."""
    try:
        query = f"""
            SELECT project_hash, last_edited_at
            FROM `{GCP_PROJECT}.{BQ_DATASET}.sync_state`
            WHERE last_edited_at IS NOT NULL
        """
        results = bq.query(query).result()
        state = {}
        for row in results:
            if row.last_edited_at:
                ts = row.last_edited_at
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=dt.timezone.utc)
                state[row.project_hash] = ts
        log.info("Loaded sync state for %d projects.", len(state))
        return state
    except Exception as e:
        log.warning("Could not load sync state (first run?): %s", e)
        return {}


def save_sync_state(bq: bigquery.Client, rows: list[dict]) -> None:
    """Upsert sync state for synced projects."""
    if not rows:
        return
    # Delete old rows for these project_hashes then insert
    hashes = ", ".join(f"'{r['project_hash']}'" for r in rows)
    bq.query(f"""
        DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.sync_state`
        WHERE project_hash IN ({hashes})
    """).result()
    table = bq.get_table(f"{GCP_PROJECT}.{BQ_DATASET}.sync_state")
    errors = bq.insert_rows_json(table, rows)
    if errors:
        log.error("Sync state write errors: %s", errors)


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACTION — one project at a time (runs in thread pool)
# ─────────────────────────────────────────────────────────────────────────────

def extract_project(project: Project, now: dt.datetime, encord_client=None) -> dict:
    """Extract all data for a single project. Returns dict of row lists."""
    ph = str(project.project_hash)
    workspace = derive_client_workspace(getattr(project, "creator_email", None))
    title = project.title
    since = getattr(project, "created_at", BACKFILL_FROM) or BACKFILL_FROM
    if since.tzinfo is None:
        since = since.replace(tzinfo=dt.timezone.utc)

    # Re-initialize project with full auth context (list_projects stubs may lack permissions)
    if encord_client:
        try:
            project = encord_client.get_project(ph)
        except Exception as e:
            log.debug("[%s] get_project re-init failed (using stub): %s", title, e)

    result = {
        "project_hash": ph,
        "project_title": title,
        "client_workspace": workspace,
        "stages": [],
        "tasks": [],
        "time": [],
        "actions": [],
    }

    # ── Workflow stages ──
    workflow_ok = False
    wf = getattr(project, "workflow", None)
    if wf:
        for stage in wf.stages:
            result["stages"].append({
                "project_hash": ph,
                "stage_uuid":   str(stage.uuid),
                "stage_title":  stage.title,
                "stage_type":   str(stage.stage_type),
                "ingested_at":  now.isoformat(),
            })

        # ── Task snapshot (current state of all tasks) ──
        for stage in wf.stages:
            is_annotation = isinstance(stage, (AnnotationStage, ConsensusAnnotationStage))
            is_review     = isinstance(stage, (ReviewStage, ConsensusReviewStage))
            is_final      = isinstance(stage, FinalStage)
            if not (is_annotation or is_review or is_final):
                continue
            try:
                for task in stage.get_tasks():
                    status   = getattr(task, "status", None)
                    assignee = getattr(task, "assignee", None)
                    result["tasks"].append({
                        "project_hash":     ph,
                        "client_workspace": workspace,
                        "project_title":    title,
                        "task_uuid":        str(task.uuid),
                        "data_hash":        str(task.data_hash) if getattr(task, "data_hash", None) else None,
                        "data_title":       getattr(task, "data_title", None),
                        "stage_uuid":       str(stage.uuid),
                        "stage_title":      stage.title,
                        "stage_type":       str(stage.stage_type),
                        "task_status":      str(status) if status else None,
                        "assignee_email":   str(assignee) if assignee else None,
                        "is_complete":      is_final,
                        "snapshot_at":      now.isoformat(),
                    })
                workflow_ok = True
            except Exception as e:
                log.warning("[%s] tasks error @ %s: %s", title, stage.title, e)

    # ── Fallback: list_label_rows_v2 if workflow stages had permission errors ──
    if not result["tasks"] and not workflow_ok:
        try:
            for lr in project.list_label_rows_v2():
                result["tasks"].append({
                    "project_hash":     ph,
                    "client_workspace": workspace,
                    "project_title":    title,
                    "task_uuid":        str(lr.data_hash),
                    "data_hash":        str(lr.data_hash),
                    "data_title":       getattr(lr, "data_title", None),
                    "stage_uuid":       None,
                    "stage_title":      None,
                    "stage_type":       None,
                    "task_status":      str(getattr(lr, "label_hash", None) or "NOT_LABELLED"),
                    "assignee_email":   getattr(lr, "annotation_task_status", None),
                    "is_complete":      str(getattr(lr, "annotation_task_status", "")).upper() == "COMPLETED",
                    "snapshot_at":      now.isoformat(),
                })
            if result["tasks"]:
                log.info("  [%s] %d tasks via list_label_rows_v2 fallback", title, len(result["tasks"]))
        except Exception as e:
            log.warning("[%s] list_label_rows_v2 fallback also failed: %s", title, e)

    # ── Time spent (full history from project start) ──
    try:
        for ts in project.list_time_spent(start=since):
            stage = ts.workflow_stage
            result["time"].append({
                "project_hash":       ph,
                "client_workspace":   workspace,
                "project_title":      title,
                "user_email":         ts.user_email,
                "project_user_role":  str(ts.project_user_role) if ts.project_user_role else None,
                "data_uuid":          str(ts.data_uuid) if ts.data_uuid else None,
                "data_title":         ts.data_title,
                "dataset_uuid":       str(ts.dataset_uuid) if ts.dataset_uuid else None,
                "dataset_title":      ts.dataset_title,
                "workflow_task_uuid": str(ts.workflow_task_uuid) if ts.workflow_task_uuid else None,
                "stage_uuid":         str(stage.uuid) if stage else None,
                "stage_title":        stage.title if stage else None,
                "stage_type":         str(stage.stage_type) if stage else None,
                "period_start_time":  ts.period_start_time.isoformat() if ts.period_start_time else None,
                "period_end_time":    ts.period_end_time.isoformat() if ts.period_end_time else None,
                "time_spent_seconds": int(ts.time_spent_seconds),
                "ingested_at":        now.isoformat(),
            })
    except Exception as e:
        log.warning("[%s] list_time_spent error: %s", title, e)

    # ── Task actions — approve / reject / submit
    #    Try: get_editor_logs (current) → get_label_logs (deprecated) → get_task_actions (fallback)
    actions_loaded = False

    # Primary: get_editor_logs (replaces deprecated get_label_logs since SDK 0.1.187)
    for method_name in ("get_editor_logs", "get_label_logs"):
        if actions_loaded:
            break
        method = getattr(project, method_name, None)
        if method is None:
            continue
        try:
            for a in method(after=since):
                action_raw = str(getattr(a, "action", "")).upper()
                # Only capture task-level actions we care about
                if not any(kw in action_raw for kw in ("SUBMIT", "APPROVE", "REJECT")):
                    continue
                # Normalize action type
                if "REJECT" in action_raw:
                    action_type = "REJECT"
                elif "APPROVE" in action_raw:
                    action_type = "APPROVE"
                elif "SUBMIT" in action_raw:
                    action_type = "SUBMIT"
                else:
                    action_type = action_raw

                result["actions"].append({
                    "project_hash":        ph,
                    "client_workspace":    workspace,
                    "project_title":       title,
                    "task_uuid":           str(getattr(a, "data_hash", "") or ""),
                    "data_unit_uuid":      str(getattr(a, "data_hash", "") or ""),
                    "workflow_stage_uuid": None,
                    "actor_email":         str(getattr(a, "user_email", "") or ""),
                    "action_type":         action_type,
                    "event_timestamp":     a.created_at.isoformat() if getattr(a, "created_at", None) else now.isoformat(),
                    "ingested_at":         now.isoformat(),
                })
            actions_loaded = True
            if result["actions"]:
                log.info("  [%s] %d actions via %s", title, len(result["actions"]), method_name)
        except Exception as e:
            log.warning("[%s] %s failed: %s", title, method_name, e)

    # Fallback: get_task_actions (newer API, doesn't work on all projects)
    if not actions_loaded:
        try:
            for a in project.get_task_actions(after=since):
                result["actions"].append({
                    "project_hash":        ph,
                    "client_workspace":    workspace,
                    "project_title":       title,
                    "task_uuid":           str(a.task_uuid),
                    "data_unit_uuid":      str(a.data_unit_uuid) if a.data_unit_uuid else None,
                    "workflow_stage_uuid": str(a.workflow_stage_uuid) if a.workflow_stage_uuid else None,
                    "actor_email":         a.actor_email,
                    "action_type":         str(a.action_type),
                    "event_timestamp":     a.timestamp.isoformat() if a.timestamp else now.isoformat(),
                    "ingested_at":         now.isoformat(),
                })
        except Exception as e:
            log.warning("[%s] get_task_actions fallback also failed: %s", title, e)

    log.info("  ✓ %s — %d tasks | %d time entries | %d actions",
             title, len(result["tasks"]), len(result["time"]), len(result["actions"]))
    return result


def extract_project_with_timeout(project: Project, now: dt.datetime, encord_client=None) -> dict:
    """Wraps extract_project with a per-project wall-clock timeout (signal-based, Linux only)."""
    # signal.alarm only works on the main thread — threads use the no-op path
    try:
        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(PROJECT_TIMEOUT)
        try:
            return extract_project(project, now, encord_client=encord_client)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    except (OSError, AttributeError):
        # signal.SIGALRM not available (Windows / non-main thread) — run without timeout
        return extract_project(project, now, encord_client=encord_client)


# ─────────────────────────────────────────────────────────────────────────────
# BIGQUERY WRITE — replace project data (tasks), append time + actions
# ─────────────────────────────────────────────────────────────────────────────

def write_project_rows(bq: bigquery.Client, project_row: dict) -> None:
    """Upsert project metadata."""
    table = bq.get_table(f"{GCP_PROJECT}.{BQ_DATASET}.projects")
    ph = project_row["project_hash"]
    bq.query(f"DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.projects` WHERE project_hash = '{ph}'").result()
    errors = bq.insert_rows_json(table, [project_row])
    if errors:
        log.error("projects write error: %s", errors)


def write_stage_rows(bq: bigquery.Client, ph: str, rows: list) -> None:
    if not rows:
        return
    bq.query(f"DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.workflow_stages` WHERE project_hash = '{ph}'").result()
    table = bq.get_table(f"{GCP_PROJECT}.{BQ_DATASET}.workflow_stages")
    errors = bq.insert_rows_json(table, rows)
    if errors:
        log.error("stages write error: %s", errors)


def write_task_snapshot(bq: bigquery.Client, ph: str, rows: list) -> None:
    """Replace today's snapshot for this project."""
    if not rows:
        return
    today = dt.date.today().isoformat()
    bq.query(f"""
        DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.tasks_snapshot`
        WHERE project_hash = '{ph}'
        AND DATE(snapshot_at) = '{today}'
    """).result()
    table = bq.get_table(f"{GCP_PROJECT}.{BQ_DATASET}.tasks_snapshot")
    # Insert in batches of 1000
    for i in range(0, len(rows), 1000):
        errors = bq.insert_rows_json(table, rows[i:i+1000])
        if errors:
            log.error("tasks_snapshot write error (batch %d): %s", i // 1000, errors)


def write_time_spent(bq: bigquery.Client, ph: str, rows: list) -> None:
    """Append time entries — idempotent via DELETE of today's rows first."""
    if not rows:
        return
    today = dt.date.today().isoformat()
    # Delete today's rows for this project before reinserting (handles reruns)
    bq.query(f"""
        DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.time_spent`
        WHERE project_hash = '{ph}'
        AND DATE(ingested_at) = '{today}'
    """).result()
    table = bq.get_table(f"{GCP_PROJECT}.{BQ_DATASET}.time_spent")
    for i in range(0, len(rows), 1000):
        errors = bq.insert_rows_json(table, rows[i:i+1000])
        if errors:
            log.error("time_spent write error (batch %d): %s", i // 1000, errors)


def write_task_actions(bq: bigquery.Client, ph: str, rows: list) -> None:
    """Append task actions — idempotent via DELETE of today's rows first."""
    if not rows:
        return
    today = dt.date.today().isoformat()
    bq.query(f"""
        DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.task_actions`
        WHERE project_hash = '{ph}'
        AND DATE(ingested_at) = '{today}'
    """).result()
    table = bq.get_table(f"{GCP_PROJECT}.{BQ_DATASET}.task_actions")
    for i in range(0, len(rows), 1000):
        errors = bq.insert_rows_json(table, rows[i:i+1000])
        if errors:
            log.error("task_actions write error (batch %d): %s", i // 1000, errors)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    now = dt.datetime.now(dt.timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    print("═" * 65)
    print(f"  Encord → BigQuery Sync  |  {today_str}")
    print(f"  Dataset     : {GCP_PROJECT}.{BQ_DATASET}")
    print(f"  EMEA domain : {ENCORD_DOMAIN}")
    if US_SSH_KEY_PATH:
        print(f"  US domain   : {US_DOMAIN}")
    print("═" * 65)

    # ── Connect to Encord (EMEA) ──
    clients = []
    try:
        emea_client = EncordUserClient.create_with_ssh_private_key(
            ssh_private_key_path=SSH_KEY_PATH,
            domain=ENCORD_DOMAIN,
        )
        clients.append((emea_client, "EMEA"))
        log.info("Connected to EMEA endpoint.")
    except Exception as e:
        log.error("Failed to connect to EMEA endpoint: %s", e)

    # ── Connect to Encord (US) if key provided ──
    if US_SSH_KEY_PATH:
        try:
            us_client = EncordUserClient.create_with_ssh_private_key(
                ssh_private_key_path=US_SSH_KEY_PATH,
                domain=US_DOMAIN,
            )
            clients.append((us_client, "US"))
            log.info("Connected to US endpoint.")
        except Exception as e:
            log.warning("Failed to connect to US endpoint (skipping US projects): %s", e)

    if not clients:
        log.error("No Encord clients available. Exiting.")
        return

    bq = bigquery.Client(project=GCP_PROJECT)

    # ── Ensure tables exist ──
    setup_tables(bq)

    # ── Load incremental sync state ──
    sync_state = load_sync_state(bq)

    # ── Discover all projects from all endpoints ──
    all_projects_with_endpoint: list[tuple] = []   # (project, endpoint_label)
    for client, endpoint_label in clients:
        try:
            log.info("Fetching project list from %s...", endpoint_label)
            endpoint_projects = list(client.list_projects(include_org_access=True))
            log.info("  %s: %d projects found.", endpoint_label, len(endpoint_projects))
            all_projects_with_endpoint.extend(
                (p, endpoint_label) for p in endpoint_projects
            )
        except Exception as e:
            log.error("Failed to list projects from %s: %s", endpoint_label, e)

    # Deduplicate by project_hash (same project can appear on both endpoints)
    seen_hashes = set()
    unique_projects = []
    for p, label in all_projects_with_endpoint:
        ph = str(p.project_hash)
        if ph not in seen_hashes:
            seen_hashes.add(ph)
            unique_projects.append((p, label))

    log.info("Total unique projects: %d", len(unique_projects))

    # ── Filter by client (if CLIENT_FILTER is set) ──
    if CLIENT_FILTER:
        log.info("CLIENT_FILTER active: %s", CLIENT_FILTER)

        # Build workspace map: {workspace_name_lower: [(project, label), ...]}
        workspace_map: dict[str, list] = {}
        for p, label in unique_projects:
            ws = derive_client_workspace(getattr(p, "creator_email", None))
            workspace_map.setdefault(ws.lower(), []).append((p, label))

        # Log all unique workspaces for debugging
        all_workspaces = sorted(workspace_map.keys())
        log.info("Available workspaces (%d unique): %s",
                 len(all_workspaces), ", ".join(all_workspaces[:100]))

        # Fuzzy match: filter term in workspace OR workspace in filter term
        filtered = []
        matched_workspaces = set()
        for ws_lower, projects_list in workspace_map.items():
            for f in CLIENT_FILTER:
                if f in ws_lower or ws_lower in f or ws_lower.replace(" ", "") == f.replace(" ", ""):
                    filtered.extend(projects_list)
                    matched_workspaces.add(ws_lower)
                    break

        log.info("Client filter: %d projects matched (%d workspaces: %s), %d skipped",
                 len(filtered), len(matched_workspaces),
                 ", ".join(sorted(matched_workspaces)),
                 len(unique_projects) - len(filtered))
        unique_projects = filtered

    # ── Determine which need syncing ──
    to_sync = []    # list of (project, endpoint_label)
    skipped = 0
    for project, endpoint_label in unique_projects:
        ph = str(project.project_hash)
        stored = sync_state.get(ph)
        proj_last_edited = getattr(project, "last_edited_at", None)

        if stored and proj_last_edited:
            if proj_last_edited.tzinfo is None:
                proj_last_edited = proj_last_edited.replace(tzinfo=dt.timezone.utc)
            if proj_last_edited <= stored:
                skipped += 1
                continue   # unchanged — skip

        to_sync.append((project, endpoint_label))

    log.info("To sync: %d | Skipped (unchanged): %d", len(to_sync), skipped)

    if not to_sync:
        log.info("Nothing to sync. All projects up to date.")
        return

    # ── Sync in parallel ──
    synced = 0
    failed = 0
    new_sync_state = []

    # Build a client lookup: endpoint_label -> encord_client
    client_lookup = {label: c for c, label in clients}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(extract_project_with_timeout, p, now,
                            encord_client=client_lookup.get(label)): (p, label)
            for p, label in to_sync
        }

        for future in as_completed(future_map):
            project, endpoint_label = future_map[future]
            ph = str(project.project_hash)
            try:
                data = future.result()

                # Write project metadata
                proj_row = {
                    "project_hash":     ph,
                    "title":            project.title,
                    "description":      getattr(project, "description", None),
                    "creator_email":    getattr(project, "creator_email", None),
                    "client_workspace": data["client_workspace"],
                    "created_at":       project.created_at.isoformat() if project.created_at else None,
                    "last_edited_at":   project.last_edited_at.isoformat() if getattr(project, "last_edited_at", None) else None,
                    "project_type":     str(project.project_type) if getattr(project, "project_type", None) else None,
                    "status":           str(project.status) if getattr(project, "status", None) else None,
                    "ontology_hash":    getattr(project, "ontology_hash", None),
                    "ingested_at":      now.isoformat(),
                }
                write_project_rows(bq, proj_row)
                write_stage_rows(bq, ph, data["stages"])
                write_task_snapshot(bq, ph, data["tasks"])
                write_time_spent(bq, ph, data["time"])
                write_task_actions(bq, ph, data["actions"])

                # Record new sync state
                new_sync_state.append({
                    "project_hash":     ph,
                    "client_workspace": data["client_workspace"],
                    "project_title":    project.title,
                    "last_edited_at":   project.last_edited_at.isoformat() if getattr(project, "last_edited_at", None) else now.isoformat(),
                    "last_synced_at":   now.isoformat(),
                    "created_at":       project.created_at.isoformat() if project.created_at else None,
                })
                synced += 1

            except Exception as e:
                log.error("  ✗ [%s] failed: %s", project.title, e)
                failed += 1

    # ── Save sync state ──
    save_sync_state(bq, new_sync_state)

    print("═" * 65)
    print(f"  Sync complete")
    print(f"  ✓ Synced  : {synced}")
    print(f"  ⏭ Skipped : {skipped}")
    print(f"  ✗ Failed  : {failed}")
    print("═" * 65)


if __name__ == "__main__":
    main()
