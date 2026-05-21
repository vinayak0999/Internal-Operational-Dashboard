"""
Encord Operations Dashboard — Data Sync
========================================
Orchestrates fetching data from Encord SDK and storing it in local SQLite.

⛔ CRITICAL DATA SAFETY RULE ⛔
===============================
- Encord operations: READ-ONLY (list, get, fetch)
- Database operations: INSERT / UPDATE only (no DELETE)
- All data is preserved — each sync adds new snapshots
"""

import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert

from database import (
    Project, Annotator, MetricSnapshot, TimeEntry,
    OutlierFlag, SyncLog,
)
from encord_client import encord_client
from metrics import (
    calculate_rejection_rate, calculate_tpt, calculate_throughput,
    detect_rejection_rate_outliers, detect_tpt_outliers,
    detect_throughput_outliers,
)
from config import settings

logger = logging.getLogger(__name__)


def sync_all_projects(db: Session) -> dict:
    """
    Main sync function: fetches data from all configured projects,
    calculates metrics, detects outliers, and stores everything locally.

    Returns summary of what was synced.
    """
    # Log sync start
    sync_log = SyncLog(
        sync_type="full",
        project_hash="all",
        status="started",
    )
    db.add(sync_log)
    db.commit()

    total_records = 0
    project_results = []

    try:
        # Get projects to sync
        project_hashes = settings.ENCORD_PROJECT_HASHES
        if not project_hashes:
            # If no specific hashes configured, try listing all accessible projects
            logger.info("No specific project hashes configured, listing all projects...")
            try:
                all_projects = encord_client.list_all_projects()
                project_hashes = [p.get("project_hash", "") for p in all_projects if p.get("project_hash")]
            except Exception as e:
                logger.warning("Could not list all projects: %s. Configure ENCORD_PROJECT_HASHES in .env", e)
                project_hashes = []

        for ph in project_hashes:
            try:
                result = sync_single_project(db, ph)
                project_results.append(result)
                total_records += result.get("records", 0)
            except Exception as e:
                db.rollback()  # Recover session state after error
                logger.error("Failed to sync project %s: %s", ph, e)
                project_results.append({"project_hash": ph, "error": str(e), "records": 0})

        # Update sync log
        sync_log.status = "completed"
        sync_log.records_synced = total_records
        sync_log.completed_at = datetime.now(timezone.utc)
        db.commit()

        return {
            "status": "completed",
            "projects_synced": len(project_results),
            "total_records": total_records,
            "details": project_results,
        }

    except Exception as e:
        db.rollback()  # Recover session state after error
        sync_log.status = "failed"
        sync_log.error_message = str(e)
        sync_log.completed_at = datetime.now(timezone.utc)
        db.commit()
        logger.error("Full sync failed: %s", e)
        return {"status": "failed", "error": str(e)}


def sync_single_project(db: Session, project_hash: str) -> dict:
    """Sync a single project's data from Encord."""
    logger.info("Syncing project: %s", project_hash)

    # 1. Fetch project info
    project = encord_client.get_project(project_hash)
    if project is None:
        return {"project_hash": project_hash, "error": "Project not found", "records": 0}

    # Upsert project record
    _upsert_project(db, project)

    # 2. Fetch label rows
    label_rows = encord_client.get_label_rows(project)

    # 3. Fetch time entries
    time_entries = encord_client.get_time_entries(project)

    # 4. Process and aggregate metrics per annotator
    annotator_metrics = _aggregate_annotator_metrics(
        project_hash, project.title, label_rows, time_entries
    )

    # 5. Store metrics and annotator records
    records = 0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for email, metrics in annotator_metrics.items():
        # Upsert annotator
        _upsert_annotator(db, email)

        # Upsert metric snapshot for today
        _upsert_metric_snapshot(db, project_hash, email, today, metrics)
        records += 1

    # 6. Store time entries
    for entry in time_entries:
        _upsert_time_entry(db, project_hash, entry)
        records += 1

    # 7. Detect outliers
    outliers = _detect_all_outliers(project_hash, project.title, annotator_metrics)
    for outlier in outliers:
        _insert_outlier_flag(db, outlier)
        records += 1

    db.commit()

    logger.info(
        "Synced project %s: %d annotators, %d time entries, %d outlier flags",
        project_hash, len(annotator_metrics), len(time_entries), len(outliers)
    )

    return {
        "project_hash": project_hash,
        "project_title": project.title,
        "annotators": len(annotator_metrics),
        "time_entries": len(time_entries),
        "outlier_flags": len([o for o in outliers if o.flag_level != "green"]),
        "records": records,
    }


def _aggregate_annotator_metrics(
    project_hash: str,
    project_title: str,
    label_rows: list,
    time_entries: list,
) -> dict:
    """
    Aggregate metrics per annotator from label rows and time entries.
    Returns dict of {email: {metrics...}}
    """
    # Time per annotator
    annotator_time = defaultdict(float)
    annotator_tasks = defaultdict(int)

    for entry in time_entries:
        email = getattr(entry, "user_email", None) or getattr(entry, "annotator", None) or "unknown"
        duration = getattr(entry, "duration", 0) or getattr(entry, "time_seconds", 0) or 0
        if isinstance(email, str) and email != "unknown":
            annotator_time[email] += float(duration)
            annotator_tasks[email] += 1

    # Label row analysis for submit/reject counts
    annotator_submitted = defaultdict(int)
    annotator_rejected = defaultdict(int)

    for lr in label_rows:
        # Use workflow_graph_node for workflow-based projects (not annotation_task_status)
        workflow_node = getattr(lr, "workflow_graph_node", None)
        created_by = getattr(lr, "created_by", None)
        last_edited_by = getattr(lr, "last_edited_by", None)

        # Determine annotator from available fields
        annotator = created_by or last_edited_by
        if annotator:
            annotator_submitted[annotator] += 1

        # If the task is in a review-rejected or re-annotate state, count as rejected
        if workflow_node:
            node_title = str(getattr(workflow_node, "title", "")).lower()
            if "reject" in node_title or "rework" in node_title or "re-annotate" in node_title:
                if annotator:
                    annotator_rejected[annotator] += 1

    # Combine all known annotators
    all_emails = set(annotator_time.keys()) | set(annotator_submitted.keys())

    metrics = {}
    for email in all_emails:
        submitted = annotator_submitted.get(email, annotator_tasks.get(email, 0))
        rejected = annotator_rejected.get(email, 0)
        total_time = annotator_time.get(email, 0.0)
        completed = annotator_tasks.get(email, submitted)

        if submitted == 0 and completed == 0 and total_time == 0:
            continue

        rr = calculate_rejection_rate(submitted, rejected)
        tpt = calculate_tpt(total_time, completed) if completed > 0 else 0.0
        hours = total_time / 3600 if total_time > 0 else 0.0
        tp = calculate_throughput(completed, hours) if hours > 0 else 0.0

        metrics[email] = {
            "tasks_submitted": submitted,
            "tasks_rejected": rejected,
            "rejection_rate": rr,
            "total_time_seconds": total_time,
            "tasks_completed": completed,
            "time_per_task_seconds": tpt,
            "throughput_per_hour": tp,
        }

    return metrics


def _detect_all_outliers(project_hash, project_title, annotator_metrics) -> list:
    """Run all outlier detection checks on the aggregated metrics."""
    if not annotator_metrics:
        return []

    rr_dict = {e: m["rejection_rate"] for e, m in annotator_metrics.items()}
    tpt_dict = {e: m["time_per_task_seconds"] for e, m in annotator_metrics.items()}
    tp_dict = {e: m["throughput_per_hour"] for e, m in annotator_metrics.items()}

    outliers = []
    outliers.extend(detect_rejection_rate_outliers(rr_dict, project_hash, project_title))
    outliers.extend(detect_tpt_outliers(tpt_dict, project_hash, project_title))
    outliers.extend(detect_throughput_outliers(tp_dict, project_hash, project_title))

    return outliers


# ─────────────────────────────────────────────
# Database Upsert Helpers (INSERT/UPDATE ONLY)
# ─────────────────────────────────────────────

def _upsert_project(db: Session, project):
    """Insert or update a project record."""
    try:
        existing = db.query(Project).filter_by(project_hash=project.project_hash).first()
        if existing:
            existing.title = project.title
            existing.description = getattr(project, "description", "")
            existing.last_synced = datetime.now(timezone.utc)
        else:
            db.add(Project(
                project_hash=project.project_hash,
                title=project.title,
                description=getattr(project, "description", ""),
                created_at=getattr(project, "created_at", None),
                last_synced=datetime.now(timezone.utc),
            ))
        db.flush()  # Flush to catch constraint errors early
    except Exception as e:
        db.rollback()
        logger.warning("Project upsert issue, retrying: %s", e)
        # Re-fetch and update
        existing = db.query(Project).filter_by(project_hash=project.project_hash).first()
        if existing:
            existing.title = project.title
            existing.last_synced = datetime.now(timezone.utc)
            db.flush()


def _upsert_annotator(db: Session, email: str):
    """Insert a new annotator if not already known."""
    existing = db.query(Annotator).filter_by(email=email).first()
    if existing:
        existing.last_synced = datetime.now(timezone.utc)
    else:
        db.add(Annotator(
            email=email,
            name=email.split("@")[0],
            first_seen=datetime.now(timezone.utc),
            last_synced=datetime.now(timezone.utc),
        ))


def _upsert_metric_snapshot(db: Session, project_hash, email, date, metrics):
    """Insert or update a metric snapshot for today."""
    existing = db.query(MetricSnapshot).filter_by(
        project_hash=project_hash,
        annotator_email=email,
        date=date,
    ).first()

    if existing:
        existing.tasks_submitted = metrics["tasks_submitted"]
        existing.tasks_rejected = metrics["tasks_rejected"]
        existing.rejection_rate = metrics["rejection_rate"]
        existing.total_time_seconds = metrics["total_time_seconds"]
        existing.tasks_completed = metrics["tasks_completed"]
        existing.time_per_task_seconds = metrics["time_per_task_seconds"]
        existing.throughput_per_hour = metrics["throughput_per_hour"]
        existing.snapshot_timestamp = datetime.now(timezone.utc)
    else:
        db.add(MetricSnapshot(
            project_hash=project_hash,
            annotator_email=email,
            date=date,
            tasks_submitted=metrics["tasks_submitted"],
            tasks_rejected=metrics["tasks_rejected"],
            rejection_rate=metrics["rejection_rate"],
            total_time_seconds=metrics["total_time_seconds"],
            tasks_completed=metrics["tasks_completed"],
            time_per_task_seconds=metrics["time_per_task_seconds"],
            throughput_per_hour=metrics["throughput_per_hour"],
        ))


def _upsert_time_entry(db: Session, project_hash, entry):
    """Insert a time entry if it doesn't already exist."""
    email = getattr(entry, "user_email", None) or getattr(entry, "annotator", None) or "unknown"
    data_hash = getattr(entry, "data_hash", "") or ""
    duration = getattr(entry, "duration", 0) or getattr(entry, "time_seconds", 0) or 0
    recorded = getattr(entry, "created_at", None) or getattr(entry, "date", None)

    # Check for duplicate
    existing = db.query(TimeEntry).filter_by(
        project_hash=project_hash,
        annotator_email=email,
        data_hash=data_hash,
    ).first()

    if existing and existing.recorded_at == recorded:
        return  # Already stored

    if not existing:
        db.add(TimeEntry(
            project_hash=project_hash,
            annotator_email=email,
            data_hash=data_hash,
            data_title=getattr(entry, "data_title", ""),
            duration_seconds=float(duration),
            recorded_at=recorded,
        ))


def _insert_outlier_flag(db: Session, outlier):
    """Insert an outlier flag."""
    db.add(OutlierFlag(
        project_hash=outlier.project_hash,
        project_title=outlier.project_title,
        annotator_email=outlier.annotator_email,
        metric_type=outlier.metric_type,
        actual_value=outlier.actual_value,
        threshold_value=outlier.threshold_value,
        flag_level=outlier.flag_level,
        description=outlier.description,
    ))
