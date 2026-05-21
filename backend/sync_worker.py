"""
Encord Operations Dashboard — Sync Worker
==========================================
The heart of the caching architecture. This module:

1. Fetches data from Encord SDK (READ-ONLY)
2. Detects changes via checksums (incremental sync)
3. Updates ProjectCache for instant API reads
4. Runs intelligence engine (health, trends, summary)
5. Stores everything in SQLite

⛔ CRITICAL DATA SAFETY RULE ⛔
- Encord operations: READ-ONLY
- Database operations: INSERT / UPDATE only (no DELETE)
"""

import json
import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import numpy as np
from sqlalchemy.orm import Session

from database import (
    SessionLocal, Project, Annotator, MetricSnapshot,
    TimeEntry, OutlierFlag, SyncLog, ProjectChecksum,
    ProjectCache, DashboardSummary,
)
from encord_client import encord_client
from metrics import (
    calculate_rejection_rate, calculate_tpt, calculate_throughput,
    detect_rejection_rate_outliers, detect_tpt_outliers,
    detect_throughput_outliers,
)
from config import settings

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═════════════════════════════════════════════════════

def run_full_sync() -> dict:
    """
    Main sync entry point. Called by scheduler every N minutes.

    Flow:
    1. Auto-discover ALL projects from SSH key (grouped by workspace via creator_email)
    2. For each project, check if data changed (incremental checksum)
    3. If changed: fetch full data, compute metrics, update cache
    4. If unchanged: skip (fast)
    5. After all projects: run intelligence engine
    """
    db = SessionLocal()
    t_start = time.time()

    sync_log = SyncLog(sync_type="incremental", project_hash="all", status="started")
    db.add(sync_log)
    db.commit()

    try:
        # Auto-discover all projects with workspace from SSH key
        discovered = encord_client.list_all_projects_with_workspace()
        if not discovered:
            # Fallback to hardcoded hashes if discovery fails
            discovered = [
                {'project_hash': h, 'title': '', 'workspace': 'Unknown', 'creator_email': ''}
                for h in settings.ENCORD_PROJECT_HASHES
            ]

        if not discovered:
            logger.warning("No projects to sync")
            sync_log.status = "completed"
            sync_log.completed_at = datetime.now(timezone.utc)
            db.commit()
            return {"status": "completed", "projects_synced": 0, "projects_skipped": 0}

        # Build workspace lookup: project_hash -> workspace
        workspace_map = {d['project_hash']: d['workspace'] for d in discovered}
        project_hashes = list(workspace_map.keys())

        logger.info(
            "Starting sync for %d projects across %d workspaces...",
            len(project_hashes),
            len(set(workspace_map.values())),
        )

        results = _sync_projects_parallel(db, project_hashes, workspace_map)

        synced  = sum(1 for r in results if r.get("status") == "synced")
        skipped = sum(1 for r in results if r.get("status") == "skipped")
        failed  = sum(1 for r in results if r.get("status") == "failed")
        total_records = sum(r.get("records", 0) for r in results)

        logger.info("Running intelligence engine...")
        _run_intelligence_engine(db)

        sync_log.status = "completed"
        sync_log.records_synced = total_records
        sync_log.completed_at = datetime.now(timezone.utc)
        db.commit()

        elapsed = time.time() - t_start
        logger.info(
            "Sync complete in %.1fs: %d synced, %d skipped, %d failed",
            elapsed, synced, skipped, failed,
        )

        return {
            "status": "completed",
            "projects_synced": synced,
            "projects_skipped": skipped,
            "projects_failed": failed,
            "total_records": total_records,
            "elapsed_seconds": round(elapsed, 1),
            "details": results,
        }

    except Exception as e:
        db.rollback()
        sync_log.status = "failed"
        sync_log.error_message = str(e)
        sync_log.completed_at = datetime.now(timezone.utc)
        db.commit()
        logger.error("Full sync failed: %s", e)
        return {"status": "failed", "error": str(e)}

    finally:
        db.close()


# ═════════════════════════════════════════════════════
# PARALLEL PROJECT SYNC
# ═════════════════════════════════════════════════════

def _sync_projects_parallel(db: Session, project_hashes: list, workspace_map: dict = None) -> list:
    """Sync multiple projects in parallel using ThreadPoolExecutor."""
    results = []
    if workspace_map is None:
        workspace_map = {}

    with ThreadPoolExecutor(max_workers=settings.MAX_PARALLEL_SYNCS) as executor:
        future_to_hash = {
            executor.submit(_sync_one_project, ph, workspace_map.get(ph, 'Unknown')): ph
            for ph in project_hashes
        }

        for future in as_completed(future_to_hash):
            ph = future_to_hash[future]
            try:
                result = future.result()
                if result.get("status") == "synced":
                    _write_project_to_db(db, result)
                results.append(result)
            except Exception as e:
                logger.error("Project %s sync error: %s", ph, e)
                results.append({
                    "project_hash": ph,
                    "status": "failed",
                    "error": str(e),
                    "records": 0,
                })

    return results


def _sync_one_project(project_hash: str, workspace: str = 'Unknown') -> dict:
    """
    Sync a single project. Runs in a thread.
    Carries workspace so it can be stored in client_tag.
    """
    t_start = time.time()

    try:
        project = encord_client.get_project(project_hash)
        if project is None:
            return {"project_hash": project_hash, "status": "failed", "error": "Not found", "records": 0}

        label_rows = list(project.list_label_rows_v2())

        if settings.INCREMENTAL_SYNC:
            current_checksum = _compute_checksum(project_hash, label_rows)
            stored = _get_stored_checksum(project_hash)

            if stored and stored == current_checksum:
                logger.info("  ⏭ Skipping %s (unchanged)", project.title)
                return {
                    "project_hash": project_hash,
                    "project_title": project.title,
                    "workspace": workspace,
                    "status": "skipped",
                    "records": 0,
                }

        logger.info("  ↻ Syncing %s [%s]...", project.title, workspace)

        time_entries = list(project.list_time_spent(
            start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end=datetime.now(timezone.utc) + timedelta(days=1),
        ))

        label_logs = []
        try:
            label_logs = list(project.get_label_logs())
        except Exception as e:
            logger.warning("Could not fetch label logs for %s: %s", project_hash, e)

        result = _process_project_data(project, label_rows, time_entries, label_logs)
        result["workspace"] = workspace
        result["checksum"] = _compute_checksum(project_hash, label_rows)

        elapsed = time.time() - t_start
        logger.info("  ✓ Synced %s in %.1fs", project.title, elapsed)

        return result

    except Exception as e:
        logger.error("  ✗ Failed to sync %s: %s", project_hash, e)
        return {
            "project_hash": project_hash,
            "status": "failed",
            "error": str(e),
            "records": 0,
        }


# ═════════════════════════════════════════════════════
# CHECKSUM / INCREMENTAL DETECTION
# ═════════════════════════════════════════════════════

_checksum_cache = {}  # In-memory cache of stored checksums


def _compute_checksum(project_hash: str, label_rows: list) -> str:
    """Compute a fingerprint of project state for change detection."""
    task_count = len(label_rows)
    completed_count = 0
    annotators = set()
    last_edited = ""

    for lr in label_rows:
        # Count completed tasks
        wf_node = getattr(lr, "workflow_graph_node", None)
        if wf_node:
            node_title = str(getattr(wf_node, "title", "")).lower()
            if any(w in node_title for w in ["complete", "done", "final", "archive"]):
                completed_count += 1
        elif lr.is_labelling_initialised:
            completed_count += 1

        # Track annotators
        created_by = getattr(lr, "created_by", None)
        if created_by:
            annotators.add(created_by)

        # Track last edit time
        edited_at = str(getattr(lr, "last_edited_at", ""))
        if edited_at > last_edited:
            last_edited = edited_at

    fingerprint = f"{task_count}:{completed_count}:{len(annotators)}:{last_edited}"
    checksum = hashlib.md5(fingerprint.encode()).hexdigest()[:12]
    return checksum


def _get_stored_checksum(project_hash: str) -> str | None:
    """Get the previously stored checksum for a project."""
    if project_hash in _checksum_cache:
        return _checksum_cache[project_hash]

    db = SessionLocal()
    try:
        row = db.query(ProjectChecksum).filter_by(project_hash=project_hash).first()
        if row:
            _checksum_cache[project_hash] = row.checksum
            return row.checksum
        return None
    finally:
        db.close()


# ═════════════════════════════════════════════════════
# DATA PROCESSING
# ═════════════════════════════════════════════════════

def _process_project_data(project, label_rows, time_entries, label_logs) -> dict:
    """Process raw Encord data into structured metrics."""
    project_hash = project.project_hash

    # ── Task Status ──
    total_tasks = len(label_rows)
    done_tasks = 0
    in_review = 0
    in_annotation = 0

    for lr in label_rows:
        wf_node = getattr(lr, "workflow_graph_node", None)
        if wf_node:
            node_title = str(getattr(wf_node, "title", "")).lower()
            stage_type = str(getattr(wf_node, "stage_type", "")).upper()
            if any(w in node_title for w in ["complete", "done", "final", "archive"]):
                done_tasks += 1
            elif "REVIEW" in stage_type or "review" in node_title:
                in_review += 1
            elif "ANNOTATION" in stage_type or "annotat" in node_title:
                in_annotation += 1
        elif lr.is_labelling_initialised:
            done_tasks += 1

    progress = round((done_tasks / total_tasks * 100), 1) if total_tasks > 0 else 0

    # ── Time Metrics ──
    annotator_time = defaultdict(float)
    annotator_task_set = defaultdict(set)

    for entry in time_entries:
        email = getattr(entry, "user_email", None) or "unknown"
        seconds = getattr(entry, "time_spent_seconds", 0) or 0
        data_title = getattr(entry, "data_title", "")
        if email != "unknown":
            annotator_time[email] += float(seconds)
            if data_title:
                annotator_task_set[email].add(data_title)

    total_time_seconds = sum(annotator_time.values())
    active_annotators = len(annotator_time)

    # ── Label Log Metrics (submit/reject) ──
    annotator_submitted = defaultdict(int)
    annotator_rejected = defaultdict(int)
    annotator_approved = defaultdict(int)

    for log in label_logs:
        action = str(getattr(log, "action", "")).upper()
        user = getattr(log, "user", "") or ""

        if "SUBMIT" in action:
            annotator_submitted[user] += 1
        elif "REJECT" in action:
            annotator_rejected[user] += 1
        elif "APPROVE" in action:
            annotator_approved[user] += 1

    # ── Per-Annotator Metrics ──
    all_emails = set(annotator_time.keys()) | set(annotator_submitted.keys())
    annotator_metrics = {}

    for email in all_emails:
        time_s = annotator_time.get(email, 0.0)
        tasks_worked = len(annotator_task_set.get(email, set()))
        submitted = annotator_submitted.get(email, tasks_worked)
        rejected = annotator_rejected.get(email, 0)
        approved = annotator_approved.get(email, 0)
        completed = max(tasks_worked, submitted)

        if completed == 0 and time_s == 0:
            continue

        rr = calculate_rejection_rate(submitted, rejected)
        tpt = calculate_tpt(time_s, completed) if completed > 0 else 0.0
        hours = time_s / 3600 if time_s > 0 else 0.0
        tp = calculate_throughput(completed, hours) if hours > 0 else 0.0

        annotator_metrics[email] = {
            "tasks_submitted": submitted,
            "tasks_rejected": rejected,
            "tasks_approved": approved,
            "rejection_rate": rr,
            "total_time_seconds": time_s,
            "tasks_completed": completed,
            "time_per_task_seconds": tpt,
            "throughput_per_hour": tp,
        }

    # ── Outlier Detection ──
    rr_dict = {e: m["rejection_rate"] for e, m in annotator_metrics.items()}
    tpt_dict = {e: m["time_per_task_seconds"] for e, m in annotator_metrics.items()}
    tp_dict = {e: m["throughput_per_hour"] for e, m in annotator_metrics.items()}

    outliers = []
    outliers.extend(detect_rejection_rate_outliers(rr_dict, project_hash, project.title))
    outliers.extend(detect_tpt_outliers(tpt_dict, project_hash, project.title))
    outliers.extend(detect_throughput_outliers(tp_dict, project_hash, project.title))

    red_flags = sum(1 for o in outliers if o.flag_level == "red")
    amber_flags = sum(1 for o in outliers if o.flag_level == "amber")

    # ── Project-Level Averages ──
    rr_values = [m["rejection_rate"] for m in annotator_metrics.values()]
    tpt_values = [m["time_per_task_seconds"] for m in annotator_metrics.values() if m["time_per_task_seconds"] > 0]
    tp_values = [m["throughput_per_hour"] for m in annotator_metrics.values() if m["throughput_per_hour"] > 0]

    avg_rr = float(np.mean(rr_values)) if rr_values else 0.0
    avg_tpt = float(np.median(tpt_values)) if tpt_values else 0.0
    avg_tp = float(np.mean(tp_values)) if tp_values else 0.0

    # ── Health Status (multi-factor) ──
    health = _compute_health(avg_rr, red_flags, amber_flags, progress)

    # ── Format time ──
    total_h = int(total_time_seconds // 3600)
    total_m = int((total_time_seconds % 3600) // 60)
    time_display = f"{total_h}h {total_m}m" if total_h > 0 else f"{total_m}m"

    return {
        "project_hash": project_hash,
        "project_title": project.title,
        "status": "synced",
        "records": len(annotator_metrics) + len(time_entries) + len(outliers),
        # Project data
        "total_tasks": total_tasks,
        "done_tasks": done_tasks,
        "in_review": in_review,
        "in_annotation": in_annotation,
        "progress": progress,
        "active_annotators": active_annotators,
        "total_time_seconds": total_time_seconds,
        "total_time_display": time_display,
        "avg_rejection_rate": avg_rr,
        "avg_tpt_seconds": avg_tpt,
        "avg_throughput": avg_tp,
        "health_status": health,
        "red_flags": red_flags,
        "amber_flags": amber_flags,
        # Detail data
        "annotator_metrics": annotator_metrics,
        "outliers": [
            {
                "email": o.annotator_email,
                "metric": o.metric_type,
                "value": o.actual_value,
                "threshold": o.threshold_value,
                "level": o.flag_level,
                "description": o.description,
            }
            for o in outliers
        ],
        "time_entries_count": len(time_entries),
        "created_at": str(getattr(project, "created_at", "")),
    }


def _compute_health(avg_rr, red_flags, amber_flags, progress) -> str:
    """
    Multi-factor health computation.
    Considers: rejection rate, flag counts, completion progress.
    """
    score = 0

    # Rejection rate factor (0-3 points)
    if avg_rr > 0.20:
        score += 3
    elif avg_rr > 0.15:
        score += 2
    elif avg_rr > 0.10:
        score += 1

    # Red flags factor (0-3 points)
    if red_flags >= 3:
        score += 3
    elif red_flags >= 2:
        score += 2
    elif red_flags >= 1:
        score += 1

    # Amber flags factor (0-1 points)
    if amber_flags >= 3:
        score += 1

    # Progress factor (only if project has tasks)
    if progress < 20:
        score += 1

    if score >= 4:
        return "red"
    elif score >= 2:
        return "amber"
    return "green"


# ═════════════════════════════════════════════════════
# DATABASE WRITE (main thread only)
# ═════════════════════════════════════════════════════

def _write_project_to_db(db: Session, result: dict):
    """Write processed project data to all database tables."""
    ph = result["project_hash"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        # 1. Update ProjectChecksum
        checksum_row = db.query(ProjectChecksum).filter_by(project_hash=ph).first()
        if checksum_row:
            checksum_row.task_count = result.get("total_tasks", 0)
            checksum_row.completed_count = result.get("done_tasks", 0)
            checksum_row.annotator_count = result.get("active_annotators", 0)
            checksum_row.total_time_seconds = result.get("total_time_seconds", 0)
            checksum_row.checksum = result.get("checksum", "")
            checksum_row.last_checked = datetime.now(timezone.utc)
            checksum_row.last_changed = datetime.now(timezone.utc)
        else:
            db.add(ProjectChecksum(
                project_hash=ph,
                task_count=result.get("total_tasks", 0),
                completed_count=result.get("done_tasks", 0),
                annotator_count=result.get("active_annotators", 0),
                total_time_seconds=result.get("total_time_seconds", 0),
                checksum=result.get("checksum", ""),
            ))

        # Update in-memory cache
        _checksum_cache[ph] = result.get("checksum", "")

        # 2. Update ProjectCache (the fast-read table)
        cache_row = db.query(ProjectCache).filter_by(project_hash=ph).first()
        cache_data = {
            "title": result.get("project_title", ""),
            "client_tag": result.get("workspace", "Unknown"),   # workspace → client_tag
            "total_tasks": result.get("total_tasks", 0),
            "done_tasks": result.get("done_tasks", 0),
            "in_review": result.get("in_review", 0),
            "in_annotation": result.get("in_annotation", 0),
            "progress": result.get("progress", 0),
            "active_annotators": result.get("active_annotators", 0),
            "total_time_seconds": result.get("total_time_seconds", 0),
            "total_time_display": result.get("total_time_display", "0s"),
            "avg_rejection_rate": result.get("avg_rejection_rate", 0),
            "avg_tpt_seconds": result.get("avg_tpt_seconds", 0),
            "avg_throughput": result.get("avg_throughput", 0),
            "health_status": result.get("health_status", "green"),
            "red_flags": result.get("red_flags", 0),
            "amber_flags": result.get("amber_flags", 0),
            "cached_json": json.dumps({
                "annotators": result.get("annotator_metrics", {}),
                "outliers": result.get("outliers", []),
            }),
            "last_synced": datetime.now(timezone.utc),
        }

        if cache_row:
            for k, v in cache_data.items():
                setattr(cache_row, k, v)
        else:
            db.add(ProjectCache(project_hash=ph, **cache_data))

        # 3. Update MetricSnapshots (per annotator)
        for email, metrics in result.get("annotator_metrics", {}).items():
            # Upsert annotator
            ann = db.query(Annotator).filter_by(email=email).first()
            if ann:
                ann.last_synced = datetime.now(timezone.utc)
            else:
                db.add(Annotator(
                    email=email,
                    name=email.split("@")[0],
                    last_synced=datetime.now(timezone.utc),
                ))

            # Upsert metric snapshot
            snap = db.query(MetricSnapshot).filter_by(
                project_hash=ph, annotator_email=email, date=today,
            ).first()

            if snap:
                snap.tasks_submitted = metrics["tasks_submitted"]
                snap.tasks_rejected = metrics["tasks_rejected"]
                snap.rejection_rate = metrics["rejection_rate"]
                snap.total_time_seconds = metrics["total_time_seconds"]
                snap.tasks_completed = metrics["tasks_completed"]
                snap.time_per_task_seconds = metrics["time_per_task_seconds"]
                snap.throughput_per_hour = metrics["throughput_per_hour"]
                snap.snapshot_timestamp = datetime.now(timezone.utc)
            else:
                db.add(MetricSnapshot(
                    project_hash=ph,
                    annotator_email=email,
                    date=today,
                    **metrics,
                ))

        # 4. Insert outlier flags
        for outlier in result.get("outliers", []):
            if outlier["level"] != "green":  # Only store amber/red
                db.add(OutlierFlag(
                    project_hash=ph,
                    project_title=result.get("project_title", ""),
                    annotator_email=outlier["email"],
                    metric_type=outlier["metric"],
                    actual_value=outlier["value"],
                    threshold_value=outlier["threshold"],
                    flag_level=outlier["level"],
                    description=outlier["description"],
                ))

        db.commit()

    except Exception as e:
        db.rollback()
        logger.error("Failed to write project %s to DB: %s", ph, e)


# ═════════════════════════════════════════════════════
# INTELLIGENCE ENGINE
# ═════════════════════════════════════════════════════

def _run_intelligence_engine(db: Session):
    """
    Post-sync intelligence: trends, summary, health overview.
    Generates auto-summary and stores in DashboardSummary.
    """
    try:
        # Get all cached projects
        projects = db.query(ProjectCache).all()
        if not projects:
            return

        # ── Trend Analysis ──
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

        for proj in projects:
            # Get current snapshots
            current_snaps = db.query(MetricSnapshot).filter_by(
                project_hash=proj.project_hash, date=today,
            ).all()

            # Get 7-day-old snapshots
            old_snaps = db.query(MetricSnapshot).filter_by(
                project_hash=proj.project_hash, date=week_ago,
            ).all()

            if current_snaps and old_snaps:
                curr_rr = np.mean([s.rejection_rate for s in current_snaps])
                old_rr = np.mean([s.rejection_rate for s in old_snaps])
                proj.rejection_rate_trend = round(float(curr_rr - old_rr), 4)

                curr_tp = np.mean([s.throughput_per_hour for s in current_snaps if s.throughput_per_hour > 0])
                old_tp = np.mean([s.throughput_per_hour for s in old_snaps if s.throughput_per_hour > 0])
                if not np.isnan(curr_tp) and not np.isnan(old_tp) and old_tp > 0:
                    proj.throughput_trend = round(float((curr_tp - old_tp) / old_tp * 100), 1)

        # ── Generate Summary ──
        total_projects = len(projects)
        total_red = sum(p.red_flags for p in projects)
        total_amber = sum(p.amber_flags for p in projects)
        all_annotators = set()

        for proj in projects:
            try:
                cached = json.loads(proj.cached_json or "{}")
                all_annotators.update(cached.get("annotators", {}).keys())
            except Exception:
                pass

        insights = []

        # High rejection rate projects
        high_rr = [p for p in projects if p.avg_rejection_rate > 0.15]
        if high_rr:
            names = ", ".join(p.title[:30] for p in high_rr[:3])
            insights.append({
                "type": "warning",
                "message": f"{len(high_rr)} project(s) have high rejection rates (>15%): {names}",
            })

        # Red-flagged projects
        red_projects = [p for p in projects if p.health_status == "red"]
        if red_projects:
            names = ", ".join(p.title[:30] for p in red_projects[:3])
            insights.append({
                "type": "critical",
                "message": f"{len(red_projects)} project(s) in critical health: {names}",
            })

        # Underperforming annotators
        if total_red > 0:
            insights.append({
                "type": "warning",
                "message": f"{total_red} red flag(s) across all projects — review annotator performance",
            })

        # Positive insights
        healthy = [p for p in projects if p.health_status == "green"]
        if healthy:
            insights.append({
                "type": "success",
                "message": f"{len(healthy)}/{total_projects} projects are healthy",
            })

        # High completion
        completed = [p for p in projects if p.progress >= 90]
        if completed:
            insights.append({
                "type": "info",
                "message": f"{len(completed)} project(s) are ≥90% complete",
            })

        # Build summary text
        summary_parts = []
        summary_parts.append(f"Monitoring {total_projects} projects with {len(all_annotators)} annotators.")
        if high_rr:
            summary_parts.append(f"{len(high_rr)} project(s) have high rejection rates.")
        if total_red > 0:
            summary_parts.append(f"{total_red} red flags detected.")
        if total_amber > 0:
            summary_parts.append(f"{total_amber} amber warnings.")
        summary_text = " ".join(summary_parts)

        # Store summary
        db.add(DashboardSummary(
            summary_text=summary_text,
            insights_json=json.dumps(insights),
            total_projects=total_projects,
            total_annotators=len(all_annotators),
            total_red_flags=total_red,
            total_amber_flags=total_amber,
        ))

        db.commit()
        logger.info("✓ Intelligence engine: %d insights generated", len(insights))

    except Exception as e:
        db.rollback()
        logger.error("Intelligence engine failed: %s", e)
