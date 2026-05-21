"""
Encord Operations Dashboard — Background Scheduler
====================================================
APScheduler-based background sync that fetches data from Encord SDK
every N minutes and updates the local cache.

The frontend NEVER calls the SDK directly — it reads from SQLite only.
"""

import logging
import threading
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import settings

logger = logging.getLogger(__name__)

# Module-level scheduler instance
_scheduler: BackgroundScheduler | None = None
_sync_lock = threading.Lock()

# Sync status (readable by API)
sync_status = {
    "is_running": False,
    "last_started": None,
    "last_completed": None,
    "last_status": "never",  # never, running, completed, failed
    "last_error": None,
    "projects_synced": 0,
    "projects_skipped": 0,
    "total_records": 0,
    "next_run": None,
}


def _run_sync_job():
    """
    The actual sync job that runs in the background.
    Uses a lock to prevent overlapping runs.
    """
    if not _sync_lock.acquire(blocking=False):
        logger.info("Sync already in progress, skipping this run")
        return

    try:
        sync_status["is_running"] = True
        sync_status["last_started"] = datetime.now(timezone.utc).isoformat()
        sync_status["last_status"] = "running"
        sync_status["last_error"] = None

        logger.info("━━━ Background sync started ━━━")

        # Import here to avoid circular imports
        from sync_worker import run_full_sync

        result = run_full_sync()

        sync_status["is_running"] = False
        sync_status["last_completed"] = datetime.now(timezone.utc).isoformat()
        sync_status["last_status"] = result.get("status", "completed")
        sync_status["projects_synced"] = result.get("projects_synced", 0)
        sync_status["projects_skipped"] = result.get("projects_skipped", 0)
        sync_status["total_records"] = result.get("total_records", 0)

        logger.info(
            "━━━ Background sync completed: %d synced, %d skipped ━━━",
            result.get("projects_synced", 0),
            result.get("projects_skipped", 0),
        )

    except Exception as e:
        sync_status["is_running"] = False
        sync_status["last_completed"] = datetime.now(timezone.utc).isoformat()
        sync_status["last_status"] = "failed"
        sync_status["last_error"] = str(e)
        logger.error("━━━ Background sync FAILED: %s ━━━", e)

    finally:
        _sync_lock.release()
        # Update next run time
        if _scheduler and _scheduler.get_jobs():
            job = _scheduler.get_jobs()[0]
            sync_status["next_run"] = str(job.next_run_time) if job.next_run_time else None


def start_scheduler():
    """Start the background scheduler. Called from main.py startup."""
    global _scheduler

    if _scheduler is not None:
        logger.warning("Scheduler already running")
        return

    _scheduler = BackgroundScheduler(daemon=True)

    # Add the sync job
    _scheduler.add_job(
        _run_sync_job,
        trigger=IntervalTrigger(minutes=settings.SYNC_INTERVAL_MINUTES),
        id="encord_sync",
        name="Encord Data Sync",
        replace_existing=True,
        max_instances=1,
    )

    _scheduler.start()
    logger.info(
        "✓ Scheduler started (sync every %d minutes)",
        settings.SYNC_INTERVAL_MINUTES,
    )

    # Run initial sync immediately in a background thread
    threading.Thread(target=_run_sync_job, daemon=True).start()


def stop_scheduler():
    """Stop the scheduler gracefully. Called from main.py shutdown."""
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("✓ Scheduler stopped")


def trigger_manual_sync():
    """Trigger a manual sync (from API call). Returns immediately."""
    if sync_status["is_running"]:
        return {"status": "already_running", "message": "Sync is already in progress"}

    threading.Thread(target=_run_sync_job, daemon=True).start()
    return {"status": "triggered", "message": "Sync started in background"}


def get_sync_status() -> dict:
    """Return current sync status for API consumption."""
    return {**sync_status}
