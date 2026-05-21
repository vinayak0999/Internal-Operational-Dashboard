"""
Human Data Operations Dashboard — FastAPI Backend
===================================================
Multi-client, multi-project dashboard with health monitoring.
Supports client/workspace selection with project-level analytics.

⛔ READ-ONLY: No data is modified on Encord. No DELETE queries.
"""

import os
import json
import time
import hashlib
import statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict
from uuid import UUID
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

# ─── Load .env ───
load_dotenv()

SSH_KEY_PATH = os.getenv("ENCORD_SSH_KEY_PATH", "")
ENCORD_DOMAIN = os.getenv("ENCORD_DOMAIN", "https://api.encord.com")
DEFAULT_PROJECT_HASH = os.getenv("ENCORD_PROJECT_HASHES", "").split(",")[0].strip()
PROJECT_CLIENT = os.getenv("PROJECT_CLIENT", "Client")
PROJECT_MODALITY = os.getenv("PROJECT_MODALITY", "Mixed")

# ─── Load Clients Config ───
CLIENTS_CONFIG_PATH = Path(__file__).parent / "clients.json"


def load_clients_config():
    """Load clients configuration from JSON file."""
    if not CLIENTS_CONFIG_PATH.exists():
        return {"clients": []}
    try:
        with open(CLIENTS_CONFIG_PATH) as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load clients.json: {e}")
        return {"clients": []}

# ─── Encord Client (lazy init) ───
user_client = None


def get_encord_client():
    """Authenticate with Encord. Raises clear errors if config is wrong."""
    global user_client
    if user_client is not None:
        return user_client

    if not SSH_KEY_PATH:
        raise ValueError(
            "ENCORD_SSH_KEY_PATH not set in .env file. "
            "Add the path to your Encord private key."
        )
    if not Path(SSH_KEY_PATH).exists():
        raise FileNotFoundError(
            f"SSH key file not found at: {SSH_KEY_PATH}. "
            f"Check the path in your .env file."
        )

    from encord import EncordUserClient
    user_client = EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=SSH_KEY_PATH,
        domain=ENCORD_DOMAIN,
    )
    print(f"  ✓ Connected to Encord ({ENCORD_DOMAIN})")
    return user_client


def get_initials(name: str) -> str:
    """Convert 'Fatima Al-Hassan' → 'FA', 'annotator4_mammo@encord.ai' → 'A4'."""
    if not name:
        return "??"
    # If it's an email, use username part
    if "@" in name:
        name = name.split("@")[0]
    parts = name.replace("_", " ").replace("-", " ").replace(".", " ").split()
    if len(parts) >= 2:
        return (parts[0][0] + parts[1][0]).upper()
    return name[:2].upper()


def format_name_from_email(email: str) -> str:
    """Best-effort display name from email."""
    local = email.split("@")[0]
    parts = local.replace(".", " ").replace("_", " ").replace("-", " ").split()
    return " ".join(p.capitalize() for p in parts) if parts else email


def format_seconds(secs: float) -> str:
    """Format seconds to human-readable: '3m 11s', '1h 45m 15s'."""
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s"


# ─── Response Cache (TTL-based) ───
_cache = {}  # key -> {"data": ..., "ts": float}
CACHE_TTL_SECONDS = 300  # 5-minute TTL

def cache_key(ph, start, end):
    """Generate a unique cache key for a project+date query."""
    raw = f"{ph}|{start}|{end}"
    return hashlib.md5(raw.encode()).hexdigest()

def get_cached(key):
    """Return cached data if fresh, else None."""
    entry = _cache.get(key)
    if entry and (time.time() - entry["ts"]) < CACHE_TTL_SECONDS:
        return entry["data"]
    return None

def set_cache(key, data):
    """Store data in cache with current timestamp."""
    _cache[key] = {"data": data, "ts": time.time()}


# ─── FastAPI App ───
app = FastAPI(title="Human Data Ops Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    print("\n  Human Data Operations Dashboard")
    print("  ──────────────────────────────────")

    # Initialize database tables
    try:
        from database import init_db
        init_db()
        print("  ✓ Database initialized")
    except Exception as e:
        print(f"  ✗ Database init warning: {e}")

    # Connect to Encord
    try:
        client = get_encord_client()
        if DEFAULT_PROJECT_HASH:
            project = client.get_project(DEFAULT_PROJECT_HASH)
            print(f"  ✓ Loaded project: {project.title}")
        print(f"  ✓ Server on http://localhost:8000")
    except Exception as e:
        print(f"  ✗ Startup warning: {e}")
        print(f"  ✓ Server on http://localhost:8000 (will retry on first API call)")

    # Start background scheduler (replaces old refresh thread)
    try:
        from scheduler import start_scheduler
        start_scheduler()
    except Exception as e:
        print(f"  ✗ Scheduler warning: {e}")
        # Fallback to old refresh thread
        _start_background_refresh()

    print()


@app.on_event("shutdown")
def shutdown():
    try:
        from scheduler import stop_scheduler
        stop_scheduler()
    except Exception:
        pass


def _background_refresh_loop():
    """Legacy background thread (fallback if scheduler fails)."""
    import time as _time
    _time.sleep(5)
    while True:
        try:
            if DEFAULT_PROJECT_HASH:
                today = datetime.now(timezone.utc)
                week_ago = today - timedelta(days=7)
                sd = week_ago.strftime("%Y-%m-%d")
                ed = today.strftime("%Y-%m-%d")
                result = get_project_data(
                    project_hash=DEFAULT_PROJECT_HASH,
                    start_date=sd,
                    end_date=ed,
                )
                if not isinstance(result, JSONResponse):
                    print(f"  🔄 Background refresh complete — cache warm")
        except Exception as e:
            print(f"  ⚠ Background refresh error: {e}")
        _time.sleep(CACHE_TTL_SECONDS - 30)


def _start_background_refresh():
    """Legacy: Start the background refresh thread."""
    t = threading.Thread(target=_background_refresh_loop, daemon=True)
    t.start()
    print(f"  ✓ Background refresh running every {CACHE_TTL_SECONDS}s")


@app.get("/")
async def root():
    return FileResponse("dashboard.html")


@app.get("/favicon.ico")
async def favicon():
    # Return empty response to suppress 404
    return JSONResponse(content={}, status_code=204)


# ─── Main Data Endpoint ───
@app.get("/api/project-data")
def get_project_data(
    project_hash: str = Query(default=None, description="Encord project hash"),
    days: int = Query(default=7, description="Number of days to look back (used if start_date/end_date not set)"),
    start_date: str = Query(default=None, description="Start date YYYY-MM-DD"),
    end_date: str = Query(default=None, description="End date YYYY-MM-DD"),
):
    """
    Fetch real data from Encord for any project hash.
    Accepts exact date range via start_date/end_date params.

    READ-ONLY — no writes to Encord.
    """
    ph = project_hash or DEFAULT_PROJECT_HASH
    if not ph:
        return JSONResponse(status_code=400, content={
            "error": "No project hash provided. Pass ?project_hash=xxx or set ENCORD_PROJECT_HASHES in .env"
        })

    try:
        client = get_encord_client()
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "error": f"Encord authentication failed: {str(e)}"
        })

    try:
        project = client.get_project(ph)
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "error": f"Could not load project '{ph}': {str(e)}"
        })

    try:
        # ═══════════════════════════════════════════
        # 1. DATE RANGE — use exact dates if provided
        # ═══════════════════════════════════════════
        if start_date and end_date:
            try:
                dt_start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                # End date should be end-of-day to include the full day
                dt_end = datetime.strptime(end_date, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59, tzinfo=timezone.utc
                )
            except ValueError:
                return JSONResponse(status_code=400, content={
                    "error": "Invalid date format. Use YYYY-MM-DD (e.g., 2026-04-11)"
                })
        else:
            dt_end = datetime.now(timezone.utc)
            dt_start = dt_end - timedelta(days=days)

        date_label = f"{dt_start.strftime('%d/%m/%Y')} to {dt_end.strftime('%d/%m/%Y')}"

        # ═══════════════════════════════════════════
        # CHECK CACHE — skip all API calls if fresh
        # ═══════════════════════════════════════════
        ck = cache_key(ph, str(dt_start), str(dt_end))
        cached = get_cached(ck)
        if cached:
            print(f"   Cache HIT — returning instantly")
            return cached

        t_api_start = time.time()
        print(f"  ⏳ Fetching data from Encord (3 parallel API calls)...")

        # ═══════════════════════════════════════════
        # 2. PARALLEL FETCH — all 3 API calls at once
        #    Instead of sequential (~5 min), runs in
        #    parallel (~1.5 min = time of slowest call)
        # ═══════════════════════════════════════════
        def fetch_time_entries():
            return list(project.list_time_spent(start=dt_start, end=dt_end))

        def fetch_label_rows():
            return list(project.list_label_rows_v2())

        def fetch_label_logs():
            return list(project.get_label_logs(after=dt_start, before=dt_end))

        with ThreadPoolExecutor(max_workers=3) as executor:
            fut_time = executor.submit(fetch_time_entries)
            fut_rows = executor.submit(fetch_label_rows)
            fut_logs = executor.submit(fetch_label_logs)

            time_entries = fut_time.result()
            label_rows = fut_rows.result()
            label_logs_raw = fut_logs.result()

        t_api_end = time.time()
        print(f"  ✓ All 3 API calls done in {t_api_end - t_api_start:.1f}s "
              f"(time_entries={len(time_entries)}, "
              f"label_rows={len(label_rows)}, "
              f"label_logs={len(label_logs_raw)})")

        # ═══════════════════════════════════════════
        # 3. GROUP BY USER + WORKFLOW STAGE TYPE
        #    This separates annotation work from review work
        # ═══════════════════════════════════════════
        # Per-user annotation data
        ann_data = defaultdict(lambda: {
            "time_seconds": 0,
            "data_uuids": set(),   # unique tasks
            "dates": [],
        })
        # Per-user review data
        rev_data = defaultdict(lambda: {
            "time_seconds": 0,
            "data_uuids": set(),
            "dates": [],
        })
        # Other/unclassified time
        other_time = 0

        for entry in time_entries:
            email = getattr(entry, "user_email", None) or "unknown"
            seconds = getattr(entry, "time_spent_seconds", 0) or 0
            data_uuid = getattr(entry, "data_uuid", None)
            period_start = getattr(entry, "period_start_time", None)
            wf_stage = getattr(entry, "workflow_stage", None)

            stage_type = None
            if wf_stage:
                stage_type = str(getattr(wf_stage, "stage_type", "")).upper()

            uuid_str = str(data_uuid) if data_uuid else None

            if "ANNOTATION" in (stage_type or ""):
                ann_data[email]["time_seconds"] += seconds
                if uuid_str:
                    ann_data[email]["data_uuids"].add(uuid_str)
                if period_start:
                    ann_data[email]["dates"].append(period_start)
            elif "REVIEW" in (stage_type or ""):
                rev_data[email]["time_seconds"] += seconds
                if uuid_str:
                    rev_data[email]["data_uuids"].add(uuid_str)
                if period_start:
                    rev_data[email]["dates"].append(period_start)
            else:
                other_time += seconds

        # ═══════════════════════════════════════════
        # 4. PROCESS LABEL ROWS for workflow state
        # ═══════════════════════════════════════════

        # Track per-annotator status from current label row state
        # A task's workflow_graph_node tells us its CURRENT stage
        task_status = defaultdict(lambda: {
            "total": 0,
            "complete": 0,
            "in_review": 0,
            "in_annotation": 0,
            "rejected_back": 0,
        })

        total_project_tasks = 0
        total_project_done = 0

        # Track which tasks are in which state
        tasks_in_complete = 0
        tasks_in_review = 0
        tasks_in_annotation = 0

        for lr in label_rows:
            total_project_tasks += 1
            wf_node = getattr(lr, "workflow_graph_node", None)
            if wf_node:
                node_title = str(getattr(wf_node, "title", "")).lower()
                stage_type = str(getattr(wf_node, "stage_type", "")).upper()

                if any(w in node_title for w in ["complete", "done", "final", "archive"]):
                    total_project_done += 1
                    tasks_in_complete += 1
                elif "REVIEW" in stage_type or "review" in node_title:
                    tasks_in_review += 1
                elif "ANNOTATION" in stage_type or "annotat" in node_title:
                    tasks_in_annotation += 1
            else:
                if lr.is_labelling_initialised:
                    total_project_done += 1

        # ═══════════════════════════════════════════
        # 5. FETCH LABEL LOGS for rejection data
        #    SUBMIT_TASK, APPROVE_TASK, REJECT_TASK
        #    actions give us exact rejection rates.
        #
        #    Key rules:
        #    - Denominator = approved + rejected (NOT submitted)
        #    - "Rejected wins": if a data_hash has BOTH approve
        #      AND reject events, it counts as REJECTED
        #    - Pending tasks (submitted but no review action)
        #      are excluded from the denominator
        # ═══════════════════════════════════════════
        from encord.orm.label_log import Action

        # Per-annotator: which tasks they submitted
        ann_submits = defaultdict(set)       # email -> set of data_hashes

        # Per-reviewer: which tasks they approved/rejected
        rev_approves = defaultdict(set)      # email -> set of data_hashes
        rev_rejects = defaultdict(set)       # email -> set of data_hashes

        # Global per-data_hash: track ALL approve/reject events
        dh_approved_by_any = set()           # data_hashes with ≥1 APPROVE_TASK
        dh_rejected_by_any = set()           # data_hashes with ≥1 REJECT_TASK

        # ── Instance label-level tracking (for Labels tab) ──
        label_totals = {"created": 0, "approved": 0, "rejected": 0, "deleted": 0}
        label_daily = defaultdict(lambda: {"created": 0, "approved": 0, "rejected": 0, "deleted": 0})
        label_ontology = defaultdict(lambda: {"created": 0, "edited": 0, "approved": 0, "rejected": 0, "deleted": 0})

        # Process label logs (already fetched in parallel above)
        for log in label_logs_raw:
            email = log.user_email
            dh = log.data_hash
            action = log.action
            label_name = getattr(log, 'label_name', None)
            log_date = log.created_at.strftime('%Y-%m-%d') if getattr(log, 'created_at', None) else None

            # Task-level actions (for rejection rates)
            if action == Action.SUBMIT_TASK:
                ann_submits[email].add(dh)
            elif action == Action.APPROVE_TASK:
                rev_approves[email].add(dh)
                dh_approved_by_any.add(dh)
            elif action == Action.REJECT_TASK:
                rev_rejects[email].add(dh)
                dh_rejected_by_any.add(dh)

            # Label-level actions (for Labels tab)
            # Created = SUBMIT_LABEL (annotator submits label for review) + ADD (label first drawn)
            # This gives INSTANCE-level counts (not frame-level like Encord UI CSV)
            if action in (Action.ADD, Action.SUBMIT_LABEL):
                label_totals['created'] += 1
                if log_date: label_daily[log_date]['created'] += 1
                if label_name: label_ontology[label_name]['created'] += 1
            elif action == Action.EDIT:
                if label_name: label_ontology[label_name]['edited'] += 1
            elif action == Action.DELETE:
                label_totals['deleted'] += 1
                if log_date: label_daily[log_date]['deleted'] += 1
                if label_name: label_ontology[label_name]['deleted'] += 1
            elif action == Action.APPROVE_LABEL:
                label_totals['approved'] += 1
                if log_date: label_daily[log_date]['approved'] += 1
                if label_name: label_ontology[label_name]['approved'] += 1
            elif action == Action.REJECT_LABEL:
                label_totals['rejected'] += 1
                if log_date: label_daily[log_date]['rejected'] += 1
                if label_name: label_ontology[label_name]['rejected'] += 1

        # ── Helper: classify a data_hash as approved/rejected/pending ──
        # "Rejected wins" — if a task was rejected then re-approved,
        # the annotator still had to redo it, so it counts as rejected.
        def classify_task(dh):
            was_rejected = dh in dh_rejected_by_any
            was_approved = dh in dh_approved_by_any
            if was_rejected:
                return "rejected"
            elif was_approved:
                return "approved"
            else:
                return "pending"

        # Log overlap for visibility
        overlap_count = len(dh_rejected_by_any & dh_approved_by_any)
        if overlap_count > 0:
            print(f"  ℹ Note: {overlap_count} tasks were rejected then later approved; counted as rejected.")

        # ── Per-annotator rejection rate ──
        # Denominator = approved + rejected (excludes pending)
        ann_rejection_rate = {}
        for email, submitted in ann_submits.items():
            n_approved = 0
            n_rejected = 0
            n_pending = 0
            for dh in submitted:
                cls = classify_task(dh)
                if cls == "rejected":
                    n_rejected += 1
                elif cls == "approved":
                    n_approved += 1
                else:
                    n_pending += 1

            reviewed = n_approved + n_rejected
            rate = round((n_rejected / reviewed * 100), 2) if reviewed > 0 else 0.0
            ann_rejection_rate[email] = {
                "submitted": len(submitted),
                "approved": n_approved,
                "rejected": n_rejected,
                "pending": n_pending,
                "rate": rate,
            }

        # ── Per-reviewer rejection rate ──
        # Uses the same "rejected wins" logic per data_hash
        rev_rejection_rate = {}
        for email in set(rev_approves.keys()) | set(rev_rejects.keys()):
            reviewer_approved = rev_approves.get(email, set())
            reviewer_rejected = rev_rejects.get(email, set())
            # For this reviewer, classify each task they touched
            n_app = 0
            n_rej = 0
            for dh in reviewer_approved | reviewer_rejected:
                if dh in reviewer_rejected:
                    n_rej += 1  # rejected wins, even if also approved
                else:
                    n_app += 1
            total = n_app + n_rej
            rev_rejection_rate[email] = {
                "approved": n_app,
                "rejected": n_rej,
                "rate": round((n_rej / total * 100), 2) if total > 0 else 0.0,
            }

        # ── Project-level rejection rate ──
        # Same denominator: approved + rejected (excludes pending)
        all_submitted_hashes = set()
        for s in ann_submits.values():
            all_submitted_hashes |= s

        proj_approved = 0
        proj_rejected = 0
        proj_pending = 0
        for dh in all_submitted_hashes:
            cls = classify_task(dh)
            if cls == "rejected":
                proj_rejected += 1
            elif cls == "approved":
                proj_approved += 1
            else:
                proj_pending += 1

        proj_reviewed = proj_approved + proj_rejected
        total_submitted = len(all_submitted_hashes)
        total_rejected = proj_rejected
        total_approved = proj_approved

        # ── Diagnostic output ──
        print(f"\n  ── Rejection Rate Diagnostic ──")
        for email in sorted(ann_submits.keys()):
            ar = ann_rejection_rate[email]
            print(f"    Annotator {email}: submitted={ar['submitted']}, "
                  f"approved={ar['approved']}, rejected={ar['rejected']}, "
                  f"pending={ar['pending']}, rejection_rate={ar['rate']}%")
        print(f"    Project:  total_submitted={total_submitted}, "
              f"approved={proj_approved}, rejected={proj_rejected}, "
              f"pending={proj_pending}, "
              f"rate={round((proj_rejected / proj_reviewed * 100), 2) if proj_reviewed > 0 else 0}%")
        print(f"    Tasks rejected-then-approved: {overlap_count}")

        # ═══════════════════════════════════════════
        # 6. BUILD ANNOTATORS TABLE
        #    (Users who did ANNOTATION work)
        # ═══════════════════════════════════════════
        annotators_list = []

        for email, ad in ann_data.items():
            tasks_worked = len(ad["data_uuids"])  # unique tasks annotated
            total_time = ad["time_seconds"]

            if tasks_worked == 0 and total_time == 0:
                continue

            # Avg time per task
            avg_tpt = total_time / tasks_worked if tasks_worked > 0 else 0

            # Days active
            dates = ad.get("dates", [])
            if len(dates) >= 2:
                try:
                    date_objs = [d if isinstance(d, datetime) else datetime.fromisoformat(str(d)) for d in dates]
                    days_active = max(1, (max(date_objs) - min(date_objs)).days + 1)
                except Exception:
                    days_active = 1
            else:
                days_active = 1

            # Throughput
            throughput = tasks_worked / days_active if days_active > 0 else 0

            name = format_name_from_email(email)
            annotators_list.append({
                "email": email,
                "name": name,
                "id": get_initials(email),
                "role": "Annotator",
                "tasks": tasks_worked,  # tasks submitted/worked on
                "days": days_active,
                "total_time_seconds": round(total_time),
                "total_time_raw": format_seconds(total_time),
                "avg_tpt_seconds": round(avg_tpt),
                "avg_tpt_raw": format_seconds(avg_tpt),
                "rejection": 0.0,  # Will be computed from label data
                "tpt": round(avg_tpt, 1),
                "tput": round(throughput, 1),
                "flags": [],
                "status": "good",
                "rejectionFlag": None,
                "tptFlag": None,
            })

        # ═══════════════════════════════════════════
        # 7. BUILD REVIEWERS TABLE
        #    (Users who did REVIEW work)
        # ═══════════════════════════════════════════
        reviewers_list = []

        for email, rd in rev_data.items():
            tasks_reviewed = len(rd["data_uuids"])
            total_time = rd["time_seconds"]

            if tasks_reviewed == 0 and total_time == 0:
                continue

            avg_tpt = total_time / tasks_reviewed if tasks_reviewed > 0 else 0

            name = format_name_from_email(email)
            reviewers_list.append({
                "email": email,
                "name": name,
                "id": get_initials(email),
                "role": "Reviewer",
                "tasks_reviewed": tasks_reviewed,
                "total_time_seconds": round(total_time),
                "total_time_raw": format_seconds(total_time),
                "avg_tpt_seconds": round(avg_tpt),
                "avg_tpt_raw": format_seconds(avg_tpt),
            })

        # ═══════════════════════════════════════════
        # 8. COMBINED ANNOTATOR LIST FOR DASHBOARD
        #    Merge annotation + review data per user
        # ═══════════════════════════════════════════
        all_users = set(ann_data.keys()) | set(rev_data.keys())
        combined_list = []

        for email in all_users:
            ad = ann_data.get(email, {"time_seconds": 0, "data_uuids": set(), "dates": []})
            rd = rev_data.get(email, {"time_seconds": 0, "data_uuids": set(), "dates": []})

            ann_tasks = len(ad["data_uuids"])
            rev_tasks = len(rd["data_uuids"])
            ann_time = ad["time_seconds"]
            rev_time = rd["time_seconds"]
            total_tasks = ann_tasks + rev_tasks
            total_time = ann_time + rev_time

            if total_tasks == 0 and total_time == 0:
                continue

            # Role determination
            has_ann = ann_tasks > 0 or ann_time > 0
            has_rev = rev_tasks > 0 or rev_time > 0
            if has_ann and has_rev:
                role = "Annotator & Reviewer"
            elif has_rev:
                role = "Reviewer"
            else:
                role = "Annotator"

            # TPT based on annotation time (primary metric)
            tpt = ann_time / ann_tasks if ann_tasks > 0 else (rev_time / rev_tasks if rev_tasks > 0 else 0)

            # Days active
            all_dates = ad.get("dates", []) + rd.get("dates", [])
            if len(all_dates) >= 2:
                try:
                    date_objs = [d if isinstance(d, datetime) else datetime.fromisoformat(str(d)) for d in all_dates]
                    days_active = max(1, (max(date_objs) - min(date_objs)).days + 1)
                except Exception:
                    days_active = 1
            else:
                days_active = 1

            # Throughput (annotation tasks per day)
            throughput = ann_tasks / days_active if days_active > 0 and ann_tasks > 0 else (
                rev_tasks / days_active if days_active > 0 else 0
            )

            name = format_name_from_email(email)
            # Rejection rate from label logs
            ann_rej = ann_rejection_rate.get(email, {})
            rev_rej = rev_rejection_rate.get(email, {})
            rej_rate = ann_rej.get("rate", 0.0)
            tasks_submitted = ann_rej.get("submitted", 0)
            tasks_rejected = ann_rej.get("rejected", 0)
            rev_approved = rev_rej.get("approved", 0)
            rev_rejected = rev_rej.get("rejected", 0)
            rev_rej_rate = rev_rej.get("rate", 0.0)

            combined_list.append({
                "email": email,
                "name": name,
                "id": get_initials(email),
                "role": role,
                "tasks": ann_tasks,
                "tasks_submitted": tasks_submitted,
                "tasks_rejected": tasks_rejected,
                "tasks_reviewed": rev_tasks,
                "rev_approved": rev_approved,
                "rev_rejected": rev_rejected,
                "rev_rejection_rate": round(rev_rej_rate, 2),
                "days": days_active,
                "annotation_time_seconds": round(ann_time),
                "annotation_time_raw": format_seconds(ann_time),
                "review_time_seconds": round(rev_time),
                "review_time_raw": format_seconds(rev_time),
                "total_time_seconds": round(total_time),
                "total_time_raw": format_seconds(total_time),
                "rejection": round(rej_rate, 2),
                "tpt": round(tpt, 1),
                "tput": round(throughput, 1),
                "flags": [],
                "status": "good",
                "rejectionFlag": None,
                "tptFlag": None,
            })

        # ═══════════════════════════════════════════
        # 9. PROJECT-LEVEL AGGREGATES
        # ═══════════════════════════════════════════
        total_ann_time = sum(ad["time_seconds"] for ad in ann_data.values())
        total_rev_time = sum(rd["time_seconds"] for rd in rev_data.values())
        total_all_time = total_ann_time + total_rev_time + other_time

        # Compute percentage based on annotation tasks
        ann_with_tasks = [a for a in combined_list if a["tasks"] > 0]
        if ann_with_tasks:
            tpt_values = [a["tpt"] for a in ann_with_tasks if a["tpt"] > 0]
            tp_values = [a["tput"] for a in ann_with_tasks if a["tput"] > 0]
            rr_values = [a["rejection"] for a in ann_with_tasks if a["tasks_submitted"] > 0]

            avg_rejection = statistics.mean(rr_values) if rr_values else 0
            median_tpt = statistics.median(tpt_values) if tpt_values else 0
            median_tp = statistics.median(tp_values) if tp_values else 0
            avg_tpt = statistics.mean(tpt_values) if tpt_values else 0
        else:
            avg_rejection = 0
            median_tpt = 0
            median_tp = 0
            avg_tpt = 0

        # Project-level rejection rate: same denominator as per-annotator
        project_rejection_rate = round(
            (total_rejected / proj_reviewed * 100), 2
        ) if proj_reviewed > 0 else 0.0

        progress_pct = round((total_project_done / total_project_tasks * 100), 1) if total_project_tasks > 0 else 0

        if project_rejection_rate > 15:
            project_status = "crit"
        elif project_rejection_rate > 10:
            project_status = "warn"
        else:
            project_status = "good"

        # ═══════════════════════════════════════════
        # 10. OUTLIER FLAGS
        # ═══════════════════════════════════════════
        crit_count = 0
        warn_count = 0

        for a in combined_list:
            flags = []

            if a["rejection"] > avg_rejection + 10:
                flags.append({"label": "high rejection", "type": "crit"})
                a["rejectionFlag"] = "high"

            if median_tpt > 0 and a["tpt"] > 0 and a["tpt"] < median_tpt * 0.2:
                flags.append({"label": "too fast", "type": "warn"})
                a["tptFlag"] = "fast"

            if median_tpt > 0 and a["tpt"] > median_tpt * 1.5:
                flags.append({"label": "too slow", "type": "warn"})
                a["tptFlag"] = "slow"

            if median_tp > 0 and a["tput"] > 0 and a["tput"] < median_tp * 0.8:
                flags.append({"label": "low throughput", "type": "warn"})

            a["flags"] = flags
            has_crit = any(f["type"] == "crit" for f in flags)
            has_warn = any(f["type"] == "warn" for f in flags)
            if has_crit:
                a["status"] = "crit"
                crit_count += 1
            elif has_warn:
                a["status"] = "warn"
                warn_count += 1

        # Sort: crit first, then warn, then good; within same status by tasks desc
        status_order = {"crit": 0, "warn": 1, "good": 2}
        combined_list.sort(key=lambda x: (status_order.get(x["status"], 3), -x["tasks"]))

        # ═══════════════════════════════════════════
        # 11. BUILD RESPONSE
        # ═══════════════════════════════════════════
        response = {
            "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "date_filter": date_label,
            "days": days,
            "project": {
                "name": project.title,
                "hash": ph,
                "client": PROJECT_CLIENT,
                "modality": PROJECT_MODALITY,
                "status": project_status,
                "progress": progress_pct,
                "done": total_project_done,
                "total": total_project_tasks,
                "rejection": round(project_rejection_rate, 1),
                "avg_rejection": round(avg_rejection, 1),
                "tpt": round(median_tpt, 1),
                "tput": round(sum(a["tput"] for a in combined_list if a["tasks"] > 0), 1),
                "total_time_raw": format_seconds(total_all_time),
                "annotation_time_raw": format_seconds(total_ann_time),
                "review_time_raw": format_seconds(total_rev_time),
                "avg_annotation_tpt_raw": format_seconds(avg_tpt),
                "tasks_in_review": tasks_in_review,
                "tasks_in_annotation": tasks_in_annotation,
                "tasks_complete": tasks_in_complete,
                "tasks_submitted": total_submitted,
                "tasks_rejected": total_rejected,
                "tasks_approved": total_approved,
            },
            "annotators": combined_list,
            "annotators_only": [a for a in combined_list if a["tasks"] > 0],
            "reviewers_only": [a for a in combined_list if a.get("tasks_reviewed", 0) > 0],
            "kpis": {
                "active_projects": 1,
                "critical_flags": crit_count,
                "warnings": warn_count,
                "critical_annotators": crit_count,
                "total_annotators": len([a for a in combined_list if a["tasks"] > 0]),
                "total_reviewers": len([a for a in combined_list if a.get("tasks_reviewed", 0) > 0]),
                "avg_rejection_rate": round(project_rejection_rate, 1),
            },
            "project_summary": {
                "total_time_all_collaborators": format_seconds(total_all_time),
                "total_time_annotating": format_seconds(total_ann_time),
                "total_time_reviewing": format_seconds(total_rev_time),
                "other_time": format_seconds(other_time),
                "avg_time_per_annotation_task": format_seconds(avg_tpt),
                "tasks_submitted": total_submitted,
                "tasks_approved": total_approved,
                "tasks_rejected": total_rejected,
                "avg_rejection_rate": round(project_rejection_rate, 1),
                "note": "Total time covers Annotator & Reviewer roles only. Admin browsing time (~1-5min) is tracked by Encord UI but not exposed via the SDK API.",
            },
            "labels_data": {
                "totals": label_totals,
                "total_actions": sum(label_totals.values()),
                "daily": [
                    {"date": d, **label_daily[d]}
                    for d in sorted(label_daily.keys())
                ],
                "ontology": [
                    {
                        "name": name,
                        "created": vals["created"],
                        "edited": vals["edited"],
                        "approved": vals["approved"],
                        "rejected": vals["rejected"],
                        "rejection_rate": round(
                            vals["rejected"] / (vals["approved"] + vals["rejected"]) * 100, 2
                        ) if (vals["approved"] + vals["rejected"]) > 0 else 0,
                        "deleted": vals["deleted"],
                    }
                    for name, vals in sorted(label_ontology.items())
                ],
            },
        }

        # Store in cache for fast subsequent loads
        set_cache(ck, response)
        t_total = time.time() - t_api_start
        print(f"  ✓ Response built and cached in {t_total:.1f}s (cache valid for {CACHE_TTL_SECONDS}s)")

        return response

    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"error": f"Data processing error: {str(e)}"}
        )


# ─── CSV Export for Power BI ───
@app.get("/api/export/annotators-csv")
def export_annotators_csv(
    project_hash: str = Query(default=None),
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
):
    """
    Export annotator data as CSV for Power BI import.
    Power BI: Data > Web > paste URL http://localhost:8000/api/export/annotators-csv
    """
    data = get_project_data(
        project_hash=project_hash,
        start_date=start_date,
        end_date=end_date,
    )
    if isinstance(data, JSONResponse):
        return data

    import csv, io
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Email", "Name", "Role", "Tasks_Annotated", "Tasks_Submitted",
        "Tasks_Rejected", "Rejection_Rate_Pct", "Tasks_Reviewed",
        "Rev_Approved", "Rev_Rejected", "Rev_Rejection_Rate_Pct",
        "Annotation_Time_Seconds", "Annotation_Time",
        "Review_Time_Seconds", "Review_Time",
        "Total_Time_Seconds", "Total_Time",
        "Avg_TPT_Seconds", "Throughput_Per_Day",
        "Days_Active", "Status",
    ])
    for a in data.get("annotators", []):
        writer.writerow([
            a.get("email",""), a.get("name",""), a.get("role",""),
            a.get("tasks",0), a.get("tasks_submitted",0),
            a.get("tasks_rejected",0), a.get("rejection",0),
            a.get("tasks_reviewed",0),
            a.get("rev_approved",0), a.get("rev_rejected",0),
            a.get("rev_rejection_rate",0),
            a.get("annotation_time_seconds",0), a.get("annotation_time_raw",""),
            a.get("review_time_seconds",0), a.get("review_time_raw",""),
            a.get("total_time_seconds",0), a.get("total_time_raw",""),
            a.get("tpt",0), a.get("tput",0),
            a.get("days",0), a.get("status",""),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=annotators_{data.get('date_filter','').replace('/','-')}.csv"}
    )


@app.get("/api/export/summary-csv")
def export_summary_csv(
    project_hash: str = Query(default=None),
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
):
    """
    Export project summary as CSV for Power BI.
    """
    data = get_project_data(
        project_hash=project_hash,
        start_date=start_date,
        end_date=end_date,
    )
    if isinstance(data, JSONResponse):
        return data

    import csv, io
    p = data.get("project", {})
    ps = data.get("project_summary", {})
    k = data.get("kpis", {})

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Metric", "Value"])
    rows = [
        ["Project_Name", p.get("name","")],
        ["Date_Range", data.get("date_filter","")],
        ["Progress_Pct", p.get("progress",0)],
        ["Tasks_Done", p.get("done",0)],
        ["Tasks_Total", p.get("total",0)],
        ["Rejection_Rate_Pct", p.get("rejection",0)],
        ["Total_Time", ps.get("total_time_all_collaborators","")],
        ["Annotation_Time", ps.get("total_time_annotating","")],
        ["Review_Time", ps.get("total_time_reviewing","")],
        ["Tasks_Submitted", ps.get("tasks_submitted",0)],
        ["Tasks_Approved", ps.get("tasks_approved",0)],
        ["Tasks_Rejected", ps.get("tasks_rejected",0)],
        ["Annotators_Active", k.get("total_annotators",0)],
        ["Reviewers_Active", k.get("total_reviewers",0)],
        ["Critical_Flags", k.get("critical_flags",0)],
        ["Warnings", k.get("warnings",0)],
    ]
    for row in rows:
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=project_summary_{data.get('date_filter','').replace('/','-')}.csv"}
    )


# ═══════════════════════════════════════════════════════════════════
# MULTI-CLIENT / MULTI-PROJECT ENDPOINTS (DYNAMIC)
# ═══════════════════════════════════════════════════════════════════

# Cache for projects list (refreshed every 10 minutes)
_projects_cache = {"data": None, "ts": 0}
PROJECTS_CACHE_TTL = 600  # 10 minutes


def get_all_projects_cached():
    """Fetch all projects from Encord, with caching."""
    global _projects_cache
    if _projects_cache["data"] and (time.time() - _projects_cache["ts"]) < PROJECTS_CACHE_TTL:
        return _projects_cache["data"]

    try:
        client = get_encord_client()
        projects = list(client.list_projects())
        _projects_cache = {"data": projects, "ts": time.time()}
        print(f"  ✓ Cached {len(projects)} projects from Encord")
        return projects
    except Exception as e:
        print(f"  ✗ Failed to fetch projects: {e}")
        return _projects_cache.get("data") or []


def extract_client_from_project(project):
    """
    Extract client name from project. Uses multiple strategies:
    1. Check if project title has a known pattern (e.g., "[CLIENT] Project Name")
    2. Use creator email domain
    """
    title = project.title or ""
    creator = project.creator_email or ""

    # Strategy 1: Check for [BRACKET] prefix in title
    import re
    bracket_match = re.match(r'^\[([^\]]+)\]', title)
    if bracket_match:
        return bracket_match.group(1).strip()

    # Strategy 2: Check for known prefixes in title
    known_prefixes = ["WOVEN", "DHL", "Morpheus", "CTC", "Forterra", "PrognosiX"]
    title_lower = title.lower()
    for prefix in known_prefixes:
        if prefix.lower() in title_lower:
            return prefix

    # Strategy 3: Use creator email domain
    if "@" in creator:
        domain = creator.split("@")[1].split(".")[0]
        # Clean up common domains
        domain_map = {
            "encord": "Encord Internal",
            "hoppr": "HOPPR",
            "gmail": "External",
            "outlook": "External",
            "hotmail": "External",
        }
        return domain_map.get(domain.lower(), domain.upper())

    return "Other"


@app.get("/api/clients")
def get_clients():
    """
    Dynamically fetch and group all projects by client.
    Returns client names and project counts.
    """
    projects = get_all_projects_cached()

    # Group projects by client
    clients_map = defaultdict(list)
    for p in projects:
        client_name = extract_client_from_project(p)
        clients_map[client_name].append(p)

    # Sort clients by project count (descending)
    clients = []
    for name, projs in sorted(clients_map.items(), key=lambda x: -len(x[1])):
        clients.append({
            "id": name.lower().replace(" ", "_"),
            "name": name,
            "project_count": len(projs),
        })

    return {
        "clients": clients,
        "total_projects": len(projects),
    }


@app.get("/api/clients/{client_id}/projects")
def get_client_projects(
    client_id: str,
    days: int = Query(default=7),
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
    quick: bool = Query(default=True, description="Quick mode: show basic info without fetching details"),
):
    """
    Get all projects for a specific client with their health status.
    Quick mode (default): Returns cached project info instantly.
    Full mode (quick=false): Fetches detailed data for each project (slower).
    """
    # Get all projects and filter by client
    all_projects = get_all_projects_cached()

    # Find projects belonging to this client
    client_name = None
    projects_for_client = []
    for p in all_projects:
        extracted_client = extract_client_from_project(p)
        extracted_id = extracted_client.lower().replace(" ", "_")
        if extracted_id == client_id:
            client_name = extracted_client
            projects_for_client.append(p)

    if not projects_for_client:
        return JSONResponse(status_code=404, content={"error": f"Client '{client_id}' not found or has no projects"})

    # Date range for label
    if start_date and end_date:
        date_label = f"{start_date} to {end_date}"
    else:
        dt_end = datetime.now(timezone.utc)
        dt_start = dt_end - timedelta(days=days)
        date_label = f"{dt_start.strftime('%d/%m/%Y')} to {dt_end.strftime('%d/%m/%Y')}"

    # QUICK MODE: Return cached project info instantly (no API calls)
    if quick:
        projects_data = []
        for p in projects_for_client:
            status_str = str(p.status).replace("ProjectStatus.", "")
            # Map status to health
            if status_str == "COMPLETED":
                health = "good"
            elif status_str == "IN_PROGRESS":
                health = "warn"  # Will be updated when user clicks
            else:
                health = "crit"

            projects_data.append({
                "hash": p.project_hash,
                "name": p.title,
                "modality": "Mixed",
                "status": health,
                "project_status": status_str,
                "progress": 0,  # Unknown without fetching
                "total_tasks": "—",
                "done_tasks": "—",
                "active_annotators": "—",
                "total_time_raw": "—",
                "created_at": str(p.created_at)[:10] if p.created_at else "",
                "quick_mode": True,
            })

        # Sort by created_at descending
        projects_data.sort(key=lambda x: x.get("created_at", ""), reverse=True)

        in_progress = len([p for p in projects_for_client if "IN_PROGRESS" in str(p.status)])
        completed = len([p for p in projects_for_client if "COMPLETED" in str(p.status)])

        return {
            "client": {"id": client_id, "name": client_name},
            "date_filter": date_label,
            "projects": projects_data,
            "summary": {
                "total_projects": len(projects_data),
                "total_tasks": "—",
                "done_tasks": "—",
                "overall_progress": "—",
                "total_time_raw": "—",
                "good_projects": completed,
                "warn_projects": in_progress,
                "crit_projects": len(projects_data) - in_progress - completed,
                "error_projects": 0,
            },
            "quick_mode": True,
            "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    # FULL MODE: Fetch detailed data (slower)
    # Convert to config format for compatibility
    projects_config = [
        {"hash": p.project_hash, "name": p.title, "modality": "Mixed"}
        for p in projects_for_client
    ]

    # Date range
    if start_date and end_date:
        try:
            dt_start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            dt_end = datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            )
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "Invalid date format"})
    else:
        dt_end = datetime.now(timezone.utc)
        dt_start = dt_end - timedelta(days=days)

    date_label = f"{dt_start.strftime('%d/%m/%Y')} to {dt_end.strftime('%d/%m/%Y')}"

    # Fetch project data in parallel
    def fetch_project_summary(proj_config):
        """Fetch quick summary for a single project."""
        ph = proj_config.get("hash")
        proj_name = proj_config.get("name", "Unknown")
        modality = proj_config.get("modality", "Mixed")

        try:
            enc_client = get_encord_client()
            project = enc_client.get_project(ph)

            # Quick fetch: just label_rows for progress and time_spent for activity
            with ThreadPoolExecutor(max_workers=2) as executor:
                fut_rows = executor.submit(lambda: list(project.list_label_rows_v2()))
                fut_time = executor.submit(lambda: list(project.list_time_spent(start=dt_start, end=dt_end)))

                label_rows = fut_rows.result()
                time_entries = fut_time.result()

            # Calculate progress
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

            # Calculate active annotators and total time
            unique_users = set()
            total_time_seconds = 0
            for entry in time_entries:
                email = getattr(entry, "user_email", None)
                if email:
                    unique_users.add(email)
                total_time_seconds += getattr(entry, "time_spent_seconds", 0) or 0

            # Quick health status based on progress and activity
            if total_tasks == 0:
                status = "warn"
            elif progress >= 50:
                status = "good"
            elif progress >= 20:
                status = "warn"
            else:
                status = "crit" if total_tasks > 10 else "warn"

            return {
                "hash": ph,
                "name": project.title,
                "modality": modality,
                "status": status,
                "progress": progress,
                "total_tasks": total_tasks,
                "done_tasks": done_tasks,
                "in_review": in_review,
                "in_annotation": in_annotation,
                "active_annotators": len(unique_users),
                "total_time_raw": format_seconds(total_time_seconds),
                "total_time_seconds": total_time_seconds,
            }
        except Exception as e:
            return {
                "hash": ph,
                "name": proj_name,
                "modality": modality,
                "status": "error",
                "error": str(e),
                "progress": 0,
                "total_tasks": 0,
                "done_tasks": 0,
                "active_annotators": 0,
                "total_time_raw": "0s",
            }

    # Fetch all projects in parallel (up to 5 at a time to avoid rate limits)
    t_start = time.time()
    print(f"  ⏳ Fetching {len(projects_config)} projects for client '{client_name}'...")

    projects_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_project_summary, p): p for p in projects_config}
        for future in as_completed(futures):
            try:
                result = future.result()
                projects_data.append(result)
            except Exception as e:
                p = futures[future]
                projects_data.append({
                    "hash": p.get("hash"),
                    "name": p.get("name", "Unknown"),
                    "status": "error",
                    "error": str(e),
                })

    # Sort: errors/crit first, then warn, then good
    status_order = {"error": 0, "crit": 1, "warn": 2, "good": 3}
    projects_data.sort(key=lambda x: (status_order.get(x.get("status"), 4), -x.get("progress", 0)))

    t_total = time.time() - t_start
    print(f"  ✓ Fetched {len(projects_data)} projects in {t_total:.1f}s")

    # Aggregate summary
    total_tasks_all = sum(p.get("total_tasks", 0) for p in projects_data)
    done_tasks_all = sum(p.get("done_tasks", 0) for p in projects_data)
    total_annotators = len(set(
        email for p in projects_data
        for email in []  # Would need full data for this
    ))
    total_time_all = sum(p.get("total_time_seconds", 0) for p in projects_data)

    crit_projects = len([p for p in projects_data if p.get("status") == "crit"])
    warn_projects = len([p for p in projects_data if p.get("status") == "warn"])
    good_projects = len([p for p in projects_data if p.get("status") == "good"])
    error_projects = len([p for p in projects_data if p.get("status") == "error"])

    return {
        "client": {
            "id": client_id,
            "name": client_name,
        },
        "date_filter": date_label,
        "projects": projects_data,
        "summary": {
            "total_projects": len(projects_data),
            "total_tasks": total_tasks_all,
            "done_tasks": done_tasks_all,
            "overall_progress": round((done_tasks_all / total_tasks_all * 100), 1) if total_tasks_all > 0 else 0,
            "total_time_raw": format_seconds(total_time_all),
            "crit_projects": crit_projects,
            "warn_projects": warn_projects,
            "good_projects": good_projects,
            "error_projects": error_projects,
        },
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@app.get("/api/overview")
def get_overview(
    days: int = Query(default=7),
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
):
    """
    Get high-level overview of all clients with project counts and health.
    Dynamically fetches from Encord.
    """
    # Use the same dynamic client fetching
    clients_response = get_clients()
    clients = clients_response.get("clients", [])

    if not clients:
        return {
            "clients": [],
            "total_projects": 0,
            "message": "No projects found in Encord"
        }

    # Date range
    if start_date and end_date:
        try:
            dt_start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            dt_end = datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            )
        except ValueError:
            dt_end = datetime.now(timezone.utc)
            dt_start = dt_end - timedelta(days=days)
    else:
        dt_end = datetime.now(timezone.utc)
        dt_start = dt_end - timedelta(days=days)

    date_label = f"{dt_start.strftime('%d/%m/%Y')} to {dt_end.strftime('%d/%m/%Y')}"

    # clients already has the right format from get_clients()
    total_projects = clients_response.get("total_projects", 0)

    return {
        "clients": clients,
        "total_projects": total_projects,
        "date_filter": date_label,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


@app.post("/api/clients/discover")
def discover_projects(limit: int = Query(default=50)):
    """
    Discover all accessible projects from Encord.
    Returns project list that can be added to clients.json.
    """
    try:
        client = get_encord_client()
        projects = list(client.list_projects())[:limit]

        return {
            "total_available": len(projects),
            "projects": [
                {
                    "hash": p.project_hash,
                    "name": p.title,
                    "status": str(p.status).replace("ProjectStatus.", ""),
                    "created_at": str(p.created_at),
                    "creator": p.creator_email,
                }
                for p in projects
            ],
            "message": f"Found {len(projects)} projects. Add them to clients.json to monitor."
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ═══════════════════════════════════════════════════════════════════
# V2 CACHE-FIRST API ENDPOINTS (Instant reads from SQLite)
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/v2/clients")
def get_clients_cached():
    """
    Get all clients with project counts — reads from ProjectCache.
    Response time: <50ms (no SDK calls).
    """
    from database import SessionLocal, ProjectCache
    db = SessionLocal()
    try:
        projects = db.query(ProjectCache).all()
        if not projects:
            return {"clients": [], "total_projects": 0, "message": "No cached data yet. Sync in progress..."}

        # Group by client_tag
        clients_map = defaultdict(list)
        for p in projects:
            tag = p.client_tag or "untagged"
            clients_map[tag].append(p)

        clients = []
        for name, projs in sorted(clients_map.items(), key=lambda x: -len(x[1])):
            healthy = sum(1 for p in projs if p.health_status == "green")
            warning = sum(1 for p in projs if p.health_status == "amber")
            critical = sum(1 for p in projs if p.health_status == "red")
            clients.append({
                "id": name.lower().replace(" ", "_"),
                "name": name,
                "project_count": len(projs),
                "healthy": healthy,
                "warning": warning,
                "critical": critical,
            })

        return {
            "clients": clients,
            "total_projects": len(projects),
            "source": "cache",
        }
    finally:
        db.close()


@app.get("/api/v2/projects")
def get_projects_cached(
    client: str = Query(default=None, description="Filter by client tag"),
    health: str = Query(default=None, description="Filter by health: green, amber, red"),
):
    """
    Get all projects with metrics — instant from cache.
    Supports filtering by client and health status.
    """
    from database import SessionLocal, ProjectCache
    db = SessionLocal()
    try:
        query = db.query(ProjectCache)
        if client:
            query = query.filter(ProjectCache.client_tag.ilike(f"%{client}%"))
        if health:
            query = query.filter(ProjectCache.health_status == health)

        projects = query.all()

        result = []
        for p in projects:
            result.append({
                "hash": p.project_hash,
                "name": p.title,
                "client": p.client_tag,
                "modality": p.modality,
                "status": p.project_status,
                "health": p.health_status,
                # Progress
                "total_tasks": p.total_tasks,
                "done_tasks": p.done_tasks,
                "in_review": p.in_review,
                "in_annotation": p.in_annotation,
                "progress": p.progress,
                # Metrics
                "active_annotators": p.active_annotators,
                "total_time": p.total_time_display,
                "total_time_seconds": p.total_time_seconds,
                "avg_rejection_rate": round(p.avg_rejection_rate * 100, 1),
                "avg_tpt_seconds": p.avg_tpt_seconds,
                "avg_throughput": p.avg_throughput,
                # Flags
                "red_flags": p.red_flags,
                "amber_flags": p.amber_flags,
                # Trends
                "rejection_rate_trend": p.rejection_rate_trend,
                "throughput_trend": p.throughput_trend,
                "tpt_trend": p.tpt_trend,
                # Meta
                "last_synced": p.last_synced.isoformat() if p.last_synced else None,
            })

        # Sort: critical first, then warning, then healthy
        order = {"red": 0, "amber": 1, "green": 2}
        result.sort(key=lambda x: (order.get(x["health"], 3), -x["progress"]))

        return {
            "projects": result,
            "total": len(result),
            "source": "cache",
        }
    finally:
        db.close()


@app.get("/api/v2/project/{project_hash}")
def get_project_detail_cached(project_hash: str):
    """
    Get detailed project data including annotator metrics and outliers.
    Reads from ProjectCache.cached_json.
    """
    import json as _json
    from database import SessionLocal, ProjectCache
    db = SessionLocal()
    try:
        p = db.query(ProjectCache).filter_by(project_hash=project_hash).first()
        if not p:
            return JSONResponse(status_code=404, content={"error": "Project not in cache. Wait for sync."})

        detail = _json.loads(p.cached_json or "{}")

        return {
            "project": {
                "hash": p.project_hash,
                "name": p.title,
                "health": p.health_status,
                "progress": p.progress,
                "total_tasks": p.total_tasks,
                "done_tasks": p.done_tasks,
                "total_time": p.total_time_display,
                "avg_rejection_rate": round(p.avg_rejection_rate * 100, 1),
                "red_flags": p.red_flags,
                "amber_flags": p.amber_flags,
            },
            "annotators": detail.get("annotators", {}),
            "outliers": detail.get("outliers", []),
            "last_synced": p.last_synced.isoformat() if p.last_synced else None,
            "source": "cache",
        }
    finally:
        db.close()


@app.post("/api/sync")
def trigger_sync():
    """Manually trigger a background sync."""
    from scheduler import trigger_manual_sync
    return trigger_manual_sync()


@app.get("/api/sync/status")
def get_sync_status_endpoint():
    """Get current sync status, last sync time, next scheduled run."""
    from scheduler import get_sync_status
    from config import settings as cfg
    status = get_sync_status()

    # Add stale data warning
    last_completed = status.get("last_completed")
    is_stale = False
    if last_completed:
        try:
            last_dt = datetime.fromisoformat(last_completed)
            age_minutes = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60
            is_stale = age_minutes > cfg.STALE_DATA_MINUTES
        except Exception:
            pass

    return {
        **status,
        "sync_interval_minutes": cfg.SYNC_INTERVAL_MINUTES,
        "is_stale": is_stale,
        "stale_threshold_minutes": cfg.STALE_DATA_MINUTES,
    }


@app.get("/api/summary")
def get_dashboard_summary():
    """Get the latest auto-generated dashboard summary and insights."""
    from database import SessionLocal, DashboardSummary
    import json as _json
    db = SessionLocal()
    try:
        latest = db.query(DashboardSummary).order_by(
            DashboardSummary.generated_at.desc()
        ).first()

        if not latest:
            return {
                "summary": "No data yet. Sync in progress...",
                "insights": [],
                "generated_at": None,
            }

        return {
            "summary": latest.summary_text,
            "insights": _json.loads(latest.insights_json or "[]"),
            "total_projects": latest.total_projects,
            "total_annotators": latest.total_annotators,
            "total_red_flags": latest.total_red_flags,
            "total_amber_flags": latest.total_amber_flags,
            "generated_at": latest.generated_at.isoformat() if latest.generated_at else None,
        }
    finally:
        db.close()


@app.get("/api/v2/outliers")
def get_all_outliers(
    level: str = Query(default=None, description="Filter by level: red, amber"),
    limit: int = Query(default=50, description="Max results"),
):
    """Get all outlier flags across projects."""
    from database import SessionLocal, OutlierFlag
    db = SessionLocal()
    try:
        query = db.query(OutlierFlag).order_by(OutlierFlag.flagged_at.desc())
        if level:
            query = query.filter(OutlierFlag.flag_level == level)
        flags = query.limit(limit).all()

        return {
            "outliers": [
                {
                    "project": f.project_title,
                    "project_hash": f.project_hash,
                    "annotator": f.annotator_email,
                    "metric": f.metric_type,
                    "value": f.actual_value,
                    "threshold": f.threshold_value,
                    "level": f.flag_level,
                    "description": f.description,
                    "flagged_at": f.flagged_at.isoformat() if f.flagged_at else None,
                }
                for f in flags
            ],
            "total": len(flags),
        }
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
