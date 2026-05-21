"""
Encord → BigQuery  |  Multi-Client Dashboard Sync
===================================================
Uses the Accelerate admin SSH key to pull all projects across
all client workspaces, computing the same metrics as the
1-single-project-view dashboard.

BigQuery Tables:
  project_health    — daily per-project KPIs + health status
  annotator_stats   — daily per-annotator metrics + outlier flags
  reviewer_stats    — daily per-reviewer metrics

Looker Studio reads these tables with a client_workspace filter
acting as the dropdown to select a client.

GitHub Actions Secrets:
  ENCORD_SSH_KEY   — Accelerate admin private key (covers all workspaces)
  GCP_SA_KEY       — GCP service account JSON key

GitHub Actions Variables:
  GCP_PROJECT      — GCP project ID
  BQ_DATASET       — BigQuery dataset (e.g. encord_accelerate)
"""

import os
import re
import datetime as dt
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from encord import EncordUserClient
from google.cloud import bigquery

# ─── Config ──────────────────────────────────────────────────────────────────
GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET  = os.environ.get("BQ_DATASET", "encord_accelerate")
DAYS_BACK   = int(os.environ.get("DAYS_BACK", "30"))          # time window for time/logs
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "5"))         # parallel project syncs

PROJECT_HEALTH_TABLE  = f"{GCP_PROJECT}.{BQ_DATASET}.project_health"
ANNOTATOR_STATS_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.annotator_stats"
REVIEWER_STATS_TABLE  = f"{GCP_PROJECT}.{BQ_DATASET}.reviewer_stats"

# ─── Schemas ─────────────────────────────────────────────────────────────────
PROJECT_HEALTH_SCHEMA = [
    bigquery.SchemaField("snapshot_date",          "DATE"),
    bigquery.SchemaField("client_workspace",        "STRING"),
    bigquery.SchemaField("creator_email",           "STRING",   mode="NULLABLE"),
    bigquery.SchemaField("project_hash",            "STRING"),
    bigquery.SchemaField("project_title",           "STRING"),
    bigquery.SchemaField("total_tasks",             "INTEGER"),
    bigquery.SchemaField("tasks_complete",          "INTEGER"),
    bigquery.SchemaField("tasks_in_review",         "INTEGER"),
    bigquery.SchemaField("tasks_in_annotation",     "INTEGER"),
    bigquery.SchemaField("tasks_skipped",           "INTEGER"),
    bigquery.SchemaField("progress_pct",            "FLOAT"),
    bigquery.SchemaField("project_rejection_rate",  "FLOAT",    mode="NULLABLE"),
    bigquery.SchemaField("total_approved",          "INTEGER"),
    bigquery.SchemaField("total_rejected",          "INTEGER"),
    bigquery.SchemaField("active_annotators",       "INTEGER"),
    bigquery.SchemaField("active_reviewers",        "INTEGER"),
    bigquery.SchemaField("total_annotation_secs",   "FLOAT"),
    bigquery.SchemaField("total_review_secs",       "FLOAT"),
    bigquery.SchemaField("avg_tpt_secs",            "FLOAT",    mode="NULLABLE"),
    bigquery.SchemaField("health_status",           "STRING"),   # Healthy / Warning / Critical
    bigquery.SchemaField("critical_flags",          "INTEGER"),
    bigquery.SchemaField("warning_flags",           "INTEGER"),
]

ANNOTATOR_STATS_SCHEMA = [
    bigquery.SchemaField("snapshot_date",          "DATE"),
    bigquery.SchemaField("client_workspace",        "STRING"),
    bigquery.SchemaField("project_hash",            "STRING"),
    bigquery.SchemaField("project_title",           "STRING"),
    bigquery.SchemaField("annotator_email",         "STRING"),
    bigquery.SchemaField("role",                    "STRING"),   # Annotator / Reviewer / Both
    bigquery.SchemaField("tasks_annotated",         "INTEGER"),
    bigquery.SchemaField("tasks_submitted",         "INTEGER"),
    bigquery.SchemaField("tasks_approved",          "INTEGER"),
    bigquery.SchemaField("tasks_rejected",          "INTEGER"),
    bigquery.SchemaField("rejection_rate",          "FLOAT",    mode="NULLABLE"),
    bigquery.SchemaField("annotation_time_secs",    "FLOAT"),
    bigquery.SchemaField("avg_tpt_secs",            "FLOAT",    mode="NULLABLE"),
    bigquery.SchemaField("days_active",             "INTEGER",  mode="NULLABLE"),
    bigquery.SchemaField("throughput_per_day",      "FLOAT",    mode="NULLABLE"),
    bigquery.SchemaField("flag_high_rejection",     "BOOLEAN"),
    bigquery.SchemaField("flag_too_fast",           "BOOLEAN"),
    bigquery.SchemaField("flag_too_slow",           "BOOLEAN"),
    bigquery.SchemaField("flag_low_throughput",     "BOOLEAN"),
    bigquery.SchemaField("health_status",           "STRING"),   # good / warn / crit
]

REVIEWER_STATS_SCHEMA = [
    bigquery.SchemaField("snapshot_date",          "DATE"),
    bigquery.SchemaField("client_workspace",        "STRING"),
    bigquery.SchemaField("project_hash",            "STRING"),
    bigquery.SchemaField("project_title",           "STRING"),
    bigquery.SchemaField("reviewer_email",          "STRING"),
    bigquery.SchemaField("tasks_reviewed",          "INTEGER"),
    bigquery.SchemaField("tasks_approved",          "INTEGER"),
    bigquery.SchemaField("tasks_rejected",          "INTEGER"),
    bigquery.SchemaField("rejection_rate",          "FLOAT",    mode="NULLABLE"),
    bigquery.SchemaField("review_time_secs",        "FLOAT"),
    bigquery.SchemaField("avg_review_tpt_secs",     "FLOAT",    mode="NULLABLE"),
]


# ─── Workspace Detection ──────────────────────────────────────────────────────
def get_workspace(creator_email: str) -> str:
    """Derive client workspace name from creator_email."""
    if not creator_email:
        return "Unknown"
    email = creator_email.lower().strip()
    # guest+<name>@encord.com pattern → client workspace
    m = re.match(r'guest\+(.+)@encord\.com', email)
    if m:
        name = m.group(1).replace('.', ' ').replace('-', ' ').replace('_', ' ')
        return name.title()
    if email.endswith('@encord.com'):
        return 'Encord (Internal)'
    domain = email.split('@')[-1]
    company = domain.split('.')[0]
    return company.replace('-', ' ').replace('_', ' ').title()


# ─── Metric Helpers ───────────────────────────────────────────────────────────
def get_workflow_stage(lr) -> str:
    node = getattr(lr, 'workflow_graph_node', None)
    if node:
        t = str(getattr(node, 'title', '') or '')
        st = str(getattr(node, 'stage_type', '') or '').upper()
        return t if t else st
    return str(getattr(lr, 'annotation_task_status', 'unknown') or 'unknown')


def classify_stage(stage: str):
    s = stage.upper()
    if any(w in s for w in ('COMPLETE', 'DONE', 'FINAL', 'ARCHIVE')):
        return 'complete'
    if 'REVIEW' in s:
        return 'review'
    if any(w in s for w in ('ANNOTATION', 'ANNOTATE')):
        return 'annotation'
    if 'SKIP' in s:
        return 'skipped'
    return 'other'


def median(values):
    if not values:
        return 0
    s = sorted(values)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


# ─── Per-Project Sync ─────────────────────────────────────────────────────────
def sync_project(project, snapshot_date: str, workspace: str):
    ph    = str(project.project_hash)
    title = str(project.title)
    creator = str(getattr(project, 'creator_email', '') or '')

    # ── 1. Label rows → task progress ────────────────────────────────────────
    label_rows = list(project.list_label_rows_v2())
    counts = {'complete': 0, 'review': 0, 'annotation': 0, 'skipped': 0, 'other': 0}
    for lr in label_rows:
        stage = get_workflow_stage(lr)
        counts[classify_stage(stage)] += 1

    total = len(label_rows)
    progress_pct = round(counts['complete'] / total * 100, 1) if total else 0.0

    # ── 2. Time entries ───────────────────────────────────────────────────────
    dt_end   = dt.datetime.now(dt.timezone.utc)
    dt_start = dt_end - dt.timedelta(days=DAYS_BACK)

    try:
        entries = list(project.list_time_spent(start=dt_start, end=dt_end))
    except Exception as ex:
        print(f"      ⚠ list_time_spent failed for {title}: {ex}")
        entries = []

    # Per-annotator: annotation_secs, dates, data_uuids
    ann_secs    = defaultdict(float)   # email → total annotation seconds
    rev_secs    = defaultdict(float)   # email → total review seconds
    ann_uuids   = defaultdict(set)     # email → set of data_uuids (annotation)
    rev_uuids   = defaultdict(set)     # email → set of data_uuids (review)
    ann_dates   = defaultdict(set)     # email → set of date strings
    rev_dates   = defaultdict(set)     # email → set of date strings

    for e in entries:
        wf     = getattr(e, 'workflow_stage', None)
        stype  = str(getattr(wf, 'stage_type', '') or '').upper() if wf else ''
        secs   = float(getattr(e, 'time_spent_seconds', 0) or 0)
        email  = str(getattr(e, 'user_email', '') or 'unknown')
        uid    = str(getattr(e, 'data_uuid', '') or '')
        ps     = getattr(e, 'period_start_time', None)
        day    = ps.date().isoformat() if ps and hasattr(ps, 'date') else snapshot_date

        if 'ANNOTATION' in stype:
            ann_secs[email]  += secs
            ann_dates[email].add(day)
            if uid:
                ann_uuids[email].add(uid)
        elif 'REVIEW' in stype:
            rev_secs[email]  += secs
            rev_dates[email].add(day)
            if uid:
                rev_uuids[email].add(uid)

    # ── 3. Label logs → rejections/approvals ─────────────────────────────────
    # data_hash → set of annotators who submitted it
    submitted_by  = defaultdict(set)   # data_hash → {email}
    approved_hashes = set()            # data_hashes that were approved (no reject ever)
    rejected_hashes = set()            # data_hashes that were rejected at any point
    reviewer_approved = defaultdict(set)  # reviewer_email → {data_hash}
    reviewer_rejected = defaultdict(set)  # reviewer_email → {data_hash}

    try:
        logs = list(project.get_label_logs())
    except Exception as ex:
        print(f"      ⚠ get_label_logs failed for {title}: {ex}")
        logs = []

    for log in logs:
        action  = str(getattr(log, 'action', '') or '').upper()
        email   = str(getattr(log, 'user_email', '') or 'unknown')
        dh      = str(getattr(log, 'data_hash', '') or '')

        if not dh:
            continue

        if 'SUBMIT' in action:
            submitted_by[dh].add(email)
        elif 'APPROVE' in action:
            reviewer_approved[email].add(dh)
            if dh not in rejected_hashes:
                approved_hashes.add(dh)
        elif 'REJECT' in action:
            reviewer_rejected[email].add(dh)
            rejected_hashes.add(dh)
            approved_hashes.discard(dh)   # "rejected wins" rule

    # Per-annotator submission outcomes
    ann_submitted = defaultdict(set)   # email → {data_hash submitted}
    ann_approved  = defaultdict(int)
    ann_rejected  = defaultdict(int)

    for dh, submitters in submitted_by.items():
        for email in submitters:
            ann_submitted[email].add(dh)
            if dh in rejected_hashes:
                ann_rejected[email] += 1
            elif dh in approved_hashes:
                ann_approved[email] += 1
            # else: pending — not counted

    # ── 4. Compute per-annotator stats ────────────────────────────────────────
    all_annotators = set(ann_secs.keys()) | set(ann_submitted.keys())
    annotator_rows = []
    annotator_rejection_rates = []
    annotator_tpts            = []
    annotator_throughputs     = []

    for email in all_annotators:
        tasks    = len(ann_uuids[email]) or len(ann_submitted[email])
        sub      = len(ann_submitted[email])
        app      = ann_approved[email]
        rej      = ann_rejected[email]
        rev_d    = (app + rej)
        rej_rate = round(rej / rev_d * 100, 2) if rev_d > 0 else None

        ann_t    = ann_secs[email]
        tpt      = round(ann_t / tasks, 2) if tasks > 0 else None

        dates    = ann_dates[email]
        if dates:
            min_d = min(dates)
            max_d = max(dates)
            days  = (dt.date.fromisoformat(max_d) - dt.date.fromisoformat(min_d)).days + 1
        else:
            days = None

        throughput = round(tasks / days, 2) if days and tasks else None

        has_ann = ann_t > 0
        has_rev = rev_secs.get(email, 0) > 0
        if has_ann and has_rev:
            role = 'Annotator & Reviewer'
        elif has_rev:
            role = 'Reviewer'
        else:
            role = 'Annotator'

        if rej_rate is not None:
            annotator_rejection_rates.append(rej_rate)
        if tpt is not None:
            annotator_tpts.append(tpt)
        if throughput is not None:
            annotator_throughputs.append(throughput)

        annotator_rows.append({
            'email': email, 'role': role,
            'tasks': tasks, 'sub': sub, 'app': app, 'rej': rej,
            'rej_rate': rej_rate, 'ann_t': ann_t, 'tpt': tpt,
            'days': days, 'throughput': throughput,
        })

    # ── 5. Compute outlier flags (needs all annotators' values first) ─────────
    proj_avg_rej   = (sum(annotator_rejection_rates) / len(annotator_rejection_rates)
                      if annotator_rejection_rates else 0)
    med_tpt        = median(annotator_tpts)
    med_throughput = median(annotator_throughputs)

    annotator_bq_rows = []
    critical_flags = 0
    warning_flags  = 0

    for a in annotator_rows:
        flag_high_rej  = bool(a['rej_rate'] is not None and
                              a['rej_rate'] > proj_avg_rej + 10)
        flag_too_fast  = bool(a['tpt'] is not None and med_tpt > 0 and
                              a['tpt'] < med_tpt * 0.2)
        flag_too_slow  = bool(a['tpt'] is not None and med_tpt > 0 and
                              a['tpt'] > med_tpt * 1.5)
        flag_low_tp    = bool(a['throughput'] is not None and med_throughput > 0 and
                              a['throughput'] < med_throughput * 0.8)

        if flag_high_rej:
            critical_flags += 1
        if flag_too_fast or flag_too_slow or flag_low_tp:
            warning_flags += 1

        status = ('crit' if flag_high_rej else
                  'warn' if (flag_too_fast or flag_too_slow or flag_low_tp) else 'good')

        annotator_bq_rows.append({
            'snapshot_date':       snapshot_date,
            'client_workspace':    workspace,
            'project_hash':        ph,
            'project_title':       title,
            'annotator_email':     a['email'],
            'role':                a['role'],
            'tasks_annotated':     a['tasks'],
            'tasks_submitted':     a['sub'],
            'tasks_approved':      a['app'],
            'tasks_rejected':      a['rej'],
            'rejection_rate':      a['rej_rate'],
            'annotation_time_secs': round(a['ann_t'], 2),
            'avg_tpt_secs':        a['tpt'],
            'days_active':         a['days'],
            'throughput_per_day':  a['throughput'],
            'flag_high_rejection': flag_high_rej,
            'flag_too_fast':       flag_too_fast,
            'flag_too_slow':       flag_too_slow,
            'flag_low_throughput': flag_low_tp,
            'health_status':       status,
        })

    # ── 6. Per-reviewer stats ─────────────────────────────────────────────────
    reviewer_bq_rows = []
    for rev_email in set(rev_secs.keys()) | set(reviewer_approved.keys()) | set(reviewer_rejected.keys()):
        r_app   = len(reviewer_approved.get(rev_email, set()))
        r_rej   = len(reviewer_rejected.get(rev_email, set()))
        r_total = r_app + r_rej
        r_rate  = round(r_rej / r_total * 100, 2) if r_total > 0 else None
        r_time  = rev_secs.get(rev_email, 0)
        r_tasks = len(rev_uuids.get(rev_email, set()))
        r_tpt   = round(r_time / r_tasks, 2) if r_tasks > 0 else None

        reviewer_bq_rows.append({
            'snapshot_date':       snapshot_date,
            'client_workspace':    workspace,
            'project_hash':        ph,
            'project_title':       title,
            'reviewer_email':      rev_email,
            'tasks_reviewed':      r_tasks,
            'tasks_approved':      r_app,
            'tasks_rejected':      r_rej,
            'rejection_rate':      r_rate,
            'review_time_secs':    round(r_time, 2),
            'avg_review_tpt_secs': r_tpt,
        })

    # ── 7. Project health row ─────────────────────────────────────────────────
    total_approved_proj = len(approved_hashes)
    total_rejected_proj = len(rejected_hashes)
    denom = total_approved_proj + total_rejected_proj
    proj_rej_rate = round(total_rejected_proj / denom * 100, 2) if denom > 0 else None

    if proj_rej_rate is None:
        health = 'Healthy'
    elif proj_rej_rate > 15:
        health = 'Critical'
    elif proj_rej_rate > 10:
        health = 'Warning'
    else:
        health = 'Healthy'

    total_ann_secs = sum(ann_secs.values())
    total_rev_secs = sum(rev_secs.values())
    total_tasks_cnt = sum(len(v) for v in ann_uuids.values())
    avg_tpt = round(total_ann_secs / total_tasks_cnt, 2) if total_tasks_cnt > 0 else None

    project_row = {
        'snapshot_date':         snapshot_date,
        'client_workspace':      workspace,
        'creator_email':         creator,
        'project_hash':          ph,
        'project_title':         title,
        'total_tasks':           total,
        'tasks_complete':        counts['complete'],
        'tasks_in_review':       counts['review'],
        'tasks_in_annotation':   counts['annotation'],
        'tasks_skipped':         counts['skipped'],
        'progress_pct':          progress_pct,
        'project_rejection_rate': proj_rej_rate,
        'total_approved':        total_approved_proj,
        'total_rejected':        total_rejected_proj,
        'active_annotators':     len(all_annotators),
        'active_reviewers':      len(set(rev_secs.keys())),
        'total_annotation_secs': round(total_ann_secs, 2),
        'total_review_secs':     round(total_rev_secs, 2),
        'avg_tpt_secs':          avg_tpt,
        'health_status':         health,
        'critical_flags':        critical_flags,
        'warning_flags':         warning_flags,
    }

    print(f"      ✓ {title[:50]:<50} | {health:<8} | "
          f"{counts['complete']}/{total} done | "
          f"{len(all_annotators)} annotators | "
          f"rej {proj_rej_rate}%")

    return project_row, annotator_bq_rows, reviewer_bq_rows


# ─── BigQuery Helpers ─────────────────────────────────────────────────────────
def bq_delete_today(bq, snapshot_date):
    for table in [PROJECT_HEALTH_TABLE, ANNOTATOR_STATS_TABLE, REVIEWER_STATS_TABLE]:
        try:
            bq.query(f"DELETE FROM `{table}` WHERE snapshot_date = '{snapshot_date}'").result()
        except Exception:
            pass  # table may not exist yet


def bq_insert(bq, rows, table, schema):
    if not rows:
        return
    job = bq.load_table_from_json(
        rows, table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=schema)
    )
    job.result()
    print(f"    ✓ {len(rows):>5} rows → {table.split('.')[-1]}")


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    snapshot_date = dt.date.today().isoformat()
    print(f"\n{'='*70}")
    print(f"  Encord Multi-Client Sync  |  {snapshot_date}")
    print(f"  Time window: last {DAYS_BACK} days")
    print(f"{'='*70}\n")

    # Connect
    key_path = os.environ.get("ENCORD_SSH_KEY_PATH")
    key_str  = os.environ.get("ENCORD_SSH_KEY")
    domain   = os.environ.get("ENCORD_DOMAIN", "https://api.encord.com")

    if key_path and os.path.exists(key_path):
        user_client = EncordUserClient.create_with_ssh_private_key(
            ssh_private_key_path=key_path, domain=domain)
    elif key_str:
        user_client = EncordUserClient.create_with_ssh_private_key(
            ssh_private_key=key_str, domain=domain)
    else:
        raise ValueError("Set ENCORD_SSH_KEY_PATH or ENCORD_SSH_KEY")

    bq = bigquery.Client(project=GCP_PROJECT)
    bq.create_dataset(bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}"), exists_ok=True)

    # Discover all projects
    print("► Discovering all projects from accelerate key...")
    all_projects = list(user_client.list_projects())
    print(f"  Found {len(all_projects)} projects\n")

    # Clear today's snapshot (idempotent)
    bq_delete_today(bq, snapshot_date)

    # Sync each project
    all_project_rows   = []
    all_annotator_rows = []
    all_reviewer_rows  = []

    print("► Syncing projects...\n")
    for i, p_meta in enumerate(all_projects, 1):
        try:
            ph = (p_meta.get('project', {}).get('project_hash', '')
                  if isinstance(p_meta, dict) else str(getattr(p_meta, 'project_hash', '')))
            project   = user_client.get_project(ph)
            creator   = str(getattr(project, 'creator_email', '') or '')
            workspace = get_workspace(creator)

            print(f"  [{i}/{len(all_projects)}] {workspace} / {project.title[:45]}")

            p_row, ann_rows, rev_rows = sync_project(project, snapshot_date, workspace)
            all_project_rows.append(p_row)
            all_annotator_rows.extend(ann_rows)
            all_reviewer_rows.extend(rev_rows)

            # Batch write every 50 projects to avoid memory build-up
            if i % 50 == 0:
                print(f"\n  ── Flushing batch to BigQuery ──")
                bq_insert(bq, all_project_rows,   PROJECT_HEALTH_TABLE,  PROJECT_HEALTH_SCHEMA)
                bq_insert(bq, all_annotator_rows, ANNOTATOR_STATS_TABLE, ANNOTATOR_STATS_SCHEMA)
                bq_insert(bq, all_reviewer_rows,  REVIEWER_STATS_TABLE,  REVIEWER_STATS_SCHEMA)
                all_project_rows   = []
                all_annotator_rows = []
                all_reviewer_rows  = []
                print()

        except Exception as ex:
            import traceback
            print(f"    ✗ Skipping: {ex}")
            traceback.print_exc()

    # Final write
    print(f"\n► Final write to BigQuery...")
    bq_insert(bq, all_project_rows,   PROJECT_HEALTH_TABLE,  PROJECT_HEALTH_SCHEMA)
    bq_insert(bq, all_annotator_rows, ANNOTATOR_STATS_TABLE, ANNOTATOR_STATS_SCHEMA)
    bq_insert(bq, all_reviewer_rows,  REVIEWER_STATS_TABLE,  REVIEWER_STATS_SCHEMA)

    print(f"\n{'='*70}")
    print(f"  ✅ Done!  {dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
