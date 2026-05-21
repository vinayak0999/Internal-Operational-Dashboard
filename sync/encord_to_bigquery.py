"""
Encord → BigQuery Sync
======================
SSH Key → Fetch all projects (all 75 client workspaces) → Push to BigQuery
Looker Studio reads from BigQuery tables.

BigQuery Tables Created:
  project_health    — one row per project per day (health, progress, rejection rate)
  annotator_stats   — one row per annotator per project per day (all flags)
  reviewer_stats    — one row per reviewer per project per day

GitHub Actions Secrets required:
  ENCORD_SSH_KEY   — Accelerate admin private key (covers all workspaces)
  GCP_SA_KEY       — GCP service account JSON

GitHub Actions Variables required:
  GCP_PROJECT      — GCP project ID  (e.g. my-gcp-project)
  BQ_DATASET       — BigQuery dataset (e.g. encord_dashboard)
"""

import os
import re
import datetime as dt
import traceback
from collections import defaultdict

from encord import EncordUserClient
from google.cloud import bigquery

# ─── Config ──────────────────────────────────────────────────────────────────
GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET  = os.environ.get("BQ_DATASET", "encord_dashboard")
DAYS_BACK   = int(os.environ.get("DAYS_BACK", "30"))

TABLE_PROJECT   = f"{GCP_PROJECT}.{BQ_DATASET}.project_health"
TABLE_ANNOTATOR = f"{GCP_PROJECT}.{BQ_DATASET}.annotator_stats"
TABLE_REVIEWER  = f"{GCP_PROJECT}.{BQ_DATASET}.reviewer_stats"

# ─── BigQuery Schemas ─────────────────────────────────────────────────────────
SCHEMA_PROJECT = [
    bigquery.SchemaField("snapshot_date",          "DATE"),
    bigquery.SchemaField("client_workspace",        "STRING"),   # ← Looker dropdown
    bigquery.SchemaField("project_hash",            "STRING"),
    bigquery.SchemaField("project_title",           "STRING"),
    bigquery.SchemaField("creator_email",           "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("total_tasks",             "INTEGER"),
    bigquery.SchemaField("tasks_complete",          "INTEGER"),
    bigquery.SchemaField("tasks_in_review",         "INTEGER"),
    bigquery.SchemaField("tasks_in_annotation",     "INTEGER"),
    bigquery.SchemaField("progress_pct",            "FLOAT"),
    bigquery.SchemaField("project_rejection_rate",  "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("total_approved",          "INTEGER"),
    bigquery.SchemaField("total_rejected",          "INTEGER"),
    bigquery.SchemaField("active_annotators",       "INTEGER"),
    bigquery.SchemaField("active_reviewers",        "INTEGER"),
    bigquery.SchemaField("total_annotation_secs",   "FLOAT"),
    bigquery.SchemaField("total_review_secs",       "FLOAT"),
    bigquery.SchemaField("avg_tpt_secs",            "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("health_status",           "STRING"),  # Healthy / Warning / Critical
    bigquery.SchemaField("critical_flags",          "INTEGER"),
    bigquery.SchemaField("warning_flags",           "INTEGER"),
]

SCHEMA_ANNOTATOR = [
    bigquery.SchemaField("snapshot_date",          "DATE"),
    bigquery.SchemaField("client_workspace",        "STRING"),
    bigquery.SchemaField("project_hash",            "STRING"),
    bigquery.SchemaField("project_title",           "STRING"),
    bigquery.SchemaField("annotator_email",         "STRING"),
    bigquery.SchemaField("role",                    "STRING"),  # Annotator / Reviewer / Both
    bigquery.SchemaField("tasks_annotated",         "INTEGER"),
    bigquery.SchemaField("tasks_submitted",         "INTEGER"),
    bigquery.SchemaField("tasks_approved",          "INTEGER"),
    bigquery.SchemaField("tasks_rejected",          "INTEGER"),
    bigquery.SchemaField("rejection_rate",          "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("annotation_time_secs",    "FLOAT"),
    bigquery.SchemaField("avg_tpt_secs",            "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("days_active",             "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("throughput_per_day",      "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("flag_high_rejection",     "BOOLEAN"),
    bigquery.SchemaField("flag_too_fast",           "BOOLEAN"),
    bigquery.SchemaField("flag_too_slow",           "BOOLEAN"),
    bigquery.SchemaField("flag_low_throughput",     "BOOLEAN"),
    bigquery.SchemaField("health_status",           "STRING"),  # good / warn / crit
]

SCHEMA_REVIEWER = [
    bigquery.SchemaField("snapshot_date",          "DATE"),
    bigquery.SchemaField("client_workspace",        "STRING"),
    bigquery.SchemaField("project_hash",            "STRING"),
    bigquery.SchemaField("project_title",           "STRING"),
    bigquery.SchemaField("reviewer_email",          "STRING"),
    bigquery.SchemaField("tasks_reviewed",          "INTEGER"),
    bigquery.SchemaField("tasks_approved",          "INTEGER"),
    bigquery.SchemaField("tasks_rejected",          "INTEGER"),
    bigquery.SchemaField("rejection_rate",          "FLOAT",   mode="NULLABLE"),
    bigquery.SchemaField("review_time_secs",        "FLOAT"),
    bigquery.SchemaField("avg_review_tpt_secs",     "FLOAT",   mode="NULLABLE"),
]


# ─── Workspace Detection ──────────────────────────────────────────────────────
def get_workspace(creator_email: str) -> str:
    """
    Derive client workspace from creator_email.
      guest+wayve@encord.com      → Wayve
      guest+boston-dynamics@encord.com → Boston Dynamics
      someone@encord.com          → Encord (Internal)
      someone@clientdomain.com    → Clientdomain
    """
    if not creator_email:
        return "Unknown"
    email = creator_email.lower().strip()
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
def classify_stage(node_title: str, stage_type: str) -> str:
    t = (node_title or '').upper()
    s = (stage_type or '').upper()
    if any(w in t for w in ('COMPLETE', 'DONE', 'FINAL', 'ARCHIVE')):
        return 'complete'
    if 'REVIEW' in s or 'REVIEW' in t:
        return 'review'
    if 'ANNOTATION' in s or 'ANNOTAT' in t:
        return 'annotation'
    return 'other'


def median(values: list) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


# ─── Per-Project Processing ───────────────────────────────────────────────────
def process_project(project, snapshot_date: str, workspace: str):
    ph    = str(project.project_hash)
    title = str(project.title)
    creator = str(getattr(project, 'creator_email', '') or '')

    print(f"    Processing: {title[:55]}")

    # ── 1. Task Progress (label rows) ─────────────────────────────────────────
    label_rows = list(project.list_label_rows_v2())
    stage_counts = defaultdict(int)
    for lr in label_rows:
        wf = getattr(lr, 'workflow_graph_node', None)
        nt = str(getattr(wf, 'title', '') or '') if wf else ''
        st = str(getattr(wf, 'stage_type', '') or '') if wf else ''
        stage_counts[classify_stage(nt, st)] += 1

    total = len(label_rows)
    progress_pct = round(stage_counts['complete'] / total * 100, 1) if total else 0.0

    # ── 2. Time Entries ───────────────────────────────────────────────────────
    dt_end   = dt.datetime.now(dt.timezone.utc)
    dt_start = dt_end - dt.timedelta(days=DAYS_BACK)

    try:
        entries = list(project.list_time_spent(start=dt_start, end=dt_end))
    except Exception as ex:
        print(f"      ⚠ list_time_spent: {ex}")
        entries = []

    ann_secs  = defaultdict(float)   # email → annotation seconds
    rev_secs  = defaultdict(float)   # email → review seconds
    ann_uuids = defaultdict(set)     # email → unique data_uuids annotated
    rev_uuids = defaultdict(set)     # email → unique data_uuids reviewed
    ann_dates = defaultdict(set)     # email → dates active

    for e in entries:
        wf    = getattr(e, 'workflow_stage', None)
        stype = str(getattr(wf, 'stage_type', '') or '').upper() if wf else ''
        secs  = float(getattr(e, 'time_spent_seconds', 0) or 0)
        email = str(getattr(e, 'user_email', '') or 'unknown')
        uid   = str(getattr(e, 'data_uuid', '') or '')
        ps    = getattr(e, 'period_start_time', None)
        day   = ps.date().isoformat() if ps and hasattr(ps, 'date') else snapshot_date

        if 'ANNOTATION' in stype:
            ann_secs[email]  += secs
            ann_dates[email].add(day)
            if uid:
                ann_uuids[email].add(uid)
        elif 'REVIEW' in stype:
            rev_secs[email] += secs
            if uid:
                rev_uuids[email].add(uid)

    # ── 3. Label Logs → Rejections / Approvals ────────────────────────────────
    submitted_by      = defaultdict(set)   # data_hash → {annotator emails}
    approved_hashes   = set()
    rejected_hashes   = set()
    reviewer_approved = defaultdict(set)   # reviewer → {data_hashes}
    reviewer_rejected = defaultdict(set)

    try:
        logs = list(project.get_editor_logs())
    except Exception:
        try:
            logs = list(project.get_label_logs())   # fallback for older SDK
        except Exception as ex:
            print(f"      ⚠ get_editor_logs: {ex}")
            logs = []

    for log in logs:
        action = str(getattr(log, 'action', '') or '').upper()
        email  = str(getattr(log, 'user_email', '') or 'unknown')
        dh     = str(getattr(log, 'data_hash', '') or '')
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

    # Per-annotator submit outcomes
    ann_submitted = defaultdict(set)
    ann_approved  = defaultdict(int)
    ann_rejected  = defaultdict(int)

    for dh, submitters in submitted_by.items():
        for email in submitters:
            ann_submitted[email].add(dh)
            if dh in rejected_hashes:
                ann_rejected[email] += 1
            elif dh in approved_hashes:
                ann_approved[email] += 1

    # ── 4. Per-Annotator Rows ─────────────────────────────────────────────────
    all_annotators = set(ann_secs.keys()) | set(ann_submitted.keys())
    ann_rows_raw   = []
    rej_rates, tpts, throughputs = [], [], []

    for email in all_annotators:
        tasks     = len(ann_uuids[email]) or len(ann_submitted[email])
        sub       = len(ann_submitted[email])
        app       = ann_approved[email]
        rej       = ann_rejected[email]
        denom     = app + rej
        rej_rate  = round(rej / denom * 100, 2) if denom > 0 else None

        ann_t     = ann_secs[email]
        tpt       = round(ann_t / tasks, 2) if tasks > 0 else None

        dates     = ann_dates[email]
        if dates:
            min_d = min(dates)
            max_d = max(dates)
            days  = (dt.date.fromisoformat(max_d) - dt.date.fromisoformat(min_d)).days + 1
        else:
            days = None

        throughput = round(tasks / days, 2) if days and tasks else None

        has_ann = ann_t > 0
        has_rev = rev_secs.get(email, 0) > 0
        role    = ('Annotator & Reviewer' if has_ann and has_rev
                   else 'Reviewer' if has_rev else 'Annotator')

        if rej_rate is not None:
            rej_rates.append(rej_rate)
        if tpt is not None:
            tpts.append(tpt)
        if throughput is not None:
            throughputs.append(throughput)

        ann_rows_raw.append(dict(
            email=email, role=role,
            tasks=tasks, sub=sub, app=app, rej=rej,
            rej_rate=rej_rate, ann_t=ann_t, tpt=tpt,
            days=days, throughput=throughput,
        ))

    # ── 5. Outlier Flags ──────────────────────────────────────────────────────
    proj_avg_rej   = sum(rej_rates) / len(rej_rates) if rej_rates else 0
    med_tpt        = median(tpts)
    med_throughput = median(throughputs)

    annotator_bq_rows = []
    critical_flags = 0
    warning_flags  = 0

    for a in ann_rows_raw:
        flag_high_rej = bool(a['rej_rate'] is not None and a['rej_rate'] > proj_avg_rej + 10)
        flag_too_fast = bool(a['tpt'] is not None and med_tpt > 0 and a['tpt'] < med_tpt * 0.2)
        flag_too_slow = bool(a['tpt'] is not None and med_tpt > 0 and a['tpt'] > med_tpt * 1.5)
        flag_low_tp   = bool(a['throughput'] is not None and med_throughput > 0
                             and a['throughput'] < med_throughput * 0.8)

        if flag_high_rej:
            critical_flags += 1
        if flag_too_fast or flag_too_slow or flag_low_tp:
            warning_flags += 1

        status = ('crit' if flag_high_rej
                  else 'warn' if (flag_too_fast or flag_too_slow or flag_low_tp)
                  else 'good')

        annotator_bq_rows.append({
            'snapshot_date':        snapshot_date,
            'client_workspace':     workspace,
            'project_hash':         ph,
            'project_title':        title,
            'annotator_email':      a['email'],
            'role':                 a['role'],
            'tasks_annotated':      a['tasks'],
            'tasks_submitted':      a['sub'],
            'tasks_approved':       a['app'],
            'tasks_rejected':       a['rej'],
            'rejection_rate':       a['rej_rate'],
            'annotation_time_secs': round(a['ann_t'], 2),
            'avg_tpt_secs':         a['tpt'],
            'days_active':          a['days'],
            'throughput_per_day':   a['throughput'],
            'flag_high_rejection':  flag_high_rej,
            'flag_too_fast':        flag_too_fast,
            'flag_too_slow':        flag_too_slow,
            'flag_low_throughput':  flag_low_tp,
            'health_status':        status,
        })

    # ── 6. Per-Reviewer Rows ──────────────────────────────────────────────────
    reviewer_bq_rows = []
    all_reviewers = (set(rev_secs.keys())
                     | set(reviewer_approved.keys())
                     | set(reviewer_rejected.keys()))

    for rev in all_reviewers:
        r_app   = len(reviewer_approved.get(rev, set()))
        r_rej   = len(reviewer_rejected.get(rev, set()))
        r_total = r_app + r_rej
        r_rate  = round(r_rej / r_total * 100, 2) if r_total > 0 else None
        r_time  = rev_secs.get(rev, 0.0)
        r_tasks = len(rev_uuids.get(rev, set()))
        r_tpt   = round(r_time / r_tasks, 2) if r_tasks > 0 else None

        reviewer_bq_rows.append({
            'snapshot_date':       snapshot_date,
            'client_workspace':    workspace,
            'project_hash':        ph,
            'project_title':       title,
            'reviewer_email':      rev,
            'tasks_reviewed':      r_tasks,
            'tasks_approved':      r_app,
            'tasks_rejected':      r_rej,
            'rejection_rate':      r_rate,
            'review_time_secs':    round(r_time, 2),
            'avg_review_tpt_secs': r_tpt,
        })

    # ── 7. Project Health Row ─────────────────────────────────────────────────
    total_app  = len(approved_hashes)
    total_rej  = len(rejected_hashes)
    denom      = total_app + total_rej
    proj_rej   = round(total_rej / denom * 100, 2) if denom > 0 else None

    health = ('Critical' if proj_rej is not None and proj_rej > 15
              else 'Warning' if proj_rej is not None and proj_rej > 10
              else 'Healthy')

    total_ann_secs  = sum(ann_secs.values())
    total_rev_secs  = sum(rev_secs.values())
    total_tasks_cnt = sum(len(v) for v in ann_uuids.values())
    avg_tpt         = round(total_ann_secs / total_tasks_cnt, 2) if total_tasks_cnt > 0 else None

    project_row = {
        'snapshot_date':         snapshot_date,
        'client_workspace':      workspace,
        'project_hash':          ph,
        'project_title':         title,
        'creator_email':         creator,
        'total_tasks':           total,
        'tasks_complete':        stage_counts['complete'],
        'tasks_in_review':       stage_counts['review'],
        'tasks_in_annotation':   stage_counts['annotation'],
        'progress_pct':          progress_pct,
        'project_rejection_rate': proj_rej,
        'total_approved':        total_app,
        'total_rejected':        total_rej,
        'active_annotators':     len(all_annotators),
        'active_reviewers':      len(all_reviewers),
        'total_annotation_secs': round(total_ann_secs, 2),
        'total_review_secs':     round(total_rev_secs, 2),
        'avg_tpt_secs':          avg_tpt,
        'health_status':         health,
        'critical_flags':        critical_flags,
        'warning_flags':         warning_flags,
    }

    print(f"      ✓ {health:<8} | {stage_counts['complete']}/{total} done "
          f"| rej {proj_rej}% | {len(all_annotators)} annotators")

    return project_row, annotator_bq_rows, reviewer_bq_rows


# ─── BigQuery Helpers ─────────────────────────────────────────────────────────
def ensure_dataset(bq: bigquery.Client):
    ds = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    ds.location = "US"
    bq.create_dataset(ds, exists_ok=True)


def delete_today(bq: bigquery.Client, snapshot_date: str):
    """Delete today's rows before re-inserting (idempotent re-runs)."""
    for table in [TABLE_PROJECT, TABLE_ANNOTATOR, TABLE_REVIEWER]:
        try:
            bq.query(
                f"DELETE FROM `{table}` WHERE snapshot_date = '{snapshot_date}'"
            ).result()
        except Exception:
            pass  # Table may not exist yet on first run


def bq_insert(bq: bigquery.Client, rows: list, table: str, schema: list):
    if not rows:
        return
    job = bq.load_table_from_json(
        rows, table,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=schema,
        )
    )
    job.result()
    print(f"    ✓ Wrote {len(rows):>5} rows → {table.split('.')[-1]}")


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    snapshot_date = dt.date.today().isoformat()

    print(f"\n{'═'*65}")
    print(f"  Encord → BigQuery Sync  |  {snapshot_date}")
    print(f"  Time window : last {DAYS_BACK} days")
    print(f"  Dataset     : {GCP_PROJECT}.{BQ_DATASET}")
    print(f"{'═'*65}\n")

    # ── Connect to Encord ─────────────────────────────────────────────────────
    key_path = os.environ.get("ENCORD_SSH_KEY_PATH", "")
    key_str  = os.environ.get("ENCORD_SSH_KEY", "")
    domain   = os.environ.get("ENCORD_DOMAIN", "https://api.encord.com")

    if key_path and os.path.exists(key_path):
        client = EncordUserClient.create_with_ssh_private_key(
            ssh_private_key_path=key_path, domain=domain)
    elif key_str:
        client = EncordUserClient.create_with_ssh_private_key(
            ssh_private_key=key_str, domain=domain)
    else:
        raise ValueError("Set ENCORD_SSH_KEY_PATH or ENCORD_SSH_KEY env variable")

    # ── Connect to BigQuery ───────────────────────────────────────────────────
    bq = bigquery.Client(project=GCP_PROJECT)
    ensure_dataset(bq)
    delete_today(bq, snapshot_date)

    # ── Discover all projects ─────────────────────────────────────────────────
    print("► Discovering projects from SSH key...")
    all_projects = list(client.list_projects())
    print(f"  Found {len(all_projects)} projects\n")

    # ── Process each project ──────────────────────────────────────────────────
    proj_rows = []
    ann_rows  = []
    rev_rows  = []

    print("► Processing projects...\n")
    for i, p_meta in enumerate(all_projects, 1):
        try:
            # Get project hash from whatever format list_projects() returns
            if isinstance(p_meta, dict):
                ph = (p_meta.get('project', {}).get('project_hash')
                      or p_meta.get('project_hash', ''))
            else:
                ph = str(getattr(p_meta, 'project_hash', ''))

            if not ph:
                continue

            project   = client.get_project(ph)
            creator   = str(getattr(project, 'creator_email', '') or '')
            workspace = get_workspace(creator)

            print(f"  [{i:>4}/{len(all_projects)}] {workspace:<30} {project.title[:40]}")

            p_row, a_rows, r_rows = process_project(project, snapshot_date, workspace)
            proj_rows.append(p_row)
            ann_rows.extend(a_rows)
            rev_rows.extend(r_rows)

            # Flush to BigQuery every 100 projects to avoid memory build-up
            if i % 100 == 0:
                print(f"\n  ── Flushing batch {i} to BigQuery ──")
                bq_insert(bq, proj_rows, TABLE_PROJECT,   SCHEMA_PROJECT)
                bq_insert(bq, ann_rows,  TABLE_ANNOTATOR, SCHEMA_ANNOTATOR)
                bq_insert(bq, rev_rows,  TABLE_REVIEWER,  SCHEMA_REVIEWER)
                proj_rows, ann_rows, rev_rows = [], [], []
                print()

        except KeyboardInterrupt:
            break
        except Exception as ex:
            print(f"    ✗ Skipping [{i}]: {ex}")
            traceback.print_exc()

    # Final flush
    print("\n► Writing final batch to BigQuery...")
    bq_insert(bq, proj_rows, TABLE_PROJECT,   SCHEMA_PROJECT)
    bq_insert(bq, ann_rows,  TABLE_ANNOTATOR, SCHEMA_ANNOTATOR)
    bq_insert(bq, rev_rows,  TABLE_REVIEWER,  SCHEMA_REVIEWER)

    print(f"\n{'═'*65}")
    print(f"  ✅  Sync complete  |  {dt.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"{'═'*65}\n")


if __name__ == "__main__":
    main()
