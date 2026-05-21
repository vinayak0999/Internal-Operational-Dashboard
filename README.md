# Internal Operational Dashboard

**Encord → BigQuery → Looker Studio**

Pulls data from all 75+ client workspaces using the Accelerate admin SSH key, computes project health metrics, and loads them into BigQuery. Looker Studio reads BigQuery directly with a **client dropdown** to filter by workspace.

---

## How It Works

```
GitHub Actions (daily cron)
        ↓
encord_to_bigquery.py
        ↓  Uses Accelerate SSH key → auto-discovers all projects
        ↓  Derives workspace from creator_email (guest+wayve@encord.com → "Wayve")
        ↓  Computes all metrics (rejection rate, TPT, throughput, outlier flags)
        ↓
BigQuery Tables
  ├── project_health     (one row per project per day)
  ├── annotator_stats    (one row per annotator per project per day)
  └── reviewer_stats     (one row per reviewer per project per day)
        ↓
Looker Studio Dashboard
  ├── Dropdown: Select Client  ← filters all charts by client_workspace
  ├── Project Cards: health 🟢🟡🔴, progress %, rejection rate
  └── Annotator Table: TPT, throughput, outlier flags 🔴🟡
```

---

## Repo Structure

```
Internal-Operational-Dashboard/
├── sync/
│   └── encord_to_bigquery.py   ← THE only script that matters
├── .github/
│   └── workflows/
│       └── sync.yml            ← runs daily at 6:30 AM IST
├── .gitignore
└── README.md
```

---

## Setup

### 1. Add GitHub Secrets

Go to repo → **Settings → Secrets and variables → Actions**

| Secret | Value |
|--------|-------|
| `ENCORD_SSH_KEY` | Contents of `encord-vinayak_label_analytics-private-key.ed25519` |
| `GCP_SA_KEY` | Contents of GCP service account JSON key |

### 2. Add GitHub Variables

| Variable | Example Value |
|----------|--------------|
| `GCP_PROJECT` | `autonex-488609` |
| `BQ_DATASET` | `encord_dashboard` |

### 3. First Run

Go to **Actions → Encord → BigQuery Sync → Run workflow**

First run takes ~90 minutes (1731 projects). After that it runs daily automatically at 6:30 AM IST.

---

## BigQuery Tables

### `project_health`
| Column | Type | Description |
|--------|------|-------------|
| `snapshot_date` | DATE | Sync date |
| `client_workspace` | STRING | **← Looker dropdown field** (Wayve, Boston Dynamics, etc.) |
| `project_title` | STRING | Project name |
| `health_status` | STRING | Healthy / Warning / Critical |
| `progress_pct` | FLOAT | % of tasks complete |
| `project_rejection_rate` | FLOAT | % rejected tasks |
| `active_annotators` | INTEGER | Annotators with activity |
| `avg_tpt_secs` | FLOAT | Avg seconds per task |
| `critical_flags` | INTEGER | # annotators with critical flags |
| `warning_flags` | INTEGER | # annotators with warning flags |

### `annotator_stats`
| Column | Type | Description |
|--------|------|-------------|
| `client_workspace` | STRING | Client name |
| `annotator_email` | STRING | Annotator |
| `rejection_rate` | FLOAT | % tasks rejected |
| `avg_tpt_secs` | FLOAT | Avg time per task |
| `throughput_per_day` | FLOAT | Tasks per day |
| `flag_high_rejection` | BOOLEAN | 🔴 rej_rate > avg + 10pp |
| `flag_too_fast` | BOOLEAN | 🟡 tpt < median × 0.2 |
| `flag_too_slow` | BOOLEAN | 🟡 tpt > median × 1.5 |
| `flag_low_throughput` | BOOLEAN | 🟡 throughput < median × 0.8 |
| `health_status` | STRING | good / warn / crit |

### `reviewer_stats`
| Column | Type | Description |
|--------|------|-------------|
| `reviewer_email` | STRING | Reviewer |
| `tasks_reviewed` | INTEGER | Tasks reviewed |
| `rejection_rate` | FLOAT | % tasks rejected by this reviewer |
| `avg_review_tpt_secs` | FLOAT | Avg review time per task |

---

## Looker Studio Setup

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com) → **Create → Report**
2. Add data source → BigQuery → `project_health`
3. Add **Drop-down list** control → field: `client_workspace`
4. Add table with: `project_title`, `health_status`, `progress_pct`, `project_rejection_rate`, `active_annotators`
5. Repeat for `annotator_stats` on Page 2 with annotator-level columns

---

## Metric Definitions

| Metric | Formula |
|--------|---------|
| **Progress %** | `tasks_complete / total_tasks × 100` |
| **Health** | rej > 15% = Critical · > 10% = Warning · ≤ 10% = Healthy |
| **Rejection Rate** | `rejected / (approved + rejected) × 100` — "rejected wins" rule |
| **Avg TPT** | `total_annotation_time / tasks_annotated` |
| **Throughput** | `tasks / days_active` |
| 🔴 High Rejection | annotator rate > project average + 10 percentage points |
| 🟡 Too Fast | annotator TPT < median TPT × 0.2 |
| 🟡 Too Slow | annotator TPT > median TPT × 1.5 |
| 🟡 Low Throughput | annotator throughput < median throughput × 0.8 |
