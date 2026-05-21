# Internal Operational Dashboard

Multi-client Encord operations dashboard — live project health, per-annotator metrics, and outlier detection across all client workspaces.

---

## What It Does

- **Client dropdown** — select any client workspace (Wayve, Boston Dynamics, 1X, etc.)
- **All projects for that client** — with health status 🟢🟡🔴, progress %, rejection rate
- **Per-annotator breakdown** — rejection rate, annotation time, avg TPT, throughput/day
- **Outlier flags** — 🔴 High Rejection, 🟡 Too Fast, 🟡 Too Slow, 🟡 Low Throughput
- **Auto-refresh every 2 minutes** — incremental sync (unchanged projects skipped instantly)

---

## Architecture

```
Encord SSH Key (Accelerate Admin)
        ↓  (auto-discovers all 1731 projects across 75 workspaces)
Backend (FastAPI + APScheduler)
        ↓  (syncs every 2 min, incremental — only changed projects refetched)
SQLite Cache (dashboard.db)
        ↓  (instant reads, no API latency)
Frontend (HTML/JS)
        ↓
Client Dropdown → Project Cards → Annotator Table
```

---

## Folder Structure

```
Internal-Operational-Dashboard/
├── backend/
│   ├── main.py              # FastAPI server + all API endpoints
│   ├── sync_worker.py       # Encord data fetch + metric computation
│   ├── scheduler.py         # APScheduler (2-min background sync)
│   ├── encord_client.py     # Read-only Encord SDK wrapper + workspace detection
│   ├── database.py          # SQLite schema (ProjectCache, MetricSnapshot, etc.)
│   ├── metrics.py           # Rejection rate, TPT, throughput, outlier detection
│   ├── config.py            # Settings from .env
│   ├── requirements.txt     # Python dependencies
│   └── .env.example         # Config template (copy to .env)
│
├── frontend/
│   ├── index.html           # Main dashboard UI
│   ├── app.js               # Client dropdown + project cards + annotator table
│   └── style.css            # Styling
│
├── pipeline/
│   └── multi_client_sync.py # BigQuery batch sync (optional — for Looker Studio)
│
└── .github/
    └── workflows/
        └── multi_client_sync.yml  # GitHub Actions daily cron
```

---

## Quick Start

### 1. Clone & Install

```bash
git clone https://github.com/vinayak0999/Internal-Operational-Dashboard.git
cd Internal-Operational-Dashboard/backend
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env — set ENCORD_SSH_KEY_PATH to your accelerate admin key
```

### 3. Run

```bash
python main.py
# Dashboard → http://localhost:8000
```

The server starts syncing immediately. First sync takes a few minutes to discover all projects. After that, only changed projects are re-fetched every 2 minutes.

---

## Metrics (identical to 1-single-project-view)

| Metric | Formula |
|--------|---------|
| **Progress %** | `complete / total × 100` |
| **Health** | rej > 15% = 🔴 Critical, > 10% = 🟡 Warning, ≤ 10% = 🟢 Healthy |
| **Rejection Rate** | `rejected / (approved + rejected) × 100` ("rejected wins" rule) |
| **Avg TPT** | `annotation_time / tasks` |
| **Throughput** | `tasks / days_active` |
| 🔴 High Rejection | annotator rej_rate > project_avg + 10pp |
| 🟡 Too Fast | tpt < median × 0.2 |
| 🟡 Too Slow | tpt > median × 1.5 |
| 🟡 Low Throughput | throughput < median × 0.8 |

---

## GitHub Actions (Optional — BigQuery / Looker Studio)

Add these secrets to your repo for daily BigQuery sync:

| Secret | Value |
|--------|-------|
| `ENCORD_SSH_KEY` | Contents of accelerate private key |
| `GCP_SA_KEY` | GCP service account JSON |

| Variable | Value |
|----------|-------|
| `GCP_PROJECT` | Your GCP project ID |
| `BQ_DATASET` | `encord_accelerate` |

---

## Security

- **Read-only** — no Encord data is ever written or modified
- Private keys are excluded from git via `.gitignore`
- All credentials are loaded from `.env` (never hardcoded)
