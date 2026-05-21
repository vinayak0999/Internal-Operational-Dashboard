# Encord Operations Dashboard — Metric Calculation Logic

> **Version**: 1.1 — DRAFT  
> **Last Updated**: 17 April 2026  
> **Author**: Ops Engineering Team  

---

## Table of Contents

1. [Data Sources (Encord APIs Used)](#1-data-sources)
2. [Time Metrics](#2-time-metrics)
3. [Task Progress](#3-task-progress)
4. [Rejection Rate](#4-rejection-rate)
5. [Per-Annotator Metrics](#5-per-annotator-metrics)
6. [Per-Reviewer Metrics](#6-per-reviewer-metrics)
7. [Outlier Detection & Flags](#7-outlier-detection--flags)
8. [Known Limitations](#8-known-limitations)

---

## 1. Data Sources

We use **three Encord SDK APIs** to pull all dashboard data. All calls are **read-only** — no data is ever written or modified.

| API Method | What It Returns | Used For |
|------------|----------------|----------|
| `project.list_time_spent(start, end)` | Per-user, per-task time entries with workflow stage | Time tracking, task counts, TPT |
| `project.list_label_rows_v2()` | All data units with current workflow stage | Task progress (complete vs in-review) |
| `project.get_label_logs(after, before)` | Editor action events (submit, approve, reject) | Rejection rates, submitted/approved counts |

### Date Filtering

- User selects a **From** and **To** date via the calendar picker
- These are sent to the API as `start_date` (YYYY-MM-DD) and `end_date` (YYYY-MM-DD)
- The end date is set to **23:59:59 UTC** to include the full day
- `list_time_spent()` and `get_label_logs()` are filtered by this range
- `list_label_rows_v2()` is **NOT** date-filtered — it always shows the current state of all tasks

---

## 2. Time Metrics

### How Time Entries Work

Each time entry from `list_time_spent()` contains:

| Field | Description |
|-------|-------------|
| `user_email` | Who spent the time |
| `time_spent_seconds` | Duration in seconds |
| `data_uuid` | The specific data unit (task) |
| `workflow_stage.stage_type` | `ANNOTATION` or `REVIEW` |
| `workflow_stage.title` | Human-readable stage name (e.g. "Annotate 1", "Review 1") |
| `period_start_time` | Timestamp of the session |

### Classification Logic

```
For each time entry:
    IF stage_type contains "ANNOTATION"  →  Annotation time
    ELIF stage_type contains "REVIEW"    →  Review time
    ELSE                                 →  Other time
```

### Metric Formulas

| Metric | Formula |
|--------|---------|
| **Total Time (Annotating)** | `SUM(time_spent_seconds)` for all entries where `stage_type = ANNOTATION` |
| **Total Time (Reviewing)** | `SUM(time_spent_seconds)` for all entries where `stage_type = REVIEW` |
| **Other Time** | `SUM(time_spent_seconds)` for entries that are neither ANNOTATION nor REVIEW |
| **Total Time (All)** | `Annotation Time + Review Time + Other Time` |
| **Avg TPT (Annotation)** | `Total Annotation Time / Total Number of Tasks Annotated` |

### Example

```
annotator4 has 11 annotation time entries totaling 2,128 seconds (35m 28s)
annotator4 has 18 review time entries totaling 2,718 seconds (45m 18s)

→ Annotation time for annotator4 = 35m 28s
→ Review time for annotator4 = 45m 18s
```

---

## 3. Task Progress

### How It Works

We call `project.list_label_rows_v2()` which returns every data unit in the project. Each has a `workflow_graph_node` property that tells us its **current** position in the workflow.

### Classification Logic

```
For each label row:
    total_tasks += 1
    
    IF workflow_graph_node.title contains "complete" OR "done" OR "final" OR "archive":
        tasks_complete += 1
        tasks_done    += 1      ← counts toward progress
    
    ELIF stage_type contains "REVIEW" OR title contains "review":
        tasks_in_review += 1
        tasks_done      += 0    ← NOT done (still under review)
    
    ELIF stage_type contains "ANNOTATION" OR title contains "annotat":
        tasks_in_annotation += 1
        tasks_done          += 0    ← NOT done
```

### Progress Percentage

```
progress = ROUND(tasks_done / total_tasks × 100, 1)
```

### Why "Review" ≠ "Done"

A task in "Review 1" means a reviewer hasn't finished checking it. Only tasks that have been reviewed AND moved to "Complete" count as done. This matches the Encord UI exactly.

| Stage | Example Count | Counts as Done? |
|-------|:---:|:---:|
| Complete | 1,007 | ✅ Yes |
| Review 1 | 5 | ❌ No |
| Annotate 1 | 0 | ❌ No |
| **Progress** | **1,007 / 1,012 = 99.5%** | |

---

## 4. Rejection Rate

### Data Source

We use `project.get_label_logs()` filtered by the selected date range. This returns every editor action (button click) with:

| Field | Description |
|-------|-------------|
| `action` | The type of action taken |
| `user_email` | Who performed the action |
| `data_hash` | Which task the action was on |
| `created_at` | Timestamp |

### Relevant Action Types

| Action | Code | Who Does It | Meaning |
|--------|:----:|-------------|---------|
| `SUBMIT_TASK` | 11 | Annotator | Annotator submits their work for review |
| `APPROVE_TASK` | 33 | Reviewer | Reviewer approves the task |
| `REJECT_TASK` | 34 | Reviewer | Reviewer rejects the task (sent back to annotator) |

### Annotator Rejection Rate

This answers: *"What percentage of this annotator's reviewed tasks were rejected?"*

The denominator is `approved + rejected` (tasks that completed review), NOT `submitted` (which includes tasks still pending review).

```
For each annotator:
    submitted = COUNT(UNIQUE data_hashes where this user did SUBMIT_TASK)

    For each submitted data_hash, classify by review outcome:
        IF data_hash has ANY REJECT_TASK event → "rejected"
           (even if also later approved — "rejected wins")
        ELIF data_hash has ≥1 APPROVE_TASK and NO rejects → "approved"
        ELSE → "pending" (excluded from denominator)

    approved = COUNT(submitted tasks classified as "approved")
    rejected = COUNT(submitted tasks classified as "rejected")

    rejection_rate = rejected / (approved + rejected) × 100

    If (approved + rejected) == 0 → rejection_rate = 0%
```

> **"Rejected wins" rule**: A task with lifecycle SUBMIT → REJECT → re-SUBMIT → APPROVE has both approve and reject events. We classify it as **rejected** because the annotator had to redo the work. This matches Encord's quality signal intent.

### Example

**Illustrative example (not from live data):**

```
annotator4 submitted 11 tasks.
Of those 11, 9 have been reviewed (2 still pending).
Of those 9 reviewed: 7 were approved, 2 were rejected.

→ rejection_rate = 2 / (7 + 2) × 100 = 22.2%

Note: The 2 pending tasks are excluded from the denominator.
```

### Reviewer Rejection Rate

This answers: *"What percentage of tasks this reviewer acted on did they reject?"*

Uses the same "rejected wins" logic per data_hash.

```
For each reviewer:
    For each data_hash this reviewer touched (APPROVE or REJECT):
        IF data_hash is in reviewer's REJECT set → count as rejected
        ELSE → count as approved

    approved = COUNT(tasks only approved by this reviewer)
    rejected = COUNT(tasks rejected by this reviewer)
    total    = approved + rejected

    rejection_rate = (rejected / total) × 100

    If total = 0 → rejection_rate = 0%
```

### Project-Level Rejection Rate

Uses the same denominator as per-annotator (approved + rejected, not submitted).

```
For each UNIQUE data_hash submitted by any annotator:
    Classify as approved/rejected/pending using the same rules above.

total_approved = COUNT(data_hashes classified as "approved")
total_rejected = COUNT(data_hashes classified as "rejected")

project_rejection = total_rejected / (total_approved + total_rejected) × 100

Pending tasks are excluded from the denominator.
```

---

## 5. Per-Annotator Metrics

For each user who has annotation-stage time entries:

| Metric | Formula | Source |
|--------|---------|--------|
| **Tasks** | Count of unique `data_uuid` in ANNOTATION entries | `list_time_spent()` |
| **Tasks Submitted** | Count of unique `data_hash` with SUBMIT_TASK action | `get_label_logs()` |
| **Tasks Rejected** | Count of submitted tasks that were rejected | `get_label_logs()` |
| **Rejection Rate** | `rejected / (approved + rejected) × 100` | Derived |
| **Annotation Time** | Sum of `time_spent_seconds` for ANNOTATION entries | `list_time_spent()` |
| **Avg TPT** | `annotation_time / tasks` (seconds per task) | Derived |
| **Days Active** | `(max_date - min_date) + 1` from session timestamps | `list_time_spent()` |
| **Throughput** | `tasks / days_active` (tasks per day) | Derived |

### Role Determination

```
IF user has ANNOTATION time AND REVIEW time → "Annotator & Reviewer"
ELIF user has only REVIEW time              → "Reviewer"
ELSE                                        → "Annotator"
```

---

## 6. Per-Reviewer Metrics

For each user who has review-stage time entries:

| Metric | Formula | Source |
|--------|---------|--------|
| **Tasks Reviewed** | Count of unique `data_uuid` in REVIEW entries | `list_time_spent()` |
| **Review Time** | Sum of `time_spent_seconds` for REVIEW entries | `list_time_spent()` |
| **Tasks Approved** | Count of unique `data_hash` with APPROVE_TASK | `get_label_logs()` |
| **Tasks Rejected** | Count of unique `data_hash` with REJECT_TASK | `get_label_logs()` |
| **Rejection Rate** | `rejected / (approved + rejected) × 100` | Derived |

---

## 7. Outlier Detection & Flags

Each annotator is automatically evaluated for performance outliers:

### Flag: "High Rejection" 🔴

```
IF annotator.rejection_rate > project_avg_rejection + 10 percentage points
    → Flag as "high rejection" (CRITICAL)
```

Example: If project avg is 64% and annotator has 81%, gap = 17pp > 10pp → flagged.

### Flag: "Too Fast" 🟡

```
IF annotator.avg_tpt < median_tpt × 0.2
    → Flag as "too fast" (WARNING)
```

Annotators with suspiciously low time-per-task may be rushing quality.

### Flag: "Too Slow" 🟡

```
IF annotator.avg_tpt > median_tpt × 1.5
    → Flag as "too slow" (WARNING)
```

### Flag: "Low Throughput" 🟡

```
IF annotator.throughput < median_throughput × 0.8
    → Flag as "low throughput" (WARNING)
```

### Status Determination

```
IF any flag is CRITICAL → annotator status = "crit" (red card border)
ELIF any flag is WARNING → annotator status = "warn" (amber card border)
ELSE                     → annotator status = "good" (green card border)
```

### Project Health Status

```
IF project_rejection_rate > 15% → "Critical" (red)
ELIF project_rejection_rate > 10% → "Warning" (amber)
ELSE                              → "Healthy" (green)
```

---

## 8. Known Limitations

### Admin Time Not Tracked

The Encord SDK's `list_time_spent()` **does not return data for Admin-role users** (role=0). These users' browsing/viewing time is tracked by the Encord web UI frontend but not exposed via the API.

| User Type | Role Code | Time in SDK? |
|-----------|:---------:|:---:|
| Annotator | 3 | ✅ Yes |
| Reviewer | 2 | ✅ Yes |
| Admin | 0 | ❌ No |

**Impact**: Total time may be ~1-5 minutes less than what the Encord Analytics UI shows. All other metrics (annotation time, review time, per-user times) match exactly.

### Label Rows Are Point-in-Time

`list_label_rows_v2()` shows the **current** workflow state of each task, not historical. If a task was in "Review 1" yesterday but moved to "Complete" today, the dashboard will now show it as Complete regardless of the date filter selected.

### Date Range Applies To Time & Actions Only

- **Time data** (`list_time_spent`): ✅ filtered by date range
- **Label logs** (`get_label_logs`): ✅ filtered by date range
- **Task progress** (`list_label_rows_v2`): ❌ always shows current state

### Rejected-Then-Approved Tasks

Tasks that were rejected and later re-submitted and approved are classified as **"rejected"** in rejection rate calculations, since the annotator had to redo the work. This matches Encord's quality signal intent. The dashboard logs the count of such tasks for visibility (see server console output).

### Days Active Methodology

`days_active = (max_submission_date - min_submission_date) + 1`. This is **calendar span**, not working days, so weekends and days off count toward the denominator. Throughput numbers may under-report for annotators with gappy schedules.

Alternative approach (not currently in use): `count of unique calendar days with activity`. The current implementation uses calendar span.

### Timezone Assumptions

Date filters apply `23:59:59 UTC` as the end boundary. If annotators work in local timezones that differ significantly from UTC, edge-of-day activity may be attributed to adjacent calendar days. Expected variance: < 1% of records for most teams.

### Workflow Stage Name Matching

Task completion is detected via **string matching** on workflow stage titles (contains "complete", "done", "final", "archive"). Renaming or localizing stage names in the Encord project will break this detection logic. Consider switching to `stage_type`-based detection if your workflow evolves.

---

## Appendix: Workflow Stage Mapping

This project's workflow has three stages:

| Stage Name | Stage Type | Who Works Here |
|------------|-----------|----------------|
| Annotate 1 | ANNOTATION | Annotators (role=3) |
| Review 1 | REVIEW | Internal reviewers (role=3 with review access) |
| Review (Client ONLY) | REVIEW | Client reviewers (role=2) — matt, julie |
| Complete | — | Terminal stage (all finished tasks) |

Both "Review 1" and "Review (Client ONLY)" time entries are classified as **Review time**.
