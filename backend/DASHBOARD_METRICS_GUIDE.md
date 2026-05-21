# Dashboard Metrics — Quick Reference Guide

> **For**: Client & Internal Teams  
> **Last Updated**: 17 April 2026  

---

## The 4 Annotator Metrics

Every annotator card on the dashboard shows these 4 numbers:

```
┌──────────────────┬──────────────┬──────────────┬──────────────┐
│  REJECTION RATE  │  ANN. TIME   │   AVG TPT    │  THROUGHPUT   │
│     81.82%       │   35m 28s    │   3m 14s     │   2.8/day     │
│     ● high       │              │              │               │
└──────────────────┴──────────────┴──────────────┴──────────────┘
```

---

### 1. REJECTION RATE

**What it tells you:**  
Out of this annotator's work that has been reviewed, how much was sent back because the quality wasn't good enough?

**Formula:**

```
Rejection Rate = Rejected Tasks ÷ (Approved Tasks + Rejected Tasks) × 100
```

**In simple words:**  
If an annotator submitted 11 tasks and reviewers have checked 9 of them so far — approving 2 and rejecting 7 — the rejection rate is:

```
7 ÷ (2 + 7) × 100 = 77.8%
```

The remaining 2 tasks that haven't been reviewed yet are NOT counted. Only tasks that went through review matter.

**Why this matters:**  
A high rejection rate means the annotator is producing low-quality work that reviewers keep sending back. This costs time — the annotator has to redo the task, and the reviewer has to re-check it.

**Color coding:**
| Color | Meaning |
|-------|---------|
| 🟢 Green | < 10% — Good quality, rarely rejected |
| 🟡 Amber | 10–15% — Needs attention |
| 🔴 Red | > 15% — Quality problem, needs training or reassignment |

**🔴 "high" flag** appears when this annotator's rate is **10+ percentage points above** the project average.

---

### 2. ANN. TIME (Annotation Time)

**What it tells you:**  
The total time this annotator spent doing annotation work during the selected date range.

**Formula:**

```
Ann. Time = SUM of all time entries where this user was in an ANNOTATION stage
```

**In simple words:**  
Every time an annotator opens a task, works on it, and moves on — Encord records that session duration. We add up all those sessions.

**Example:**
```
annotator4 worked on 11 tasks across 4 days.
Session 1: 5m 12s   (Task A)
Session 2: 3m 44s   (Task B)
Session 3: 2m 10s   (Task A again — came back to fix it)
... (and so on for all sessions)

Total Annotation Time = 35m 28s
```

**Why this matters:**  
Shows who is actively contributing and how much real work time was spent. If someone has high annotation time but low task count, they might be struggling. If they have low time but high tasks, they might be rushing.

**Note:** This does NOT include time spent reviewing. If a person both annotates and reviews, their annotation time and review time are tracked separately.

---

### 3. AVG TPT (Average Time Per Task)

**What it tells you:**  
On average, how many seconds/minutes does this annotator spend on each task?

**Formula:**

```
Avg TPT = Total Annotation Time ÷ Number of Tasks Annotated
```

**In simple words:**
```
annotator4:  35m 28s total time ÷ 11 tasks = 3m 14s per task
annotator5:  13m 14s total time ÷ 9 tasks  = 1m 28s per task
```

**Why this matters:**  

- **Too fast** (🟡 "fast" flag): Annotator is spending very little time per task. This could mean they're rushing and not checking their work properly — which often leads to higher rejection rates.

- **Too slow** (🟡 "slow" flag): Annotator is spending too much time per task. Could mean the task is too complex for them, they need training, or there's a tooling issue.

- **Normal**: Somewhere in the middle means they're working at a sustainable pace.

**How "too fast" / "too slow" is determined:**
```
Median TPT = the middle value across all annotators

Too Fast:  annotator's TPT < median × 0.2  (less than 20% of median)
Too Slow:  annotator's TPT > median × 1.5  (more than 150% of median)
```

---

### 4. THROUGHPUT

**What it tells you:**  
How many tasks per day is this annotator completing?

**Formula:**

```
Throughput = Number of Tasks Annotated ÷ Days Active
```

Where:
```
Days Active = (Last Active Date − First Active Date) + 1
```

**In simple words:**
```
annotator4 annotated 11 tasks and was active across a 4-day span.
Throughput = 11 ÷ 4 = 2.8 tasks/day
```

**Why this matters:**  
Shows productivity. If you're paying annotators per-day and one person does 1.1 tasks/day while another does 3.0, you can see who needs support or reassignment.

**🟡 "low throughput" flag** appears when:
```
annotator's throughput < median throughput × 0.8
```

---

## Reviewer Metrics

Reviewer cards show slightly different metrics:

```
┌─────────────────────┬──────────────┬──────────────┬──────────────┐
│  REJ. RATE          │ REVIEW TIME  │ TASKS        │  TOTAL TIME  │
│  (as reviewer)      │              │ REVIEWED     │              │
│     31.58%          │   1h 12m     │   19         │  1h 12m      │
└─────────────────────┴──────────────┴──────────────┴──────────────┘
```

### REJ. RATE (as reviewer)

**What it tells you:**  
When this person reviews tasks, how often do they reject vs approve?

**Formula:**

```
Reviewer Rej. Rate = Tasks Rejected ÷ (Tasks Approved + Tasks Rejected) × 100
```

**In simple words:**  
Julie reviewed 19 tasks. She approved 13 and rejected 6.

```
6 ÷ (13 + 6) × 100 = 31.58%
```

This means Julie rejects about 1 in 3 tasks she reviews.

**Why this matters:**  
- A **high reviewer rejection rate** (like Julie at 31.58%) means that reviewer has **strict quality standards** — not that they're doing something wrong.
- A **very low reviewer rejection rate** (like Annotator6 at 0.28%) could mean they're being too lenient and approving work that shouldn't pass.
- Comparing reviewer rejection rates helps identify if quality standards are consistent across your review team.

**Key difference from annotator rejection rate:**

| Metric | Measures | Perspective |
|--------|----------|-------------|
| Annotator Rejection Rate | The annotator's **work quality** | "Is their work good enough?" |
| Reviewer Rejection Rate | The reviewer's **strictness** | "How high are their standards?" |

Same rejected task, two sides of the coin.

---

## Project-Level Metrics

### AVG REJECTION RATE (KPI strip)

```
Project Rejection Rate = Total Rejected Tasks ÷ (Total Approved + Total Rejected) × 100
```

**Color coding:**
| Color | Threshold | Meaning |
|-------|-----------|---------|
| 🟢 Green | < 10% | Project is healthy — annotators produce quality work |
| 🟡 Amber | 10–15% | Elevated — some annotators may need retraining |
| 🔴 Red | > 15% | Quality problem — action required |

---

## Quick Summary Table

| Metric | Formula | Good Sign | Bad Sign |
|--------|---------|-----------|----------|
| **Rejection Rate** | rejected ÷ (approved + rejected) | Low (< 10%) | High (> 15%) |
| **Ann. Time** | sum of annotation sessions | Proportional to tasks | Very high or very low |
| **Avg TPT** | total time ÷ tasks | Near the team median | Way above or below median |
| **Throughput** | tasks ÷ days active | Consistent, > 2/day | Very low (< 1/day) |
| **Rej. Rate (reviewer)** | rejected ÷ (approved + rejected) | Consistent across reviewers | Wildly different between reviewers |
