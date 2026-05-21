"""
Encord Operations Dashboard — Metrics Engine
=============================================
Calculates dashboard metrics and detects outliers.

Metrics:
  1. Rejection Rate = rejected_tasks / total_submitted_tasks (per annotator)
  2. Time Per Task (TPT) = total_time / tasks_completed (per annotator)
  3. Throughput = tasks_completed / active_hours (per annotator)

Outlier Detection:
  - Rejection Rate: Flag if > project_avg + 10% margin
  - TPT: Flag if < 20% of project median OR > 50% of project median
  - Throughput: Flag if > 20% below project median

RAG (Red/Amber/Green) Levels:
  - GREEN: Within normal range
  - AMBER: Within 5% of threshold boundary
  - RED: Breached threshold
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class AnnotatorMetrics:
    """Computed metrics for a single annotator in a single project."""
    annotator_email: str
    project_hash: str
    date: str
    tasks_submitted: int = 0
    tasks_rejected: int = 0
    rejection_rate: float = 0.0
    total_time_seconds: float = 0.0
    tasks_completed: int = 0
    time_per_task_seconds: float = 0.0
    throughput_per_hour: float = 0.0


@dataclass
class OutlierResult:
    """Result of an outlier check."""
    annotator_email: str
    project_hash: str
    project_title: str
    metric_type: str  # rejection_rate, tpt, throughput
    actual_value: float
    threshold_value: float
    flag_level: str  # red, amber, green
    description: str


def calculate_rejection_rate(submitted: int, rejected: int) -> float:
    """Calculate annotator rejection rate as a percentage (0.0 - 1.0)."""
    if submitted <= 0:
        return 0.0
    return round(rejected / submitted, 4)


def calculate_tpt(total_seconds: float, completed: int) -> float:
    """Calculate average time per task in seconds."""
    if completed <= 0:
        return 0.0
    return round(total_seconds / completed, 2)


def calculate_throughput(completed: int, total_hours: float) -> float:
    """Calculate tasks completed per hour."""
    if total_hours <= 0:
        return 0.0
    return round(completed / total_hours, 2)


# ─────────────────────────────────────────────
# Outlier Detection
# ─────────────────────────────────────────────

def detect_rejection_rate_outliers(
    annotator_rates: dict[str, float],
    project_hash: str,
    project_title: str,
) -> list[OutlierResult]:
    """
    Flag annotators whose rejection rate exceeds the project average + margin.

    Logic: Flag if annotator_rate > project_avg + REJECTION_RATE_MARGIN
    """
    if not annotator_rates:
        return []

    values = list(annotator_rates.values())
    project_avg = float(np.mean(values))
    threshold = project_avg + settings.REJECTION_RATE_MARGIN
    amber_threshold = threshold - 0.05  # 5% before red = amber

    results = []
    for email, rate in annotator_rates.items():
        if rate >= threshold:
            level = "red"
            desc = (
                f"Rejection rate {rate:.1%} exceeds project average "
                f"{project_avg:.1%} + {settings.REJECTION_RATE_MARGIN:.0%} margin "
                f"(threshold: {threshold:.1%})"
            )
        elif rate >= amber_threshold:
            level = "amber"
            desc = (
                f"Rejection rate {rate:.1%} approaching threshold "
                f"(threshold: {threshold:.1%}, project avg: {project_avg:.1%})"
            )
        else:
            level = "green"
            desc = f"Rejection rate {rate:.1%} within normal range"

        results.append(OutlierResult(
            annotator_email=email,
            project_hash=project_hash,
            project_title=project_title,
            metric_type="rejection_rate",
            actual_value=rate,
            threshold_value=threshold,
            flag_level=level,
            description=desc,
        ))

    return results


def detect_tpt_outliers(
    annotator_tpts: dict[str, float],
    project_hash: str,
    project_title: str,
) -> list[OutlierResult]:
    """
    Flag annotators whose TPT is too fast (<20% of median) or too slow (>50% of median).

    Logic:
      - Flag RED if < 20% of median (suspiciously fast)
      - Flag RED if > 150% of median (too slow)
      - Flag AMBER if approaching boundaries
    """
    if not annotator_tpts:
        return []

    values = [v for v in annotator_tpts.values() if v > 0]
    if not values:
        return []

    median = float(np.median(values))
    if median <= 0:
        return []

    low_threshold = median * settings.TPT_LOW_THRESHOLD  # 20% of median
    high_threshold = median * (1 + settings.TPT_HIGH_THRESHOLD)  # 150% of median
    low_amber = median * (settings.TPT_LOW_THRESHOLD + 0.10)  # 30% of median
    high_amber = median * (1 + settings.TPT_HIGH_THRESHOLD - 0.10)  # 140% of median

    results = []
    for email, tpt in annotator_tpts.items():
        if tpt <= 0:
            continue

        if tpt < low_threshold:
            level = "red"
            desc = (
                f"TPT {tpt:.0f}s is suspiciously fast — below "
                f"{settings.TPT_LOW_THRESHOLD:.0%} of project median {median:.0f}s"
            )
        elif tpt < low_amber:
            level = "amber"
            desc = f"TPT {tpt:.0f}s is approaching lower boundary (median: {median:.0f}s)"
        elif tpt > high_threshold:
            level = "red"
            desc = (
                f"TPT {tpt:.0f}s exceeds {1 + settings.TPT_HIGH_THRESHOLD:.0%} "
                f"of project median {median:.0f}s"
            )
        elif tpt > high_amber:
            level = "amber"
            desc = f"TPT {tpt:.0f}s approaching upper boundary (median: {median:.0f}s)"
        else:
            level = "green"
            desc = f"TPT {tpt:.0f}s within normal range (median: {median:.0f}s)"

        results.append(OutlierResult(
            annotator_email=email,
            project_hash=project_hash,
            project_title=project_title,
            metric_type="tpt",
            actual_value=tpt,
            threshold_value=median,
            flag_level=level,
            description=desc,
        ))

    return results


def detect_throughput_outliers(
    annotator_throughputs: dict[str, float],
    project_hash: str,
    project_title: str,
) -> list[OutlierResult]:
    """
    Flag annotators whose throughput is significantly below the project median.

    Logic: Flag if throughput < median * (1 - THROUGHPUT_LOW_THRESHOLD)
    """
    if not annotator_throughputs:
        return []

    values = [v for v in annotator_throughputs.values() if v > 0]
    if not values:
        return []

    median = float(np.median(values))
    if median <= 0:
        return []

    threshold = median * (1 - settings.THROUGHPUT_LOW_THRESHOLD)  # 80% of median
    amber_threshold = median * (1 - settings.THROUGHPUT_LOW_THRESHOLD + 0.05)  # 85%

    results = []
    for email, tp in annotator_throughputs.items():
        if tp < threshold:
            level = "red"
            desc = (
                f"Throughput {tp:.1f} tasks/hr is >{settings.THROUGHPUT_LOW_THRESHOLD:.0%} "
                f"below project median {median:.1f} tasks/hr"
            )
        elif tp < amber_threshold:
            level = "amber"
            desc = (
                f"Throughput {tp:.1f} tasks/hr approaching threshold "
                f"(median: {median:.1f} tasks/hr)"
            )
        else:
            level = "green"
            desc = f"Throughput {tp:.1f} tasks/hr within normal range"

        results.append(OutlierResult(
            annotator_email=email,
            project_hash=project_hash,
            project_title=project_title,
            metric_type="throughput",
            actual_value=tp,
            threshold_value=median,
            flag_level=level,
            description=desc,
        ))

    return results


def compute_project_summary(snapshots: list[dict]) -> dict:
    """
    Compute aggregate summary for a project from individual annotator snapshots.
    Returns: avg rejection rate, median TPT, avg throughput, annotator count, RAG status.
    """
    if not snapshots:
        return {
            "avg_rejection_rate": 0.0,
            "median_tpt": 0.0,
            "avg_throughput": 0.0,
            "total_tasks": 0,
            "annotator_count": 0,
            "overall_rag": "green",
        }

    rejection_rates = [s.get("rejection_rate", 0) for s in snapshots]
    tpts = [s.get("time_per_task_seconds", 0) for s in snapshots if s.get("time_per_task_seconds", 0) > 0]
    throughputs = [s.get("throughput_per_hour", 0) for s in snapshots if s.get("throughput_per_hour", 0) > 0]
    total_tasks = sum(s.get("tasks_completed", 0) for s in snapshots)

    avg_rr = float(np.mean(rejection_rates)) if rejection_rates else 0.0
    median_tpt = float(np.median(tpts)) if tpts else 0.0
    avg_tp = float(np.mean(throughputs)) if throughputs else 0.0

    # Overall RAG: worst of any metric
    if avg_rr > 0.15:
        overall = "red"
    elif avg_rr > 0.10:
        overall = "amber"
    else:
        overall = "green"

    return {
        "avg_rejection_rate": round(avg_rr, 4),
        "median_tpt": round(median_tpt, 2),
        "avg_throughput": round(avg_tp, 2),
        "total_tasks": total_tasks,
        "annotator_count": len(snapshots),
        "overall_rag": overall,
    }
