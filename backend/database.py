"""
Encord Operations Dashboard — Database Layer
=============================================
SQLite database via SQLAlchemy.

⛔ CRITICAL DATA SAFETY RULE ⛔
===============================
This file must NEVER contain:
  - DELETE statements
  - DROP TABLE statements
  - TRUNCATE statements
  - Any destructive SQL operations

All data operations are INSERT or UPDATE (upsert) ONLY.
This is client data — nothing gets deleted, ever.
"""

from sqlalchemy import (
    create_engine,
    Column,
    String,
    Float,
    Integer,
    DateTime,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timezone

from config import settings

engine = create_engine(
    settings.DATABASE_URL,
    connect_args={"check_same_thread": False},  # SQLite specific
    echo=False,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ─────────────────────────────────────────────
# ORM Models
# ─────────────────────────────────────────────

class Project(Base):
    """Cached project metadata from Encord."""
    __tablename__ = "projects"

    project_hash = Column(String, primary_key=True, index=True)
    title = Column(String, nullable=False)
    description = Column(Text, default="")
    created_at = Column(DateTime, nullable=True)
    modality = Column(String, default="unknown")  # audio, video, image, text
    client_tag = Column(String, default="untagged")  # client name / tag
    last_synced = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Annotator(Base):
    """Known annotators across all projects."""
    __tablename__ = "annotators"

    email = Column(String, primary_key=True, index=True)
    name = Column(String, default="")
    first_seen = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_synced = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class MetricSnapshot(Base):
    """
    Point-in-time metric snapshot per annotator per project.
    Each sync creates a new snapshot — historical data is preserved.
    """
    __tablename__ = "metric_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_hash = Column(String, nullable=False, index=True)
    annotator_email = Column(String, nullable=False, index=True)
    date = Column(String, nullable=False)  # YYYY-MM-DD
    # Metrics
    tasks_submitted = Column(Integer, default=0)
    tasks_rejected = Column(Integer, default=0)
    rejection_rate = Column(Float, default=0.0)
    total_time_seconds = Column(Float, default=0.0)
    tasks_completed = Column(Integer, default=0)
    time_per_task_seconds = Column(Float, default=0.0)
    throughput_per_hour = Column(Float, default=0.0)
    # Snapshot metadata
    snapshot_timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint(
            "project_hash", "annotator_email", "date",
            name="uq_snapshot_per_day"
        ),
    )


class OutlierFlag(Base):
    """
    Outlier flags generated from metric analysis.
    Each flag records what was flagged, the value, threshold, and severity.
    """
    __tablename__ = "outlier_flags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_hash = Column(String, nullable=False, index=True)
    project_title = Column(String, default="")
    annotator_email = Column(String, nullable=False, index=True)
    metric_type = Column(String, nullable=False)  # rejection_rate, tpt, throughput
    actual_value = Column(Float, nullable=False)
    threshold_value = Column(Float, nullable=False)
    flag_level = Column(String, nullable=False)  # red, amber, green
    description = Column(Text, default="")
    flagged_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class TimeEntry(Base):
    """Individual time entries from Encord (cached)."""
    __tablename__ = "time_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_hash = Column(String, nullable=False, index=True)
    annotator_email = Column(String, nullable=False, index=True)
    data_hash = Column(String, default="")
    data_title = Column(String, default="")
    duration_seconds = Column(Float, default=0.0)
    recorded_at = Column(DateTime, nullable=True)
    synced_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint(
            "project_hash", "annotator_email", "data_hash", "recorded_at",
            name="uq_time_entry"
        ),
    )


class SyncLog(Base):
    """Tracks when data was last synced from Encord."""
    __tablename__ = "sync_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sync_type = Column(String, nullable=False)  # full, project, incremental
    project_hash = Column(String, default="all")
    status = Column(String, default="started")  # started, completed, failed
    records_synced = Column(Integer, default=0)
    error_message = Column(Text, default="")
    started_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime, nullable=True)


class ProjectChecksum(Base):
    """
    Tracks the 'fingerprint' of each project to detect changes.
    Used for incremental sync — skip projects that haven't changed.
    """
    __tablename__ = "project_checksums"

    project_hash = Column(String, primary_key=True, index=True)
    task_count = Column(Integer, default=0)
    completed_count = Column(Integer, default=0)
    label_count = Column(Integer, default=0)
    annotator_count = Column(Integer, default=0)
    total_time_seconds = Column(Float, default=0.0)
    last_activity_time = Column(String, default="")
    checksum = Column(String, default="")  # hash of above fields
    last_checked = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_changed = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class ProjectCache(Base):
    """
    Pre-computed project data for instant API reads.
    Updated by sync worker — frontend reads ONLY from this table.
    """
    __tablename__ = "project_cache"

    project_hash = Column(String, primary_key=True, index=True)
    title = Column(String, default="")
    client_tag = Column(String, default="untagged", index=True)
    modality = Column(String, default="unknown")
    project_status = Column(String, default="unknown")

    # Progress
    total_tasks = Column(Integer, default=0)
    done_tasks = Column(Integer, default=0)
    in_review = Column(Integer, default=0)
    in_annotation = Column(Integer, default=0)
    progress = Column(Float, default=0.0)

    # Metrics
    active_annotators = Column(Integer, default=0)
    total_time_seconds = Column(Float, default=0.0)
    total_time_display = Column(String, default="0s")
    avg_rejection_rate = Column(Float, default=0.0)
    avg_tpt_seconds = Column(Float, default=0.0)
    avg_throughput = Column(Float, default=0.0)

    # Health
    health_status = Column(String, default="green")  # green, amber, red
    red_flags = Column(Integer, default=0)
    amber_flags = Column(Integer, default=0)

    # Trends (vs 7 days ago)
    rejection_rate_trend = Column(Float, default=0.0)
    throughput_trend = Column(Float, default=0.0)
    tpt_trend = Column(Float, default=0.0)

    # Full JSON blob for detailed views
    cached_json = Column(Text, default="{}")

    # Timestamps
    created_at_encord = Column(DateTime, nullable=True)
    last_synced = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class DashboardSummary(Base):
    """
    Auto-generated summary insights from the intelligence engine.
    One row per sync run — keeps history.
    """
    __tablename__ = "dashboard_summaries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    summary_text = Column(Text, default="")  # Plain English summary
    insights_json = Column(Text, default="[]")  # JSON array of insight objects
    total_projects = Column(Integer, default=0)
    total_annotators = Column(Integer, default=0)
    total_red_flags = Column(Integer, default=0)
    total_amber_flags = Column(Integer, default=0)
    generated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ─────────────────────────────────────────────
# Database Initialization
# ─────────────────────────────────────────────

def init_db():
    """Create all tables if they don't exist. Never drops existing tables."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """Dependency for FastAPI — yields a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
