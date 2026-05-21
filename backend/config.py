"""
Encord Operations Dashboard — Configuration
============================================
Loads settings from .env file. All credentials are environment-based,
never hardcoded.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""

    # Encord SDK
    ENCORD_SSH_KEY_PATH: str = os.getenv("ENCORD_SSH_KEY_PATH", "")
    ENCORD_DOMAIN: str = os.getenv("ENCORD_DOMAIN", "https://api.encord.com")
    ENCORD_PROJECT_HASHES: list[str] = [
        h.strip()
        for h in os.getenv("ENCORD_PROJECT_HASHES", "").split(",")
        if h.strip()
    ]

    # Outlier thresholds (configurable via .env)
    # Rejection rate: flag if annotator rate > project_avg + this margin
    REJECTION_RATE_MARGIN: float = float(os.getenv("REJECTION_RATE_MARGIN", "0.10"))
    # TPT: flag if annotator TPT < (median * this) or > (median * (1 + this))
    TPT_LOW_THRESHOLD: float = float(os.getenv("TPT_LOW_THRESHOLD", "0.20"))
    TPT_HIGH_THRESHOLD: float = float(os.getenv("TPT_HIGH_THRESHOLD", "0.50"))
    # Throughput: flag if annotator throughput > this % below median
    THROUGHPUT_LOW_THRESHOLD: float = float(os.getenv("THROUGHPUT_LOW_THRESHOLD", "0.20"))

    # Scheduler / Sync
    SYNC_INTERVAL_MINUTES: int = int(os.getenv("SYNC_INTERVAL_MINUTES", "10"))
    MAX_PARALLEL_SYNCS: int = int(os.getenv("MAX_PARALLEL_SYNCS", "5"))
    INCREMENTAL_SYNC: bool = os.getenv("INCREMENTAL_SYNC", "true").lower() == "true"
    STALE_DATA_MINUTES: int = int(os.getenv("STALE_DATA_MINUTES", "30"))

    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./dashboard.db")

    # API
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))


settings = Settings()
