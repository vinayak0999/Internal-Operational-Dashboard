"""
Encord Operations Dashboard — SDK Client Wrapper
=================================================
READ-ONLY wrapper around the Encord SDK.

⛔ CRITICAL DATA SAFETY RULE ⛔
===============================
This module ONLY uses READ operations from the Encord SDK:
  - list_projects()
  - get_project()
  - list_label_rows_v2()
  - list_time_spent()
  - list_collaborator_timers()
  - get_label_logs()

The following methods are NEVER called:
  - save(), submit(), reject(), approve(), delete(), remove()
  - workflow_complete(), workflow_reopen()
  - Any method that modifies data on the Encord platform
"""

import re
import logging
from datetime import datetime, timezone, timedelta
from encord import EncordUserClient

from config import settings

logger = logging.getLogger(__name__)


def _derive_workspace(creator_email: str) -> str:
    """
    Derive client workspace name from creator_email.

    Patterns:
      guest+<name>@encord.com  →  <Name>   (client workspace)
      *@encord.com             →  Encord (Internal)
      *@clientdomain.com       →  Clientdomain  (from email domain)
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


class EncordReadOnlyClient:
    """
    Read-only wrapper around Encord SDK.
    Exposes ONLY data-fetching methods. No write operations.
    """

    def __init__(self):
        self._client = None

    def _get_client(self) -> EncordUserClient:
        """Lazily initialize the Encord client."""
        if self._client is None:
            if not settings.ENCORD_SSH_KEY_PATH:
                raise ValueError(
                    "ENCORD_SSH_KEY_PATH not set. "
                    "Copy .env.example to .env and fill in your credentials."
                )
            self._client = EncordUserClient.create_with_ssh_private_key(
                ssh_private_key_path=settings.ENCORD_SSH_KEY_PATH,
                domain=settings.ENCORD_DOMAIN,
            )
            logger.info("Encord client initialized (domain: %s)", settings.ENCORD_DOMAIN)
        return self._client

    # ─────────────────────────────────────────
    # Project Operations (READ-ONLY)
    # ─────────────────────────────────────────

    def list_all_projects(self) -> list:
        """List all projects accessible to the authenticated user."""
        client = self._get_client()
        try:
            projects = list(client.list_projects())
            logger.info("Fetched %d projects from Encord", len(projects))
            return projects
        except Exception as e:
            logger.error("Failed to list projects: %s", e)
            return []

    def list_all_projects_with_workspace(self) -> list:
        """
        Auto-discover all projects with workspace derived from creator_email.
        Returns list of dicts: {project_hash, title, workspace, creator_email}
        """
        client = self._get_client()
        try:
            projects = list(client.list_projects())
            result = []
            for p in projects:
                if isinstance(p, dict):
                    ph = p.get('project', {}).get('project_hash', '')
                    title = p.get('project', {}).get('title', '')
                    creator = ''
                else:
                    ph = str(getattr(p, 'project_hash', ''))
                    title = str(getattr(p, 'title', ''))
                    creator = str(getattr(p, 'creator_email', '') or '')
                workspace = _derive_workspace(creator)
                if ph:
                    result.append({
                        'project_hash': ph,
                        'title': title,
                        'workspace': workspace,
                        'creator_email': creator,
                    })
            logger.info(
                "Discovered %d projects across %d workspaces",
                len(result),
                len(set(r['workspace'] for r in result)),
            )
            return result
        except Exception as e:
            logger.error("Failed to list projects with workspace: %s", e)
            return []

    def get_project(self, project_hash: str):
        """Get a single project by its hash."""
        client = self._get_client()
        try:
            project = client.get_project(project_hash)
            logger.info("Fetched project: %s (%s)", project.title, project_hash)
            return project
        except Exception as e:
            logger.error("Failed to get project %s: %s", project_hash, e)
            return None

    # ─────────────────────────────────────────
    # Label Operations (READ-ONLY)
    # ─────────────────────────────────────────

    def get_label_rows(self, project, **filters):
        """
        Fetch label rows from a project.
        Supports filters: workflow_graph_node_title_eq, data_title_eq, etc.
        Labels are initialized but NEVER saved back.
        """
        try:
            label_rows = project.list_label_rows_v2(**filters)
            logger.info(
                "Fetched %d label rows from project %s",
                len(label_rows), project.project_hash
            )
            return label_rows
        except Exception as e:
            logger.error("Failed to get label rows: %s", e)
            return []

    def initialize_labels_bulk(self, project, label_rows, bundle_size=100):
        """
        Initialize label data in bulk using bundles.
        This is a READ operation — it loads label data into memory.
        Labels are NEVER saved back to Encord.
        """
        try:
            with project.create_bundle(bundle_size=bundle_size) as bundle:
                for lr in label_rows:
                    lr.initialise_labels(bundle=bundle)
            logger.info("Initialized %d label rows", len(label_rows))
            return label_rows
        except Exception as e:
            logger.error("Failed to initialize labels: %s", e)
            return label_rows

    # ─────────────────────────────────────────
    # Time & Activity Operations (READ-ONLY)
    # ─────────────────────────────────────────

    def get_time_entries(self, project, start_date=None, end_date=None) -> list:
        """
        Fetch time spent data from a project.
        Returns list of time entries with annotator info and durations.
        """
        if start_date is None:
            start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        if end_date is None:
            end_date = datetime.now(timezone.utc) + timedelta(days=1)
        try:
            entries = list(project.list_time_spent(start=start_date, end=end_date))
            logger.info(
                "Fetched %d time entries from project %s",
                len(entries), project.project_hash
            )
            return entries
        except Exception as e:
            logger.error("Failed to get time entries: %s", e)
            return []

    def get_collaborator_timers(self, project, weeks_back=4) -> list:
        """
        Fetch collaborator session timers.
        Returns session-level time tracking data.
        """
        try:
            after = datetime.now(timezone.utc) - timedelta(weeks=weeks_back)
            timers = list(project.list_collaborator_timers(after=after))
            logger.info(
                "Fetched %d collaborator timers from project %s",
                len(timers), project.project_hash
            )
            return timers
        except Exception as e:
            logger.error("Failed to get collaborator timers: %s", e)
            return []

    def get_workflow_stages(self, project) -> list:
        """Get all workflow stages for a project."""
        try:
            stages = list(project.workflow.stages)
            logger.info(
                "Fetched %d workflow stages from project %s",
                len(stages), project.project_hash
            )
            return stages
        except Exception as e:
            logger.error("Failed to get workflow stages: %s", e)
            return []

    def get_label_row_workflow_info(self, label_rows) -> dict:
        """
        Extract workflow status info from label rows.
        Returns dict of data_hash -> {workflow_stage, annotation_status, etc.}
        """
        info = {}
        for lr in label_rows:
            info[lr.data_hash] = {
                "data_hash": lr.data_hash,
                "data_title": lr.data_title,
                "workflow_graph_node": getattr(lr, "workflow_graph_node", None),
                "is_labelling_initialised": lr.is_labelling_initialised,
                "created_at": str(getattr(lr, "created_at", "")),
                "last_edited_at": str(getattr(lr, "last_edited_at", "")),
            }
        return info


# Singleton instance
encord_client = EncordReadOnlyClient()
