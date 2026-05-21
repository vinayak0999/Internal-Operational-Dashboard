"""
Test script for project.get_task_actions()
==========================================
Tests the analytics endpoint that should match the Encord dashboard.
"""

import os
from datetime import datetime, timezone
from collections import Counter, defaultdict
from dotenv import load_dotenv

load_dotenv()

SSH_KEY_PATH = os.getenv("ENCORD_SSH_KEY_PATH", "")
ENCORD_DOMAIN = os.getenv("ENCORD_DOMAIN", "https://api.encord.com")
PROJECT_HASH = os.getenv("ENCORD_PROJECT_HASHES", "").split(",")[0].strip()

print(f"SSH Key: {SSH_KEY_PATH}")
print(f"Domain:  {ENCORD_DOMAIN}")
print(f"Project: {PROJECT_HASH}")
print()

# ─── Connect ───
from encord import EncordUserClient

client = EncordUserClient.create_with_ssh_private_key(
    ssh_private_key_path=SSH_KEY_PATH,
    domain=ENCORD_DOMAIN,
)
print("✓ Connected to Encord")

project = client.get_project(PROJECT_HASH)
print(f"✓ Loaded project: {project.title}")
print()

# ─── Call get_task_actions() ───
print("Calling project.get_task_actions()...")
print("Date range: 2026-03-12 to 2026-04-10")
print()

try:
    actions = list(project.get_task_actions(
        after=datetime(2026, 3, 12, tzinfo=timezone.utc),
        before=datetime(2026, 4, 10, 23, 59, 59, tzinfo=timezone.utc),
    ))

    print(f"✓ Got {len(actions)} task actions")
    print()

    # Count by action type
    by_type = Counter()
    by_user = defaultdict(lambda: Counter())
    by_day = defaultdict(lambda: Counter())

    for a in actions:
        action_type = a.action_type.value
        by_type[action_type] += 1
        by_user[a.actor_email][action_type] += 1
        day = a.timestamp.strftime("%Y-%m-%d")
        by_day[day][action_type] += 1

    print("=== ACTION TYPES ===")
    for k, v in by_type.most_common():
        print(f"  {k:20s}: {v}")

    print()
    print("=== PER USER (top 5) ===")
    for email in sorted(by_user.keys())[:5]:
        counts = by_user[email]
        print(f"  {email}: {dict(counts)}")

    print()
    print("=== DAILY (first 5 days) ===")
    for day in sorted(by_day.keys())[:5]:
        counts = by_day[day]
        print(f"  {day}: {dict(counts)}")

    print()
    print("=== SAMPLE ACTION ===")
    if actions:
        a = actions[0]
        print(f"  timestamp:    {a.timestamp}")
        print(f"  action_type:  {a.action_type}")
        print(f"  actor_email:  {a.actor_email}")
        print(f"  task_uuid:    {a.task_uuid}")
        print(f"  data_unit_uuid: {a.data_unit_uuid}")
        print(f"  project_uuid: {a.project_uuid}")
        print(f"  workflow_stage_uuid: {a.workflow_stage_uuid}")

except Exception as e:
    print(f"✗ FAILED: {type(e).__name__}: {e}")
    print()
    print("This error means the API key doesn't have permission")
    print("to access the /v2/analytics/task-actions endpoint.")
    print()
    print("To fix: Create an organisation-level API key in Encord")
    print("Settings → API Keys → Create Key (with Organisation scope)")
