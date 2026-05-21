#!/usr/bin/env python3
"""
List all accessible Encord projects.
Use this to find project hashes to add to clients.json.

Usage:
    python list_projects.py
    python list_projects.py --search "lung"
    python list_projects.py --limit 50
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SSH_KEY_PATH = os.getenv("ENCORD_SSH_KEY_PATH", "")
ENCORD_DOMAIN = os.getenv("ENCORD_DOMAIN", "https://api.encord.com")


def main():
    parser = argparse.ArgumentParser(description="List Encord projects")
    parser.add_argument("--search", "-s", help="Filter by title (case-insensitive)")
    parser.add_argument("--limit", "-l", type=int, default=100, help="Max projects to show")
    parser.add_argument("--json", action="store_true", help="Output as JSON for clients.json")
    args = parser.parse_args()

    if not SSH_KEY_PATH or not Path(SSH_KEY_PATH).exists():
        print(f"Error: ENCORD_SSH_KEY_PATH not set or file not found")
        sys.exit(1)

    from encord import EncordUserClient

    print(f"Connecting to Encord ({ENCORD_DOMAIN})...")
    client = EncordUserClient.create_with_ssh_private_key(
        ssh_private_key_path=SSH_KEY_PATH,
        domain=ENCORD_DOMAIN,
    )

    print("Fetching projects...")
    projects = list(client.list_projects())
    print(f"Found {len(projects)} projects total\n")

    # Filter if search term provided
    if args.search:
        search_lower = args.search.lower()
        projects = [p for p in projects if search_lower in p.title.lower()]
        print(f"Filtered to {len(projects)} projects matching '{args.search}'\n")

    # Limit results
    projects = projects[:args.limit]

    if args.json:
        import json
        output = []
        for p in projects:
            output.append({
                "hash": p.project_hash,
                "name": p.title,
                "modality": "Mixed"
            })
        print(json.dumps(output, indent=2))
    else:
        print(f"{'#':<3} {'Project Hash':<40} {'Title':<50} {'Status':<15}")
        print("-" * 110)
        for i, p in enumerate(projects, 1):
            status = str(p.status).replace("ProjectStatus.", "")
            title = p.title[:48] + ".." if len(p.title) > 50 else p.title
            print(f"{i:<3} {p.project_hash:<40} {title:<50} {status:<15}")

        print(f"\n{'─' * 60}")
        print("To add a project to clients.json, copy its hash and add to the projects array.")
        print("Example:")
        print('  {"hash": "abc123...", "name": "Project Name", "modality": "CT"}')


if __name__ == "__main__":
    main()
