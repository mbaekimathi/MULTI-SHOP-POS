#!/usr/bin/env python3
"""
Apply database schema: create DB if missing, create tables, run column migrations.

Run after `git pull` on the server (cPanel/SSH), or via GitHub Actions deploy workflow.
Loads the same .env as app.py (project root next to this file).

Usage:
  python3 migrate_db.py
  echo $?   # 0 = success
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main() -> int:
    # Match app.py: load .env before database config is read
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env")
    except ImportError:
        pass

    try:
        from database import init_schema
    except Exception as e:
        print("Failed to import database:", e, file=sys.stderr)
        return 1

    if not init_schema():
        print("Schema initialization failed (see server logs).", file=sys.stderr)
        return 1
    print("Database schema is up to date.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
