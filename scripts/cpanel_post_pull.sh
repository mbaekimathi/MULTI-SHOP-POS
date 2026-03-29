#!/usr/bin/env bash
# Run from project root after: git pull
# Applies MySQL migrations and requests a Phusion Passenger / cPanel app restart.
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

PY=""
if command -v python3 >/dev/null 2>&1; then
  PY="python3"
elif command -v python >/dev/null 2>&1; then
  PY="python"
else
  echo "python3 or python not found in PATH" >&2
  exit 1
fi

"$PY" migrate_db.py
mkdir -p tmp
touch tmp/restart.txt
echo "Database updated; Passenger restart requested (tmp/restart.txt)."
