#!/usr/bin/env bash
set -e
file="pages/02_Commercial_KPI.py"
[ -f "$file" ] || { echo "Missing $file"; exit 1; }
mkdir -p backups
cp -f "$file" backups/02_Commercial_KPI.py
git hash-object "$file" > backups/02_Commercial_KPI.sha1
# Windows read-only + git ignore changes
attrib +R "pages\\02_Commercial_KPI.py" 2>/dev/null || true
git update-index --skip-worktree "$file"
echo "🔒 Locked $file (backup + checksum written)."
