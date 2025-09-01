#!/usr/bin/env bash
set -e
file="pages/02_Commercial_KPI.py"
backup="backups/02_Commercial_KPI.py"
sha="backups/02_Commercial_KPI.sha1"

if [ -f "$backup" ]; then
  cp -f "$backup" "$file"
  echo "Restored from backups/."
else
  echo "No local backup found; restoring from tag 'commercial-stable'..."
  git checkout commercial-stable -- "$file"
fi

# relock
attrib +R "pages\\02_Commercial_KPI.py" 2>/dev/null || true
git update-index --skip-worktree "$file"
echo "✅ Restored and re-locked."
