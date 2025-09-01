#!/usr/bin/env bash
set -e
file="pages/02_Commercial_KPI.py"
# remove Windows read-only + let git see changes again
attrib -R "pages\\02_Commercial_KPI.py" 2>/dev/null || true
git update-index --no-skip-worktree "$file"
echo "✏️  Unlocked $file. Make edits on a *new branch*, commit, then run ./lock_commercial.sh"
