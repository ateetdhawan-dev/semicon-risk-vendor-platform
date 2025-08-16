#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# activate venv
source .venv/Scripts/activate
# ingest latest feeds
python scripts/news_ingest.py
# (optional) reclassify with your latest rules
if [ -f scripts/reclassify_db.py ]; then
  python scripts/reclassify_db.py
fi
# recompute primary fields if you want to use them under flag later
if [ -f scripts/reclassify_primary.py ]; then
  python scripts/reclassify_primary.py
fi
echo "[OK] Ingest + reclassify complete."
