#!/usr/bin/env python
import os, time, sqlite3, subprocess, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd

try:
    import requests
except Exception:
    # requests should already be in requirements; this is just a guard
    requests = None

DB_PATH   = os.getenv("DB_PATH", "data/news.db")
SLACK_URL = (os.getenv("SLACK_WEBHOOK_URL") or "").strip()

def _slack(msg: str) -> None:
    """Post to Slack if webhook is configured; otherwise do nothing."""
    if not SLACK_URL or not requests:
        return
    try:
        requests.post(SLACK_URL, json={"text": msg}, timeout=8)
    except Exception:
        # Never crash the loop on Slack errors
        pass

def run_ingest() -> None:
    """
    Try to run your existing ingest/tagging.
    1) Prefer Python modules (works in containers/venv)
    2) Fall back to run_ingest.cmd if present (Windows convenience)
    """
    cmds = [
        [sys.executable, "-m", "src.ingest", "--days", "1", "--limit", "200"],
        [sys.executable, "-m", "src.tag_rules"],
    ]
    ran_any = False
    for cmd in cmds:
        try:
            print(f"[ingest] running: {' '.join(cmd)}", flush=True)
            rc = subprocess.call(cmd)
            print(f"[ingest] exit code: {rc}", flush=True)
            ran_any = True
        except Exception as e:
            print(f"[ingest] failed: {e}", flush=True)

    if not ran_any:
        cmd = Path("run_ingest.cmd")
        if cmd.exists():
            print(f"[ingest] fallback to {cmd}", flush=True)
            os.system(f'"{cmd}"')

def _pick(cols, candidates):
    for c in candidates:
        if c in cols:
            return c
    return None

def alert_new_high_risk() -> None:
    """
    Look for High/Critical news in the last 24h and post a compact Slack alert.
    Handles schema variations (id/hash_id, published/published_at, headline/title, severity/risk_severity, vendor/vendor_primary).
    """
    db = Path(DB_PATH)
    if not db.exists():
        print(f"[alerts] DB not found at {DB_PATH}", flush=True)
        return

    con = sqlite3.connect(db)
    try:
        # Ensure news_events exists
        exists = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='news_events'"
        ).fetchone()
        if not exists:
            print("[alerts] table news_events not found; skipping", flush=True)
            return

        # Peek columns
        empty = pd.read_sql_query("SELECT * FROM news_events LIMIT 0", con)
        cols = list(empty.columns)

        idc    = _pick(cols, ["id", "hash_id"])
        datec  = _pick(cols, ["published", "published_at", "date_utc", "dt"])
        headc  = _pick(cols, ["headline", "title"])
        sevc   = _pick(cols, ["severity", "risk_severity"])
        vendc  = _pick(cols, ["vendor", "vendor_primary", "supplier"])

        if not (datec and headc and sevc):
            print("[alerts] missing required columns; skipping", flush=True)
            return

        selects = []
        if idc:   selects.append(f"{idc} AS id")
        selects.append(f"{datec} AS dt")
        selects.append(f"{headc} AS headline")
        selects.append(f"{sevc} AS severity")
        if vendc: selects.append(f"{vendc} AS vendor")
        q = f"""
        SELECT {', '.join(selects)}
        FROM news_events
        WHERE COALESCE({sevc},'') <> ''
          AND LOWER({sevc}) IN ('high','critical')
        ORDER BY {datec} DESC
        LIMIT 50
        """
        df = pd.read_sql_query(q, con)
        if df.empty:
            print("[alerts] no high/critical rows", flush=True)
            return

        # Filter to last 24h safely
        def _parse_dt(x):
            try:
                return pd.to_datetime(x, utc=True)
            except Exception:
                return pd.NaT

        df["dt_parsed"] = df["dt"].apply(_parse_dt)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        df = df[df["dt_parsed"] >= cutoff]
        if df.empty:
            print("[alerts] none in last 24h", flush=True)
            return

        lines = ["*New High/Critical risk items (last 24h):*"]
        for _, r in df.head(10).iterrows():
            v = r.get("vendor") if "vendor" in df.columns else None
            v = v if (isinstance(v, str) and v) else "—"
            sev = str(r["severity"])
            head = str(r["headline"])
            lines.append(f"- [{v}] {sev}: {head}")

        msg = "\n".join(lines)
        print(msg, flush=True)
        _slack(msg)
    finally:
        con.close()

def _loop_once() -> None:
    run_ingest()
    alert_new_high_risk()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] cycle complete", flush=True)

def main() -> None:
    one_shot = (os.getenv("SCHEDULER_ONESHOT") or "").lower() in ("1","true","yes","y")
    if one_shot:
        _loop_once()
        return

    _slack(":rocket: Scheduler started")
    while True:
        try:
            _loop_once()
        except Exception as e:
            msg = f":warning: Scheduler error: {e}"
            print(msg, flush=True)
            _slack(msg)
        time.sleep(6 * 60 * 60)  # every 6 hours

if __name__ == "__main__":
    main()
