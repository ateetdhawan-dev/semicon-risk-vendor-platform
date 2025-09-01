# scheduler.py — tolerant scheduler that ingests, tags, and alerts
# Works with varying column names in news_events.

import os, time, sqlite3, subprocess, sys, shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
import requests

DB_PATH = os.getenv("DB_PATH", "data/news.db")
SLACK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
SLEEP_SECONDS = int(os.getenv("SCHEDULER_INTERVAL_SECONDS", str(6*60*60)))

def iso_utc(dt=None):
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

def _run(cmd_list, label):
    print(f"[ingest] running: {repr(' '.join(cmd_list))}")
    try:
        res = subprocess.run(cmd_list, capture_output=True, text=True)
        if res.stdout: print(res.stdout.rstrip())
        if res.stderr: print(res.stderr.rstrip())
        print(f"[ingest] exit code: {res.returncode}")
        return res.returncode
    except Exception as e:
        print(f"[ingest] {label} failed: {e}")
        return 1

def backup_db():
    """Prefer scripts/backup_db.py if present (prints stats), else safe copy."""
    p = Path(DB_PATH)
    if not p.exists():
        return
    backup_script = Path("scripts/backup_db.py")
    if backup_script.exists():
        _run([sys.executable, str(backup_script)], "backup_db.py")
        return
    outdir = Path("backups")
    outdir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    target = outdir / f"news-{stamp}.db"
    if not target.exists():
        try:
            shutil.copy2(p, target)
            print(f"[OK] Backup -> {target.as_posix()}")
        except Exception as e:
            print(f"[WARN] backup skipped: {e}")

def run_ingest_cycle():
    py = sys.executable
    ing_min = Path("ingest_min.py")
    tag_min = Path("tag_rules_min.py")

    # Run minimal scripts if present
    if ing_min.exists():
        _run([py, str(ing_min)], "ingest_min")
    else:
        _run([py, "-m", "src.ingest", "--days", "1", "--limit", "200"], "src.ingest")

    if tag_min.exists():
        _run([py, str(tag_min)], "tag_rules_min")
    else:
        _run([py, "-m", "src.tag_rules"], "src.tag_rules")

def get_cols(con, table):
    try:
        return {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
    except Exception:
        return set()

def pick(avail, options, default=None):
    for name in options:
        if name in avail:
            return name
    return default

def post_slack(text):
    if not SLACK_URL:
        return
    try:
        requests.post(SLACK_URL, json={"text": text}, timeout=8)
    except Exception:
        pass

def send_alerts():
    p = Path(DB_PATH)
    if not p.exists():
        print("[alerts] DB not found; skipping")
        return

    try:
        con = sqlite3.connect(p)
        con.row_factory = sqlite3.Row
        cols = get_cols(con, "news_events")
        if not cols:
            print("[alerts] table news_events not found; skipping")
            con.close()
            return

        # tolerant column mapping
        head_col      = pick(cols, ["headline","title","summary"])
        published_col = pick(cols, ["published","published_at","date_utc","date","dt"])
        vendor_col    = pick(cols, ["vendor","vendor_primary"])
        source_col    = pick(cols, ["source","source_name"])
        url_col       = pick(cols, ["url","link"])

        print("[alerts] column map:",
              f"headline={head_col}, published={published_col}, vendor={vendor_col}, source={source_col}, url={url_col}")

        if not head_col or not published_col:
            print("[alerts] need at least a headline/title and a published/date column; skipping")
            con.close()
            return

        # Build SELECT that tolerates missing fields with COALESCE('') fallbacks
        severity_expr = "severity" if "severity" in cols else "'' AS severity"
        vendor_expr   = vendor_col if vendor_col else "''"
        source_expr   = source_col if source_col else "''"
        url_expr      = url_col if url_col else "''"

        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(timespec="seconds")

        q = f"""
            SELECT
              COALESCE({vendor_expr}, '') AS vendor,
              {head_col} AS headline,
              COALESCE(risk_type,'') AS risk_type,
              {severity_expr} AS severity,
              COALESCE({source_expr}, '') AS source,
              COALESCE({url_expr}, '') AS url,
              {published_col} AS published
            FROM news_events
            WHERE {published_col} >= ?
            ORDER BY {published_col} DESC
            LIMIT 20
        """
        rows = list(con.execute(q, (since,)))
        con.close()

        if not rows:
            print("[alerts] no rows to alert in last 24h")
            return

        sev_emoji = {"Critical": "🟥", "High": "🟧"}
        lines = ["*New vendor risk items (last 24h):*",""]
        for r in rows:
            sev = (r["severity"] or "").strip()
            emoji = sev_emoji.get(sev, "🟦") if sev else "🟦"
            risk = r["risk_type"] or "—"
            vendor = r["vendor"] or "—"
            lines.append(f"{emoji} *{vendor}* | {risk} {f'({sev})' if sev else ''} — {r['source']}")
            lines.append(r["headline"])
            if r["url"]:
                lines.append(r["url"])
            lines.append("")
        msg = "\n".join(lines)
        print(msg)
        post_slack(msg)

    except Exception as e:
        print(f"[alerts] query failed: {e}")

def main():
    while True:
        print(f"[{iso_utc()}] cycle start")
        backup_db()
        run_ingest_cycle()
        send_alerts()
        print(f"[{iso_utc()}] cycle complete")
        if os.getenv("SCHEDULER_ONESHOT","0") == "1":
            break
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
