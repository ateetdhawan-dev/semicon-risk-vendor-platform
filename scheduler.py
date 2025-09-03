import os, time, json, sqlite3, datetime, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "news.db"
BACKUP_SCRIPT = ROOT / "scripts" / "backup_db.py"
INGEST = ROOT / "ingest_min.py"
TAG = ROOT / "tag_rules_min.py"
SLACK = os.getenv("SLACK_WEBHOOK_URL", "").strip()

def ts():
    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S%z")

def run(args):
    print(f"[{ts()}] [run] {args if isinstance(args,str) else ' '.join(map(str,args))}")
    r = subprocess.run(args, shell=isinstance(args,str))
    print(f"[{ts()}] [exit] code={r.returncode}")
    return r.returncode

def slack(text: str):
    if not SLACK:
        return
    try:
        import requests
        requests.post(SLACK, json={"text": text}, timeout=8)
    except Exception as e:
        print(f"[{ts()}] [slack] failed: {e}")

def alert_new_rows():
    if not DB.exists():
        print(f"[{ts()}] [alerts] no DB")
        return
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    try:
        cols = {r["name"] for r in con.execute("pragma table_info(news_events)")}
    except Exception as e:
        print(f"[{ts()}] [alerts] table error: {e}")
        con.close()
        return

    def pick(*cands):
        for c in cands:
            if c in cols:
                return c

    c_published = pick("published","published_at","date_utc")
    c_vendor    = pick("vendor","vendor_primary","primary_vendor","company")
    c_source    = pick("source")
    c_title     = pick("headline","title")
    c_url       = pick("url","link")

    if not all([c_published,c_vendor,c_source,c_title,c_url]):
        print(f"[{ts()}] [alerts] missing required columns; skipping")
        con.close()
        return

    q = f"""
      select {c_published} as published,
             {c_vendor}    as vendor,
             {c_source}    as source,
             {c_title}     as title,
             {c_url}       as url,
             coalesce(risk_type,'unclassified') as risk,
             coalesce(severity,'') as sev
      from news_events
      where datetime({c_published}) >= datetime('now','-24 hours')
      order by {c_published} desc
      limit 15
    """
    try:
        rows = list(con.execute(q))
    finally:
        con.close()

    if not rows:
        print(f"[{ts()}] [alerts] no rows in last 24h")
        return

    lines = ["*New vendor risk items (last 24h):*",""]
    for r in rows:
        vendor = r["vendor"] or "—"
        risk   = r["risk"]
        lines.append(f"🟦 *{vendor}* | {risk}  — {r['source']}\n{r['title']}\n{r['url']}")
        lines.append("")
    text = "\n".join(lines).strip()
    print(f"[{ts()}] [alerts] sending {len(rows)} items")
    slack(text)

def cycle():
    print(f"[{ts()}] cycle start")
    if BACKUP_SCRIPT.exists(): run([sys.executable, str(BACKUP_SCRIPT)])
    else: print(f"[{ts()}] [warn] no {BACKUP_SCRIPT}")

    if INGEST.exists(): run([sys.executable, str(INGEST)])
    else: print(f"[{ts()}] [warn] no {INGEST}")

    if TAG.exists(): run([sys.executable, str(TAG)])
    else: print(f"[{ts()}] [warn] no {TAG}")

    alert_new_rows()
    print(f"[{ts()}] cycle complete")

if __name__ == "__main__":
    if os.getenv("SCHEDULER_ONESHOT",""):
        cycle()
    else:
        while True:
            cycle()
            time.sleep(6*60*60)
