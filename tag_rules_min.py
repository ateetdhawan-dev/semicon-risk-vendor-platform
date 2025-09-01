# tag_rules_min.py — lightweight tagger for news_events

import sqlite3, re
from pathlib import Path

DB_PATH = "data/news.db"

CATS = [
    # (regex, risk_type, severity)
    (r"\b(export control|sanction|tariff|embargo|BIS|ITAR|geopolitic|licen[cs]e ban)\b", "Export/Geo", "Critical"),
    (r"\b(earthquake|flood|typhoon|fire|explosion|outage|power cut|facility shutdown)\b", "Operations/BCP", "High"),
    (r"\b(recall|yield issue|defect|quality problem|RMA|field failure)\b", "Product/Quality", "High"),
    (r"\b(antitrust|lawsuit|litigation|regulator|fine|SEC|DOJ|probe|investigation)\b", "Legal/Compliance", "High"),
    (r"\b(shortage|supply disruption|backlog|lead[-\s]?time|logistics|port delay)\b", "Supply Chain", "High"),
    (r"\b(downgrade|guidance cut|profit warning|misses estimates|liquidity)\b", "Financial", "Medium"),
]

def ensure_columns(con):
    cols = {r[1] for r in con.execute("PRAGMA table_info(news_events)")}
    if "risk_type" not in cols:
        con.execute("ALTER TABLE news_events ADD COLUMN risk_type TEXT")
    if "severity" not in cols:
        con.execute("ALTER TABLE news_events ADD COLUMN severity TEXT")
    con.commit()

def tag_text(txt: str):
    t = txt.lower()
    for pat, rt, sev in CATS:
        if re.search(pat, t, flags=re.I):
            return rt, sev
    return None, None

def main():
    p = Path(DB_PATH)
    if not p.exists():
        print("[tag] DB not found")
        return

    con = sqlite3.connect(p)
    con.row_factory = sqlite3.Row

    # basic existence
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "news_events" not in tables:
        print("[tag] news_events table not found")
        con.close()
        return

    ensure_columns(con)

    # tolerate headline/title
    cols = {r[1] for r in con.execute("PRAGMA table_info(news_events)")}
    head_col = "headline" if "headline" in cols else ("title" if "title" in cols else None)
    if not head_col:
        print("[tag] no headline/title column; nothing to tag")
        con.close()
        return

    cur = con.cursor()
    cur.execute(f"""
        SELECT rowid, {head_col} AS headline, COALESCE(risk_type,'') AS risk_type, COALESCE(severity,'') AS severity
        FROM news_events
        WHERE (risk_type IS NULL OR risk_type='')
           OR (severity IS NULL OR severity='')
        LIMIT 10000
    """)
    rows = cur.fetchall()

    updated = 0
    for r in rows:
        rt, sev = tag_text(r["headline"] or "")
        if not rt and not sev:
            continue
        if not r["risk_type"] and rt:
            con.execute("UPDATE news_events SET risk_type=? WHERE rowid=?", (rt, r["rowid"]))
            updated += 1
        if not r["severity"] and sev:
            con.execute("UPDATE news_events SET severity=? WHERE rowid=?", (sev, r["rowid"]))
            updated += 1

    con.commit()
    con.close()
    print(f"[tag] updated {updated} fields")

if __name__ == "__main__":
    main()
