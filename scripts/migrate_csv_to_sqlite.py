#!/usr/bin/env python3
"""
CSV -> SQLite migration for your Day 4 CSV
Maps:
  event_id/hash_id -> id
  published_at     -> date_utc
  source,title,summary,link -> same
  vendor_matches   -> matched_keywords
  risk_type        -> risk_types
If id is missing, generates sha256(title+link).
"""
import argparse, csv, sqlite3, sys, hashlib, re
from pathlib import Path

SCHEMA_SQL = (Path(__file__).resolve().parents[1] / "config" / "db_schema.sql").read_text(encoding="utf-8")

def ensure_db(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.executescript(SCHEMA_SQL)
    con.commit()
    return con

def clean_html(s: str) -> str:
    if not s: return ""
    # quick tag strip for the Google News <a> blob
    return re.sub(r"<[^>]+>", "", s).strip()

def to_record(row: dict) -> dict|None:
    # Source CSV columns:
    # event_id,published_at,source,title,summary,link,risk_type,region_guess,vendor_matches,sentiment,hash_id,ingested_at
    title = (row.get("title") or "").strip()
    link  = (row.get("link") or "").strip()
    if not title:  # require at least title + date
        return None
    date_utc = (row.get("published_at") or "").strip()
    if not date_utc:
        return None

    rid = (row.get("hash_id") or row.get("event_id") or "").strip()
    if not rid:
        h = hashlib.sha256()
        h.update(title.encode("utf-8"))
        h.update(link.encode("utf-8"))
        rid = h.hexdigest()

    summary = clean_html(row.get("summary") or "")
    source  = (row.get("source") or "").strip()
    matched_keywords = (row.get("vendor_matches") or "").strip()
    risk_types = (row.get("risk_type") or "").strip()

    return {
        "id": rid,
        "date_utc": date_utc,
        "title": title,
        "source": source,
        "link": link,
        "summary": summary,
        "matched_keywords": matched_keywords,
        "risk_types": risk_types
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="data/news_events.csv")
    ap.add_argument("--db", default="data/news.db")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    db_path  = Path(args.db)
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        sys.exit(1)

    con = ensure_db(db_path)
    cur = con.cursor()

    inserted = skipped = 0
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rec = to_record(row)
            if not rec:
                skipped += 1
                continue
            cur.execute("""
                INSERT OR IGNORE INTO news_events
                (id, date_utc, title, source, link, summary, matched_keywords, risk_types)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (rec["id"], rec["date_utc"], rec["title"], rec["source"], rec["link"], rec["summary"], rec["matched_keywords"], rec["risk_types"]))
            inserted += cur.rowcount
    con.commit()
    con.close()
    print(f"[INFO] Done. Inserted {inserted} new rows. Skipped {skipped} rows.")
if __name__ == "__main__":
    main()
