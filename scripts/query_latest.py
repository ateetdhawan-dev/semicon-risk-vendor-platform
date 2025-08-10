#!/usr/bin/env python3
"""
Query helper for SQLite
Usage examples:
  python scripts/query_latest.py --limit 10
  python scripts/query_latest.py --risk Geopolitical --limit 5
  python scripts/query_latest.py --q "TSMC" --limit 5
"""
import argparse, sqlite3, sys
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/news.db")
    ap.add_argument("--risk", help="Filter by risk type (e.g., Geopolitical, Vendor, Material)")
    ap.add_argument("--q", help="Full-text LIKE filter over title/summary/source")
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[ERROR] DB not found: {db_path}. Run migration or ingest first.")
        sys.exit(1)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    where = []
    params = []
    if args.risk:
        where.append("instr(risk_types, ?) > 0")
        params.append(args.risk)
    if args.q:
        where.append("(title LIKE ? OR summary LIKE ? OR source LIKE ?)")
        like = f"%{args.q}%"
        params.extend([like, like, like])

    sql = "SELECT date_utc, source, title, risk_types, link FROM news_events"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY date_utc DESC LIMIT ?"
    params.append(args.limit)

    for row in cur.execute(sql, params):
        print(f"- [{row['date_utc']}] ({row['risk_types']}) {row['source']}: {row['title']}")
        print(f"  {row['link']}")
    con.close()

if __name__ == "__main__":
    main()
