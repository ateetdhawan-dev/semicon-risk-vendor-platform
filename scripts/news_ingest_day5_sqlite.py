#!/usr/bin/env python3
"""
Day 5: ingest -> SQLite (replaces CSV append)
Usage:
  python scripts/news_ingest_day5_sqlite.py
"""
from pathlib import Path
import json, re, hashlib, sys
from datetime import datetime, timezone
import sqlite3
import feedparser
from dateutil import parser as dtparser

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "news.db"

SOURCES_FILE = CONFIG_DIR / "news_sources.json"
KEYWORDS_FILE = CONFIG_DIR / "keywords.json"
SCHEMA_FILE = CONFIG_DIR / "db_schema.sql"

def load_json(path: Path):
    if not path.exists():
        print(f"[ERROR] Missing required file: {path}")
        sys.exit(1)
    return json.loads(path.read_text(encoding="utf-8"))

def compile_keyword_patterns(kw_config):
    all_kw = set()
    for v in kw_config.values():
        if isinstance(v, list):
            all_kw.update(v)
    if not all_kw:
        return re.compile("$^", flags=re.I), set()
    pat = r"(" + "|".join(re.escape(k) for k in sorted(all_kw, key=len, reverse=True)) + r")"
    return re.compile(pat, flags=re.IGNORECASE), all_kw

def extract_risk_types(text, kw_config):
    matched_types = set()
    text_l = text.lower()
    for rtype, kws in kw_config.items():
        if not isinstance(kws, list):
            continue
        for k in kws:
            if k.lower() in text_l:
                matched_types.add(rtype)
                break
    return ";".join(sorted(matched_types))

def normalize_date(entry):
    for key in ("published", "updated", "created"):
        val = entry.get(key)
        if val:
            try:
                dt = dtparser.parse(val)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat()
            except Exception:
                pass
    return datetime.now(timezone.utc).isoformat()

def source_name_from_entry(entry, default):
    src = entry.get("source")
    if isinstance(src, dict) and src.get("title"):
        return src["title"]
    if entry.get("publisher"):
        return entry["publisher"]
    if entry.get("author"):
        return entry["author"]
    return default

def hash_row(title, link):
    h = hashlib.sha256()
    h.update((title or "").encode("utf-8"))
    h.update((link or "").encode("utf-8"))
    return h.hexdigest()

def ensure_db(db_path: Path):
    con = sqlite3.connect(db_path)
    schema = SCHEMA_FILE.read_text(encoding="utf-8")
    con.executescript(schema)
    con.commit()
    return con

def main():
    print("[INFO] Starting Day 5 ingest to SQLite")
    sources = load_json(SOURCES_FILE)
    kw_config = load_json(KEYWORDS_FILE)
    feeds = sources.get("feeds", [])
    if not feeds:
        print("[ERROR] No feeds in config/news_sources.json under 'feeds' key.")
        sys.exit(1)

    kw_regex, _ = compile_keyword_patterns(kw_config)
    con = ensure_db(DB_PATH)
    cur = con.cursor()

    added = 0
    for url in feeds:
        print(f"[INFO] Fetching: {url}")
        parsed = feedparser.parse(url)
        default_source = parsed.feed.get("title", "Unknown")
        for entry in parsed.entries:
            title = (entry.get("title") or "").strip()
            summary = (entry.get("summary") or entry.get("description") or "").strip()
            link = (entry.get("link") or "").strip()
            blob = f"{title}\n{summary}"
            if not kw_regex.findall(blob):
                continue

            rid = hash_row(title, link)
            date_utc = normalize_date(entry)
            source = source_name_from_entry(entry, default=default_source)
            risk_types = extract_risk_types(blob, kw_config)
            matched_keywords = ";".join(sorted(set(m.strip() for m in kw_regex.findall(blob))))

            cur.execute("""
                INSERT OR IGNORE INTO news_events
                (id, date_utc, title, source, link, summary, matched_keywords, risk_types)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (rid, date_utc, title, source, link, summary, matched_keywords, risk_types))
            added += cur.rowcount
    con.commit()
    con.close()
    print(f"[INFO] Done. Inserted {added} new rows into {DB_PATH}")

if __name__ == "__main__":
    main()
