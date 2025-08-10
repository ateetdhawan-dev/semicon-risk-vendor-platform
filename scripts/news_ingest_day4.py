#!/usr/bin/env python3
"""
Day 4 — News Ingestion Script (Semiconductor Risk MVP)
- Pulls Google News RSS for semiconductor-related queries
- Filters and normalizes items
- De-duplicates by URL hash
- Appends to data/news_events.csv
Run:
  pip install feedparser python-dateutil
  python scripts/news_ingest_day4.py
"""
import os, json, csv, hashlib, datetime as dt, time, re
from urllib.parse import quote_plus
from dateutil import parser as dtparser
import feedparser

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "news_events.csv")
KEYWORDS_PATH = os.path.join(BASE_DIR, "config", "keywords.json")
SOURCES_PATH = os.path.join(BASE_DIR, "config", "news_sources.json")

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_existing_hashes(csv_path):
    hashes = set()
    if not os.path.exists(csv_path):
        return hashes
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("hash_id"):
                hashes.add(row["hash_id"])
    return hashes

def hash_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]

def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def guess_source(link: str) -> str:
    try:
        return re.sub(r"^https?://(www\.)?", "", link).split("/")[0]
    except Exception:
        return ""

def item_published(entry) -> str:
    # Try multiple fields for publish date
    for k in ("published", "updated", "pubDate"):
        if k in entry:
            try:
                dtobj = dtparser.parse(entry[k])
                return dtobj.strftime("%Y-%m-%d %H:%M:%S%z")
            except Exception:
                continue
    # Fallback: now
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+0000")

def should_exclude(text: str, excludes):
    text_l = (text or "").lower()
    return any(e.lower() in text_l for e in excludes)

def make_event_id(published_str: str, hash_id: str) -> str:
    return f"NE-{published_str[:10]}-{hash_id[:8]}"

def build_queries(include_keywords):
    return list(set(include_keywords))[:25]  # cap for now

def fetch_items(keywords, sources):
    items = []
    # Google News queries
    for kw in build_queries(keywords["include"]):
        for tmpl in sources["google_news_rss_templates"]:
            url = tmpl.format(query=quote_plus(kw))
            feed = feedparser.parse(url)
            for e in feed.entries:
                items.append((kw, e))
        time.sleep(0.2)  # be polite
    # Direct feeds (optional)
    for feed_url in sources.get("direct_feeds", []):
        feed = feedparser.parse(feed_url)
        for e in feed.entries:
            items.append(("direct", e))
    return items

def main():
    keywords = load_json(KEYWORDS_PATH)
    sources = load_json(SOURCES_PATH)
    existing_hashes = load_existing_hashes(DATA_PATH)

    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    file_exists = os.path.exists(DATA_PATH)

    with open(DATA_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "event_id","published_at","source","title","summary","link",
            "risk_type","region_guess","vendor_matches","sentiment","hash_id","ingested_at"
        ])
        if not file_exists:
            writer.writeheader()

        fetched = fetch_items(keywords, sources)
        count_new = 0
        for kw, e in fetched:
            title = normalize_text(getattr(e, "title", ""))
            summary = normalize_text(getattr(e, "summary", "") or getattr(e, "description", ""))
            link = getattr(e, "link", "")
            if not link:
                continue
            if should_exclude(f"{title} {summary}", keywords.get("exclude", [])):
                continue

            hash_id = hash_url(link)
            if hash_id in existing_hashes:
                continue

            published = item_published(e)
            source = guess_source(link)

            row = {
                "event_id": make_event_id(published, hash_id),
                "published_at": published,
                "source": source,
                "title": title,
                "summary": summary,
                "link": link,
                # To be filled by classification step (Day 8-10)
                "risk_type": "",
                "region_guess": "",
                "vendor_matches": "",
                "sentiment": "",
                "hash_id": hash_id,
                "ingested_at": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+0000")
            }
            writer.writerow(row)
            existing_hashes.add(hash_id)
            count_new += 1

    print(f"Ingestion complete. New records added: {count_new}")

if __name__ == "__main__":
    main()
