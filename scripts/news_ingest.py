import hashlib, feedparser, json, sqlite3, re
from datetime import datetime
from dateutil import parser as dtp

DB = "data/news.db"

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def hash_id(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def classify(entry, vendors, geo_terms, mat_terms):
    text = " ".join([
        entry.get("title",""),
        entry.get("summary","")
    ]).lower()
    vm = [v for v in vendors if re.search(r'\b'+re.escape(v.lower())+r'\b', text)]
    risk = set()
    if any(t in text for t in geo_terms): risk.add("geopolitical")
    if any(t in text for t in mat_terms): risk.add("material")
    if vm and not risk: risk.add("vendor")
    return ", ".join(vm), ", ".join(sorted(risk)) if risk else ""

def main():
    cfg = load_json("config/news_sources.json")
    kw  = load_json("config/keywords.json")
    vendors   = [v.lower() for v in kw.get("vendors",[])]
    geo_terms = [t.lower() for t in kw.get("geopolitical_terms",[])]
    mat_terms = [t.lower() for t in kw.get("materials_terms",[])]

    con = sqlite3.connect(DB)
    cur = con.cursor()

    # Ensure table exists
    cur.execute("""
    CREATE TABLE IF NOT EXISTS news_events (
      hash_id TEXT PRIMARY KEY,
      published_at TEXT,
      title TEXT,
      source TEXT,
      link TEXT,
      summary TEXT,
      vendor_matches TEXT,
      risk_type TEXT
    )
    """)

    for url in cfg.get("google_news_rss", []):
        feed = feedparser.parse(url)
        for e in feed.entries:
            title = e.get("title","").strip()
            link  = e.get("link","").strip()
            source = (e.get("source",{}) or {}).get("title","") or e.get("author","") or "Google News"
            summary = re.sub(r'<[^>]+>','', e.get("summary","")).strip()
            published_at = ""
            if "published" in e:
                try:
                    published_at = dtp.parse(e.published).isoformat()
                except Exception:
                    pass
            if not published_at:
                published_at = datetime.utcnow().isoformat()

            hid = hash_id(title + link)
            vm, risk = classify({"title":title,"summary":summary}, vendors, geo_terms, mat_terms)

            cur.execute("""
            INSERT OR IGNORE INTO news_events
            (hash_id, published_at, title, source, link, summary, vendor_matches, risk_type)
            VALUES (?,?,?,?,?,?,?,?)
            """, (hid, published_at, title, source, link, summary, vm, risk))
    con.commit()

    # Create/refresh view
    cur.execute("DROP VIEW IF EXISTS news")
    cur.execute("""
    CREATE VIEW news AS
    SELECT
      hash_id AS id,
      published_at AS date_utc,
      title, source, link, summary,
      COALESCE(vendor_matches,'') AS matched_keywords,
      COALESCE(risk_type,'') AS risk_types
    FROM news_events
    """)
    con.commit()
    con.close()
    print("[OK] Ingest complete.")

if __name__ == "__main__":
    main()
