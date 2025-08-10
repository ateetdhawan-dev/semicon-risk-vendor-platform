#!/usr/bin/env python3
import sqlite3, json, re
from pathlib import Path

DB_PATH = Path("data/news.db")
KEYWORDS_FILE = Path("config/keywords.json")

def load_keywords():
    with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    # compile a per-category list for quick matching
    compiled = {}
    for cat, terms in cfg.items():
        compiled[cat] = [re.compile(r"\b" + re.escape(t.lower()) + r"\b", re.I) for t in terms]
    # flat list for matched_keywords
    flat_terms = sorted({t.lower() for terms in cfg.values() for t in terms})
    flat_regexes = [re.compile(r"\b" + re.escape(t) + r"\b", re.I) for t in flat_terms]
    return compiled, flat_terms, flat_regexes

def classify(title, summary, source, compiled, flat_terms, flat_regexes):
    text = " ".join([(title or ""), (summary or ""), (source or "")]).lower()
    # risk_types
    risk_hits = []
    for cat, regexes in compiled.items():
        if any(rx.search(text) for rx in regexes):
            risk_hits.append(cat)
    risk_types = ";".join(sorted(set(risk_hits)))

    # matched_keywords (flat terms actually found)
    kw_hits = [flat_terms[i] for i, rx in enumerate(flat_regexes) if rx.search(text)]
    matched_keywords = ";".join(sorted(set(kw_hits)))
    return matched_keywords, risk_types

def main():
    if not DB_PATH.exists():
        print(f"[ERROR] Database not found: {DB_PATH}")
        return

    compiled, flat_terms, flat_regexes = load_keywords()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # rows missing either field
    cur.execute("""
        SELECT id, title, summary, source, matched_keywords, risk_types
        FROM news_events
        WHERE COALESCE(TRIM(matched_keywords),'')='' OR COALESCE(TRIM(risk_types),'')=''
    """)
    rows = cur.fetchall()

    updated = 0
    for r in rows:
        mk, rt = classify(r["title"], r["summary"], r["source"], compiled, flat_terms, flat_regexes)
        # only update if we actually found something new
        new_mk = mk if (r["matched_keywords"] or "").strip()=="" else r["matched_keywords"]
        new_rt = rt if (r["risk_types"] or "").strip()=="" else r["risk_types"]
        if new_mk != (r["matched_keywords"] or "") or new_rt != (r["risk_types"] or ""):
            cur.execute(
                "UPDATE news_events SET matched_keywords = ?, risk_types = ? WHERE id = ?",
                (new_mk, new_rt, r["id"])
            )
            updated += 1

    con.commit()
    con.close()
    print(f"[INFO] Updated {updated} rows.")
if __name__ == "__main__":
    main()
