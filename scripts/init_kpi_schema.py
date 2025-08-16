import sqlite3, os
os.makedirs("data", exist_ok=True)
con = sqlite3.connect("data/news.db")
cur = con.cursor()

# --- Core entities ---
cur.execute("""
CREATE TABLE IF NOT EXISTS companies (
  id   INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  type TEXT CHECK(type IN ('vendor','customer','both')) NOT NULL DEFAULT 'both'
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS relationships (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_id   INTEGER NOT NULL,
  customer_id INTEGER NOT NULL,
  UNIQUE(vendor_id, customer_id),
  FOREIGN KEY(vendor_id)   REFERENCES companies(id),
  FOREIGN KEY(customer_id) REFERENCES companies(id)
);
""")

# --- KPI dictionaries ---
cur.execute("""
CREATE TABLE IF NOT EXISTS kpi_definitions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  key          TEXT UNIQUE NOT NULL,   -- e.g., 'tool_availability_pct'
  display_name TEXT NOT NULL,          -- e.g., 'Tool Availability'
  unit         TEXT,                   -- e.g., '%', 'units', 'hours'
  description  TEXT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name        TEXT NOT NULL,           -- e.g., 'ASML Q2 2025 Report'
  url         TEXT,
  type        TEXT,                    -- e.g., 'report','transcript','internal','manual'
  accessed_at TEXT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS kpi_values (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  relationship_id INTEGER NOT NULL,
  kpi_id         INTEGER NOT NULL,
  period_start   TEXT NOT NULL,        -- YYYY-MM-DD
  period_end     TEXT NOT NULL,        -- YYYY-MM-DD
  value          REAL NOT NULL,
  source_id      INTEGER,
  notes          TEXT,
  UNIQUE(relationship_id, kpi_id, period_start, period_end),
  FOREIGN KEY(relationship_id) REFERENCES relationships(id),
  FOREIGN KEY(kpi_id)         REFERENCES kpi_definitions(id),
  FOREIGN KEY(source_id)      REFERENCES sources(id)
);
""")

# --- Views ---
cur.execute("DROP VIEW IF EXISTS kpi_latest")
cur.execute("""
CREATE VIEW kpi_latest AS
SELECT kv.relationship_id, kv.kpi_id, kv.value, kv.period_end
FROM kpi_values kv
JOIN (
  SELECT relationship_id, kpi_id, MAX(period_end) AS max_end
  FROM kpi_values
  GROUP BY relationship_id, kpi_id
) m
  ON m.relationship_id = kv.relationship_id
 AND m.kpi_id         = kv.kpi_id
 AND m.max_end        = kv.period_end;
""")

cur.execute("DROP VIEW IF EXISTS relationship_news")
cur.execute("""
CREATE VIEW relationship_news AS
WITH rel AS (
  SELECT r.id AS relationship_id, v.name AS vendor, c.name AS customer
  FROM relationships r
  JOIN companies v ON v.id = r.vendor_id
  JOIN companies c ON c.id = r.customer_id
)
SELECT
  ne.hash_id     AS news_id,
  rel.relationship_id,
  ne.published_at,
  ne.title,
  ne.source,
  ne.link,
  ne.summary
FROM news_events ne
JOIN rel
  ON (
    lower(COALESCE(ne.vendor_matches,'')) LIKE '%' || lower(rel.vendor)   || '%'
    OR lower(COALESCE(ne.title,''))        LIKE '%' || lower(rel.vendor)   || '%'
  )
 AND (
    lower(COALESCE(ne.vendor_matches,'')) LIKE '%' || lower(rel.customer) || '%'
    OR lower(COALESCE(ne.title,''))        LIKE '%' || lower(rel.customer) || '%'
  );
""")

con.commit()
con.close()
print("[OK] KPI schema and views created.")
