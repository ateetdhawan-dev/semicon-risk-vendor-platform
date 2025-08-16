import sqlite3, datetime
DB="data/news.db"
con=sqlite3.connect(DB); cur=con.cursor()

def get_or_create_company(name, ctype="both"):
    cur.execute("SELECT id FROM companies WHERE name=?",(name,))
    row=cur.fetchone()
    if row: return row[0]
    cur.execute("INSERT INTO companies(name,type) VALUES (?,?)",(name,ctype))
    return cur.lastrowid

asml_id = get_or_create_company("ASML","vendor")
tsmc_id = get_or_create_company("TSMC","customer")

cur.execute("INSERT OR IGNORE INTO relationships(vendor_id, customer_id) VALUES (?,?)",(asml_id, tsmc_id))
cur.execute("SELECT id FROM relationships WHERE vendor_id=? AND customer_id=?",(asml_id, tsmc_id))
rel_id = cur.fetchone()[0]

def upsert_kpi(key, display, unit, desc):
    cur.execute("INSERT OR IGNORE INTO kpi_definitions(key,display_name,unit,description) VALUES (?,?,?,?)",
                (key, display, unit, desc))
    cur.execute("SELECT id FROM kpi_definitions WHERE key=?",(key,))
    return cur.fetchone()[0]

kpi_avail   = upsert_kpi("tool_availability_pct","Tool Availability","%","% of time tools are available (service uptime)")
kpi_ship    = upsert_kpi("euv_systems_shipped","EUV Systems Shipped","units","Units shipped in period")
kpi_inst    = upsert_kpi("installed_base_tools","Installed Base Tools","units","Total installed systems at customer")
kpi_ontime  = upsert_kpi("on_time_delivery_pct","On-Time Delivery","%","Deliveries meeting promised date")

cur.execute("""
INSERT OR IGNORE INTO sources(name,url,type,accessed_at)
VALUES (?,?,?,?)
""", ("SAMPLE placeholder","", "manual", datetime.datetime.utcnow().isoformat()))
cur.execute("SELECT id FROM sources WHERE name='SAMPLE placeholder'")
source_id = cur.fetchone()[0]

rows = [
    (kpi_avail,  "2025-04-01","2025-06-30", 95.6,  "Q2'25 estimated service availability"),
    (kpi_ship,   "2025-04-01","2025-06-30",  9.0,  "Q2'25 EUV units to TSMC (illustrative)"),
    (kpi_inst,   "2025-04-01","2025-06-30", 95.0,  "Installed base at Q2'25 end (illustrative)"),
    (kpi_ontime, "2025-04-01","2025-06-30", 92.0,  "OTD for Q2'25 (illustrative)")
]
for kpi_id, ps, pe, val, note in rows:
    cur.execute("""
    INSERT OR IGNORE INTO kpi_values(relationship_id,kpi_id,period_start,period_end,value,source_id,notes)
    VALUES (?,?,?,?,?,?,?)
    """,(rel_id, kpi_id, ps, pe, float(val), source_id, note))

con.commit(); con.close()
print("[OK] Seeded ASML → TSMC sample KPIs.")
