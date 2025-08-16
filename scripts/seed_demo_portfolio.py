import sqlite3, datetime, random
DB="data/news.db"
con=sqlite3.connect(DB); cur=con.cursor()
random.seed(42)

def get_company(name, typ="both"):
    cur.execute("INSERT OR IGNORE INTO companies(name,type) VALUES (?,?)",(name,typ))
    cur.execute("SELECT id FROM companies WHERE name=?",(name,))
    return cur.fetchone()[0]

def get_rel(vendor, customer):
    vid=get_company(vendor,"vendor"); cid=get_company(customer,"customer")
    cur.execute("INSERT OR IGNORE INTO relationships(vendor_id,customer_id) VALUES (?,?)",(vid,cid))
    cur.execute("SELECT id FROM relationships WHERE vendor_id=? AND customer_id=?",(vid,cid))
    return cur.fetchone()[0]

def kpi(key, name, unit, desc=""):
    cur.execute("INSERT OR IGNORE INTO kpi_definitions(key,display_name,unit,description) VALUES (?,?,?,?)",
                (key,name,unit,desc))
    cur.execute("SELECT id FROM kpi_definitions WHERE key=?",(key,))
    return cur.fetchone()[0]

def src(name):
    cur.execute("INSERT OR IGNORE INTO sources(name,type,accessed_at) VALUES (?,?,?)",
                (name,"manual",datetime.datetime.utcnow().isoformat()))
    cur.execute("SELECT id FROM sources WHERE name=?",(name,))
    return cur.fetchone()[0]

def add(rel_id,kpi_id,ps,pe,val,source_id,notes="DEMO"):
    cur.execute("""INSERT OR IGNORE INTO kpi_values
    (relationship_id,kpi_id,period_start,period_end,value,source_id,notes)
    VALUES (?,?,?,?,?,?,?)""",(rel_id,kpi_id,ps,pe,float(val),source_id,notes))

vendors=["ASML","Applied Materials","Lam Research","Tokyo Electron","KLA"]
customers=["TSMC","Samsung","Intel","Micron","SK Hynix"]

rel_ids={(v,c): get_rel(v,c) for v in vendors for c in customers}

# KPI defs
tool_avail = kpi("tool_availability_pct","Tool Availability","%","Tool uptime")
otd        = kpi("on_time_delivery_pct","On-Time Delivery","%","Deliveries on time")
installed  = kpi("installed_base_tools","Installed Base Tools","units","Tools installed at customer")
euv_ship   = kpi("euv_systems_shipped","EUV Systems Shipped","units","Quarterly EUV shipments")
pfr        = kpi("parts_fill_rate_pct","Parts Fill Rate","%","Immediate parts availability")
mttr       = kpi("mttr_hours","Mean Time To Repair","hours","Avg time to restore tool")
mtbf       = kpi("mtbf_hours","Mean Time Between Failures","hours","Avg time between failures")
S=src("DEMO seed")

# last 4 quarters ending 2024-09-30 .. 2025-06-30
qends=["2024-09-30","2024-12-31","2025-03-31","2025-06-30"]
def qstart(pe):
    y,m,d=map(int,pe.split("-"))
    m0 = {3:1,6:4,9:7,12:10}[m]; y0 = y if m!=3 else y
    return f"{y0:04d}-{m0:02d}-01"

for (v,c),rid in rel_ids.items():
    base_avail=95 + (2 if c in ["TSMC","Samsung"] else 0)
    base_otd  = 91 + (2 if v in ["ASML","Applied Materials"] else 0)
    base_inst = 30 + (20 if c=="TSMC" else 0) + (10 if c=="Samsung" else 0)
    base_pfr  = 95
    base_mttr = 6.5 - (0.7 if c in ["TSMC","Samsung"] else 0)
    base_mtbf = 180 + (40 if c in ["TSMC","Samsung"] else 0)
    for pe in qends:
        ps=qstart(pe)
        # common KPIs
        add(rid, tool_avail, ps, pe, base_avail + random.uniform(-1.2,1.2), S)
        add(rid, otd,        ps, pe, base_otd   + random.uniform(-3,3),   S)
        add(rid, installed,  ps, pe, base_inst  + random.randint(0,5),    S)
        add(rid, pfr,        ps, pe, base_pfr   + random.uniform(-1.5,1.5), S)
        add(rid, mttr,       ps, pe, max(2.5, base_mttr + random.uniform(-0.8,0.8)), S)
        add(rid, mtbf,       ps, pe, base_mtbf + random.uniform(-25,25),  S)
        # EUV only meaningful for ASML
        if v=="ASML":
            base_euv = 8 if c in ["TSMC","Samsung"] else (2 if c=="Intel" else 0)
            add(rid, euv_ship, ps, pe, max(0, base_euv + random.randint(-1,2)), S)

con.commit(); con.close()
print("[OK] Demo portfolio seeded (5 vendors x 5 customers, 4 quarters).")
