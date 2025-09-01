# scripts/make_commercial_config.py
from pathlib import Path
import sqlite3, json, pandas as pd

CANON = {
    "date":     ["date","period","as_of","period_date","published","published_at","dt"],
    "vendor":   ["vendor","vendor_name","supplier","seller","partner"],
    "customer": ["customer","customer_name","client","buyer","account"],
    "region":   ["region","geo","market","country"],
    "product":  ["product","sku","line","family"],
}
METRICS = ["revenue","bookings","pipeline","units","risk_score","win_rate","deal_size","margin"]

def first(cols, cands):
    for c in cands:
        if c in cols:
            return c

def score(cols):
    s = 0
    if first(cols, CANON["vendor"]): s += 10
    if first(cols, CANON["customer"]): s += 10
    if first(cols, CANON["date"]): s += 10
    s += sum(1 for m in METRICS if m in cols)
    return s

def try_db():
    for db in sorted(Path("data").glob("*.db")):
        try:
            with sqlite3.connect(db) as con:
                cur = con.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                for (t,) in cur.fetchall():
                    try:
                        df = pd.read_sql_query(f"SELECT * FROM {t} LIMIT 5000", con)
                    except Exception:
                        continue
                    cols = set(map(str, df.columns))
                    if score(cols) < 11:
                        continue
                    mapping = {}
                    for k in ["date","vendor","customer","region","product"]:
                        c = first(cols, CANON[k])
                        if c: mapping[k] = c
                    mapping["metrics"] = [m for m in METRICS if m in cols]
                    return ("db", db.as_posix(), t, mapping)
        except Exception:
            pass
    return None

def try_csv():
    candidates = list(Path("data").glob("*.csv")) + list(Path("export").glob("*.csv"))
    candidates = sorted(candidates, key=lambda p: (0 if any(k in p.name.lower() for k in ["kpi","commercial","portfolio"]) else 1, p.name.lower()))
    for csv in candidates:
        try:
            df = pd.read_csv(csv, nrows=5000)
        except Exception:
            continue
        if df.empty:
            continue
        cols = set(map(str, df.columns))
        if score(cols) < 11:
            continue
        mapping = {}
        for k in ["date","vendor","customer","region","product"]:
            c = first(cols, CANON[k])
            if c: mapping[k] = c
        mapping["metrics"] = [m for m in METRICS if m in cols]
        return ("csv", csv.as_posix(), None, mapping)
    return None

def main():
    picked = try_db() or try_csv()
    if not picked:
        print("❌ No suitable KPI source found in data/*.db or data/*.csv/export/*.csv")
        return
    source, path, table, mapping = picked
    cfg = {"source": source, "path": path, "table": table, "columns": mapping}
    Path("config").mkdir(parents=True, exist_ok=True)
    Path("config/commercial_loader.json").write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    print("✅ Wrote config/commercial_loader.json:")
    print(json.dumps(cfg, indent=2))

if __name__ == "__main__":
    main()
