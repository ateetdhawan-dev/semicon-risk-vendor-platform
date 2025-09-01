# seed_kpi.py  — creates data/kpi_portfolio.csv with realistic, high-impact vendor KPIs
import csv, random
from pathlib import Path
from datetime import date
import pandas as pd

out = Path("data"); out.mkdir(parents=True, exist_ok=True)
csv_path = out / "kpi_portfolio.csv"

# Vendors (equipment-centric)
vendors = [
    "ASML","Applied Materials","Lam Research","KLA","TEL Tokyo Electron",
    "NVIDIA","AMD","Intel","Micron","SK hynix","Samsung","TSMC","GlobalFoundries","UMC"
]
# Foundry/IDM customers (who deploy the tools)
customers = ["TSMC","Samsung","Intel","Micron","SK hynix","GlobalFoundries","UMC"]

# Region to customer mapping (approx)
cust_region = {
    "TSMC":"APAC","Samsung":"APAC","Intel":"NA","Micron":"NA","SK hynix":"APAC","GlobalFoundries":"NA","UMC":"APAC"
}

# Months (last 6 complete months, 1st-of-month)
months = pd.date_range(end=pd.Timestamp.today().normalize().replace(day=1), periods=6, freq="MS")

# KPI list (all numeric; % expressed as 0-100)
# NOTE: include spares_score & support_score to feed serviceability composite in your page
kpis = [
    "bookings_usd_m",        # $m — for B2B ratio
    "billings_usd_m",        # $m — for B2B ratio
    "tool_uptime_pct",       # %
    "oee_pct",               # %
    "wafers_per_hour",       # wph
    "install_cycle_days",    # days to install & qualify
    "lead_time_weeks",       # supply lead time
    "mttr_hours",            # mean time to repair
    "spares_score",          # 0–100 (fill rate + lead time composite)
    "support_score",         # 0–100 (FSE coverage + response SLA)
]

random.seed(42)

def base_by_vendor(v):
    # Baselines so distributions look plausible
    if v == "ASML":
        return dict(wafers_per_hour=165, tool_uptime_pct=91, oee_pct=82, lead_time_weeks=26, install_cycle_days=45, mttr_hours=6)
    if v == "Applied Materials":
        return dict(wafers_per_hour=140, tool_uptime_pct=93, oee_pct=85, lead_time_weeks=18, install_cycle_days=35, mttr_hours=5)
    if v == "Lam Research":
        return dict(wafers_per_hour=130, tool_uptime_pct=92, oee_pct=84, lead_time_weeks=16, install_cycle_days=32, mttr_hours=5)
    if v == "KLA":
        return dict(wafers_per_hour=80,  tool_uptime_pct=95, oee_pct=86, lead_time_weeks=14, install_cycle_days=28, mttr_hours=4)
    if v == "TEL Tokyo Electron":
        return dict(wafers_per_hour=120, tool_uptime_pct=93, oee_pct=83, lead_time_weeks=16, install_cycle_days=33, mttr_hours=5)
    # For chip vendors used as “vendor” in your data model, keep sane defaults
    return dict(wafers_per_hour=100, tool_uptime_pct=92, oee_pct=82, lead_time_weeks=16, install_cycle_days=30, mttr_hours=6)

rows = []
for d in months:
    for v in vendors:
        b = base_by_vendor(v)
        for c in customers:
            # Bookings/Billings in $m — vary by customer + vendor
            book = max(5, random.gauss(60 if c in ("TSMC","Samsung") else 30, 12))
            bill = max(5, book * random.uniform(0.85, 1.15))
            # Other KPIs with slight month-to-month noise
            def jitter(x, pct=0.05): return x * random.uniform(1-pct, 1+pct)

            tool_uptime = min(99.5, max(85, jitter(b["tool_uptime_pct"], 0.03)))
            oee         = min(95.0, max(75, jitter(b["oee_pct"], 0.04)))
            wph         = max(50, jitter(b["wafers_per_hour"], 0.07))
            install     = max(20, jitter(b["install_cycle_days"], 0.10))
            lead_w      = max(8,  jitter(b["lead_time_weeks"], 0.15))
            mttr        = max(2,  jitter(b["mttr_hours"], 0.12))
            spares      = min(98, max(60, random.gauss(85, 6)))
            support     = min(98, max(60, random.gauss(88, 5)))

            vals = {
                "bookings_usd_m": round(book,1),
                "billings_usd_m": round(bill,1),
                "tool_uptime_pct": round(tool_uptime,1),
                "oee_pct": round(oee,1),
                "wafers_per_hour": round(wph,0),
                "install_cycle_days": round(install,0),
                "lead_time_weeks": round(lead_w,1),
                "mttr_hours": round(mttr,1),
                "spares_score": round(spares,0),
                "support_score": round(support,0),
            }

            for kpi, value in vals.items():
                rows.append({
                    "date": d.date().isoformat(),
                    "vendor": v,
                    "customer": c,
                    "kpi": kpi,
                    "value": value,
                    "region": cust_region[c],
                })

# Write CSV
with csv_path.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["date","vendor","customer","kpi","value","region"])
    w.writeheader()
    w.writerows(rows)

print(f"Wrote {csv_path} with {len(rows)} rows across {len(vendors)} vendors, {len(customers)} customers, {len(kpis)} KPIs × {len(months)} months.")
