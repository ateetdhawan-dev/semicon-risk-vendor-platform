# scripts/seed_commercial_csv.py
from pathlib import Path
from datetime import date
import pandas as pd

today = date.today().replace(day=1)
months = [pd.to_datetime(today, utc=True) - pd.DateOffset(months=i) for i in range(5, -1, -1)]

vendors   = ["ASML","Applied Materials","Lam Research","Tokyo Electron"]
customers = ["TSMC","Intel","Samsung","Micron","Texas Instruments","Qualcomm"]
regions   = ["US","EU","KR","TW","JP"]
products  = ["Lithography","Deposition","Etch","Metrology"]

rows = []
for dt in months:
    for v in vendors:
        for c in customers:
            h = hash((str(dt.date()), v, c))
            rows.append(dict(
                date=dt.date(),
                vendor=v,
                customer=c,
                region=regions[h % len(regions)],
                product=products[(h // 7) % len(products)],
                revenue   = 5_000_000 + (abs(h) % 3_000_000),
                bookings  = 4_000_000 + (abs(h // 3) % 2_500_000),
                pipeline  = 8_000_000 + (abs(h // 5) % 5_000_000),
                units     = 10 + (abs(h // 11) % 40),
                risk_score= round(50 + (abs(h // 13) % 50) * 0.5, 1),
                win_rate  = round(30 + (abs(h // 17) % 60) * 0.6, 1),
                deal_size = round(120_000 + (abs(h // 19) % 180_000), 2),
                margin    = round(20 + (abs(h // 23) % 25) * 0.5, 1),
            ))

df = pd.DataFrame(rows)
Path("export").mkdir(parents=True, exist_ok=True)
out = Path("export/commercial_kpi.csv")
df.to_csv(out, index=False)
print(f"✓ Wrote {out} with {len(df):,} rows and columns: {list(df.columns)}")
