import pandas as pd, sys, json, os
INPUT = sys.argv[1] if len(sys.argv)>1 else "data/news_events_annotated.csv"
OUT   = "logs/quality_report.json"

if not os.path.exists(INPUT):
    print(f"[warn] {INPUT} not found")
    raise SystemExit(0)

df = pd.read_csv(INPUT)
report = {
    "file": INPUT,
    "rows": int(len(df)),
    "cols": list(df.columns),
    "nulls": {c:int(df[c].isna().sum()) for c in df.columns},
    "risk_counts": df.get("risk_type", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
    "severity_counts": df.get("severity", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
    "top_sources": df.get("source", pd.Series(dtype=str)).value_counts().head(10).to_dict(),
}
os.makedirs("logs", exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)
print(f"[done] wrote {OUT}")
