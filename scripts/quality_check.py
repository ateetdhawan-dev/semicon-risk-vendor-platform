import sqlite3, pandas as pd
con = sqlite3.connect("data/news.db")
df = pd.read_sql_query("SELECT published_at, title, vendor_matches, risk_type, vendor_primary, risk_primary FROM news_events", con)
con.close()
total = len(df)
uncl = df["risk_type"].fillna("").str.contains("unclassified").sum()
empty_vendor = (df["vendor_matches"].fillna("")=="").sum()
primary_missing = (df["risk_primary"].fillna("")=="").sum() + (df["vendor_primary"].fillna("")=="").sum()
print(f"Total: {total}")
print(f"Unclassified (multi): {uncl}")
print(f"Empty vendor_matches: {empty_vendor}")
print(f"Missing primary fields: {primary_missing}")
# simple threshold warning
if total and (uncl/total > 0.3):
    print("[WARN] >30% unclassified; consider expanding risk_keywords.json.")
