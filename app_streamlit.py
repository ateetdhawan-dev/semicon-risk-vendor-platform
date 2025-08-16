import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

DB = "data/news.db"

@st.cache_data(show_spinner=False, ttl=300)
def load_news():
    con = sqlite3.connect(DB)
    df = pd.read_sql_query("SELECT * FROM news", con, parse_dates=["date_utc"])
    con.close()
    # Normalize tz just in case
    df["date_utc"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")
    return df.sort_values("date_utc", ascending=False)

st.set_page_config(page_title="Semicon Risk Monitor", layout="wide")
st.title("Semiconductor Supply Chain Risk Monitor")

# --- Controls ---
with st.sidebar:
    st.header("Filters")
    default_days = st.session_state.get("window_days", 7)
    days = st.slider("Window (days)", 3, 60, int(default_days), 1)
    st.session_state["window_days"] = days

    risk_filter = st.multiselect("Risk Types", ["geopolitical", "material", "vendor"], default=[])
    vendor_query = st.text_input("Vendor contains", "")
    title_query = st.text_input("Title contains", "")

df = load_news()

# Time window
cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=days)
df = df[df["date_utc"] >= cutoff]

# Apply filters
if risk_filter:
    df = df[df["risk_types"].fillna("").apply(lambda x: any(r in x for r in risk_filter))]
if vendor_query.strip():
    q = vendor_query.strip().lower()
    df = df[
        df["matched_keywords"].fillna("").str.lower().str.contains(q)
        | df["title"].fillna("").str.lower().str.contains(q)
    ]
if title_query.strip():
    t = title_query.strip().lower()
    df = df[df["title"].fillna("").str.lower().str.contains(t)]

# Dynamic window label
start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
end   = datetime.utcnow().strftime("%Y-%m-%d")
st.subheader(f"News from {start} to {end} (UTC) — {len(df)} items")

# --- KPIs ---
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Items", len(df))
k2.metric("Geopolitical", (df["risk_types"].fillna("").str.contains("geopolitical")).sum())
k3.metric("Material", (df["risk_types"].fillna("").str.contains("material")).sum())
k4.metric("Vendor", (df["risk_types"].fillna("").str.contains("vendor")).sum())

# --- Table ---
show_cols = ["date_utc","title","source","matched_keywords","risk_types","link"]
st.dataframe(df[show_cols], use_container_width=True)

# --- Simple daily bar ---
if not df.empty:
    by_day = df.copy()
    by_day["day"] = by_day["date_utc"].dt.date
    agg = by_day.groupby("day").size().reset_index(name="count").sort_values("day")
    st.bar_chart(agg.set_index("day")["count"])

# --- Export ---
if not df.empty:
    csv = df[show_cols].to_csv(index=False).encode("utf-8")
    st.download_button("Download filtered CSV", csv, "semicon_risk_news.csv", "text/csv")
