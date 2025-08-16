import sqlite3, json
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
import streamlit as st
import altair as alt

ROOT = Path(__file__).resolve().parent
DB   = ROOT / "data" / "news.db"
CFG  = ROOT / "config"

@st.cache_data(show_spinner=False, ttl=300)
def load_df():
    con = sqlite3.connect(str(DB))
    try:
        df = pd.read_sql_query("SELECT * FROM news", con)
    except Exception:
        q = """
        SELECT
          hash_id AS id,
          published_at AS date_utc,
          title, source, link, summary,
          COALESCE(vendor_matches,'') AS matched_keywords,
          COALESCE(risk_type,'')      AS risk_types,
          vendor_primary, risk_primary, risk_score
        FROM news_events
        """
        df = pd.read_sql_query(q, con)
    finally:
        con.close()

    for c in ["title","source","link","summary","matched_keywords","risk_types","vendor_primary","risk_primary"]:
        if c not in df.columns: df[c]=""
        df[c]=df[c].fillna("")
    if "risk_score" not in df.columns: df["risk_score"]=0.0

    df["date_utc"] = pd.to_datetime(df.get("date_utc"), utc=True, errors="coerce")
    df["day"] = df["date_utc"].dt.date
    return df.sort_values("date_utc", ascending=False)

@st.cache_data(show_spinner=False)
def load_risks():
    p = CFG / "risk_types.json"
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            xs = [r for r in data.get("risks",[]) if isinstance(r,str) and r.strip()]
            if xs: return xs
        except: pass
    return ["geopolitical","material","vendor","logistics","financial","regulatory","cybersecurity","workforce","environmental","capacity","unclassified"]

@st.cache_data(show_spinner=False)
def load_vendors():
    p = CFG / "vendors_master.csv"
    if p.exists():
        try:
            t = pd.read_csv(p)
            col = "vendor" if "vendor" in t.columns else t.columns[0]
            vs = [v for v in t[col].dropna().astype(str).str.strip().tolist() if v]
            return list(dict.fromkeys(vs))
        except: pass
    return []

st.set_page_config(page_title="Semicon Risk Monitor", layout="wide")
st.title("Semiconductor Supply Chain Risk Monitor")

df = load_df()
risks = load_risks()
vendors_cfg = load_vendors()

with st.sidebar:
    st.header("Filters")
    days = st.slider("Window (days)", 3, 180, value=30, step=1)
    min_score = st.slider("Min risk score", 0.0, 1.5, 0.0, 0.05)
    risk_sel = st.multiselect("Primary Risk", options=risks, default=[])
    vend_sel = st.multiselect("Primary Vendor", options=vendors_cfg, default=[])
    include_kw = st.text_input("Title includes (comma-separated)", "")
    exclude_kw = st.text_input("Title excludes (comma-separated)", "")

now_utc = datetime.now(timezone.utc)
cutoff = pd.Timestamp(now_utc) - pd.Timedelta(days=days)

view = df[(df["date_utc"]>=cutoff) & (df["risk_score"]>=min_score)].copy()

if risk_sel:
    sel = {r.lower() for r in risk_sel}
    view = view[view["risk_primary"].str.lower().isin(sel)]
if vend_sel:
    sel = {v.lower() for v in vend_sel}
    view = view[view["vendor_primary"].str.lower().isin(sel)]

def any_match(text, needles):
    t = (text or "").lower()
    return any((n or "").strip().lower() in t for n in needles if (n or "").strip())

incs = [x.strip() for x in include_kw.split(",")] if include_kw.strip() else []
excs = [x.strip() for x in exclude_kw.split(",")] if exclude_kw.strip() else []
if incs:
    view = view[view["title"].apply(lambda t: any_match(t, incs))]
if excs:
    view = view[~view["title"].apply(lambda t: any_match(t, excs))]

start = (now_utc - timedelta(days=days)).date().isoformat()
end   = now_utc.date().isoformat()
st.subheader(f"{len(view)} items — {start} to {end} (UTC)")

# KPIs by primary
c1, c2, c3 = st.columns(3)
c1.metric("Articles", len(view))
c2.metric("Unique Vendors", view["vendor_primary"].replace("", pd.NA).nunique())
c3.metric("Avg Risk Score", round(view["risk_score"].mean(),3) if len(view)>0 else 0)

# Charts
if not view.empty:
    daily = view.groupby("day").size().reset_index(name="count")
    st.altair_chart(
        alt.Chart(daily).mark_bar().encode(x=alt.X("day:T", title="Day"), y=alt.Y("count:Q", title="Articles"))
        .properties(height=220),
        use_container_width=True
    )
    by_risk = view.groupby("risk_primary").size().reset_index(name="count")
    st.altair_chart(
        alt.Chart(by_risk).mark_bar().encode(x=alt.X("risk_primary:N", sort="-y", title="Primary Risk"), y=alt.Y("count:Q"))
        .properties(height=220),
        use_container_width=True
    )
    by_vendor = view.groupby("vendor_primary").size().reset_index(name="count").sort_values("count", ascending=False).head(12)
    st.altair_chart(
        alt.Chart(by_vendor).mark_bar().encode(x=alt.X("vendor_primary:N", sort="-y", title="Primary Vendor"), y=alt.Y("count:Q"))
        .properties(height=220),
        use_container_width=True
    )

# Table (clean, primary fields first)
if not view.empty:
    tbl = view[["date_utc","title","source","vendor_primary","risk_primary","risk_score","link"]].copy()
    tbl["Open"] = tbl["link"].where(tbl["link"].str.len()>0,"")
    st.dataframe(
        tbl[["date_utc","title","source","vendor_primary","risk_primary","risk_score","Open"]],
        use_container_width=True,
        column_config={
            "date_utc": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm", step=60),
            "risk_score": st.column_config.NumberColumn(format="%.2f"),
            "Open": st.column_config.LinkColumn("Open", help="Open source article")
        },
        hide_index=True
    )
    csv = tbl.drop(columns=["link"]).to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV (primary fields)", csv, "semicon_risk_primary.csv", "text/csv")
else:
    st.info("No articles match the filters.")
