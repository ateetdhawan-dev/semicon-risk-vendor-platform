import sqlite3, json
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd
import streamlit as st
import altair as alt

# Resolve project root (this file lives under /pages)
BASE = Path(__file__).resolve().parents[1]
DB   = BASE / "data" / "news.db"
CFG  = BASE / "config"

def read_json(path, default):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return default

FLAGS = read_json(CFG / "flags.json", {"use_primary": False})

@st.cache_data(show_spinner=False, ttl=300)
def load_df():
    # Ensure data dir exists
    (BASE / "data").mkdir(parents=True, exist_ok=True)
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

    # Normalize essential columns
    for c in ["title","source","link","summary","matched_keywords","risk_types",
              "vendor_primary","risk_primary"]:
        if c not in df.columns: df[c] = ""
        df[c] = df[c].fillna("")
    if "risk_score" not in df.columns: df["risk_score"] = 0.0

    # Timestamps & helper lists
    df["date_utc"] = pd.to_datetime(df.get("date_utc"), utc=True, errors="coerce")
    df["risk_list"]   = df["risk_types"].apply(lambda s: [x.strip() for x in str(s).split(",") if x.strip()])
    df["vendor_list"] = df["matched_keywords"].apply(lambda s: [x.strip() for x in str(s).split(",") if x.strip()])
    df["day"] = df["date_utc"].dt.date
    return df.sort_values("date_utc", ascending=False)

@st.cache_data(show_spinner=False)
def load_risks():
    cfg = read_json(CFG / "risk_types.json", {})
    risks = cfg.get("risks")
    if risks and isinstance(risks, list): return risks
    return ["geopolitical","material","vendor"]

@st.cache_data(show_spinner=False)
def load_vendors():
    p = CFG / "vendors_master.csv"
    if p.exists():
        try:
            t = pd.read_csv(p)
            col = "vendor" if "vendor" in t.columns else t.columns[0]
            vs = [v for v in t[col].dropna().astype(str).str.strip().tolist() if v]
            return list(dict.fromkeys(vs))
        except:
            pass
    df = load_df()
    return sorted({v for vs in df["vendor_list"] for v in vs})

st.set_page_config(page_title="Semiconductor Risk Monitor", layout="wide")
st.title("Semiconductor Supply Chain Risk Monitor (News)")

df        = load_df()
risks_cfg = load_risks()
vendors   = load_vendors()
use_primary = bool(FLAGS.get("use_primary", False))

with st.sidebar:
    st.header("Filters")
    days = st.slider("Window (days)", 3, 120, value=30, step=1)
    if use_primary:
        min_score = st.slider("Min risk score", 0.0, 1.5, 0.0, 0.05)
        risk_sel  = st.multiselect("Primary Risk", options=risks_cfg, default=[])
        vend_sel  = st.multiselect("Primary Vendor", options=vendors, default=[])
    else:
        risk_sel  = st.multiselect("Risk Types (multi)", options=risks_cfg, default=[])
        vend_sel  = st.multiselect("Vendors (multi)", options=vendors, default=[])
    source_opts = sorted({s for s in df["source"].unique() if s})
    source_sel  = st.multiselect("Sources", options=source_opts, default=[])
    include_kw  = st.text_input("Title includes (comma-separated)", "")
    exclude_kw  = st.text_input("Title excludes (comma-separated)", "")

# Time window
now_utc = datetime.now(timezone.utc)
cutoff = pd.Timestamp(now_utc) - pd.Timedelta(days=days)
view = df[df["date_utc"] >= cutoff].copy()

# Safety: ensure lists exist on filtered view
if "risk_types" not in view.columns: view["risk_types"] = ""
if "matched_keywords" not in view.columns: view["matched_keywords"] = ""
view["risk_list"]   = view["risk_types"].apply(lambda s: [x.strip() for x in str(s).split(",") if x.strip()])
view["vendor_list"] = view["matched_keywords"].apply(lambda s: [x.strip() for x in str(s).split(",") if x.strip()])

def any_match(text, needles):
    t = (text or "").lower()
    return any((n or "").strip().lower() in t for n in needles if (n or "").strip())

incs = [x.strip() for x in include_kw.split(",")] if include_kw.strip() else []
excs = [x.strip() for x in exclude_kw.split(",")] if exclude_kw.strip() else []

# Filtering
if use_primary:
    if source_sel: view = view[view["source"].isin(set(source_sel))]
    if 'risk_primary' in view.columns and risk_sel:
        view = view[view["risk_primary"].str.lower().isin({r.lower() for r in risk_sel})]
    if 'vendor_primary' in view.columns and vend_sel:
        view = view[view["vendor_primary"].str.lower().isin({v.lower() for v in vend_sel})]
    if incs:       view = view[view["title"].apply(lambda t: any_match(t, incs))]
    if excs:       view = view[~view["title"].apply(lambda t: any_match(t, excs))]
    view = view[view.get("risk_score", 0.0) >= float(locals().get("min_score", 0.0))]
else:
    if source_sel: view = view[view["source"].isin(set(source_sel))]
    if risk_sel:
        sel = {r.lower() for r in risk_sel}
        view = view[view["risk_list"].apply(lambda rs: any(r.lower() in sel for r in rs))]
    if vend_sel:
        sel = {v.lower() for v in vend_sel}
        view = view[view["vendor_list"].apply(lambda vs: any(v.lower() in sel for v in vs))]
    if incs: view = view[view["title"].apply(lambda t: any_match(t, incs))]
    if excs: view = view[~view["title"].apply(lambda t: any_match(t, excs))]

# Header
start = (now_utc - timedelta(days=days)).date().isoformat()
end   = now_utc.date().isoformat()
st.subheader(f"{len(view)} items — {start} to {end} (UTC)  |  Mode: {'PRIMARY' if use_primary else 'CLASSIC'}")

# KPIs
c1, c2, c3 = st.columns(3)
c1.metric("Articles", len(view))
if use_primary:
    c2.metric("Unique Vendors", view.get("vendor_primary","").replace("", pd.NA).nunique() if "vendor_primary" in view.columns else 0)
    c3.metric("Avg Risk Score", round(view.get("risk_score",0).mean(),3) if len(view)>0 and "risk_score" in view.columns else 0)
else:
    c2.metric("Unique Vendors (any match)", view["vendor_list"].astype(str).nunique())
    c3.metric("Risks in window", sum(len(x) for x in view["risk_list"]))

# Charts
if not view.empty:
    daily = view.groupby("day").size().reset_index(name="count")
    st.altair_chart(
        alt.Chart(daily).mark_bar().encode(
            x=alt.X("day:T", title="Day"),
            y=alt.Y("count:Q", title="Articles")
        ).properties(height=220),
        use_container_width=True
    )
    if use_primary and "risk_primary" in view.columns:
        by_risk = view.groupby("risk_primary").size().reset_index(name="count")
        st.altair_chart(
            alt.Chart(by_risk).mark_bar().encode(
                x=alt.X("risk_primary:N", sort="-y", title="Primary Risk"),
                y=alt.Y("count:Q")
            ).properties(height=220),
            use_container_width=True
        )
    else:
        ex = view.explode("risk_list")
        if not ex.empty:
            by_risk = ex.groupby("risk_list").size().reset_index(name="count")
            st.altair_chart(
                alt.Chart(by_risk).mark_bar().encode(
                    x=alt.X("risk_list:N", sort="-y", title="Risk Type"),
                    y=alt.Y("count:Q")
                ).properties(height=220),
                use_container_width=True
            )

# Table
if not view.empty:
    if use_primary and all(c in view.columns for c in ["vendor_primary","risk_primary","risk_score"]):
        tbl = view[["date_utc","title","source","vendor_primary","risk_primary","risk_score","link"]].copy()
    else:
        tbl = view[["date_utc","title","source","matched_keywords","risk_types","link"]].copy()
        tbl.rename(columns={"matched_keywords":"vendors","risk_types":"risks"}, inplace=True)
    tbl["Open"] = tbl["link"].where(tbl["link"].str.len()>0,"")
    show_cols = ["date_utc","title","source","Open"]
    show_cols += ["vendor_primary","risk_primary","risk_score"] if "vendor_primary" in tbl.columns else ["vendors","risks"]
    st.dataframe(
        tbl[show_cols],
        use_container_width=True,
        column_config={
            "date_utc": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm", step=60),
            "risk_score": st.column_config.NumberColumn(format="%.2f"),
            "Open": st.column_config.LinkColumn("Open", help="Open source article")
        },
        hide_index=True
    )
else:
    st.info("No articles match the filters.")
