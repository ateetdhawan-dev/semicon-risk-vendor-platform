import sqlite3
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
import altair as alt

DB="data/news.db"

st.set_page_config(page_title="Vendor→Customer KPI Monitor", layout="wide")
st.title("Vendor → Customer KPI Monitor (Commercial View)")

@st.cache_data(ttl=300, show_spinner=False)
def load_dim():
    con=sqlite3.connect(DB)
    vendors = pd.read_sql_query("SELECT id,name FROM companies WHERE type IN ('vendor','both') ORDER BY name", con)
    customers = pd.read_sql_query("SELECT id,name FROM companies WHERE type IN ('customer','both') ORDER BY name", con)
    kpis = pd.read_sql_query("SELECT id,key,display_name,unit FROM kpi_definitions ORDER BY display_name", con)
    rels = pd.read_sql_query("""
        SELECT r.id, v.name AS vendor, c.name AS customer
        FROM relationships r
        JOIN companies v ON v.id=r.vendor_id
        JOIN companies c ON c.id=r.customer_id
        ORDER BY v.name, c.name
    """, con)
    con.close()
    return vendors, customers, kpis, rels

@st.cache_data(ttl=300, show_spinner=False)
def load_kpi_series(rel_id:int):
    con=sqlite3.connect(DB)
    df = pd.read_sql_query("""
        SELECT kv.*, kd.display_name, kd.unit
        FROM kpi_values kv
        JOIN kpi_definitions kd ON kd.id=kv.kpi_id
        WHERE relationship_id=?
        ORDER BY period_end
    """, con, params=(rel_id,))
    news = pd.read_sql_query("""
        SELECT published_at, title, source, link
        FROM relationship_news
        WHERE relationship_id=?
        ORDER BY published_at DESC
        LIMIT 50
    """, con, params=(rel_id,))
    con.close()
    if not df.empty:
        df["period_end"] = pd.to_datetime(df["period_end"], utc=True, errors="coerce")
    if not news.empty:
        news["published_at"] = pd.to_datetime(news["published_at"], utc=True, errors="coerce")
    return df, news

vendors, customers, kpis_def, rels = load_dim()

# --- Select relationship ---
col1, col2 = st.columns(2)
vend_list = vendors["name"].tolist()
cust_list = customers["name"].tolist()
vend = col1.selectbox("Vendor", vend_list, index=vend_list.index("ASML") if "ASML" in vend_list else 0)
cust = col2.selectbox("Customer", cust_list, index=cust_list.index("TSMC") if "TSMC" in cust_list else 0)

sel = rels[(rels["vendor"]==vend) & (rels["customer"]==cust)]
if sel.empty:
    st.warning("No relationship yet. Seed data or import KPI CSVs.")
    st.stop()
rel_id = int(sel["id"].iloc[0])

df, news = load_kpi_series(rel_id)

# --- KPI cards (latest values) ---
st.subheader(f"KPI Summary — {vend} → {cust}")
if df.empty:
    st.info("No KPI values yet.")
else:
    latest = df.sort_values("period_end").groupby("display_name").tail(1)
    cols = st.columns(min(4, max(1, len(latest))))
    for i, (_, row) in enumerate(latest.iterrows()):
        c = cols[i % len(cols)]
        unit = f" {row['unit']}" if row.get('unit') and row['unit'] != "None" else ""
        c.metric(row["display_name"], f"{row['value']}{unit}", help=f"Period end: {row['period_end'].date()}")

    # Trends
    st.markdown("### Trends")
    for display, grp in df.groupby("display_name"):
        chart = alt.Chart(grp).mark_line(point=True).encode(
            x=alt.X("period_end:T", title="Period"),
            y=alt.Y("value:Q", title=display),
            tooltip=["period_end:T","value:Q"]
        ).properties(height=220)
        st.altair_chart(chart, use_container_width=True)

# --- Related news ---
st.markdown("### Recent News impacting this relationship")
if news.empty:
    st.caption("No related items found yet.")
else:
    nt = news[["published_at","source","title","link"]].copy()
    nt.rename(columns={"published_at":"date_utc"}, inplace=True)
    nt["Open"] = nt["link"]
    st.dataframe(
        nt[["date_utc","source","title","Open"]],
        use_container_width=True,
        column_config={
            "date_utc": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm", step=60),
            "Open": st.column_config.LinkColumn("Open", help="Open source article")
        },
        hide_index=True
    )
