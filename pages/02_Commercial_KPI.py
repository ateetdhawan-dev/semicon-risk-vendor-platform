import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st
import altair as alt

DB = str((Path(__file__).resolve().parents[1] / "data" / "news.db").resolve())

st.set_page_config(page_title="Vendor→Customer KPI Monitor", layout="wide")
st.title("Vendor → Customer KPI Monitor")

@st.cache_data(ttl=300, show_spinner=False)
def load_dims():
    con=sqlite3.connect(DB)
    vendors = pd.read_sql_query("SELECT DISTINCT name FROM companies WHERE type IN ('vendor','both') ORDER BY name", con)["name"].tolist()
    customers = pd.read_sql_query("SELECT DISTINCT name FROM companies WHERE type IN ('customer','both') ORDER BY name", con)["name"].tolist()
    kpis = pd.read_sql_query("SELECT id,key,display_name,unit FROM kpi_definitions ORDER BY display_name", con)
    con.close()
    return vendors, customers, kpis

@st.cache_data(ttl=300, show_spinner=False)
def latest_kpis():
    con=sqlite3.connect(DB)
    df = pd.read_sql_query("""
        WITH latest AS (
          SELECT relationship_id, kpi_id, MAX(period_end) AS max_end
          FROM kpi_values GROUP BY relationship_id, kpi_id
        )
        SELECT
          v.name AS vendor, c.name AS customer,
          kd.id AS kpi_id, kd.key, kd.display_name, kd.unit,
          kv.value, kv.period_end
        FROM latest l
        JOIN kpi_values kv ON kv.relationship_id=l.relationship_id AND kv.kpi_id=l.kpi_id AND kv.period_end=l.max_end
        JOIN relationships r ON r.id = kv.relationship_id
        JOIN companies v ON v.id = r.vendor_id
        JOIN companies c ON c.id = r.customer_id
        JOIN kpi_definitions kd ON kd.id = kv.kpi_id
    """, con)
    con.close()
    if not df.empty:
        df["period_end"] = pd.to_datetime(df["period_end"])
    return df

@st.cache_data(ttl=300, show_spinner=False)
def kpi_series():
    con=sqlite3.connect(DB)
    df = pd.read_sql_query("""
        SELECT
          v.name AS vendor, c.name AS customer,
          kd.display_name, kd.key, kd.unit,
          kv.value, kv.period_end
        FROM kpi_values kv
        JOIN relationships r ON r.id = kv.relationship_id
        JOIN companies v ON v.id = r.vendor_id
        JOIN companies c ON c.id = r.customer_id
        JOIN kpi_definitions kd ON kd.id = kv.kpi_id
        ORDER BY kv.period_end
    """, con)
    con.close()
    if not df.empty:
        df["period_end"] = pd.to_datetime(df["period_end"])
    return df

vendors, customers, kpis_def = load_dims()
latest = latest_kpis()
series = kpi_series()

mode = st.radio("Mode", ["Portfolio","Single relationship"], index=0, horizontal=True)

if mode=="Single relationship":
    col1,col2 = st.columns(2)
    vend = col1.selectbox("Vendor", vendors, index=(vendors.index("ASML") if "ASML" in vendors else 0))
    cust = col2.selectbox("Customer", customers, index=(customers.index("TSMC") if "TSMC" in customers else 0))
    # cards
    sel = latest[(latest["vendor"]==vend) & (latest["customer"]==cust)]
    st.subheader(f"Latest KPIs — {vend} → {cust}")
    if sel.empty:
        st.info("No KPIs yet for this pair.")
    else:
        cols = st.columns(min(4, max(1, len(sel))))
        for i, (_, row) in enumerate(sel.sort_values("display_name").iterrows()):
            c = cols[i % len(cols)]
            unit = f" {row['unit']}" if row['unit'] and row['unit']!="None" else ""
            c.metric(row["display_name"], f"{row['value']}{unit}", help=f"Period end: {row['period_end'].date()}")
        # trends
        st.markdown("### Trends")
        tsel = series[(series["vendor"]==vend) & (series["customer"]==cust)]
        for name, grp in tsel.groupby("display_name"):
            ch = alt.Chart(grp).mark_line(point=True).encode(
                x=alt.X("period_end:T", title="Period"),
                y=alt.Y("value:Q", title=name),
                tooltip=["period_end:T","value:Q"]
            ).properties(height=220)
            st.altair_chart(ch, use_container_width=True)

else:
    c1,c2,c3 = st.columns(3)
    vend_sel = c1.multiselect("Vendors", vendors, default=vendors)
    cust_sel = c2.multiselect("Customers", customers, default=customers)
    kpi_names = kpis_def["display_name"].tolist()
    kpi_sel = c3.multiselect("KPIs", kpi_names, default=[n for n in kpi_names if n in ["Tool Availability","On-Time Delivery","Installed Base Tools","EUV Systems Shipped"]][:4])

    view = latest.copy()
    if vend_sel: view = view[view["vendor"].isin(vend_sel)]
    if cust_sel: view = view[view["customer"].isin(cust_sel)]
    if kpi_sel:  view = view[view["display_name"].isin(kpi_sel)]

    st.subheader(f"Portfolio overview — {len(view[['vendor','customer']].drop_duplicates())} relationships")

    if view.empty:
        st.info("No data for the current filters.")
    else:
        # Heatmaps per KPI (limit to 4 for readability)
        for name in view["display_name"].drop_duplicates().tolist()[:4]:
            sub = view[view["display_name"]==name]
            pivot = sub.pivot_table(index="vendor", columns="customer", values="value", aggfunc="mean")
            plot = pivot.reset_index().melt(id_vars="vendor", var_name="customer", value_name="value")
            st.markdown(f"**{name}** (latest)")
            chart = alt.Chart(plot).mark_rect().encode(
                x=alt.X("customer:N", title="Customer"),
                y=alt.Y("vendor:N", title="Vendor"),
                color=alt.Color("value:Q", title=name),
                tooltip=["vendor","customer","value"]
            ).properties(height=220)
            text = alt.Chart(plot).mark_text(baseline='middle').encode(
                x="customer:N", y="vendor:N",
                text=alt.Text("value:Q", format=".2f")
            )
            st.altair_chart(chart + text, use_container_width=True)

        # Latest table
        tbl = view.sort_values(["display_name","vendor","customer"])[["display_name","vendor","customer","value","unit","period_end"]]
        st.dataframe(
            tbl.rename(columns={"display_name":"KPI","period_end":"As of"}),
            use_container_width=True,
            column_config={"As of": st.column_config.DatetimeColumn(format="YYYY-MM-DD")}
        )

        # Trends for a chosen KPI across all selected pairs
        pick = st.selectbox("Trend for KPI", sorted(view["display_name"].unique().tolist()))
        tsel = series[(series["display_name"]==pick)]
        if vend_sel: tsel = tsel[tsel["vendor"].isin(vend_sel)]
        if cust_sel: tsel = tsel[tsel["customer"].isin(cust_sel)]
        if not tsel.empty:
            ch = alt.Chart(tsel).mark_line(point=True).encode(
                x=alt.X("period_end:T", title="Period"),
                y=alt.Y("value:Q", title=pick),
                color="vendor:N",
                strokeDash="customer:N",
                tooltip=["vendor","customer","period_end:T","value:Q"]
            ).properties(height=260)
            st.altair_chart(ch, use_container_width=True)
