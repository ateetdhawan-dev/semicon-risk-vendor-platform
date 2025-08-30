import os
import re
import math
import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from datetime import date

st.set_page_config(page_title="Commercial KPIs", page_icon="📈")

# ----------------- Helpers -----------------
def parse_numeric(x):
    if x is None:
        return np.nan
    s = str(x).strip()
    if not s:
        return np.nan
    m = re.search(r"[+-]?\d[\d,]*(?:\.\d+)?", s.replace("\u00A0"," "))
    if not m:
        return np.nan
    try:
        return float(m.group(0).replace(",", ""))
    except Exception:
        return np.nan

def format_value(val, unit):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "—"
    absval = abs(val)
    if absval >= 1_000_000_000:
        s = f"{val/1_000_000_000:.2f}B"
    elif absval >= 1_000_000:
        s = f"{val/1_000_000:.2f}M"
    elif absval >= 1_000:
        s = f"{val/1_000:.2f}K"
    else:
        s = f"{val:.2f}"
    return f"{s} {unit}".strip()

def normalize(values: pd.Series, method: str = "minmax"):
    arr = values.astype(float)
    if method == "zscore":
        mu = np.nanmean(arr)
        sd = np.nanstd(arr)
        if sd == 0 or np.isnan(sd):
            return pd.Series(np.full_like(arr, 0.0), index=values.index)
        return (arr - mu) / sd
    # minmax
    vmin = np.nanmin(arr) if not np.isnan(arr).all() else np.nan
    vmax = np.nanmax(arr) if not np.isnan(arr).all() else np.nan
    if vmin == vmax or np.isnan(vmin) or np.isnan(vmax):
        return pd.Series(np.full_like(arr, 0.5), index=values.index)
    return (arr - vmin) / (vmax - vmin)

# ----------------- Loaders -----------------
@st.cache_data
def load_taxonomy():
    path = "config/kpi_taxonomy.csv"
    if not os.path.exists(path):
        # default minimal taxonomy
        return pd.DataFrame({
            "kpi": ["Revenue TTM","Backlog","Installed base","Wafer starts","Lead time","Gross Margin %","Process node"],
            "category": ["Financial","Orders","Scale","Manufacturing","Supply","Profitability","Technology"],
            "preferred_unit": ["billion USD","billion USD","tools","wafer/month","weeks","percent","node"],
            "direction": ["higher_good","higher_good","higher_good","higher_good","lower_good","higher_good","higher_good"],
            "description": ["" for _ in range(7)]
        })
    return pd.read_csv(path)

@st.cache_data
def load_kpis():
    # Prefer v2 if exists
    p2 = "config/kpis_v2.csv"
    p1 = "config/kpis.csv"
    if os.path.exists(p2):
        df = pd.read_csv(p2)
    elif os.path.exists(p1):
        df = pd.read_csv(p1)
    else:
        return pd.DataFrame(columns=["vendor","kpi","value","unit","currency","as_of","source","notes"])

    # Ensure columns exist
    for col in ["vendor","kpi","value","unit","currency","as_of","source","notes"]:
        if col not in df.columns:
            df[col] = ""

    # Normalize
    df["vendor"] = df["vendor"].astype(str).str.strip()
    df["kpi"]    = df["kpi"].astype(str).str.strip()
    df["unit"]   = df["unit"].astype(str).str.strip()
    df["currency"] = df["currency"].astype(str).str.strip()
    df["source"] = df["source"].astype(str).str.strip()
    df["notes"]  = df["notes"].astype(str).str.strip()
    df["value"]  = df["value"].apply(parse_numeric).astype(float)
    df["as_of"]  = pd.to_datetime(df["as_of"], errors="coerce")

    # Drop empties
    df = df[(df["vendor"]!="") & (df["kpi"]!="")]
    return df

# ----------------- Data prep -----------------
tax = load_taxonomy()
df  = load_kpis()

st.title("📈 Commercial KPI Intelligence")

if df.empty:
    st.info("No KPI data found. Please populate `config/kpis_v2.csv` (preferred) or `config/kpis.csv`.")
    st.stop()

# Join taxonomy to get category + direction
tax_small = tax[["kpi","category","preferred_unit","direction"]].drop_duplicates()
df = df.merge(tax_small, on="kpi", how="left")

# ----------------- Filters -----------------
all_vendors = sorted(df["vendor"].dropna().unique())
all_kpis    = sorted(df["kpi"].dropna().unique())
all_cats    = sorted([x for x in df["category"].dropna().unique() if str(x).strip()])

c1, c2, c3 = st.columns([2,2,2])
with c1:
    sel_vendors = st.multiselect("Vendors", all_vendors, default=all_vendors[:min(6,len(all_vendors))])
with c2:
    sel_categories = st.multiselect("Categories", all_cats, default=all_cats)
with c3:
    # choose normalization
    norm_method = st.selectbox("Normalization", ["minmax","zscore"], index=0)

# Date filter row
c4, c5 = st.columns([2,2])
with c4:
    # KPI picker limited by chosen categories
    kpi_pool = sorted(df[df["category"].isin(sel_categories)]["kpi"].dropna().unique()) if sel_categories else all_kpis
    sel_kpis = st.multiselect("KPIs", kpi_pool, default=kpi_pool[:min(8,len(kpi_pool))])
with c5:
    if df["as_of"].notna().any():
        dmin, dmax = df["as_of"].min().date(), df["as_of"].max().date()
        date_range = st.date_input("As-of range", (dmin, dmax), min_value=dmin, max_value=dmax)
    else:
        date_range = None

flt = df.copy()
if sel_vendors:
    flt = flt[flt["vendor"].isin(sel_vendors)]
if sel_categories:
    flt = flt[flt["category"].isin(sel_categories)]
if sel_kpis:
    flt = flt[flt["kpi"].isin(sel_kpis)]
if date_range and isinstance(date_range, tuple) and len(date_range)==2:
    start_d, end_d = date_range
    flt = flt[(flt["as_of"].dt.date>=start_d) & (flt["as_of"].dt.date<=end_d)]

st.caption(f"{len(flt)} records after filters")

# ----------------- Overview cards -----------------
latest = flt.copy()
if latest["as_of"].notna().any():
    latest = latest.sort_values("as_of").groupby(["vendor","kpi"], as_index=False).tail(1)
else:
    latest = latest.groupby(["vendor","kpi"], as_index=False).agg({"value":"mean","unit":"first","category":"first","direction":"first"})

cA,cB,cC,cD = st.columns(4)
with cA: st.metric("Vendors", len(set(flt["vendor"])))
with cB: st.metric("KPIs", len(set(flt["kpi"])))
with cC: st.metric("Latest Points", int(latest.shape[0]))
with cD:
    if flt["as_of"].notna().any():
        recency_days = (pd.Timestamp(date.today()) - flt["as_of"].max()).days
        st.metric("Data recency (days)", int(recency_days))
    else:
        st.metric("Data recency (days)", "—")

st.divider()

# ----------------- Tabs -----------------
tab1, tab2, tab3, tab4 = st.tabs(["📋 Explorer","📉 Trends","🗺️ Advanced Heatmap","🧩 Coverage"])

# ---- Explorer ----
with tab1:
    view = flt.copy()
    view["value_display"] = [format_value(v,u) for v,u in zip(view["value"], view["unit"])]
    cols = ["vendor","category","kpi","value_display","value","unit","currency","as_of","source","notes"]
    cols = [c for c in cols if c in view.columns]
    # sort for readability
    if "as_of" in view.columns:
        view = view.sort_values(["vendor","category","kpi","as_of"])
    st.dataframe(view[cols], width="stretch", height=450)

# ---- Trends ----
with tab2:
    if not flt.empty and flt["as_of"].notna().any():
        t1,t2 = st.columns([2,2])
        with t1:
            trend_vendor = st.selectbox("Vendor", ["(All)"] + sel_vendors if sel_vendors else ["(All)"] + all_vendors)
        with t2:
            trend_kpi = st.selectbox("KPI", ["(All)"] + sel_kpis if sel_kpis else ["(All)"])

        data = flt.copy()
        if trend_vendor != "(All)":
            data = data[data["vendor"]==trend_vendor]
        if trend_kpi != "(All)":
            data = data[data["kpi"]==trend_kpi]

        if data.empty:
            st.info("No data to plot for this selection.")
        else:
            data = data.dropna(subset=["as_of"]).sort_values("as_of")
            data["series"] = data["vendor"] + " · " + data["kpi"]
            piv = data.pivot_table(index="as_of", columns="series", values="value", aggfunc="mean").sort_index()
            if piv.empty:
                st.info("Nothing to plot.")
            else:
                fig, ax = plt.subplots(figsize=(9,4), dpi=140)
                for col in piv.columns:
                    ax.plot(piv.index, piv[col], label=str(col))
                ax.set_title("KPI Trends")
                ax.set_xlabel("As of date")
                ax.set_ylabel("Value")
                if len(piv.columns) <= 10:
                    ax.legend(loc="best", fontsize=8)
                ax.grid(True, alpha=0.3)
                st.pyplot(fig)
    else:
        st.info("No dated series available. Add `as_of` dates to see trends.")

# ---- Advanced Heatmap ----
with tab3:
    # Aggregate: latest by date if present, else mean
    agg = flt.copy()
    by = ["vendor","kpi","category","direction","unit"]
    if agg["as_of"].notna().any():
        agg = agg.sort_values("as_of").groupby(by, as_index=False).tail(1)
    else:
        agg = agg.groupby(by, as_index=False).agg({"value":"mean"})

    if agg.empty:
        st.info("No values to display.")
    else:
        # Normalize per KPI with direction
        # Build matrix vendor x KPI (values)
        mat = agg.pivot_table(index="vendor", columns="kpi", values="value", aggfunc="mean")
        # Keep units for annotations: choose most common per vendor/kpi
        units = agg.groupby(["vendor","kpi"])["unit"].agg(lambda s: s.mode().iloc[0] if len(s.mode()) else "").reset_index()
        units_mat = units.pivot(index="vendor", columns="kpi", values="unit").reindex(index=mat.index, columns=mat.columns)

        # Direction-aware normalization per KPI
        norm_cols = {}
        for k in mat.columns:
            col = mat[k]
            col_norm = normalize(col, method=norm_method)
            # direction: if lower_good, invert
            dir_k = agg[agg["kpi"]==k]["direction"].dropna().unique()
            if len(dir_k) and dir_k[0].lower().startswith("lower"):
                col_norm = 1 - col_norm
            norm_cols[k] = col_norm
        norm_mat = pd.DataFrame(norm_cols, index=mat.index)

        # Prepare annotation text: value + unit
        text_mat = pd.DataFrame(index=mat.index, columns=mat.columns)
        for i in mat.index:
            for j in mat.columns:
                v = mat.loc[i,j]
                u = units_mat.loc[i,j] if (i in units_mat.index and j in units_mat.columns) else ""
                text_mat.loc[i,j] = format_value(v,u) if pd.notna(v) else ""

        if norm_mat.empty:
            st.info("No values to display.")
        else:
            fig_w = max(6, len(norm_mat.columns) * 0.9)
            fig_h = max(4, len(norm_mat.index) * 0.5)
            fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=140)
            im = ax.imshow(norm_mat.values, aspect="auto")
            ax.set_xticks(np.arange(norm_mat.shape[1]))
            ax.set_yticks(np.arange(norm_mat.shape[0]))
            ax.set_xticklabels(norm_mat.columns, rotation=45, ha="right")
            ax.set_yticklabels(norm_mat.index)
            ax.set_title(f"Advanced Heatmap ({norm_method}, direction-aware)")
            ax.set_xlabel("KPI")
            ax.set_ylabel("Vendor")
            cbar = fig.colorbar(im, ax=ax)
            cbar.ax.set_ylabel("Normalized score", rotation=270, labelpad=12)

            # Annotate with original values
            if norm_mat.shape[0]*norm_mat.shape[1] <= 400:
                for r in range(norm_mat.shape[0]):
                    for c in range(norm_mat.shape[1]):
                        txt = str(text_mat.iloc[r,c])
                        if txt and txt != "nan":
                            ax.text(c, r, txt, ha="center", va="center", fontsize=7, color="black")

            st.pyplot(fig)

# ---- Coverage (data completeness) ----
with tab4:
    # Count observations per (vendor,kpi) and last as_of
    cov = flt.copy()
    cov["obs"] = 1
    counts = cov.groupby(["vendor","kpi"], as_index=False)["obs"].sum()
    last = cov[cov["as_of"].notna()].sort_values("as_of").groupby(["vendor","kpi"], as_index=False).tail(1)[["vendor","kpi","as_of"]]
    cov_tbl = counts.merge(last, on=["vendor","kpi"], how="left")

    # Pivot to matrix for heatmap of counts
    cm = cov_tbl.pivot_table(index="vendor", columns="kpi", values="obs", aggfunc="sum").fillna(0)

    if cm.empty:
        st.info("No coverage to display.")
    else:
        fig_w = max(6, len(cm.columns) * 0.9)
        fig_h = max(4, len(cm.index) * 0.5)
        fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=140)
        im = ax.imshow(cm.values, aspect="auto")
        ax.set_xticks(np.arange(cm.shape[1]))
        ax.set_yticks(np.arange(cm.shape[0]))
        ax.set_xticklabels(cm.columns, rotation=45, ha="right")
        ax.set_yticklabels(cm.index)
        ax.set_title("Coverage Heatmap (observations count)")
        ax.set_xlabel("KPI")
        ax.set_ylabel("Vendor")
        cbar = fig.colorbar(im, ax=ax)
        cbar.ax.set_ylabel("# of observations", rotation=270, labelpad=12)

        # annotate with last as_of (short date) if available & matrix not huge
        if cm.shape[0]*cm.shape[1] <= 400:
            # Quick lookup for last dates
            last_map = {(r.vendor, r.kpi): (str(pd.to_datetime(r.as_of).date()) if pd.notna(r.as_of) else "") for _, r in cov_tbl.iterrows()}
            for i, v in enumerate(cm.index):
                for j, k in enumerate(cm.columns):
                    last_str = last_map.get((v,k), "")
                    if last_str:
                        ax.text(j, i, last_str, ha="center", va="center", fontsize=7, color="black")
        st.pyplot(fig)
