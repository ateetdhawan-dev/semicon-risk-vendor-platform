import os
import re
import math
import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from datetime import date

st.set_page_config(page_title="Commercial KPIs", page_icon="📈")

# =========================
# Helpers
# =========================
def _col_in(df, names):
    """Return first existing column (case-insensitive match) or None."""
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None

def parse_numeric(x):
    """Turn '1,400,000' or '39' into float; return NaN if not parseable."""
    if x is None:
        return np.nan
    s = str(x).strip()
    if s == "":
        return np.nan
    # extract first number (supports 1,234.56)
    m = re.search(r"[+-]?\d[\d,]*(?:\.\d+)?", s.replace("\u00A0"," "))
    if not m:
        return np.nan
    try:
        return float(m.group(0).replace(",", ""))
    except Exception:
        return np.nan

def format_value(val, unit):
    """Pretty-print number with unit."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "—"
    # compact big numbers
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

def normalize_by_kpi(matrix_df):
    """Per-KPI (column-wise) min-max normalization to [0,1], keeping NaNs."""
    norm = matrix_df.copy()
    for col in norm.columns:
        col_vals = norm[col].astype(float)
        vmin = np.nanmin(col_vals.values) if not np.isnan(col_vals.values).all() else np.nan
        vmax = np.nanmax(col_vals.values) if not np.isnan(col_vals.values).all() else np.nan
        if vmin == vmax or np.isnan(vmin) or np.isnan(vmax):
            # all equal or all NaN -> set to 0.5 so it renders neutral
            norm[col] = 0.5
        else:
            norm[col] = (col_vals - vmin) / (vmax - vmin)
    return norm

# =========================
# Load & normalize
# =========================
@st.cache_data
def load_kpis():
    path = "config/kpis.csv"
    if not os.path.exists(path):
        # empty scaffold
        return pd.DataFrame(columns=["vendor","kpi","value","unit","date"])

    df = pd.read_csv(path)

    # Try to map flexible column names
    vendor_col = _col_in(df, ["vendor", "company", "supplier"])
    kpi_col    = _col_in(df, ["kpi", "metric", "name"])
    value_col  = _col_in(df, ["value", "amount", "val"])
    unit_col   = _col_in(df, ["unit", "units"])
    date_col   = _col_in(df, ["date", "as_of", "period_end", "period"])

    # Ensure all present in the working frame
    out = pd.DataFrame()
    out["vendor"] = df[vendor_col].astype(str) if vendor_col else ""
    out["kpi"]    = df[kpi_col].astype(str)    if kpi_col    else ""
    out["unit"]   = df[unit_col].astype(str)   if unit_col   else ""

    # numeric value (parse if needed)
    if value_col:
        out["value"] = df[value_col].apply(parse_numeric).astype(float)
    else:
        out["value"] = np.nan

    # date (optional)
    if date_col:
        out["date"] = pd.to_datetime(df[date_col], errors="coerce")
    else:
        out["date"] = pd.NaT

    # Clean blanks
    out["vendor"] = out["vendor"].fillna("").str.strip()
    out["kpi"]    = out["kpi"].fillna("").str.strip()
    out["unit"]   = out["unit"].fillna("").str.strip()

    # Drop rows with no vendor or KPI
    out = out[(out["vendor"] != "") & (out["kpi"] != "")]
    return out

df = load_kpis()

st.title("📈 Commercial KPI Dashboard")

if df.empty:
    st.info("No KPI data found. Please populate `config/kpis.csv` with columns like: vendor,kpi,value,unit,date.")
    st.stop()

# =========================
# Filters
# =========================
all_vendors = sorted([v for v in df["vendor"].dropna().unique() if str(v).strip()])
all_kpis    = sorted([k for k in df["kpi"].dropna().unique() if str(k).strip()])

c1, c2, c3 = st.columns([2, 2, 2])
with c1:
    sel_vendors = st.multiselect("Vendors", all_vendors, default=all_vendors[: min(5, len(all_vendors))])
with c2:
    sel_kpis = st.multiselect("KPIs", all_kpis, default=all_kpis[: min(5, len(all_kpis))])
with c3:
    has_dates = df["date"].notna().any()
    if has_dates:
        dmin, dmax = df["date"].min().date(), df["date"].max().date()
        date_range = st.date_input("Date range", value=(dmin, dmax), min_value=dmin, max_value=dmax)
    else:
        date_range = None

flt = df.copy()
if sel_vendors:
    flt = flt[flt["vendor"].isin(sel_vendors)]
if sel_kpis:
    flt = flt[flt["kpi"].isin(sel_kpis)]
if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
    start_d, end_d = date_range
    flt = flt[(flt["date"].dt.date >= start_d) & (flt["date"].dt.date <= end_d)]

st.caption(f"{len(flt)} rows after filters")

# =========================
# Overview cards (latest per vendor/KPI)
# =========================
colA, colB, colC = st.columns(3)
latest_any_date = flt[flt["date"].notna()]
latest_count = 0
if not latest_any_date.empty:
    latest_count = latest_any_date.sort_values("date").groupby(["vendor","kpi"]).tail(1).shape[0]

with colA:
    st.metric("Vendors", value=len(set(flt["vendor"])))
with colB:
    st.metric("KPIs", value=len(set(flt["kpi"])))
with colC:
    st.metric("Latest points", value=int(latest_count))

st.divider()

# =========================
# Tabs
# =========================
tab1, tab2, tab3 = st.tabs(["📋 Table", "📉 Trends", "🗺️ Heatmap"])

# ---- Table ----
with tab1:
    # Present friendly value+unit
    view = flt.copy()
    view["value_display"] = [format_value(v, u) for v, u in zip(view["value"], view["unit"])]
    # Reorder columns for readability
    cols = ["vendor","kpi","value_display","value","unit","date"]
    cols = [c for c in cols if c in view.columns]
    view = view[cols]
    # Sort if possible
    sort_cols = [c for c in ["vendor","kpi","date"] if c in view.columns]
    if sort_cols:
        try:
            view = view.sort_values(sort_cols)
        except Exception:
            pass
    st.dataframe(view, width="stretch", height=420)

# ---- Trends ----
with tab2:
    if flt.empty:
        st.info("No data to chart.")
    else:
        # Choose one KPI (or All) and one Vendor (or All)
        t1, t2 = st.columns(2)
        with t1:
            kpi_pick = st.selectbox("KPI for trends", ["(All)"] + all_kpis, index=0)
        with t2:
            vendor_pick = st.selectbox("Vendor for trends", ["(All)"] + all_vendors, index=0)

        trend = flt.copy()
        if kpi_pick != "(All)":
            trend = trend[trend["kpi"] == kpi_pick]
        if vendor_pick != "(All)":
            trend = trend[trend["vendor"] == vendor_pick]

        # if no dates, show latest bar by vendor/kpi
        if trend["date"].notna().any():
            trend = trend.dropna(subset=["date"])
            if trend.empty:
                st.info("No dated values to plot.")
            else:
                # Build series label and pivot
                trend = trend.sort_values("date")
                trend["series"] = trend["vendor"] + " · " + trend["kpi"]
                piv = trend.pivot_table(index="date", columns="series", values="value", aggfunc="mean").sort_index()
                if piv.empty:
                    st.info("Nothing to plot.")
                else:
                    fig, ax = plt.subplots(figsize=(9, 4), dpi=140)
                    for col in piv.columns:
                        ax.plot(piv.index, piv[col], label=str(col))
                    ax.set_xlabel("Date")
                    ax.set_ylabel("Value")
                    ax.set_title("KPI Trends")
                    if len(piv.columns) <= 10:
                        ax.legend(loc="best", fontsize=8)
                    ax.grid(True, alpha=0.3)
                    st.pyplot(fig)
        else:
            # No dates at all → bar of latest/mean by vendor & KPI
            latest = trend.copy()
            if "date" in latest.columns and latest["date"].notna().any():
                latest = latest.sort_values("date").groupby(["vendor","kpi"], as_index=False).tail(1)
            else:
                latest = latest.groupby(["vendor","kpi"], as_index=False)["value"].mean()
            if latest.empty:
                st.info("No values to plot.")
            else:
                bar = latest.pivot_table(index="vendor", columns="kpi", values="value", aggfunc="mean")
                st.bar_chart(bar)

# ---- Heatmap ----
with tab3:
    if flt.empty:
        st.info("No data to display.")
    else:
        # Let user choose 'Latest by date' vs 'Average'
        agg_choice = st.radio(
            "Heatmap aggregation",
            ["Latest by date (if any)", "Average"],
            index=0,
            horizontal=True,
        )

        work = flt.copy()
        # For annotations, we also want units. We'll create two matrices:
        #   values_matrix: numeric values
        #   text_matrix:   formatted value + unit for display
        if agg_choice.startswith("Latest") and work["date"].notna().any():
            work = work.sort_values("date").dropna(subset=["date"])
            latest = work.groupby(["vendor","kpi"], as_index=False).tail(1)
            values_matrix = latest.pivot_table(index="vendor", columns="kpi", values="value", aggfunc="mean")
            # Build matching text matrix (value + unit)
            latest["val_txt"] = [format_value(v, u) for v,u in zip(latest["value"], latest["unit"])]
            text_matrix = latest.pivot_table(index="vendor", columns="kpi", values="val_txt", aggfunc=lambda x: x.iloc[0] if len(x)>0 else "")
            title = "Heatmap (Latest values)"
        else:
            # Average over filters
            values_matrix = work.pivot_table(index="vendor", columns="kpi", values="value", aggfunc="mean")
            # Build unit-aware text by taking the most common unit for each (vendor,kpi)
            unit_pick = work.groupby(["vendor","kpi"])["unit"].agg(lambda s: s.mode().iloc[0] if len(s.mode()) else "")
            unit_pick = unit_pick.reset_index().rename(columns={"unit":"_unit"})
            mean_with_u = work.groupby(["vendor","kpi"], as_index=False)["value"].mean().merge(unit_pick, on=["vendor","kpi"], how="left")
            mean_with_u["val_txt"] = [format_value(v, u) for v,u in zip(mean_with_u["value"], mean_with_u["_unit"])]
            text_matrix = mean_with_u.pivot_table(index="vendor", columns="kpi", values="val_txt", aggfunc=lambda x: x.iloc[0] if len(x)>0 else "")
            title = "Heatmap (Average values)"

        if values_matrix.empty:
            st.info("No values to display in heatmap with current filters.")
        else:
            # Normalize per KPI for fair coloring across units/scales
            norm_matrix = normalize_by_kpi(values_matrix.astype(float))

            # Plot heatmap with annotations (original text values)
            fig_w = max(6, len(values_matrix.columns) * 0.9)
            fig_h = max(4, len(values_matrix.index) * 0.5)
            fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=140)
            im = ax.imshow(norm_matrix.values, aspect="auto")  # default colormap

            ax.set_xticks(np.arange(values_matrix.shape[1]))
            ax.set_yticks(np.arange(values_matrix.shape[0]))
            ax.set_xticklabels(values_matrix.columns, rotation=45, ha="right")
            ax.set_yticklabels(values_matrix.index)
            ax.set_title(title)
            ax.set_xlabel("KPI")
            ax.set_ylabel("Vendor")

            cbar = fig.colorbar(im, ax=ax)
            cbar.ax.set_ylabel("Normalized intensity", rotation=270, labelpad=12)

            # Annotate with original value + unit
            # Limit annotations if matrix is very large
            if values_matrix.shape[0] * values_matrix.shape[1] <= 400:
                for i in range(values_matrix.shape[0]):
                    for j in range(values_matrix.shape[1]):
                        txt = ""
                        try:
                            txt = str(text_matrix.iloc[i, j])
                        except Exception:
                            pass
                        if txt and txt != "nan":
                            ax.text(j, i, txt, ha="center", va="center", fontsize=7, color="black")

            st.pyplot(fig)
