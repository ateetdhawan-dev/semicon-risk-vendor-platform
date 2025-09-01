# pages/02_Commercial_KPI.py
# Robust, foundry- & equipment-relevant commercial/risk KPIs with safe fallbacks.
from pathlib import Path
from datetime import date
import json, sqlite3
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st

st.set_page_config(page_title="Commercial KPIs (Foundry & Equipment)", layout="wide")
st.title("Commercial KPIs — Foundry Deployment & Equipment Supply")

# ---------- Personas & KPI intent ----------
# We center KPIs that actually matter for (a) foundry ramp/readiness and (b) equipment vendors’ allocation risk.
# Pillars (all computed ONLY if columns exist; no crashes):
# 1) Supply Tightness: Book-to-Bill 3MMA, Backlog Coverage (months)
# 2) Delivery Reliability: Lead Time (median), OTIF%
# 3) Concentration Risk: Max Customer Share %, HHI
# 4) Momentum & Share: Portfolio Share %, 3-month Momentum
# 5) Serviceability (optional if present): Spares Fill %, Field FTE
# These drive a Foundry Readiness Score (0–100). Volume KPIs (billings/bookings) are NOT shown as tiles by default.

KPI_GLOSSARY = {
    "Book-to-Bill (3MMA)":
        "Rolling 3-month orders-to-billings ratio (>1 = demand > supply; allocation risk).",
    "Backlog Coverage (months)":
        "Backlog ÷ (TTM billings/12). Higher means longer queue/time-to-ship.",
    "Lead Time (median, weeks)":
        "Median quoted/shipped lead time for tools/parts.",
    "OTIF (%)":
        "On-Time-In-Full delivery rate (derived if explicit flags missing).",
    "Max Customer Share (%)":
        "Largest single-customer share for a vendor (allocation dependency).",
    "HHI (0–10,000)":
        "Customer concentration index per vendor; higher = more concentrated.",
    "Portfolio Share (%)":
        "Share of selected portfolio captured by each vendor.",
    "Momentum 3m (%)":
        "3-month rolling value vs prior 3-month window (leading indicator).",
    "Foundry Readiness Score (0–100)":
        "Composite of delivery reliability, tightness, concentration & serviceability (weighted by what exists in data)."
}

# Major equipment & materials vendors (coverage check) + key IDMs/foundries (for selection lists)
VENDOR_MASTER = [
    # Lithography & patterning
    "ASML","Nikon","Canon",
    # Deposition/Etch/Clean
    "Applied Materials","Lam Research","Tokyo Electron","ASM International","SCREEN",
    # Metrology/Test
    "KLA","Advantest","Teradyne","Hitachi High-Tech","ACCRETECH (Tokyo Seimitsu)",
    # Vacuum/CMP/Gas/Chemicals/Materials/Contamination control
    "Ebara","Edwards","MKS Instruments","Entegris","JSR","TOK (Tokyo Ohka Kogyo)",
    "Shin-Etsu Chemical","SUMCO","DuPont","Linde","Air Liquide","Versum","BASF",
    # Facilities/Parts/Service (examples)
    "Applied Global Services","Novatek Microelectronics","Thorlabs","Brooks",
]

CUSTOMER_MASTER = [
    "TSMC","Samsung","Intel","Micron","SK hynix","Texas Instruments","STMicroelectronics",
    "Infineon","NXP","Renesas","GlobalFoundries","UMC","SMIC","Tower","Bosch"
]

# Synonyms so we can auto-map arbitrary sources
SYN = {
    "date":     ["date","period","as_of","period_date","published","published_at","dt"],
    "vendor":   ["vendor","vendor_name","supplier","seller","partner","oem","maker"],
    "customer": ["customer","customer_name","client","buyer","account","fab","foundry","idm"],
    "region":   ["region","geo","market","country"],
    "product":  ["product","sku","tool","line","family","platform","node","node_name"],
    # commercial values (quietly used to compute tightness/ratios; not shown unless enabled)
    "orders":   ["orders","order_value","order_usd","po_value","po_usd","bookings"],
    "billings": ["billings","shipments","invoice_value","billed_usd","revenue","sales"],
    "backlog":  ["backlog","backlog_usd","open_orders_usd"],
    "units":    ["units","qty","quantity","shipped_units"],
    # delivery quality / OTIF derivation
    "lead_time_weeks": ["lead_time_weeks","lt_weeks","leadtime_weeks"],
    "lead_time_days":  ["lead_time_days","lt_days","leadtime_days"],
    "promise_date":    ["promise_date","promised_date","requested_date"],
    "delivery_date":   ["delivery_date","delivered_date","ship_date","shipment_date"],
    "committed_qty":   ["committed_qty","promised_qty","po_qty"],
    "delivered_qty":   ["delivered_qty","shipped_qty","actual_qty"],
    "on_time_flag":    ["on_time","ontime","on_time_flag"],
    "in_full_flag":    ["in_full","infull","in_full_flag","complete_delivery"],
    # optional serviceability
    "spares_fill_rate": ["spares_fill_rate","spares_service_level","spares_otif"],
    "fte_on_site":      ["fte_on_site","field_eng_onsite","support_coverage","tech_coverage"],
    "risk_score":       ["risk_score","riskindex","risk_index"],
    "asp":              ["asp","avg_selling_price"]
}

# Where we look for data
CONFIG_PATH   = Path("config/commercial_loader.json")   # optional {source: db|csv, path, table?, columns?}
FALLBACK_CSV  = Path("export/commercial_kpi.csv")       # simple CSV fallback if no config
WEIGHTS_PATH  = Path("config/kpi_weights.json")         # optional weights override for readiness score

# ---------- helpers ----------
def _first_col(df, keys):
    lut = {c.lower(): c for c in df.columns}
    for k in keys:
        if k in lut: return lut[k]
    return None

def _coerce_dates(df):
    if "date" in df.columns:
        d = pd.to_datetime(df["date"], errors="coerce", utc=True)
        try:
            df["date"] = d.dt.tz_convert(None)
        except Exception:
            df["date"] = d
        df["date"] = df["date"].dt.normalize()
    return df

def _to_month(s):
    dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.to_period("M").dt.to_timestamp()

def _numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _safe_div(num, den):
    # works for Series/ndarray/scalars without raising
    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        out = num / den
    if isinstance(out, pd.Series):
        return out.replace([np.inf, -np.inf], np.nan)
    return np.nan if not np.isfinite(out) else out

def _map_df(raw, mapping):
    out = pd.DataFrame()
    for k, v in mapping.items():
        if k == "metrics":
            for m in v:
                if m in raw.columns:
                    out[m] = pd.to_numeric(raw[m], errors="coerce")
        else:
            if v and v in raw.columns:
                out[k] = raw[v]
    return _coerce_dates(out)

def _post_read(df):
    # rename synonyms onto standard names
    ren = {}
    for std, syns in SYN.items():
        c = _first_col(df, syns)
        if c and c != std:
            ren[c] = std
    if ren:
        df = df.rename(columns=ren)
    df = _coerce_dates(df)
    if "lead_time_weeks" not in df.columns and "lead_time_days" in df.columns:
        df["lead_time_weeks"] = pd.to_numeric(df["lead_time_days"], errors="coerce") / 7.0
    _numeric(df, [
        "orders","billings","backlog","units",
        "lead_time_weeks","committed_qty","delivered_qty",
        "risk_score","asp","spares_fill_rate","fte_on_site"
    ])
    if "asp" not in df.columns and ("billings" in df.columns and "units" in df.columns):
        df["asp"] = _safe_div(df["billings"], df["units"])
    return df

# ---------- load data ----------
def _load_from_config():
    if not CONFIG_PATH.exists():
        return None, {"mode":"no-config"}
    meta = {"mode":"config"}
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        meta["config"] = cfg
        src, path, table = cfg.get("source"), cfg.get("path"), cfg.get("table")
        cols = cfg.get("columns", {})
        p = Path(path) if path else None
        if not src or not p or not p.exists():
            return None, {**meta, "error":"Invalid source/path in config"}
        if src == "db":
            if not table: return None, {**meta, "error":"Missing table for db source"}
            with sqlite3.connect(p) as con:
                raw = pd.read_sql_query(f"SELECT * FROM {table}", con)
        elif src == "csv":
            raw = pd.read_csv(p)
        else:
            return None, {**meta, "error":f"Unknown source {src}"}

        if cols:
            df = _map_df(raw, cols)
        else:
            # infer basic mapping
            mapping = {}
            for k in ["date","vendor","customer","region","product"]:
                c = _first_col(raw, SYN[k]);  mapping[k] = c if c else None
            metrics = []
            for key in ["orders","billings","backlog","units","lead_time_weeks","lead_time_days",
                        "committed_qty","delivered_qty","risk_score","asp",
                        "spares_fill_rate","fte_on_site"]:
                c = _first_col(raw, SYN[key])
                if c: metrics.append(c)
            mapping["metrics"] = metrics
            df = _map_df(raw, mapping)
            meta["inferred_mapping"] = mapping

        return _post_read(df), meta
    except Exception as e:
        return None, {**meta, "error": str(e)}

def _load_from_csv():
    if not FALLBACK_CSV.exists():
        return None, {"mode":"no-csv"}
    try:
        df = pd.read_csv(FALLBACK_CSV)
        return _post_read(df), {"mode":"csv","path": str(FALLBACK_CSV)}
    except Exception as e:
        return None, {"mode":"csv","error": str(e)}

@st.cache_data(ttl=300, show_spinner=False)
def load_df():
    df, meta = _load_from_config()
    if df is None:
        df, meta = _load_from_csv()
    return df, meta

df, meta = load_df()
if df is None or df.empty:
    st.error("No commercial data found. Provide config/commercial_loader.json or export/commercial_kpi.csv and reload.")
    st.stop()

# ---------- Glossary & Source ----------
with st.expander("KPI Glossary (click to expand)"):
    for k, v in KPI_GLOSSARY.items():
        st.markdown(f"**{k}** — {v}")
with st.expander("Data source / mapping", expanded=False):
    st.write(meta)
    st.write("Columns:", list(df.columns))

# ---------- Filters (top-of-page, not sidebar) ----------
has_date = "date" in df.columns and df["date"].notna().any()
dmin = pd.to_datetime(df["date"]).min().date() if has_date else date.today()
dmax = pd.to_datetime(df["date"]).max().date() if has_date else date.today()

vendors   = sorted(df["vendor"].dropna().unique())   if "vendor" in df.columns else []
customers = sorted(df["customer"].dropna().unique()) if "customer" in df.columns else []
regions   = sorted(df["region"].dropna().unique())   if "region" in df.columns else []

c1,c2,c3,c4 = st.columns([2,2,2,3])
with c1: pick_vendor   = st.multiselect("Vendor",   vendors,   default=vendors)
with c2: pick_customer = st.multiselect("Customer", customers, default=customers)
with c3: pick_region   = st.multiselect("Region",   regions,   default=regions)
with c4:
    dr = st.date_input("Date range", value=(dmin, dmax), min_value=dmin, max_value=dmax,
                       format="YYYY-MM-DD", disabled=not has_date)

view = df.copy()
if pick_vendor and "vendor" in view.columns:
    view = view[view["vendor"].isin(pick_vendor)]
if pick_customer and "customer" in view.columns:
    view = view[view["customer"].isin(pick_customer)]
if pick_region and "region" in view.columns:
    view = view[view["region"].isin(pick_region)]
if has_date and isinstance(dr,(list,tuple)) and len(dr)==2:
    start, end = dr
    dd = pd.to_datetime(view["date"], errors="coerce")
    view = view[(dd.dt.date >= start) & (dd.dt.date <= end)]

if view.empty:
    st.info("No data for these filters.")
    st.stop()

view["month"] = _to_month(view["date"])

# Base numeric columns (quiet internal use; not displayed as tiles unless user opts in)
ORD_COL = _first_col(view, ["orders"]) or _first_col(view, SYN["orders"])
BIL_COL = _first_col(view, ["billings"]) or _first_col(view, SYN["billings"])
BASE_COL = BIL_COL or ORD_COL  # used for shares/momentum if needed

# ---------- Coverage vs master lists (optional) ----------
with st.expander("Coverage vs master vendor/customer lists", expanded=False):
    present_v = set(view["vendor"].dropna().unique())   if "vendor" in view.columns else set()
    present_c = set(view["customer"].dropna().unique()) if "customer" in view.columns else set()
    miss_v = sorted(set(VENDOR_MASTER) - present_v)
    miss_c = sorted(set(CUSTOMER_MASTER) - present_c)
    st.write(f"Vendors present: {len(present_v)} / {len(VENDOR_MASTER)}")
    st.caption(", ".join(sorted(present_v)) if present_v else "—")
    st.write("Missing vendors:", ", ".join(miss_v) if miss_v else "—")
    st.write(f"Customers present: {len(present_c)} / {len(CUSTOMER_MASTER)}")
    st.caption(", ".join(sorted(present_c)) if present_c else "—")
    st.write("Missing customers:", ", ".join(miss_c) if miss_c else "—")

# ---------- KPI helpers (safe, no .iloc on ndarray, no scalar .replace) ----------
def book_to_bill_3mma(v):
    if "month" not in v.columns or ORD_COL is None or BIL_COL is None: return np.nan
    g = v.groupby("month", dropna=False)[[ORD_COL, BIL_COL]].sum(min_count=1).sort_index()
    if g.empty: return np.nan
    g["ord_3m"] = g[ORD_COL].rolling(3, min_periods=1).sum()
    g["bil_3m"] = g[BIL_COL].rolling(3, min_periods=1).sum()
    r = _safe_div(g["ord_3m"], g["bil_3m"])
    return float(r.iloc[-1]) if len(r) else np.nan

def backlog_coverage_months(v):
    if "backlog" not in v.columns or BIL_COL is None or "month" not in v.columns: return np.nan
    g = v.groupby("month", dropna=False)[["backlog", BIL_COL]].sum(min_count=1).sort_index()
    if g.empty: return np.nan
    ttm_bil = g[BIL_COL].tail(12).sum(skipna=True)
    if not ttm_bil or float(ttm_bil) == 0.0: return np.nan
    latest_backlog = float(g["backlog"].iloc[-1])
    return latest_backlog / (float(ttm_bil)/12.0)

def dependency_index_max_share(v):
    if BASE_COL is None or "vendor" not in v.columns or "customer" not in v.columns: return np.nan
    g = (v.groupby(["vendor","customer"], dropna=False)[BASE_COL]
            .sum(min_count=1).reset_index().rename(columns={BASE_COL:"val"}))
    if g.empty: return np.nan
    tot = g.groupby("vendor", as_index=False)["val"].sum().rename(columns={"val":"vtot"})
    g = g.merge(tot, on="vendor", how="left")
    g["share"] = _safe_div(g["val"], g["vtot"]).clip(lower=0, upper=1)
    per_vendor_max = g.groupby("vendor", as_index=False)["share"].max()
    return float(per_vendor_max["share"].max()*100.0) if not per_vendor_max.empty else np.nan

def hhi_vendor_max(v):
    if BASE_COL is None or "vendor" not in v.columns or "customer" not in v.columns: return np.nan
    g = (v.groupby(["vendor","customer"], dropna=False)[BASE_COL]
            .sum(min_count=1).reset_index().rename(columns={BASE_COL:"val"}))
    if g.empty: return np.nan
    tot = g.groupby("vendor", as_index=False)["val"].sum().rename(columns={"val":"vtot"})
    g = g.merge(tot, on="vendor", how="left")
    g["share"] = _safe_div(g["val"], g["vtot"]).clip(lower=0, upper=1)
    hhi = g.groupby("vendor")["share"].apply(lambda s: int(round((s**2).sum()*10000)))
    return int(hhi.max()) if len(hhi) else np.nan

# ---------- Portfolio tiles (risk-first)
k1,k2,k3,k4 = st.columns(4)
b2b  = book_to_bill_3mma(view)
bcov = backlog_coverage_months(view)
dep  = dependency_index_max_share(view)
hhi  = hhi_vendor_max(view)

k1.metric("Book-to-Bill (3MMA)", f"{b2b:.2f}" if pd.notna(b2b) else "—", help=KPI_GLOSSARY["Book-to-Bill (3MMA)"])
k2.metric("Backlog Coverage (months)", f"{bcov:.1f}" if pd.notna(bcov) else "—", help=KPI_GLOSSARY["Backlog Coverage (months)"])
k3.metric("Highest Vendor Dependency", f"{dep:.1f}%" if pd.notna(dep) else "—", help=KPI_GLOSSARY["Max Customer Share (%)"])
k4.metric("Max HHI (vendor)", f"{hhi:.0f}" if pd.notna(hhi) else "—", help=KPI_GLOSSARY["HHI (0–10,000)"])

st.markdown("---")

# ---------- Vendor × Customer Risk Matrix ----------
st.subheader("Vendor × Customer Matrix")
show_volume = st.checkbox("Include volume metrics (billings/bookings/units/ASP)", value=False)

choices = []
if "backlog" in view.columns: choices.append(("Backlog", "backlog"))
# If there is no backlog, allow fallback to base metric even if volume toggle is off
if not choices and BASE_COL:
    choices.append(("Base Metric", BASE_COL))
if show_volume:
    if BIL_COL: choices.append(("Billings", BIL_COL))
    if ORD_COL: choices.append(("Bookings", ORD_COL))
    if "units" in view.columns: choices.append(("Units", "units"))
    if "asp" in view.columns:   choices.append(("ASP", "asp"))

if not choices:
    st.info("No numeric metric available for the matrix. Add Backlog or enable a volume metric.")
else:
    labels = [lbl for lbl,_ in choices]
    keys   = [key for _,key in choices]
    left, right = st.columns([2,2])
    with left:
        sel_lbl = st.selectbox("Matrix metric", options=labels, index=0)
        sel_col = keys[labels.index(sel_lbl)]
    with right:
        mode = st.radio("Cell value", ["Absolute", "Within-Vendor Share (%)"], horizontal=True)

    base = view.dropna(subset=[sel_col]) if sel_col in view.columns else pd.DataFrame()
    if base.empty or "vendor" not in base.columns or "customer" not in base.columns:
        st.info("Matrix needs vendor, customer, and a numeric metric.")
    else:
        abs_df = (base.groupby(["vendor","customer"], dropna=False)[sel_col]
                        .sum(min_count=1).reset_index().rename(columns={sel_col:"value"}))
        if mode == "Absolute":
            to_plot = abs_df.copy()
            ctitle, fmt = sel_lbl, ",.0f"
        else:
            tot = abs_df.groupby("vendor", as_index=False)["value"].sum().rename(columns={"value":"vtot"})
            to_plot = abs_df.merge(tot, on="vendor", how="left")
            to_plot["value"] = _safe_div(to_plot["value"], to_plot["vtot"]) * 100.0
            ctitle, fmt = f"{sel_lbl} Share %", ".1f"

        chart = alt.Chart(to_plot).mark_rect().encode(
            x=alt.X("customer:N", title="Customer", sort="ascending"),
            y=alt.Y("vendor:N",   title="Vendor",   sort="ascending"),
            color=alt.Color("value:Q", title=ctitle),
            tooltip=[alt.Tooltip("vendor:N"), alt.Tooltip("customer:N"),
                     alt.Tooltip("value:Q", title=ctitle, format=fmt)]
        )
        st.altair_chart(chart, use_container_width=True)

# ---------- Trends (risk-first)
st.markdown("---")
st.subheader("Trends")
tcols = []
if "backlog" in view.columns: tcols.append(("Backlog (sum)", "backlog", "sum"))
if show_volume and "asp" in view.columns: tcols.append(("ASP (avg)", "asp", "mean"))
if tcols and "month" in view.columns:
    sel_t = st.selectbox("Trend series", options=[t[0] for t in tcols], index=0)
    _, tcol, agg = next(t for t in tcols if t[0]==sel_t)
    tr = (view.groupby("month", dropna=False)[tcol]
            .agg(agg).reset_index().rename(columns={tcol:"value"}).sort_values("month"))
    line = alt.Chart(tr).mark_line(point=True).encode(
        x=alt.X("month:T", title="Month"),
        y=alt.Y("value:Q", title=sel_t),
        tooltip=[alt.Tooltip("month:T"), alt.Tooltip("value:Q", format=",.0f")]
    )
    st.altair_chart(line, use_container_width=True)
else:
    st.caption("No trend-ready series for the current filters.")

# ---------- Per-Vendor KPIs & Readiness ----------
st.markdown("---")
st.subheader("Vendor KPIs for Foundry Ramp")

# Default weights (can override via config/kpi_weights.json)
WEIGHTS = {
    "otif": 0.25,
    "lead_time": 0.25,
    "supply_tightness": 0.20,
    "concentration": 0.20,
    "serviceability": 0.10,  # combines spares & FTE if present
}
try:
    if WEIGHTS_PATH.exists():
        override = json.loads(WEIGHTS_PATH.read_text(encoding="utf-8"))
        for k,v in override.items():
            if k in WEIGHTS and isinstance(v,(int,float)): WEIGHTS[k]=float(v)
except Exception:
    pass

if BASE_COL and "vendor" in view.columns:
    base_by_vendor = view.groupby("vendor", dropna=False)[BASE_COL].sum(min_count=1)
    port_total = float(base_by_vendor.sum(skipna=True)) if base_by_vendor.size else 0.0

    # B2B 3MMA per vendor
    b2b_df = pd.DataFrame(columns=["vendor","b2b_3mma"])
    if ORD_COL and BIL_COL and "month" in view.columns:
        gb = view.groupby(["vendor","month"], dropna=False)[[ORD_COL, BIL_COL]].sum(min_count=1).reset_index()
        rows = []
        for v, g in gb.groupby("vendor", dropna=False):
            g = g.sort_values("month")
            g["o3"] = g[ORD_COL].rolling(3, min_periods=1).sum()
            g["b3"] = g[BIL_COL].rolling(3, min_periods=1).sum()
            r = _safe_div(g["o3"], g["b3"])
            rows.append({"vendor": v, "b2b_3mma": float(r.iloc[-1]) if len(r) else np.nan})
        b2b_df = pd.DataFrame(rows)

    # Backlog coverage per vendor
    bcov_df = pd.DataFrame(columns=["vendor","backlog_months"])
    if "backlog" in view.columns and BIL_COL and "month" in view.columns:
        gb = view.groupby(["vendor","month"], dropna=False)[["backlog", BIL_COL]].sum(min_count=1).reset_index()
        rows = []
        for v, g in gb.groupby("vendor", dropna=False):
            g = g.sort_values("month")
            ttm_bil = float(g[BIL_COL].tail(12).sum(skipna=True))
            cov = np.nan
            if ttm_bil and ttm_bil != 0.0 and len(g):
                cov = float(g["backlog"].iloc[-1]) / (ttm_bil/12.0)
            rows.append({"vendor": v, "backlog_months": cov})
        bcov_df = pd.DataFrame(rows)
    if "vendor" not in bcov_df.columns:
        bcov_df = pd.DataFrame(columns=["vendor","backlog_months"])

    # Dependency & HHI
    dep_df = pd.DataFrame(columns=["vendor","dep_max_share"])
    hhi_df = pd.DataFrame(columns=["vendor","hhi"])
    if "customer" in view.columns:
        g = (view.groupby(["vendor","customer"], dropna=False)[BASE_COL]
                .sum(min_count=1).reset_index().rename(columns={BASE_COL:"val"}))
        if not g.empty:
            tot = g.groupby("vendor", as_index=False)["val"].sum().rename(columns={"val":"vtot"})
            g = g.merge(tot, on="vendor", how="left")
            g["share"] = _safe_div(g["val"], g["vtot"]).clip(lower=0, upper=1)
            dep_df = g.groupby("vendor", as_index=False)["share"].max().rename(columns={"share":"dep_max_share"})
            hhi_df = g.groupby("vendor", as_index=False)["share"].apply(
                lambda s: int(round((s**2).sum()*10000))
            ).rename(columns={"share":"hhi"})
    if "vendor" not in dep_df.columns: dep_df = pd.DataFrame(columns=["vendor","dep_max_share"])
    if "vendor" not in hhi_df.columns: hhi_df = pd.DataFrame(columns=["vendor","hhi"])

    # Lead time (median weeks)
    lt_df = pd.DataFrame(columns=["vendor","lead_time_median_w"])
    if "lead_time_weeks" in view.columns:
        lt_df = (view.dropna(subset=["lead_time_weeks"])
                    .groupby("vendor", dropna=False)["lead_time_weeks"]
                    .median().reset_index().rename(columns={"lead_time_weeks":"lead_time_median_w"}))

    # OTIF (%), derive if flags missing
    otif_df = pd.DataFrame(columns=["vendor","otif_pct"])
    has_otif = (("on_time_flag" in view.columns) or (("promise_date" in view.columns) and ("delivery_date" in view.columns))) and \
               (("in_full_flag" in view.columns) or (("committed_qty" in view.columns) and ("delivered_qty" in view.columns)))
    if has_otif:
        ot = view.copy()
        if "on_time_flag" not in ot.columns and "promise_date" in ot.columns and "delivery_date" in ot.columns:
            p = pd.to_datetime(ot["promise_date"], errors="coerce")
            d = pd.to_datetime(ot["delivery_date"], errors="coerce")
            ot["on_time_flag"] = (d <= p)
        if "in_full_flag" not in ot.columns and "committed_qty" in ot.columns and "delivered_qty" in ot.columns:
            committed = pd.to_numeric(ot["committed_qty"], errors="coerce")
            delivered = pd.to_numeric(ot["delivered_qty"], errors="coerce")
            ot["in_full_flag"] = delivered >= committed
        # boolean mean -> %; guard empties
        def _otif_pct(x):
            try:
                return (x["on_time_flag"].astype("boolean") & x["in_full_flag"].astype("boolean")).mean(skipna=True)*100
            except Exception:
                return np.nan
        tmp = ot.groupby("vendor", dropna=False).apply(_otif_pct).reset_index(name="otif_pct")
        otif_df = tmp

    # Serviceability (optional)
    support_df = pd.DataFrame(columns=["vendor","fte_on_site"])
    if "fte_on_site" in view.columns:
        support_df = (view.groupby("vendor", dropna=False)["fte_on_site"]
                         .median().reset_index())
    spares_df = pd.DataFrame(columns=["vendor","spares_fill_rate"])
    if "spares_fill_rate" in view.columns:
        spares_df = (view.groupby("vendor", dropna=False)["spares_fill_rate"]
                        .mean().reset_index())

    # Portfolio share & momentum
    share_df = pd.DataFrame(columns=["vendor","portfolio_share_pct"])
    if port_total and port_total != 0.0:
        share_df = (base_by_vendor / port_total * 100.0).rename("portfolio_share_pct").reset_index()

    mom_df = pd.DataFrame(columns=["vendor","mom_3m_pct"])
    if "month" in view.columns:
        m = (view.groupby(["vendor","month"], dropna=False)[BASE_COL]
                .sum(min_count=1).reset_index().sort_values(["vendor","month"]))
        rows = []
        for v, g in m.groupby("vendor", dropna=False):
            g = g.sort_values("month")
            s3  = g[BASE_COL].rolling(3, min_periods=1).sum()
            s3p = s3.shift(3)
            mom = (s3 - s3p) / s3p * 100.0
            rows.append({"vendor": v, "mom_3m_pct": float(mom.iloc[-1]) if len(mom) else np.nan})
        mom_df = pd.DataFrame(rows)

    # Assemble safely
    out = pd.DataFrame({"vendor": sorted(base_by_vendor.index.tolist())})
    for d in [share_df, b2b_df, bcov_df, dep_df, hhi_df, lt_df, otif_df, support_df, spares_df, mom_df]:
        if d is None: 
            continue
        if "vendor" not in d.columns:
            # keep merge stable even for empty frames
            d = pd.DataFrame(columns=["vendor"])
        out = out.merge(d, on="vendor", how="left")

    # ---- Readiness Score (normalize; use only available parts) ----
    def _norm_desc(x, lo, hi):  # lower is better
        x = pd.to_numeric(x, errors="coerce")
        return np.clip((hi - x) / (hi - lo), 0, 1)
    def _norm_asc(x, lo, hi):   # higher is better
        x = pd.to_numeric(x, errors="coerce")
        return np.clip((x - lo) / (hi - lo), 0, 1)

    # supply tightness subscore (higher tightness => worse, so invert)
    out["tight_b2b"]  = 1 - _norm_asc(out.get("b2b_3mma"),         0.8, 1.4)
    out["tight_bcov"] = 1 - _norm_asc(out.get("backlog_months"),   1.0, 9.0)
    out["supply_tightness_score"] = np.nanmean(out[["tight_b2b","tight_bcov"]].to_numpy(dtype=float), axis=1)

    # concentration subscore (lower dependency/hhi => better)
    out["conc_dep"] = 1 - pd.to_numeric(out.get("dep_max_share"), errors="coerce")
    out["conc_hhi"] = 1 - _norm_asc(out.get("hhi"), 1000, 8000)
    out["concentration_score"] = np.nanmean(out[["conc_dep","conc_hhi"]].to_numpy(dtype=float), axis=1)

    # delivery subscores
    out["lead_time_score"] = _norm_desc(out.get("lead_time_median_w"), 4.0, 26.0)
    out["otif_score"]      = _norm_asc(out.get("otif_pct"), 70.0, 98.0)

    # serviceability (optional)
    out["spares_score"]  = _norm_asc(out.get("spares_fill_rate"), 80.0, 99.0)
    out["support_score"] = _norm_asc(out.get("fte_on_site"), 0.0, 5.0)
    out["serviceability_score"] = np.nanmean(out[["spares_score","support_score"]].to_numpy(dtype=float), axis=1)

    def _row_score(r):
        parts = {}
        if pd.notna(r.get("otif_score")):               parts["otif"] = r["otif_score"]
        if pd.notna(r.get("lead_time_score")):          parts["lead_time"] = r["lead_time_score"]
        if pd.notna(r.get("supply_tightness_score")):   parts["supply_tightness"] = r["supply_tightness_score"]
        if pd.notna(r.get("concentration_score")):      parts["concentration"] = r["concentration_score"]
        # only include serviceability if either subscore exists
        serv = r.get("serviceability_score")
        if pd.notna(serv):                               parts["serviceability"] = serv

        if not parts:
            return np.nan
        w = {k: WEIGHTS[k] for k in parts.keys()}
        s = sum(w.values())
        if s <= 0: return np.nan
        w = {k: v/s for k,v in w.items()}
        return float(sum(parts[k]*w[k] for k in parts)) * 100.0

    out["readiness_score"] = out.apply(_row_score, axis=1)

    # numeric coercion before rounding (prevents object-dtype crash)
    for c in ["portfolio_share_pct","b2b_3mma","backlog_months","dep_max_share",
              "hhi","lead_time_median_w","otif_pct","mom_3m_pct",
              "readiness_score","spares_fill_rate","fte_on_site"]:
        if c in out.columns: out[c] = pd.to_numeric(out[c], errors="coerce")

    if "dep_max_share" in out.columns:
        out["dep_max_share_pct"] = (out["dep_max_share"]*100.0).round(1)
    if "b2b_3mma" in out.columns: out["b2b_3mma"] = out["b2b_3mma"].round(2)
    for c in ["portfolio_share_pct","backlog_months","lead_time_median_w","otif_pct","mom_3m_pct","readiness_score"]:
        if c in out.columns: out[c] = out[c].round(1)

    cols = ["vendor","readiness_score","portfolio_share_pct","b2b_3mma",
            "backlog_months","dep_max_share_pct","hhi","lead_time_median_w",
            "otif_pct","mom_3m_pct"]
    cols = [c for c in cols if c in out.columns]
    out = out[cols].sort_values(["readiness_score","portfolio_share_pct","vendor"],
                                ascending=[False, False, True])

    st.dataframe(out, use_container_width=True, hide_index=True)

    if "readiness_score" in out.columns and len(out):
        bar = alt.Chart(out).mark_bar().encode(
            y=alt.Y("vendor:N", sort="-x", title="Vendor"),
            x=alt.X("readiness_score:Q", title="Foundry Readiness Score (0–100)"),
            tooltip=["vendor","readiness_score","lead_time_median_w","otif_pct","b2b_3mma","backlog_months"]
        )
        st.altair_chart(bar, use_container_width=True)

else:
    st.info("Need a base numeric column (billings or bookings) and a vendor column to compute shares & momentum. "
            "Other risk KPIs still work when present (backlog, lead time, OTIF).")

# ---------- Lead time detail (if present) ----------
if "lead_time_weeks" in view.columns:
    st.markdown("---")
    st.subheader("Lead Time (weeks)")
    lt = view.dropna(subset=["lead_time_weeks"]).copy()
    if not lt.empty:
        left, right = st.columns([2,2])
        with left:
            box = alt.Chart(lt).mark_boxplot().encode(
                y=alt.Y("lead_time_weeks:Q", title="Lead time (weeks)"),
                x=alt.X("vendor:N", title="Vendor", sort="ascending"),
                tooltip=[alt.Tooltip("lead_time_weeks:Q", format=".1f")]
            )
            st.altair_chart(box, use_container_width=True)
        with right:
            ltg = (lt.groupby("month", dropna=False)["lead_time_weeks"]
                     .median().reset_index().rename(columns={"lead_time_weeks":"median_weeks"}))
            ltl = alt.Chart(ltg).mark_line(point=True).encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("median_weeks:Q", title="Median lead time (weeks)"),
                tooltip=[alt.Tooltip("month:T"), alt.Tooltip("median_weeks:Q", format=".1f")]
            )
            st.altair_chart(ltl, use_container_width=True)

# ---------- Detail table (audit) ----------
st.markdown("---")
st.subheader("Detail table")
pref = ["date","vendor","customer","region","product",
        "backlog","orders","billings","units","asp",
        "lead_time_weeks","promise_date","delivery_date",
        "committed_qty","delivered_qty","on_time_flag","in_full_flag",
        "spares_fill_rate","fte_on_site","risk_score"]
cols = [c for c in pref if c in view.columns]
if not cols: cols = list(view.columns)
st.dataframe(view[cols], use_container_width=True, hide_index=True)
