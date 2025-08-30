import os
import re
import json
import pandas as pd
import streamlit as st
from urllib.parse import urlparse

st.set_page_config(page_title="Semiconductor Risk News", page_icon="📰")

PUBLISHER_FONT_RE = re.compile(r"<font[^>]*>([^<]+)</font>", re.IGNORECASE)
TAG_STRIP_RE = re.compile(r"<[^<]+?>")

def strip_html(s: str) -> str:
    return TAG_STRIP_RE.sub("", s or "")

def extract_publisher(row) -> str:
    summary_html = str(row.get("summary", "") or "")
    m = PUBLISHER_FONT_RE.search(summary_html)
    if m:
        return m.group(1).strip()
    url = row.get("link") or row.get("url") or ""
    if isinstance(url, str) and url.startswith("http"):
        try:
            netloc = urlparse(url).netloc
            for pref in ("www.", "news.google.com", "news.yahoo.com"):
                if netloc.startswith(pref):
                    netloc = netloc[len(pref):]
            return netloc
        except Exception:
            pass
    return str(row.get("source", "") or "").replace("news.google.com", "Google News").strip()

def explode_vendors(series: pd.Series) -> list:
    vendors = set()
    for v in series.dropna():
        s = str(v).strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                items = json.loads(s)
                for it in items:
                    name = str(it).strip()
                    if name: vendors.add(name)
                continue
            except Exception:
                pass
        for token in re.split(r"[|,]", s):
            name = token.strip()
            if name: vendors.add(name)
    return sorted(vendors)

@st.cache_data
def load_news():
    path_annot = "data/news_events_annotated.csv"
    path_raw   = "data/news_events.csv"
    path = path_annot if os.path.exists(path_annot) else path_raw

    df = pd.read_csv(path)
    for c in ["risk_type","severity","source","title","summary","published_at","link","url","vendor_matches","region_guess"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    if "published_at" in df.columns:
        df["date"] = pd.to_datetime(df["published_at"], errors="coerce").dt.date
    else:
        df["date"] = pd.NaT

    df["publisher"] = df.apply(extract_publisher, axis=1)
    df["summary_plain"] = df["summary"].map(strip_html)
    return df

st.title("📰 Semiconductor Risk News")
df = load_news()

risk_options = sorted([x for x in df.get("risk_type", pd.Series(dtype=str)).dropna().unique() if str(x).strip() and x != "nan"])
sev_options  = sorted([x for x in df.get("severity",  pd.Series(dtype=str)).dropna().unique() if str(x).strip() and x != "nan"])
pub_options  = sorted([x for x in df.get("publisher", pd.Series(dtype=str)).dropna().unique() if str(x).strip() and x != "nan"])
vend_options = explode_vendors(df.get("vendor_matches", pd.Series(dtype=str))) if "vendor_matches" in df.columns else []

col1, col2, col3 = st.columns([2,2,3])
with col1:
    risk_sel = st.multiselect("Risk Type", options=risk_options, default=[])
with col2:
    sev_sel = st.multiselect("Severity", options=sev_options, default=[])
with col3:
    pub_sel = st.multiselect("Publisher", options=pub_options, default=[])

col4, col5, col6 = st.columns([2,2,3])
with col4:
    vend_sel = st.multiselect("Vendor", options=vend_options, default=[])
with col5:
    q = st.text_input("Search (title / summary)", "")
with col6:
    if "date" in df and df["date"].notna().any():
        min_d, max_d = df["date"].min(), df["date"].max()
        date_rng = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)
    else:
        date_rng = None

flt = df.copy()
if risk_sel: flt = flt[flt["risk_type"].isin(risk_sel)]
if sev_sel:  flt = flt[flt["severity"].isin(sev_sel)]
if pub_sel:  flt = flt[flt["publisher"].isin(pub_sel)]
if vend_sel and "vendor_matches" in flt.columns:
    pattern = "|".join([re.escape(v) for v in vend_sel])
    flt = flt[flt["vendor_matches"].str.contains(pattern, case=False, na=False)]
if q:
    ql = q.lower()
    flt = flt[flt["title"].str.lower().str.contains(ql, na=False) | flt["summary_plain"].str.lower().str.contains(ql, na=False)]
if date_rng and isinstance(date_rng, tuple) and len(date_rng) == 2:
    start, end = date_rng
    flt = flt[(flt["date"] >= start) & (flt["date"] <= end)]

st.caption(f"{len(flt)} results")

for _, r in flt.head(300).iterrows():
    with st.container(border=True):
        st.markdown(f"**{r.get('title','(no title)')}**")
        summary = r.get("summary_plain","") or ""
        if summary.strip():
            st.write(summary)
        meta = []
        if r.get("risk_type"): meta.append(f"risk: {r['risk_type']}")
        if r.get("severity"):  meta.append(f"severity: {r['severity']}")
        if r.get("publisher"): meta.append(f"publisher: {r['publisher']}")
        if pd.notna(r.get("date")): meta.append(f"date: {r['date']}")
        if meta:
            st.caption(" · ".join(meta))
        url = r.get("link") or r.get("url")
        if isinstance(url, str) and url.startswith("http"):
            st.link_button("🔗 Open article", url)
