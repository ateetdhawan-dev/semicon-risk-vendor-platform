import sqlite3, json, csv, re
from pathlib import Path

DB = "data/news.db"
CFG = Path("config")

def load_vendors():
    p = CFG / "vendors_master.csv"
    canon = []
    if p.exists():
        with open(p, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            col = "vendor" if "vendor" in r.fieldnames else r.fieldnames[0]
            for row in r:
                v=(row.get(col) or "").strip()
                if v: canon.append(v)
    return canon

def load_aliases():
    p = CFG / "vendor_aliases.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

def load_risk_types():
    p = CFG / "risk_types.json"
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            xs = [r for r in data.get("risks",[]) if isinstance(r,str) and r.strip()]
            if xs: return xs
        except: pass
    return ["geopolitical","material","vendor","logistics","financial","regulatory","cybersecurity","workforce","environmental","capacity","unclassified"]

def load_risk_keywords():
    p = CFG / "risk_keywords.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

def load_risk_model():
    p = CFG / "risk_model.json"
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {"precedence": [], "weights": {}, "severity_boost": {"major":[],"minor":[]}, "severity_weights":{"major":0,"minor":0}}

def mk_vendor_patterns(canon, aliases):
    base = canon if canon else list(aliases.keys())
    pats=[]
    for c in base:
        names=[c]+aliases.get(c,[])
        subs=[]
        for nm in names:
            tokens=re.split(r"[\s\-\.]+", nm.strip())
            if not tokens: continue
            part=r"\s*[\-\.\s]\s*".join([re.escape(t) for t in tokens if t])
            subs.append(rf"(?<!\w){part}(?!\w)")
        if subs:
            pats.append((c,re.compile("|".join(subs), re.IGNORECASE)))
    return pats

def mk_risk_patterns(risk_kw):
    pats={}
    for r,kws in risk_kw.items():
        kws=[k for k in kws if isinstance(k,str) and k.strip()]
        if not kws: continue
        pats[r]=re.compile("|".join(re.escape(k) for k in kws), re.IGNORECASE)
    return pats

def score_risks(text, r_pats, model):
    weights=model.get("weights",{})
    sev_boost=model.get("severity_boost",{"major":[],"minor":[]})
    sev_w=model.get("severity_weights",{"major":0,"minor":0})

    scores={r:0.0 for r in r_pats.keys()}
    for r,pat in r_pats.items():
        if pat.search(text): scores[r]+=weights.get(r,0.0)

    low=text.lower()
    if any(k in low for k in sev_boost.get("major",[])):
        for r in scores: scores[r]+=sev_w.get("major",0.0)
    elif any(k in low for k in sev_boost.get("minor",[])):
        for r in scores: scores[r]+=sev_w.get("minor",0.0)

    return scores

def pick_primary(scores, precedence):
    # choose max score; break ties by precedence
    if not scores: return "unclassified", 0.0
    maxv=max(scores.values()) if scores else 0.0
    tied=[r for r,v in scores.items() if v==maxv and v>0]
    if not tied:
        return "unclassified", 0.0
    if precedence:
        for r in precedence:
            if r in tied: return r, maxv
    return tied[0], maxv

def reclassify_primary():
    canon=load_vendors()
    aliases=load_aliases()
    risk_types=load_risk_types()
    risk_kw=load_risk_keywords()
    model=load_risk_model()

    v_pats=mk_vendor_patterns(canon, aliases)
    r_pats=mk_risk_patterns(risk_kw)
    precedence=model.get("precedence",[])

    con=sqlite3.connect(DB)
    cur=con.cursor()
    # add cols if missing
    cur.execute("PRAGMA table_info(news_events)")
    cols=[c[1] for c in cur.fetchall()]
    if "vendor_primary" not in cols:
        cur.execute("ALTER TABLE news_events ADD COLUMN vendor_primary TEXT")
    if "risk_primary" not in cols:
        cur.execute("ALTER TABLE news_events ADD COLUMN risk_primary TEXT")
    if "risk_score" not in cols:
        cur.execute("ALTER TABLE news_events ADD COLUMN risk_score REAL")

    cur.execute("SELECT hash_id, title, summary FROM news_events")
    rows=cur.fetchall()
    updated=0
    for hid,title,summary in rows:
        text=f"{title or ''} {summary or ''}"
        # vendor_primary = first canonical that matches, ordered by vendors_master.csv listing
        vp=""
        for c,pat in v_pats:
            if pat.search(text):
                vp=c; break
        # risk scores
        scores=score_risks(text, r_pats, model)
        rp, sc = pick_primary(scores, precedence)
        # defaulting rules
        if not rp or rp=="unclassified":
            # basic fallbacks
            if any(k in text.lower() for k in ["tariff","export control","sanction","embargo"]):
                rp="geopolitical"
                sc=max(sc, 0.6)
            elif vp:
                rp="vendor"
                sc=max(sc, 0.4)
            else:
                rp="unclassified"
        cur.execute("UPDATE news_events SET vendor_primary=?, risk_primary=?, risk_score=? WHERE hash_id=?",
                    (vp, rp, float(sc), hid))
        updated+=1
    con.commit()
    con.close()
    print(f"[OK] Updated {updated} rows with vendor_primary, risk_primary, risk_score.")

if __name__=="__main__":
    reclassify_primary()
