import sqlite3, json, re, csv
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
                v = (row.get(col) or "").strip()
                if v: canon.append(v)
    return canon

def load_aliases():
    p = CFG / "vendor_aliases.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

def load_risks():
    # list
    risks = ["geopolitical","material","vendor","logistics","financial","regulatory","cybersecurity","workforce","environmental","capacity","unclassified"]
    rt_file = CFG / "risk_types.json"
    if rt_file.exists():
        try:
            data = json.loads(rt_file.read_text(encoding="utf-8"))
            xs = [r for r in data.get("risks", []) if isinstance(r,str) and r.strip()]
            if xs: risks = xs
        except Exception:
            pass
    # keywords per risk
    rk_file = CFG / "risk_keywords.json"
    risk_kw = json.loads(rk_file.read_text(encoding="utf-8")) if rk_file.exists() else {}
    return risks, risk_kw

def mk_vendor_patterns(canon, aliases):
    pats = {}
    base = canon if canon else list(aliases.keys())
    for c in base:
        names = [c] + aliases.get(c, [])
        subs = []
        for nm in names:
            tokens = re.split(r"[\s\-\.]+", nm.strip())
            if not tokens: continue
            part = r"\s*[\-\.\s]\s*".join([re.escape(t) for t in tokens if t])
            subs.append(rf"(?<!\w){part}(?!\w)")
        if subs:
            pats[c] = re.compile("|".join(subs), re.IGNORECASE)
    return pats

def mk_risk_patterns(risk_kw):
    rpat = {}
    for r, kws in risk_kw.items():
        kws = [k for k in kws if isinstance(k, str) and k.strip()]
        if not kws: continue
        rpat[r] = re.compile("|".join(re.escape(k) for k in kws), re.IGNORECASE)
    return rpat

def classify(title, summary, v_pats, r_pats, risk_list):
    text = f"{title or ''} {summary or ''}"
    vendors = [c for c,pat in v_pats.items() if pat.search(text)]
    risks = [r for r,pat in r_pats.items() if pat.search(text)]

    # Heuristic boosts (simple phrases)
    low = text.lower()
    if "tariff" in low or "export control" in low or "sanction" in low or "embargo" in low:
        if "geopolitical" in risk_list and "geopolitical" not in risks: risks.append("geopolitical")
        if "regulatory" in risk_list and "regulatory" not in risks: risks.append("regulatory")
    if any(w in low for w in ["shutdown","halt production","line down","fab outage","blackout","power outage"]):
        if "capacity" in risk_list and "capacity" not in risks: risks.append("capacity")
    if any(w in low for w in ["strike","walkout","layoff"]):
        if "workforce" in risk_list and "workforce" not in risks: risks.append("workforce")

    # Defaults
    if vendors and not risks and "vendor" in risk_list:
        risks.append("vendor")
    if not risks and "unclassified" in risk_list:
        risks.append("unclassified")

    vendors = list(dict.fromkeys(vendors))
    risks   = list(dict.fromkeys(risks))
    return ", ".join(vendors), ", ".join(risks)

def reclassify():
    canon = load_vendors()
    aliases = load_aliases()
    risk_list, risk_kw = load_risks()
    v_pats = mk_vendor_patterns(canon, aliases)
    r_pats = mk_risk_patterns(risk_kw)

    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("SELECT hash_id, title, summary FROM news_events")
    rows = cur.fetchall()
    updated = 0
    for hid, title, summary in rows:
        vm, rk = classify(title, summary, v_pats, r_pats, risk_list)
        cur.execute("UPDATE news_events SET vendor_matches=?, risk_type=? WHERE hash_id=?", (vm, rk, hid))
        updated += 1
    con.commit()
    con.close()
    print(f"[OK] Reclassified {updated} rows. No article left without a risk (uses 'unclassified' as last resort).")

if __name__ == "__main__":
    reclassify()
