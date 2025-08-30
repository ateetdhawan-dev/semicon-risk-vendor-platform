import os, json, time, argparse
import pandas as pd

# Try both OpenAI client styles to be robust
OPENAI_AVAILABLE = False
client = None
model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

api_key = os.getenv("OPENAI_API_KEY")
if api_key:
    try:
        # new-style client
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        OPENAI_AVAILABLE = True
    except Exception:
        try:
            # old-style openai
            import openai
            openai.api_key = api_key
            client = openai
            OPENAI_AVAILABLE = True
        except Exception:
            OPENAI_AVAILABLE = False

RISK_KEYWORDS = {
    "vendor": ["supplier", "vendor", "contract", "shipment", "production halt", "earnings", "guidance", "recall", "factory", "plant"],
    "geopolitical": ["sanction", "tariff", "export control", "trade ban", "war", "conflict", "geopolit", "visa", "policy", "regulation", "compliance"],
    "material": ["silicon wafer", "photoresist", "neon", "palladium", "copper foil", "substrate", "chemicals", "gas shortage", "materials"]
}

def rule_based_classify(text: str):
    text_l = (text or "").lower()
    risk_type = "other"
    severity = "low"
    if any(k in text_l for k in RISK_KEYWORDS["geopolitical"]):
        risk_type = "geopolitical"
    elif any(k in text_l for k in RISK_KEYWORDS["vendor"]):
        risk_type = "vendor"
    elif any(k in text_l for k in RISK_KEYWORDS["material"]):
        risk_type = "material"

    # naive severity hint
    if any(w in text_l for w in ["halt", "shutdown", "ban", "sanction", "fire", "flood", "bankrupt"]):
        severity = "high"
    elif any(w in text_l for w in ["delay", "probe", "investigate", "warning"]):
        severity = "medium"
    return {"risk_type": risk_type, "severity": severity}

def llm_classify(text: str):
    prompt = (
        "You are a concise classifier for semiconductor supply-chain risk.\n"
        "Classify the news into JSON with keys: risk_type ∈ {vendor, geopolitical, material, other}, "
        "severity ∈ {low, medium, high}. Keep it to a single JSON object. Text:\n\n"
        f"{text}"
    )
    try:
        # new-style
        if hasattr(client, "chat") and hasattr(client, "chat") and OPENAI_AVAILABLE:
            resp = client.chat.completions.create(
                model=model_name,
                messages=[{"role":"user","content":prompt}],
                temperature=0
            )
            content = resp.choices[0].message.content.strip()
        else:
            # old-style
            resp = client.ChatCompletion.create(
                model=model_name,
                messages=[{"role":"user","content":prompt}],
                temperature=0
            )
            content = resp.choices[0].message["content"].strip()

        # try parse JSON; if the model replies with text+json, extract braces
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1:
            content = content[start:end+1]
        data = json.loads(content)
        # minimal sanity
        rt = str(data.get("risk_type","other")).lower()
        sv = str(data.get("severity","low")).lower()
        if rt not in {"vendor","geopolitical","material","other"}: rt = "other"
        if sv not in {"low","medium","high"}: sv = "low"
        return {"risk_type": rt, "severity": sv, "_raw": content}
    except Exception as e:
        # fallback to rules
        rb = rule_based_classify(text)
        rb["_error"] = str(e)
        return rb

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", "-i", default="data/news_events.csv")
    ap.add_argument("--output", "-o", default="data/news_events_annotated.csv")
    ap.add_argument("--limit", type=int, default=0, help="Annotate only first N rows (0=all)")
    ap.add_argument("--sleep", type=float, default=0.7, help="Seconds to sleep between API calls")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input CSV not found: {args.input}")

    df = pd.read_csv(args.input)
    if df.empty:
        print("[warn] Input CSV is empty.")
        df.to_csv(args.output, index=False)
        print(f"[done] Wrote empty output: {args.output}")
        return

    print(f"[info] Loaded {len(df)} rows from {args.input}")
    rows = df if args.limit == 0 else df.head(args.limit)

    risk_type_col, severity_col, json_col = [], [], []
    for idx, row in rows.iterrows():
        text = f"{row.get('title','')} {row.get('summary','')}".strip()
        res = llm_classify(text) if OPENAI_AVAILABLE else rule_based_classify(text)
        risk_type_col.append(res.get("risk_type","other"))
        severity_col.append(res.get("severity","low"))
        json_col.append(json.dumps(res, ensure_ascii=False))
        if OPENAI_AVAILABLE:
            time.sleep(args.sleep)

    # write back into a copy of df (preserve all original rows; annotate only processed subset)
    df.loc[rows.index, "risk_type"] = risk_type_col
    df.loc[rows.index, "severity"] = severity_col
    df.loc[rows.index, "annotation_json"] = json_col

    # Ensure Arrow-friendly dtypes
    for c in ["risk_type","severity","annotation_json"]:
        df[c] = df[c].astype("string")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False)
    print(f"[done] Saved: {args.output}")

if __name__ == "__main__":
    main()
