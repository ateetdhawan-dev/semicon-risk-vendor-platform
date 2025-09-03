# ingest_min.py  — wider coverage + safe schema + SHA1 dedupe
import sqlite3, hashlib, re, calendar
from pathlib import Path
from datetime import datetime, timezone
import feedparser

DB_PATH = "data/news.db"

# Broader vendor universe (equipment, EDA, foundry/IDM, OSAT, fabless, analog/mixed-signal, memory, materials)
VENDORS = [
    # Litho/Dep/Etch/Metrology/Test/Assembly
    "ASML","Applied Materials","Lam Research","KLA","TEL Tokyo Electron","ASM International","SCREEN Semiconductor",
    "Disco Corporation","Advantest","Teradyne","BESI","Kulicke & Soffa","Axcelis Technologies","Entegris","MKS Instruments",
    "Tokyo Ohka (TOK)","Veeco","FormFactor",

    # EDA
    "Synopsys","Cadence","Siemens EDA",

    # Foundries / IDMs
    "TSMC","Samsung Foundry","Intel","UMC","GlobalFoundries","SMIC","Tower Semiconductor","VIS Vanguard","Hua Hong",

    # OSATs
    "ASE Technology","Amkor","JCET","Powertech Technology","SPIL",

    # Fabless / Compute
    "NVIDIA","AMD","Qualcomm","Broadcom","MediaTek","Marvell",

    # Memory
    "Micron","SK hynix","Kioxia","Western Digital",

    # Analog / Mixed-signal / Power
    "Texas Instruments","Analog Devices","STMicroelectronics","NXP","Infineon","Microchip","Renesas","onsemi"
]

ALIASES = {
    # Equipment aliases
    "TEL": "TEL Tokyo Electron",
    "Tokyo Electron": "TEL Tokyo Electron",
    "Lam": "Lam Research",
    "ASML Holding": "ASML",
    "ASML Holdings": "ASML",
    "ASM": "ASM International",
    "SCREEN": "SCREEN Semiconductor",
    "Disco": "Disco Corporation",
    "Axcelis": "Axcelis Technologies",
    "Kulicke and Soffa": "Kulicke & Soffa",
    "TOK": "Tokyo Ohka (TOK)",

    # EDA aliases
    "Mentor Graphics": "Siemens EDA",

    # Foundry/IDM aliases
    "Taiwan Semiconductor": "TSMC",
    "Taiwan Semiconductor Manufacturing": "TSMC",
    "Samsung Electronics Foundry": "Samsung Foundry",
    "Global Foundries": "GlobalFoundries",
    "GF": "GlobalFoundries",
    "Vanguard International Semiconductor": "VIS Vanguard",
    "Hua Hong Semiconductor": "Hua Hong",

    # OSAT aliases
    "ASE": "ASE Technology",
    "Siliconware": "SPIL",
    "SPIL Corp": "SPIL",

    # Fabless aliases
    "NVDA": "NVIDIA",

    # Memory aliases
    "SK Hynix": "SK hynix",

    # Analog/power aliases
    "ON Semiconductor": "onsemi",
    "STMicro": "STMicroelectronics",
}

def iso_utc(dt=None):
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

def ensure_schema(con: sqlite3.Connection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS news_events (
        hash_id         TEXT PRIMARY KEY,
        published_at    TEXT,
        title           TEXT,
        source          TEXT,
        link            TEXT,
        summary         TEXT,
        vendor_primary  TEXT,
        vendor_matches  TEXT,
        risk_type       TEXT,
        risk_primary    TEXT,
        risk_score      INTEGER,
        created_at      TEXT,
        updated_at      TEXT
    )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_news_published   ON news_events(published_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_news_vendor_prim ON news_events(vendor_primary)")
    con.commit()

def strip_html(x: str) -> str:
    if not x: return ""
    return re.sub(r"<[^>]+>", " ", x).strip()

def rss_url_for(vendor: str) -> str:
    from urllib.parse import quote_plus
    q = quote_plus(vendor)
    return f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"

def parse_time(entry) -> str:
    t = getattr(entry, "published_parsed", None)
    if not t:
        return iso_utc()
    ts = calendar.timegm(t)  # treat as UTC
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")

def canonical_vendor(name: str) -> str:
    return ALIASES.get(name, name)

def make_hash(vendor: str, title: str, link: str) -> str:
    base = f"{vendor}||{title}||{link}".encode("utf-8", "ignore")
    return hashlib.sha1(base).hexdigest()

def upsert(con: sqlite3.Connection, row: dict):
    con.execute("""
    INSERT INTO news_events (hash_id, published_at, title, source, link, summary,
                             vendor_primary, vendor_matches, risk_type, risk_primary,
                             risk_score, created_at, updated_at)
    VALUES (:hash_id, :published_at, :title, :source, :link, :summary,
            :vendor_primary, :vendor_matches, :risk_type, :risk_primary,
            :risk_score, :created_at, :updated_at)
    ON CONFLICT(hash_id) DO UPDATE SET
        updated_at     = excluded.updated_at,
        summary        = CASE WHEN coalesce(news_events.summary,'')='' THEN excluded.summary ELSE news_events.summary END,
        vendor_primary = COALESCE(news_events.vendor_primary, excluded.vendor_primary),
        vendor_matches = CASE
                           WHEN coalesce(news_events.vendor_matches,'')='' THEN excluded.vendor_matches
                           WHEN excluded.vendor_matches IS NULL OR excluded.vendor_matches='' THEN news_events.vendor_matches
                           ELSE news_events.vendor_matches
                         END
    """, row)

def run(max_items_per_vendor: int = 100):
    Path("data").mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    try:
        ensure_schema(con)
        total_new = 0

        for vend in VENDORS:
            canon = canonical_vendor(vend)
            feed = feedparser.parse(rss_url_for(canon))
            added = 0

            for entry in getattr(feed, "entries", [])[:max_items_per_vendor]:
                title = (getattr(entry, "title", "") or "").strip()
                link  = (getattr(entry, "link", "") or "").strip()
                src   = (getattr(getattr(entry, "source", {}), "title", "") or
                         getattr(entry, "source", {}).get("title", "") or
                         getattr(feed, "feed", {}).get("title", "") or "").strip()
                summ  = strip_html(getattr(entry, "summary", "") or "")
                pub   = parse_time(entry)

                if not title:
                    continue
                hid = make_hash(canon, title, link or title)

                row = {
                    "hash_id": hid,
                    "published_at": pub,
                    "title": title,
                    "source": src,
                    "link": link,
                    "summary": summ,
                    "vendor_primary": canon,
                    "vendor_matches": canon,
                    "risk_type": None,
                    "risk_primary": None,
                    "risk_score": None,
                    "created_at": iso_utc(),
                    "updated_at": iso_utc(),
                }
                try:
                    upsert(con, row)
                    # If it was a true insert, changes increased at least by 1
                    # (This is a coarse signal; fine for noisy logs)
                    added += 1
                except Exception:
                    pass

            print(f"[ingest] {canon}: +{added} upserts")
            total_new += added

        con.commit()
        print(f"[ingest] done. processed rows: {total_new}")
    finally:
        con.close()

if __name
