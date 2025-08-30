# Semicon Risk Vendor Platform – Setup & Run Guide

## 1. Clone the repo
\`\`\`bash
git clone https://github.com/ateetdhawan-dev/semicon-risk-vendor-platform.git
cd semicon-risk-vendor-platform
\`\`\`

## 2. Create Python virtual environment
\`\`\`bash
python -m venv .venv
source .venv/Scripts/activate   # Windows Git Bash
\`\`\`

## 3. Install dependencies
\`\`\`bash
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt || pip install streamlit feedparser python-dateutil pandas openai python-dotenv pyarrow
\`\`\`

## 4. Add your secrets
Create `.env` in project root:
\`\`\`
OPENAI_API_KEY=your_key_here
\`\`\`

> `.env` is **ignored by Git** — safe to keep locally.

## 5. Run news ingestion
\`\`\`bash
python scripts/news_ingest.py
\`\`\`

## 6. Run annotation
\`\`\`bash
python scripts/annotate_csv.py -i data/news_events.csv -o data/news_events_annotated.csv
\`\`\`

## 7. Launch Streamlit dashboards
\`\`\`bash
streamlit run app.py
\`\`\`
- News Dashboard → [http://localhost:8501](http://localhost:8501)
- Commercial KPI Dashboard → same app, different page tab
