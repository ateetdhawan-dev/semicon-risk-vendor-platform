# Semicon Risk Vendor Platform

An experimental SaaS MVP for tracking and classifying semiconductor supply-chain risks.
It ingests industry news, applies automated annotation (LLM + rules), and visualizes risks
and vendor KPIs via Streamlit dashboards.

---

## 🚀 Quick Start

- **Setup guide:** [SETUP_AND_RUN.md](SETUP_AND_RUN.md)
- **Daily usage:** [DAILY_TASKS.md](DAILY_TASKS.md)

---

## 📊 Features

- **News Ingestion** → collects semiconductor news into CSV/SQLite
- **Risk Annotation** → classifies each article by risk type (vendor, geopolitical, material, other) and severity (low, medium, high)
- **Dashboards (Streamlit)**  
  - *News View* – explore risk-tagged news  
  - *Commercial KPI View* – track vendor and KPI data  

---

## 🛠️ Tech Stack

- Python 3.11+
- Streamlit
- OpenAI API (optional for richer annotation; rule-based fallback available)
- SQLite / CSV data storage
- GitHub Actions / Task Scheduler for automation (planned)

---

## 🔒 Secrets

All API keys go into `.env` (never committed):


---

## 📅 Roadmap

- [x] Ingestion pipeline (news → CSV/DB)  
- [x] Streamlit dashboards (News + KPIs)  
- [x] Risk annotation pipeline (CSV + OpenAI API)  
- [ ] Automated daily ingestion + annotation (Task Scheduler / cron)  
- [ ] Vendor KPI enrichment  
- [ ] SaaS deployment (cloud hosting)

---

## 👤 Author

**Ateet Dhawan**  
Semiconductor & AI Enthusiast | Building SaaS MVPs  
