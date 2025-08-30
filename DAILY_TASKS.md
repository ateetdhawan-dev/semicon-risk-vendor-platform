# Daily Tasks – Semicon Risk Vendor Platform

Every new session:

1. **Open Git Bash** and activate environment:
\`\`\`bash
cd ~/semicon-risk-vendor-platform
source .venv/Scripts/activate
\`\`\`

2. **Ingest latest news**
\`\`\`bash
python scripts/news_ingest.py
\`\`\`

3. **Annotate new articles**
\`\`\`bash
python scripts/annotate_csv.py -i data/news_events.csv -o data/news_events_annotated.csv
\`\`\`

4. **Run dashboards**
\`\`\`bash
streamlit run app.py
\`\`\`

---

## Optional Automation
To avoid manual runs:
- **Windows:** use Task Scheduler to call  
  \`bash.exe -lc "$HOME/semicon-risk-vendor-platform/scripts/ingest_and_annotate.sh"\`
- **Linux/Mac:** add cron entry.

This will refresh news + annotations daily.
