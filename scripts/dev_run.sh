#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/Scripts/activate
streamlit run app_streamlit.py --server.port=8501
