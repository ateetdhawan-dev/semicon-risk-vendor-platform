@echo off
cd /d "%~dp0"
call .venv\Scripts\activate.bat
scripts\ingest_once.sh
python scripts\backup_db.py
python scripts\quality_check.py
