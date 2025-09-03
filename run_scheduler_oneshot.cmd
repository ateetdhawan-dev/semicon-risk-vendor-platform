@echo off
cd /d C:\Users\ateet\Desktop\semicon-risk-vendor-platform
call .venv312\Scripts\activate
set SCHEDULER_ONESHOT=1
if not exist logs mkdir logs
python scheduler.py >> logs\scheduler.log 2>&1
