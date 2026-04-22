@echo off
setlocal
chcp 65001 >nul

cd /d %~dp0

if not exist .venv (
  py -3 -m venv .venv
)

call .venv\Scripts\activate.bat
python -c "import yt_dlp, browser_cookie3, playwright" >nul 2>nul
if errorlevel 1 (
  echo Installing missing dependencies...
  python -m pip install -r requirements.txt
)
python -m playwright install chromium >nul 2>nul

python bilibili_gui.py
