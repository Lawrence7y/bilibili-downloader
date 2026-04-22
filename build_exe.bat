@echo off
setlocal
chcp 65001 >nul

cd /d %~dp0

if not exist .venv (
  py -3 -m venv .venv
)

call .venv\Scripts\activate.bat
python -m pip install -r requirements.txt
python -m pip install pyinstaller

for /f %%i in ('python -c "from version import APP_VERSION; print(APP_VERSION)"') do set APP_VERSION=%%i
if not defined APP_VERSION set APP_VERSION=unknown

set APP_NAME=BillBillDownloader_CN

if exist build rmdir /s /q build
if not exist dist mkdir dist
if exist dist\%APP_NAME%.exe del /f /q dist\%APP_NAME%.exe

pyinstaller --noconfirm --clean BillBillDownloader_CN.spec

echo.
echo Build done. Version: %APP_VERSION%
echo EXE path:
echo %~dp0dist\%APP_NAME%.exe
pause
