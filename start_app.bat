@echo off
setlocal
chcp 65001 >nul

cd /d %~dp0

set APP_EXE=%~dp0dist\BillBillDownloader_CN.exe

if exist "%APP_EXE%" (
  start "" "%APP_EXE%"
  exit /b 0
)

echo EXE not found. Building first...
call "%~dp0build_exe.bat"

if exist "%APP_EXE%" (
  start "" "%APP_EXE%"
  exit /b 0
)

echo Build failed. Please check errors above.
pause
