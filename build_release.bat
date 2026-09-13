@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
set DDL_PROJECT_ROOT=%CD%

echo ========================================================
echo   BillBill Downloader - Release Build
echo ========================================================
echo.

echo [1/4] Building Rust (release)...
cargo build --release -p server
if errorlevel 1 goto :fail

echo [2/4] Building frontend...
pushd frontend
call npm run build
if errorlevel 1 (
  popd
  goto :fail
)
popd

echo [3/4] Assembling dist\BillBillDL ...
if exist dist\BillBillDL rmdir /s /q dist\BillBillDL
mkdir dist\BillBillDL
mkdir dist\BillBillDL\frontend
xcopy /E /I /Y frontend\dist dist\BillBillDL\frontend\dist >nul
copy /Y target\release\server.exe dist\BillBillDL\ >nul
copy /Y sidecar\*.py dist\BillBillDL\sidecar_tmp\ >nul 2>nul
mkdir dist\BillBillDL\sidecar 2>nul
copy /Y sidecar\*.py dist\BillBillDL\sidecar\ >nul
copy /Y "启动网页版.bat" dist\BillBillDL\ >nul
copy /Y README.md dist\BillBillDL\ >nul

echo [4/4] Done.
echo.
echo Output: %CD%\dist\BillBillDL\server.exe
echo Note: Python venv + ffmpeg must be available on the target machine.
echo       Or set DDL_PROJECT_ROOT / keep folder layout from the repo.
echo.
pause
exit /b 0

:fail
echo Build failed.
pause
exit /b 1
