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
mkdir dist\BillBillDL\sidecar 2>nul
copy /Y sidecar\*.py dist\BillBillDL\sidecar\ >nul
copy /Y README.md dist\BillBillDL\ >nul
copy /Y bilibili-downloader\requirements.txt dist\BillBillDL\ >nul 2>nul

rem Create launcher for distributed release package
(
echo @echo off
echo chcp 65001 ^>nul
echo title BillBill Downloader
echo cd /d "%%~dp0"
echo set DDL_PROJECT_ROOT=%%CD%%
echo if "%%DDL_PORT%%"=="" set DDL_PORT=18080
echo start "" http://127.0.0.1:%%DDL_PORT%%
echo server.exe
echo pause
) > dist\BillBillDL\启动程序.bat

echo [4/4] Creating ZIP archive...
powershell -Command "Compress-Archive -Path 'dist\BillBillDL\*' -DestinationPath 'dist\BillBillDownloader-v1.0.0-windows-x64.zip' -Force"

echo.
echo ========================================================
echo   Build Successful!
echo   Folder:  dist\BillBillDL
echo   Archive: dist\BillBillDownloader-v1.0.0-windows-x64.zip
echo ========================================================

:fail
echo Build failed.
pause
exit /b 1
