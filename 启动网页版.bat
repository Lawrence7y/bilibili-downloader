@echo off
chcp 65001 >nul
title BillBill Downloader - Rust Web Edition

echo ========================================================
echo       BillBill Downloader (Rust + Web)
echo ========================================================
echo.

rem Ensure we run from the project root (this .bat's directory)
cd /d "%~dp0"

rem Clear dead or stale process-level proxy variables so they don't break local network requests
if "%HTTP_PROXY%"=="http://127.0.0.1:8780" set HTTP_PROXY=
if "%HTTPS_PROXY%"=="http://127.0.0.1:8780" set HTTPS_PROXY=

set DDL_PROJECT_ROOT=%CD%
if "%DDL_PORT%"=="" set DDL_PORT=18080

if not exist "target\debug\server.exe" (
    echo [1/3] Compiling Rust backend...
    cargo build -p server
    if errorlevel 1 (
        echo Build failed.
        pause
        exit /b 1
    )
)

if not exist "frontend\dist\index.html" (
    echo [2/3] Building frontend...
    pushd frontend
    call npm run build
    popd
    if errorlevel 1 (
        echo Frontend build failed.
        pause
        exit /b 1
    )
)

echo [3/3] Starting server...
echo 本机电脑访问: http://127.0.0.1:%DDL_PORT%
echo 局域网/手机访问: http://192.168.124.5:%DDL_PORT% (需连接同一 WiFi)
echo.

start "" http://127.0.0.1:%DDL_PORT%
target\debug\server.exe
pause
