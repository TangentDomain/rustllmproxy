@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo Starting Anthropic Proxy...
taskkill /F /IM anthropic-proxy.exe >nul 2>&1
start /B "" "%~dp0target\release\anthropic-proxy.exe" configs\anthropic.toml

ping -n 4 127.0.0.1 >nul
curl -s http://127.0.0.1:8092/health
echo.
echo Done. Anthropic Proxy running on :8092
