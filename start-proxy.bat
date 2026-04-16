@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo Starting LLM Proxy...
taskkill /F /IM openai-proxy.exe >nul 2>&1
start /B "" "%~dp0target\release\openai-proxy.exe" configs\openai.toml

ping -n 4 127.0.0.1 >nul
curl -s http://127.0.0.1:8091/health
echo.
echo Done. Proxy running on :8091
