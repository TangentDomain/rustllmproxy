@echo off
chcp 65001 >nul
cd /d "%~dp0"

taskkill /F /IM unified-proxy-home.exe >nul 2>&1

if not exist run-home mkdir run-home
copy /Y target\release\unified-proxy.exe run-home\unified-proxy-home.exe >nul

echo Starting Unified LLM Proxy (HOME)...
start /B "" "%~dp0run-home\unified-proxy-home.exe" configs\unified-home.toml

ping -n 4 127.0.0.1 >nul
curl.exe -s -S --max-time 5 http://127.0.0.1:8090/health
echo.
echo Done. HOME Proxy running on :8090
echo.
echo   OpenAI:    http://localhost:8090/openai/v1/chat/completions
echo   Anthropic: http://localhost:8090/anthropic/v1/messages
