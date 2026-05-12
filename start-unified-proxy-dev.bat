@echo off
chcp 65001 >nul
cd /d "%~dp0"

taskkill /F /IM unified-proxy-dev.exe >nul 2>&1

if not exist run-dev mkdir run-dev
copy /Y target\release\unified-proxy.exe run-dev\unified-proxy-dev.exe >nul

echo Starting Unified LLM Proxy (DEV)...
start /B "" "%~dp0run-dev\unified-proxy-dev.exe" configs\unified-dev.toml

ping -n 4 127.0.0.1 >nul
curl.exe -s -S --max-time 5 http://127.0.0.1:8091/health
echo.
echo Done. DEV Proxy running on :8091
echo.
echo   OpenAI:    http://localhost:8091/openai/v1/chat/completions
echo   Anthropic: http://localhost:8091/anthropic/v1/messages
