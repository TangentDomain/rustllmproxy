@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo Loading environment variables from .env...
for /f "tokens=*" %%a in ('type .env ^| findstr /v "^#" ^| findstr /v "^$"') do set %%a

echo Starting Unified LLM Proxy...
taskkill /F /IM unified-proxy.exe >nul 2>&1
start /B "" "%~dp0target\release\unified-proxy.exe" configs\unified.toml

ping -n 4 127.0.0.1 >nul
curl -s http://127.0.0.1:8090/health
echo.
echo Done. Unified Proxy running on :8090
echo.
echo Supports both OpenAI and Anthropic APIs:
echo   OpenAI:    http://localhost:8090/openai/v1/chat/completions
echo   Anthropic: http://localhost:8090/anthropic/v1/messages
