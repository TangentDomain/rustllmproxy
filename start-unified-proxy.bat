@echo off
chcp 65001 >nul
cd /d "%~dp0"

call "%~dp0stop-unified-proxy.bat" >nul 2>&1

if not exist run mkdir run
copy /Y target\release\unified-proxy.exe run\unified-proxy.exe >nul

echo Starting Unified LLM Proxy...
start /B "" "%~dp0run\unified-proxy.exe" configs\unified.toml

ping -n 4 127.0.0.1 >nul
curl -s http://127.0.0.1:8090/health
echo.
echo Done. Unified Proxy running on :8090
echo.
echo Supports both OpenAI and Anthropic APIs:
echo   OpenAI:    http://localhost:8090/openai/v1/chat/completions
echo   Anthropic: http://localhost:8090/anthropic/v1/messages
