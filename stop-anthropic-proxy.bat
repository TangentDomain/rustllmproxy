@echo off
chcp 65001 >nul
taskkill /F /IM anthropic-proxy.exe >nul 2>&1
echo Stopped.
