@echo off
chcp 65001 >nul

REM 只停止本仓库 run\ 目录下的 unified-proxy.exe，避免误杀其他实例
powershell -NoProfile -Command "$target = (Resolve-Path '.\run\unified-proxy.exe' -ErrorAction SilentlyContinue).Path; if ($target) { foreach ($p in (Get-Process -Name 'unified-proxy' -ErrorAction SilentlyContinue)) { if ($p.Path -eq $target) { Stop-Process -Id $p.Id -Force } } }"

echo Unified proxy stopped.
