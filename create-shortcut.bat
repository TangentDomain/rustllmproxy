@echo off
chcp 65001 >nul

echo ========================================
echo 创建桌面快捷方式
echo ========================================

set DESKTOP=%USERPROFILE%\Desktop
set TARGET=%~dp0start-proxy.bat
set SHORTCUT=%DESKTOP%\LLM Proxy.lnk
set WORKDIR=%~dp0

powershell -Command "$ws = New-Object -ComObject WScript.Shell; $s = $ws.CreateShortcut('%SHORTCUT%'); $s.TargetPath = '%TARGET%'; $s.WorkingDirectory = '%WORKDIR%'; $s.Description = '启动 LLM 代理服务'; $s.Save()"

echo 快捷方式已创建: %SHORTCUT%
echo ========================================
pause
