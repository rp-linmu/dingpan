@echo off
rem ============================================================
rem  Rolling-match monitor launcher
rem  Runs monitor.mjs in THIS window (UTF-8 console).
rem  Keep this window OPEN while monitoring; closing it stops
rem  the monitor (dashboard goes offline too).
rem ============================================================
chcp 65001 >nul
cd /d "%~dp0"
title Rolling-match Monitor - keep this window OPEN

echo ============================================================
echo   Rolling-match monitor starting...
echo   1) Make sure debug Chrome (port 9222) is running and the
echo      rolling-match page is open and logged in.
echo   2) Dashboard will open automatically at http://127.0.0.1:8787
echo   3) Keep THIS window open. Closing it stops monitoring.
echo ============================================================
echo.

node monitor.mjs
echo.
echo ============================================================
echo   Monitor exited. Read the message above.
echo   (EADDRINUSE = another monitor window is already running)
echo ============================================================
pause
