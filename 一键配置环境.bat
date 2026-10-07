@echo off
rem ============================================================
rem  One-click environment setup for the dashboard tool.
rem  Checks Node.js (v22+); installs the LTS automatically
rem  (winget first, official MSI fallback, UAC self-elevation).
rem  All real logic lives in setup-node.ps1 (this file stays
rem  pure ASCII on purpose - non-ASCII breaks cmd parsing).
rem ============================================================
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "setup-node.ps1"
echo.
pause
