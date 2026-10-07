@echo off
rem ============================================================
rem  Package this tool for distribution.
rem  Creates dingpan-share.zip with ONLY the program files:
rem    scripts, program, docs, lib
rem  EXCLUDES (private - never share):
rem    dingpan-chrome-profile/  (your login session!)
rem    dingpan-edge-profile/    (your login session!)
rem    data/                    (your captured market data)
rem    the real expectation-price xlsx (YOUR private prices!)
rem    .ai-memory/ .zcode/ article/
rem  (template/ ships the BLANK copy only - kept so this script
rem   also works on the recipient's machine)
rem ============================================================
rem  NOTE: keep this file pure ASCII. Non-ASCII characters in a
rem  bat file break cmd line parsing (encoding mismatch).
chcp 65001 >nul
cd /d "%~dp0"

rem Ship a blank expectation-price template (expect-blank.xlsx):
rem recipients rename it to the tool's template file name and
rem fill their own prices, or paste prices in the dashboard.
rem setup-node.ps1 = one-click Node.js installer (launched by the
rem setup bat, which the *.bat wildcard already packs).
rem mcp-server.mjs stays LOCAL on purpose (AI-host integration
rem is not part of the share package).
powershell -NoProfile -Command ^
  "Copy-Item 'template\blank.xlsx' 'expect-blank.xlsx' -Force; Compress-Archive -Path 'monitor.mjs','parser.mjs','config.json','dashboard.html','lib','template','setup-node.ps1','*.bat','*.md','expect-blank.xlsx' -DestinationPath 'dingpan-share.zip' -Force; Remove-Item 'expect-blank.xlsx' -Force"

if exist dingpan-share.zip (
  echo.
  echo OK: dingpan-share.zip created in this folder.
  echo Send this zip to your colleague. It contains NO login
  echo data, NO captured market data and NO private prices -
  echo each user logs in with their own account.
  echo.
) else (
  echo FAILED to create zip. Check PowerShell availability.
)
pause
