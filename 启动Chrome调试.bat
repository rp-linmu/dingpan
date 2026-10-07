@echo off
rem ============================================================
rem  Rolling-match monitor - Debug Chrome launcher
rem  Uses a dedicated profile (dingpan-chrome-profile).
rem  Your normal Chrome is NOT affected (both can run together).
rem  First run: log in to the trading platform in this window.
rem  The login session is kept in the profile for next runs.
rem ============================================================
set PROFILE=%~dp0dingpan-chrome-profile
set CHROME=C:\Program Files\Google\Chrome\Application\chrome.exe
if not exist "%CHROME%" set CHROME=%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe

start "" "%CHROME%" --remote-debugging-port=9222 --user-data-dir="%PROFILE%" --no-first-run --no-default-browser-check "https://pm.gx.csg.cn/GXJYQD/index.html#/portal"

echo.
echo Chrome started. Debug port: 9222   Profile: dingpan-chrome-profile
echo Portal page opened - log in HERE, then navigate to the
echo rolling-match page yourself. Keep that tab open.
echo The monitor does not care how you got there.
pause
