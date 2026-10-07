@echo off
rem ============================================================
rem  Rolling-match monitor - Debug EDGE launcher
rem  Opens ONE tab: the GXJYQD portal (login entry).
rem  You log in and navigate to the trading pages yourself.
rem  Uses a dedicated profile (dingpan-edge-profile).
rem  NOTE: use Chrome OR Edge launcher, not both at the same
rem  time (they share debug port 9222 - the monitor listens
rem  on that port, whichever browser owns it).
rem ============================================================
set PROFILE=%~dp0dingpan-edge-profile
set EDGE=C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe
if not exist "%EDGE%" set EDGE=C:\Program Files\Microsoft\Edge\Application\msedge.exe

start "" "%EDGE%" --remote-debugging-port=9222 --user-data-dir="%PROFILE%" --no-first-run --no-default-browser-check "https://pm.gx.csg.cn/GXJYQD/index.html#/portal"

echo.
echo Edge started. Debug port: 9222   Profile: dingpan-edge-profile
echo Portal page opened - log in HERE, then navigate to the
echo rolling-match page yourself. Keep that tab open.
echo First run on Edge needs a fresh login (separate profile).
pause
