@echo off
rem ============================================================
rem  Open the dashboard page only (does NOT touch the monitor).
rem  Use this whenever you closed the page and want it back.
rem  If the page fails to load, the monitor is not running -
rem  then use the monitor launcher bat instead.
rem ============================================================
start "" "http://127.0.0.1:8787/"
