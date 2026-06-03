@echo off
cd /d "%~dp0"
set PORT=7722
start "" http://localhost:7722/?v=split-universes
python server.py
pause
