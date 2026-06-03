#!/bin/sh
cd "$(dirname "$0")"
( sleep 1; python3 -m webbrowser http://localhost:7722/?v=split-universes >/dev/null 2>&1 ) &
PORT=7722 python3 server.py
