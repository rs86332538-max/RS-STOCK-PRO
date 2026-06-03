STOCKINTEL PRO ETF - WEBSITE PACKAGE
====================================

This zip is a deploy-ready copy of the local app. The original folder and the
original local server address were not changed.

Local start
-----------
Windows:
  start_windows.bat

Mac / Linux:
  sh start_mac_linux.sh

Local address:
  http://localhost:7722/

Website hosting
---------------
This app is not a static HTML-only site. It needs the Python backend because the
HTML pages call /api/... endpoints for scans, ETF lookup, AI infra data and
MarketPulse.

The package includes common hosting files:
  requirements.txt
  Procfile
  runtime.txt
  Dockerfile
  render.yaml

On Render/Railway/Fly/etc. use:
  Build command: pip install -r requirements.txt
  Start command: python server.py

The server reads the PORT environment variable supplied by the host and binds to
HOST=0.0.0.0 for public web hosting. If PORT is not supplied, it still uses 7722
locally.

API keys
--------
Your real .keys.json and api_keys.json files were intentionally not included in
this zip. Use the in-app key settings after deployment, or create these files
from the examples:
  .keys.example.json
  api_keys.example.json

Runtime files
-------------
The app will recreate cache files, scan-state.json content and SQLite runtime
files as it runs. For persistent hosting, use a platform with persistent disk if
you want saved keys, scan history and cache to survive restarts.
