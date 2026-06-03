STOCK PRO 10X · VQM7776 · MULTI-YEAR ENGINE V3 + WEEK BREAKOUT SCANNER
============================================================================

Port: 7722
Åbn: http://localhost:7722/

START
-----
Windows:
  start_windows.bat

Mac / Linux:
  sh start_mac_linux.sh

NYT
---
- Ny fane: WEEK BREAKOUT
- Manual ticker-scan via samme lokale server
- Universe scan for Momentum / AI Infra / Small-Mid Momentum / Semiconductor / All
- Breakout Score: 30% Breakout, 25% Volume, 20% Trend, 15% Relative Strength, 10% VCP
- Close Strength, Closing Range Expansion og Risk Penalties
- TradingView weekly chart med ROC + RSI når du klikker på en ticker

Bemærk: Serveren bruger gratis Yahoo Finance data og TradingView embed i browseren.
Ikke finansiel rådgivning.


OPDATERING — WEEK BREAKOUT SCANNER
----------------------------------
Breakout-siden har nu samme univers-valg som STOCK PRO Screener:
S&P 500 + QQQ, Semiconductor, Russell 2000, Nuclear, Space/Defence/Aerospace/Robotics, AI Data/Power/Infra, All Mega Themes, Small Caps og All Screener Universes.
Server: http://localhost:7722/


SMALL CAPS FIX
--------------
- Small Caps forsøger nu at hente op til 1000 tickers via Nasdaq + Yahoo + Finviz.
- Batch cap hævet til 250, så frontend-batches på 200 ikke bliver halveret til 100.
- Hvis eksterne kilder blokeres, falder serveren tilbage til lokal fallback-liste.


FIX v10.4
--------
- Screener og Week Breakout bruger nu to separate universe-datastrukturer.
- Screener bruger stadig /api/universe og /api/smallcaps.
- Week Breakout bruger /api/breakout-tickers med separat lokal universe-map.
- Small Caps i Week Breakout kan scanne op til 1000 fra den separate Russell/small-cap liste.
