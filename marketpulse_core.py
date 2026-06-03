"""
MarketPulse Multi-Source Volume Aggregator — v3
================================================
Aggregerer volumen fra ALLE tilgængelige gratis og betalte kilder:

GRATIS KILDER (virker uden API-nøgle):
  1. Yahoo Finance  — konsolideret US volumen (primær børs + ECN)
  2. yfinance batch — henter mange tickers parallelt effektivt
  3. Stooq          — alternativ kilde, dækker US + EU markeder

GRATIS MED API-NØGLE (sæt i .env):
  4. Massive.com    — alle 19 US exchanges + dark pools + FINRA + OTC (bedste kilde)
  5. Finnhub        — real-time quotes med volumen
  6. Tiingo         — EOD + intraday volumen

BETALT (premium, fuld dækning):
  7. Massive Starter+ — 15 min delay, full snapshot
  8. Massive Advanced — real-time, alle data-typer

Hvad er "konsolideret volumen"?
  En aktie handles på NYSE, NASDAQ, BATS (CBOE BZX/EDGX), ARCA, IEX,
  dark pools (Goldman, Morgan Stanley, etc.) og internalizers.
  Yahoo Finance rapporterer CTA/UTP consolidated tape — dækker 99%+.
  Massive.com dækker alle 19 US exchanges + dark pools + FINRA + OTC — 100%.
  Massive bruger samme API-struktur som Polygon.io (/v2/aggs/, /v2/snapshot/).

Start:  python server_v3.py
        python server_v3.py --massive YOUR_KEY --finnhub YOUR_KEY
"""

import os, sys, time, logging, argparse, math, json, sqlite3
import concurrent.futures
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

import requests
import yfinance as yf
from flask import Flask, jsonify, request
from flask_cors import CORS

# ── CLI args / env ────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="MarketPulse Multi-Source Volume Server")
parser.add_argument("--massive",     default=os.getenv("MASSIVE_KEY",  ""), help="Massive.com API key")
parser.add_argument("--finnhub",     default=os.getenv("FINNHUB_KEY",  ""), help="Finnhub API key")
parser.add_argument("--alphavantage",default=os.getenv("AV_KEY",       ""), help="Alpha Vantage API key")
parser.add_argument("--tiingo",      default=os.getenv("TIINGO_KEY",   ""), help="Tiingo API key")
parser.add_argument("--port",        default=5000, type=int)
parser.add_argument("--cache-ttl",   default=300,  type=int)
parser.add_argument("--workers",     default=30,   type=int)
ARGS, _ = parser.parse_known_args()

# ── Persistent key store (.keys.json next to this script) ─────────────────────
KEYS_FILE = Path(__file__).parent / ".keys.json"
DB_FILE   = Path(__file__).parent / "marketpulse.db"

# ══════════════════════════════════════════════════════════════════════════════
#  LOCAL SQLite DATABASE
#  Persists scan results across server restarts.
#  Schema:
#    scans(id, scanned_at, elapsed, scanned_count, active_sources, source_config)
#    scan_rows(scan_id, list_type, rank, data_json)
# ══════════════════════════════════════════════════════════════════════════════

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_FILE))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # safe concurrent reads
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def db_init():
    with db_connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS scans (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                scanned_at     INTEGER NOT NULL,
                elapsed        REAL,
                scanned_count  INTEGER,
                active_sources TEXT,
                source_config  TEXT
            );
            CREATE TABLE IF NOT EXISTS scan_rows (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id   INTEGER NOT NULL REFERENCES scans(id) ON DELETE CASCADE,
                list_type TEXT NOT NULL,   -- 'volume' or 'ratio'
                rank      INTEGER NOT NULL,
                data_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_scan_rows_scan ON scan_rows(scan_id, list_type);
            CREATE INDEX IF NOT EXISTS idx_scans_time     ON scans(scanned_at DESC);
        """)
    log.info(f"Database ready: {DB_FILE}")

def db_save_scan(result: dict) -> int:
    """Persist a completed scan result. Returns new scan id."""
    ts = int(result.get("fetchedAt", time.time()))
    with db_connect() as conn:
        cur = conn.execute(
            """INSERT INTO scans (scanned_at, elapsed, scanned_count, active_sources, source_config)
               VALUES (?,?,?,?,?)""",
            (
                ts,
                result.get("elapsed"),
                result.get("scannedCount"),
                json.dumps(result.get("activeSources", [])),
                json.dumps(result.get("sourceConfig",  {})),
            )
        )
        scan_id = cur.lastrowid
        rows = []
        for rank, row in enumerate(result.get("topVolume", []), 1):
            rows.append((scan_id, "volume", rank, json.dumps(row)))
        for rank, row in enumerate(result.get("topRatio", []), 1):
            rows.append((scan_id, "ratio", rank, json.dumps(row)))
        conn.executemany(
            "INSERT INTO scan_rows (scan_id, list_type, rank, data_json) VALUES (?,?,?,?)",
            rows
        )
    log.info(f"Saved scan #{scan_id} to database ({len(rows)} rows)")
    return scan_id

def db_load_latest() -> dict | None:
    """Load the most recent scan from the database."""
    with db_connect() as conn:
        scan = conn.execute(
            "SELECT * FROM scans ORDER BY scanned_at DESC LIMIT 1"
        ).fetchone()
        if not scan:
            return None
        scan_id = scan["id"]
        rows = conn.execute(
            "SELECT list_type, rank, data_json FROM scan_rows WHERE scan_id=? ORDER BY list_type, rank",
            (scan_id,)
        ).fetchall()

    top_volume = [json.loads(r["data_json"]) for r in rows if r["list_type"] == "volume"]
    top_ratio  = [json.loads(r["data_json"]) for r in rows if r["list_type"] == "ratio"]

    return {
        "topVolume":     top_volume,
        "topRatio":      top_ratio,
        "scannedCount":  scan["scanned_count"] or 0,
        "elapsed":       scan["elapsed"],
        "activeSources": json.loads(scan["active_sources"] or "[]"),
        "sourceConfig":  json.loads(scan["source_config"]  or "{}"),
        "fetchedAt":     scan["scanned_at"],
        "fromDb":        True,
    }

def db_scan_history(limit: int = 20) -> list[dict]:
    """Return metadata for the last N scans (no row data)."""
    with db_connect() as conn:
        rows = conn.execute(
            """SELECT id, scanned_at, elapsed, scanned_count, active_sources
               FROM scans ORDER BY scanned_at DESC LIMIT ?""",
            (limit,)
        ).fetchall()
    return [
        {
            "id":           r["id"],
            "scannedAt":    r["scanned_at"],
            "scannedAtStr": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["scanned_at"])),
            "elapsed":      r["elapsed"],
            "scannedCount": r["scanned_count"],
            "activeSources":json.loads(r["active_sources"] or "[]"),
        }
        for r in rows
    ]

def db_load_scan_by_id(scan_id: int) -> dict | None:
    """Load a specific historical scan by ID."""
    with db_connect() as conn:
        scan = conn.execute("SELECT * FROM scans WHERE id=?", (scan_id,)).fetchone()
        if not scan:
            return None
        rows = conn.execute(
            "SELECT list_type, rank, data_json FROM scan_rows WHERE scan_id=? ORDER BY list_type, rank",
            (scan_id,)
        ).fetchall()
    top_volume = [json.loads(r["data_json"]) for r in rows if r["list_type"] == "volume"]
    top_ratio  = [json.loads(r["data_json"]) for r in rows if r["list_type"] == "ratio"]
    return {
        "topVolume":     top_volume,
        "topRatio":      top_ratio,
        "scannedCount":  scan["scanned_count"] or 0,
        "elapsed":       scan["elapsed"],
        "activeSources": json.loads(scan["active_sources"] or "[]"),
        "sourceConfig":  json.loads(scan["source_config"]  or "{}"),
        "fetchedAt":     scan["scanned_at"],
        "scanId":        scan_id,
        "fromDb":        True,
    }

def _load_keys() -> dict:
    if KEYS_FILE.exists():
        try:
            return json.loads(KEYS_FILE.read_text())
        except Exception:
            pass
    return {}

def _save_keys(d: dict):
    KEYS_FILE.write_text(json.dumps(d, indent=2))
    log.info(f"Keys saved to {KEYS_FILE}")

# Runtime key store — merges CLI/env args with persisted keys
# CLI/env args take priority over file; file fills missing values
_KEYS: dict = {}

def _init_keys():
    global _KEYS
    persisted = _load_keys()
    _KEYS = {
        "massive":     get_key("massive")     or persisted.get("massive",     ""),
        "finnhub":     get_key("finnhub")     or persisted.get("finnhub",     ""),
        "alphavantage":get_key("alphavantage") or persisted.get("alphavantage",""),
        "tiingo":      get_key("tiingo")      or persisted.get("tiingo",      ""),
    }
    # Persist merged result so later restarts remember everything
    _save_keys(_KEYS)

def get_key(name: str) -> str:
    return _KEYS.get(name, "")

def set_key(name: str, value: str):
    _KEYS[name] = value.strip()
    _save_keys(_KEYS)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  RATE LIMITERS
#  Token-bucket algorithm — thread-safe, precise, respects burst + sustained rate
# ══════════════════════════════════════════════════════════════════════════════

import threading

class RateLimiter:
    """
    Thread-safe token-bucket rate limiter.

    Args:
        max_calls:   Maximum number of calls allowed per window
        period_secs: Window duration in seconds
        name:        Human-readable name for logging

    Example:
        limiter = RateLimiter(60, 60, "Finnhub")  # 60 calls per 60 seconds
        limiter.acquire()  # blocks if needed, then proceeds
    """

    def __init__(self, max_calls: int, period_secs: float, name: str = ""):
        self.max_calls   = max_calls
        self.period      = period_secs
        self.name        = name
        self._lock       = threading.Lock()
        self._tokens     = float(max_calls)          # start full
        self._last_refill= time.monotonic()
        self._total_calls= 0
        self._total_waits= 0
        log.info(f"RateLimiter [{name}]: {max_calls} calls / {period_secs}s  "
                 f"({max_calls/period_secs:.2f} req/s)")

    @property
    def calls_per_second(self) -> float:
        return self.max_calls / self.period

    def _refill(self):
        """Add tokens proportional to elapsed time (called under lock)."""
        now     = time.monotonic()
        elapsed = now - self._last_refill
        gained  = elapsed * self.calls_per_second
        self._tokens     = min(self.max_calls, self._tokens + gained)
        self._last_refill = now

    def acquire(self, tokens: int = 1):
        """
        Block until `tokens` tokens are available, then consume them.
        Safe to call from multiple threads simultaneously.
        """
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens    -= tokens
                    self._total_calls += tokens
                    return
                # Calculate how long until we have enough tokens
                deficit      = tokens - self._tokens
                wait_secs    = deficit / self.calls_per_second
                self._total_waits += 1

            # Sleep outside the lock so other threads aren't blocked
            log.debug(f"RateLimiter [{self.name}] throttling — sleeping {wait_secs:.2f}s "
                      f"(total waits: {self._total_waits})")
            time.sleep(wait_secs)

    def stats(self) -> dict:
        with self._lock:
            self._refill()
            return {
                "name":         self.name,
                "maxCalls":     self.max_calls,
                "periodSecs":   self.period,
                "tokensLeft":   round(self._tokens, 2),
                "totalCalls":   self._total_calls,
                "totalWaits":   self._total_waits,
            }

    def __repr__(self):
        return f"RateLimiter({self.name!r}, {self.max_calls}/{self.period}s)"


class HourlyBudgetLimiter:
    """
    Hard hourly cap — once the budget is exhausted, calls are refused
    (not just slowed) until the hour resets.

    Used for Tiingo (max 50 tickers/hour on free tier).
    Resets at the top of each clock hour.
    """

    def __init__(self, max_per_hour: int, name: str = ""):
        self.max_per_hour = max_per_hour
        self.name         = name
        self._lock        = threading.Lock()
        self._used        = 0
        self._hour_start  = self._current_hour()
        log.info(f"HourlyBudgetLimiter [{name}]: {max_per_hour} tickers / hour")

    @staticmethod
    def _current_hour() -> int:
        return int(time.time() // 3600)

    def _maybe_reset(self):
        """Reset counter if we've crossed into a new clock hour."""
        h = self._current_hour()
        if h != self._hour_start:
            log.info(f"HourlyBudgetLimiter [{self.name}]: hour reset "
                     f"(used {self._used}/{self.max_per_hour} last hour)")
            self._used       = 0
            self._hour_start = h

    def acquire(self, count: int = 1) -> int:
        """
        Try to acquire `count` slots from the hourly budget.
        Returns the number of slots actually granted (may be < count or 0).
        """
        with self._lock:
            self._maybe_reset()
            available = max(0, self.max_per_hour - self._used)
            granted   = min(count, available)
            self._used += granted
            if granted < count:
                log.warning(f"HourlyBudgetLimiter [{self.name}]: budget exhausted — "
                             f"wanted {count}, granted {granted} "
                             f"({self._used}/{self.max_per_hour} used). "
                             f"Resets at top of hour.")
            return granted

    def remaining(self) -> int:
        with self._lock:
            self._maybe_reset()
            return max(0, self.max_per_hour - self._used)

    def secs_until_reset(self) -> int:
        return 3600 - int(time.time() % 3600)

    def stats(self) -> dict:
        with self._lock:
            self._maybe_reset()
            return {
                "name":          self.name,
                "maxPerHour":    self.max_per_hour,
                "used":          self._used,
                "remaining":     max(0, self.max_per_hour - self._used),
                "secsUntilReset":self.secs_until_reset(),
            }


# ── Instantiate limiters ──────────────────────────────────────────────────────
#   Finnhub:  60 requests / 60 seconds  (1 req/s sustained, burst up to 60)
#   Tiingo:   50 tickers  / 1 hour      (hard budget cap, not just throttle)

FINNHUB_LIMITER = RateLimiter(max_calls=60, period_secs=60, name="Finnhub")
TIINGO_LIMITER  = HourlyBudgetLimiter(max_per_hour=50, name="Tiingo")

app = Flask(__name__)
CORS(app)

CACHE_TTL = ARGS.cache_ttl
_cache    = {"data": None, "ts": 0.0}

# ── Data model ────────────────────────────────────────────────────────────────
@dataclass
class VolumeRecord:
    ticker:      str
    name:        str        = ""
    sector:      str        = "Unknown"
    price:       float      = 0.0
    changePct:   float      = 0.0
    # Volume from each source (shares)
    vol_yahoo:   int        = 0
    vol_massive: int        = 0
    vol_finnhub: int        = 0
    vol_stooq:   int        = 0
    vol_tiingo:  int        = 0
    # Derived
    volume:      int        = 0   # best consolidated total
    avgVolume:   int        = 0
    volRatio:    float      = 0.0
    marketCap:   int        = 0
    sources:     list       = field(default_factory=list)  # which sources contributed
    # UI colours
    sectorColor:  str       = "rgba(100,116,139,0.12)"
    sectorBorder: str       = "rgba(100,116,139,0.35)"
    sectorText:   str       = "#64748b"

SECTOR_COLORS = {
    "Technology":            {"bg":"rgba(56,189,248,0.12)",  "border":"rgba(56,189,248,0.35)",  "text":"#38bdf8"},
    "Financial Services":    {"bg":"rgba(52,211,153,0.12)",  "border":"rgba(52,211,153,0.35)",  "text":"#34d399"},
    "Consumer Cyclical":     {"bg":"rgba(251,191,36,0.12)",  "border":"rgba(251,191,36,0.35)",  "text":"#fbbf24"},
    "Healthcare":            {"bg":"rgba(52,211,153,0.12)",  "border":"rgba(52,211,153,0.35)",  "text":"#34d399"},
    "Energy":                {"bg":"rgba(251,191,36,0.12)",  "border":"rgba(251,191,36,0.35)",  "text":"#fbbf24"},
    "Communication Services":{"bg":"rgba(56,189,248,0.12)",  "border":"rgba(56,189,248,0.35)",  "text":"#38bdf8"},
    "Industrials":           {"bg":"rgba(251,191,36,0.12)",  "border":"rgba(251,191,36,0.35)",  "text":"#fbbf24"},
    "Consumer Defensive":    {"bg":"rgba(248,113,113,0.12)", "border":"rgba(248,113,113,0.35)", "text":"#f87171"},
    "Basic Materials":       {"bg":"rgba(251,191,36,0.12)",  "border":"rgba(251,191,36,0.35)",  "text":"#fbbf24"},
    "Real Estate":           {"bg":"rgba(129,140,248,0.12)", "border":"rgba(129,140,248,0.35)", "text":"#818cf8"},
    "Utilities":             {"bg":"rgba(129,140,248,0.12)", "border":"rgba(129,140,248,0.35)", "text":"#818cf8"},
    "ETF":                   {"bg":"rgba(129,140,248,0.12)", "border":"rgba(129,140,248,0.35)", "text":"#818cf8"},
    "Unknown":               {"bg":"rgba(100,116,139,0.12)", "border":"rgba(100,116,139,0.35)", "text":"#64748b"},
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 1 — Yahoo Finance (via yfinance)
#  Coverage: CTA + UTP consolidated tape = all US lit exchanges (~99% of volume)
#  Dark pools / internalizers: NOT included
# ══════════════════════════════════════════════════════════════════════════════

# fetch_yahoo_batch removed — enrich_single_yahoo now uses history() directly

def enrich_single_yahoo(ticker: str) -> dict:
    """
    Fetch EXACTLY what the user wants:

      volume        = ONLY the last completed trading day's volume
                      (the day before today / the most recent closed session)
      volumeHistory = last 5 trading days as [{date, volume}, ...]
                      so the dashboard can show a 5-day sparkline
      avgVol        = 20-day average computed from history
                      (NOT info['averageVolume'] which is unreliable)
      volRatio      = volume / avgVol  — today-vs-history ratio

    History fetch: period="1mo", interval="1d"
      - Returns ~21 trading days regardless of weekends/holidays
      - iloc[-1] = last CLOSED trading session (yesterday if market open,
                   today if after close)
      - We take ONLY iloc[-1] for volume — never intraday partial volume
      - avg computed from iloc[-21:-1] (the 20 days BEFORE the vol day)
        so the ratio compares this session against its own rolling baseline
    """
    try:
        t = yf.Ticker(ticker)

        # ── Step 1: Pull 1 month of daily history ────────────────────────────
        vol_day      = 0          # volume for the target day
        vol_day_date = ""         # date string for logging
        last_price   = 0.0
        prev_close   = 0.0
        avg_vol      = 0
        vol_history  = []         # [{date, volume}, ...] last 5 sessions

        try:
            hist = t.history(period="1mo", interval="1d", auto_adjust=True)

            if not hist.empty and len(hist) >= 2:
                # Last row = most recent completed session
                target_row = hist.iloc[-1]
                prev_row   = hist.iloc[-2]

                vol_day      = int(target_row["Volume"] or 0)
                last_price   = float(target_row["Close"]  or 0)
                prev_close   = float(prev_row["Close"]    or last_price)
                vol_day_date = str(target_row.name.date()) if hasattr(target_row.name, "date") else ""

                # 20-day average from the 20 sessions BEFORE the target row
                # (excludes the target day itself — clean baseline)
                baseline = hist.iloc[:-1]  # everything before last row
                if len(baseline) >= 5:
                    avg_vol = int(baseline["Volume"].tail(20).mean())
                elif len(baseline) >= 1:
                    avg_vol = int(baseline["Volume"].mean())

                # Last 5 completed sessions for sparkline (oldest first)
                for row_ts, row in hist.tail(5).iterrows():
                    d = str(row_ts.date()) if hasattr(row_ts, "date") else str(row_ts)[:10]
                    v = int(row["Volume"] or 0)
                    vol_history.append({"date": d, "volume": v})

        except Exception as he:
            log.debug(f"[{ticker}] history failed: {he}")

        # ── Step 2: Metadata from info ───────────────────────────────────────
        sector = "Unknown"
        name   = ticker
        mcap   = 0

        try:
            info = t.info

            # Price fallback if history failed
            if last_price == 0:
                last_price = float(
                    info.get("regularMarketPrice") or
                    info.get("currentPrice") or 0
                )
            if prev_close == 0:
                prev_close = float(
                    info.get("previousClose") or
                    info.get("regularMarketPreviousClose") or last_price
                )
            # Volume fallback (only if history returned 0)
            if vol_day == 0:
                vol_day = int(
                    info.get("regularMarketVolume") or
                    info.get("volume") or 0
                )
            # Avg vol fallback (only if history too short)
            if avg_vol == 0:
                avg_vol = int(
                    info.get("averageVolume") or
                    info.get("averageDailyVolume10Day") or
                    info.get("averageVolume10days") or 0
                )

            qt = str(info.get("quoteType") or "").upper()
            if qt in ("ETF", "MUTUALFUND"):
                sector = "ETF"
                name   = info.get("shortName") or ticker
            else:
                sector = info.get("sector") or "Unknown"
                name   = info.get("longName") or info.get("shortName") or ticker
            mcap = int(info.get("marketCap") or 0)

        except Exception as ie:
            log.debug(f"[{ticker}] info failed: {ie}")

        # ── Step 3: Derived fields ───────────────────────────────────────────
        chg = 0.0
        if last_price and prev_close and prev_close > 0:
            chg = round((last_price - prev_close) / prev_close * 100, 2)

        if vol_day == 0:
            log.debug(f"[{ticker}] skipping — no volume data for {vol_day_date}")
            return {}

        log.debug(
            f"[{ticker}] vol={vol_day:,} ({vol_day_date})  "
            f"avg20={avg_vol:,}  "
            f"ratio={round(vol_day/avg_vol,2) if avg_vol else 'N/A'}  "
            f"price={last_price}"
        )

        return {
            "vol":           vol_day,       # last closed session only
            "volDate":       vol_day_date,  # which date that volume is from
            "avgVol":        avg_vol,       # 20-day baseline (excludes target day)
            "volHistory":    vol_history,   # [{date, volume}] last 5 days
            "price":         last_price,
            "changePct":     chg,
            "sector":        sector,
            "name":          name,
            "marketCap":     mcap,
        }

    except Exception as e:
        log.debug(f"[yahoo single {ticker}] unexpected: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 2 — Stooq (free, no key)
#  Coverage: US consolidated + some dark pool data via their data partner
#  Note: may lag 15 min
# ══════════════════════════════════════════════════════════════════════════════

def fetch_stooq_batch(tickers: list[str]) -> dict[str, int]:
    """
    Fetch today's volume from stooq.com for a list of tickers.
    Returns {ticker: volume}
    """
    results = {}
    today = time.strftime("%Y%m%d")

    def fetch_one(tkr):
        url = f"https://stooq.com/q/d/l/?s={tkr.lower()}.us&d1={today}&d2={today}&i=d"
        try:
            r = requests.get(url, headers=HEADERS, timeout=6)
            if r.status_code != 200 or "No data" in r.text:
                return tkr, 0
            lines = r.text.strip().split("\n")
            if len(lines) < 2:
                return tkr, 0
            # CSV: Date,Open,High,Low,Close,Volume
            parts = lines[-1].split(",")
            vol = int(float(parts[5])) if len(parts) >= 6 else 0
            return tkr, vol
        except Exception:
            return tkr, 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        for tkr, vol in pool.map(fetch_one, tickers):
            if vol > 0:
                results[tkr] = vol
    log.info(f"Stooq: got {len(results)}/{len(tickers)} results")
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 3 — Massive.com
#  Coverage: ALL 19 US exchanges + dark pools (ATS/FINRA) + OTC — 100%
#  API structure is identical to Polygon.io (/v2/aggs/, /v2/snapshot/)
#  Base URL: https://api.massiveapi.com
#  Free tier: EOD data. Starter ($29/mo): 15-min delay. Advanced: real-time.
#  Get a free key at: https://massive.com/dashboard/signup
# ══════════════════════════════════════════════════════════════════════════════

MASSIVE_BASE = "https://api.massiveapi.com"


def fetch_massive_snapshot(api_key: str) -> dict[str, dict]:
    """
    Massive full-market snapshot — fetches ALL 10,000+ US tickers in ONE call.
    Returns {ticker: {vol, price, changePct, vwap}}

    Endpoint: GET /v2/snapshot/locale/us/markets/stocks/tickers
    Clears daily at 3:30 AM EST, repopulates from ~4:00 AM EST.
    Free tier: not included. Starter+: 15-min delay. Advanced: real-time.
    """
    if not api_key:
        return {}
    url = f"{MASSIVE_BASE}/v2/snapshot/locale/us/markets/stocks/tickers?include_otc=true&apiKey={api_key}"
    try:
        r = requests.get(url, timeout=25, headers=HEADERS)
        if r.status_code in (401, 403):
            log.warning(f"Massive snapshot: auth error {r.status_code} — check API key / plan")
            return {}
        r.raise_for_status()
        data = r.json()
        out  = {}
        for item in data.get("tickers", []):
            tkr = item.get("ticker", "")
            if not tkr:
                continue
            day     = item.get("day", {})
            prev    = item.get("prevDay", {})
            vol     = int(day.get("v", 0) or 0)
            price   = day.get("c") or item.get("lastTrade", {}).get("p", 0) or 0
            prev_c  = prev.get("c", price) or price
            chg     = round((price - prev_c) / max(prev_c, 0.01) * 100, 2) if prev_c else 0
            vwap    = day.get("vw", 0)
            if vol > 0:
                out[tkr] = {"vol": vol, "price": price, "changePct": chg, "vwap": vwap}
        log.info(f"Massive snapshot: {len(out)} tickers")
        return out
    except Exception as e:
        log.warning(f"Massive snapshot failed: {e}")
        return {}


def fetch_massive_batch(tickers: list[str], api_key: str) -> dict[str, dict]:
    """
    Per-ticker daily bars from Massive — same URL pattern as Polygon.io.
    Used as fallback when snapshot endpoint is not available on current plan.
    Endpoint: GET /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}
    """
    if not api_key:
        return {}
    results = {}
    today   = time.strftime("%Y-%m-%d")
    prev    = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400))

    def fetch_one(tkr: str):
        url = (f"{MASSIVE_BASE}/v2/aggs/ticker/{tkr}/range/1/day/{prev}/{today}"
               f"?adjusted=true&sort=asc&limit=2&apiKey={api_key}")
        try:
            r = requests.get(url, timeout=8, headers=HEADERS)
            if r.status_code in (401, 403):
                return tkr, {}
            if r.status_code != 200:
                return tkr, {}
            data = r.json()
            bars = data.get("results", [])
            if not bars:
                return tkr, {}
            last   = bars[-1]
            prev_c = bars[-2]["c"] if len(bars) > 1 else last["c"]
            chg    = round((last["c"] - prev_c) / max(prev_c, 0.01) * 100, 2)
            return tkr, {
                "vol":       int(last.get("v", 0)),
                "vwap":      last.get("vw", last["c"]),
                "price":     last["c"],
                "changePct": chg,
            }
        except Exception as e:
            log.debug(f"[massive {tkr}] {e}")
            return tkr, {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        for tkr, data in pool.map(fetch_one, tickers):
            if data:
                results[tkr] = data
    log.info(f"Massive batch: got {len(results)}/{len(tickers)} results")
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 4 — Finnhub (free: 60 req/min; paid: higher limits)
#  Coverage: Real-time US consolidated + some dark pool via their feed
# ══════════════════════════════════════════════════════════════════════════════

def fetch_finnhub_batch(tickers: list[str], api_key: str) -> dict[str, dict]:
    """
    Fetch candle data from Finnhub with strict rate limiting.

    Rate limit: 60 requests / 60 seconds (free tier).
    Uses token-bucket RateLimiter — each ticker = 1 request.
    Threads acquire a token before firing the HTTP call, so we never
    exceed 60 req/min regardless of how many threads are running.
    """
    if not api_key:
        return {}

    results  = {}
    today_ts     = int(time.time())
    yesterday_ts = today_ts - 86400
    limit_info   = FINNHUB_LIMITER.stats()
    log.info(f"Finnhub: fetching {len(tickers)} tickers "
             f"(limiter: {limit_info['tokensLeft']:.1f}/{limit_info['maxCalls']} tokens available)")

    def fetch_one(tkr: str):
        # Block here until we have a token — thread-safe
        FINNHUB_LIMITER.acquire(tokens=1)
        url = (f"https://finnhub.io/api/v1/stock/candle"
               f"?symbol={tkr}&resolution=D&from={yesterday_ts}&to={today_ts}&token={api_key}")
        try:
            r = requests.get(url, timeout=8)
            if r.status_code == 429:
                log.warning(f"Finnhub 429 on {tkr} — rate limit hit despite limiter (API may reset differently)")
                return tkr, {}
            if r.status_code != 200:
                return tkr, {}
            d = r.json()
            if d.get("s") == "no_data" or not d.get("v"):
                return tkr, {}
            vol   = int(d["v"][-1])
            price = d["c"][-1]
            prev  = d["c"][-2] if len(d["c"]) > 1 else price
            chg   = round((price - prev) / max(prev, 0.01) * 100, 2)
            return tkr, {"vol": vol, "price": price, "changePct": chg}
        except Exception as e:
            log.debug(f"[finnhub {tkr}] {e}")
            return tkr, {}

    # Workers can be high — the RateLimiter serialises them to 60/min
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(fetch_one, tkr): tkr for tkr in tickers}
        for f in concurrent.futures.as_completed(futures):
            tkr, data = f.result()
            if data:
                results[tkr] = data

    stats = FINNHUB_LIMITER.stats()
    log.info(f"Finnhub: got {len(results)}/{len(tickers)} results | "
             f"limiter: {stats['totalCalls']} total calls, {stats['totalWaits']} waits")
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 5 — Tiingo (free: 500 req/day; paid: more)
#  Coverage: US consolidated + IEX real-time
# ══════════════════════════════════════════════════════════════════════════════

def fetch_tiingo_batch(tickers: list[str], api_key: str) -> dict[str, dict]:
    """
    Fetch IEX real-time quotes from Tiingo with hard hourly budget cap.

    Rate limit: max 50 tickers per clock hour (free tier).
    Uses HourlyBudgetLimiter — we check the remaining budget before each
    chunk and stop early if the hourly cap is reached.

    Tiingo's IEX endpoint supports comma-separated bulk requests, so
    50 tickers = 1 HTTP call but consumes 50 units of the hourly budget.
    The limiter counts individual tickers, not HTTP requests.
    """
    if not api_key:
        return {}

    results   = {}
    remaining = TIINGO_LIMITER.remaining()
    log.info(f"Tiingo: {remaining}/{TIINGO_LIMITER.max_per_hour} hourly budget remaining, "
             f"want {len(tickers)} tickers, "
             f"resets in {TIINGO_LIMITER.secs_until_reset()}s")

    if remaining == 0:
        log.warning(f"Tiingo: hourly budget exhausted — skipping. "
                    f"Resets in {TIINGO_LIMITER.secs_until_reset()}s")
        return {}

    # Only take as many tickers as our hourly budget allows
    allowed  = tickers[:remaining]
    skipped  = len(tickers) - len(allowed)
    if skipped > 0:
        log.warning(f"Tiingo: truncating to {len(allowed)} tickers "
                    f"(skipping {skipped} — would exceed hourly cap of {TIINGO_LIMITER.max_per_hour})")

    # Acquire from budget (this is instantaneous — just accounting)
    granted = TIINGO_LIMITER.acquire(len(allowed))
    if granted < len(allowed):
        allowed = allowed[:granted]
        log.warning(f"Tiingo: budget granted only {granted} tickers (race condition guard)")

    if not allowed:
        return {}

    # Single bulk request — Tiingo IEX endpoint handles up to 50 tickers at once
    try:
        url = f"https://api.tiingo.com/iex/?tickers={','.join(allowed)}&token={api_key}"
        r   = requests.get(url, timeout=12)
        if r.status_code == 429:
            log.warning("Tiingo 429 — server-side rate limit hit")
            return {}
        if r.status_code != 200:
            log.warning(f"Tiingo HTTP {r.status_code}")
            return {}
        for item in r.json():
            tkr   = item.get("ticker", "")
            vol   = item.get("volume", 0) or item.get("lastVolume", 0) or 0
            price = item.get("last", 0) or item.get("tngoLast", 0) or 0
            prev  = item.get("prevClose", price) or price
            chg   = round((price - prev) / max(prev, 0.01) * 100, 2)
            if tkr and vol:
                results[tkr] = {"vol": int(vol), "price": price, "changePct": chg}
    except Exception as e:
        log.warning(f"Tiingo request error: {e}")

    budget = TIINGO_LIMITER.stats()
    log.info(f"Tiingo: got {len(results)}/{len(allowed)} results | "
             f"budget: {budget['used']}/{budget['maxPerHour']} used this hour, "
             f"{budget['remaining']} remaining, resets in {budget['secsUntilReset']}s")
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 6 — Yahoo Finance Screener (dynamic ticker discovery)
# ══════════════════════════════════════════════════════════════════════════════

def get_most_active_tickers(count: int = 250) -> list[str]:
    """Get most-active ticker list from Yahoo screener + fallback list."""
    url = (
        "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
        f"?formatted=false&lang=en-US&region=US&scrIds=most_actives&count={count}&start=0"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        tickers = [q["symbol"] for q in data["finance"]["result"][0]["quotes"] if q.get("symbol")]
        log.info(f"Yahoo screener: {len(tickers)} tickers")
        return tickers
    except Exception as e:
        log.warning(f"Yahoo screener unavailable ({e}), using fallback list")

    # If Massive key is available, use snapshot endpoint for full market universe
    if get_key("massive"):
        snapshot = fetch_massive_snapshot(get_key("massive"))
        if snapshot:
            sorted_tickers = sorted(snapshot.keys(), key=lambda t: snapshot[t].get("vol", 0), reverse=True)
            return sorted_tickers[:500]

    # Broad fallback
    return list(dict.fromkeys([
        "NVDA","TSLA","AAPL","AMD","MSFT","META","AMZN","GOOGL","GOOG","PLTR",
        "BAC","F","INTC","WFC","XOM","JPM","SOFI","NIO","MARA","RIVN",
        "CLSK","COIN","RIOT","LCID","SNAP","UBER","PYPL","DIS","PFE","T",
        "NFLX","HOOD","RKLB","ARM","SMCI","GME","BA","SHOP","BABA","TSM",
        "C","GS","KO","V","WMT","ABBV","ORCL","CRM","NKLA","MU",
        "MSTR","SQ","RBLX","DKNG","PINS","BYND","AMC","SPCE","WKHS","ATER",
        "SNDL","TLRY","ACB","CGC","GRWG","SPRT","CLOV","GOEV","PROG","GME",
        "SPY","QQQ","IWM","XLF","XLE","XLK","SQQQ","TQQQ","SPXU","UVXY",
        "VXX","SOXL","SOXS","LABU","LABD","TNA","TZA","ARKK","GDX","SLV",
        "AVGO","LLY","UNH","BRK-B","MA","HD","PG","MRK","COST","CVX",
        "PEP","ACN","MCD","CSCO","ABT","TMO","ADBE","CAT","IBM","NOW",
        "ISRG","QCOM","GE","PM","TXN","BKNG","VZ","NEE","RTX","HON",
        "LOW","SPGI","AMGN","ELV","MDT","AXP","SYK","GILD","BLK","SCHW",
        "CRWD","SNOW","DDOG","ZM","DOCU","OKTA","TWLO","MDB","NET","HUBS",
        "ABNB","DASH","LYFT","SPOT","CVNA","OPEN","RDFN","WISH","CLOV","BB",
    ]))


# ══════════════════════════════════════════════════════════════════════════════
#  AGGREGATION ENGINE
#  Combines all sources, picks best volume, flags source provenance
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_sources(
    tickers: list[str],
    yahoo_data:   dict,
    massive_data: dict,
    finnhub_data: dict,
    stooq_data:   dict,
    tiingo_data:  dict,
    meta_data:    dict,   # name/sector/mcap from yfinance
) -> list[dict]:
    """
    For each ticker, pick the HIGHEST volume reading across all sources
    (consolidated tape typically reports the same total, but some sources
    may have later intraday cutoffs, giving a higher number).
    """
    records = []
    for tkr in tickers:
        y = yahoo_data.get(tkr, {})
        p = massive_data.get(tkr, {})
        f = finnhub_data.get(tkr, {})
        s = stooq_data.get(tkr, 0)
        ti = tiingo_data.get(tkr, {})
        m = meta_data.get(tkr, {})

        vols = {
            "Yahoo":   y.get("vol",  0) or 0,
            "Massive": p.get("vol",  0) or 0,
            "Finnhub": f.get("vol",  0) or 0,
            "Stooq":   s if isinstance(s, int) else 0,
            "Tiingo":  ti.get("vol", 0) or 0,
        }

        active_sources = {k: v for k, v in vols.items() if v > 0}
        if not active_sources:
            continue

        # Best volume = highest reading (most complete source wins)
        best_vol = max(active_sources.values())
        best_src = max(active_sources, key=active_sources.get)

        # Price preference: Massive (most accurate) > Yahoo > Finnhub > Tiingo
        price = (p.get("price") or y.get("price") or f.get("price") or ti.get("price") or 0)
        chg   = (p.get("changePct") or y.get("changePct") or f.get("changePct") or ti.get("changePct") or 0)

        avg_vol  = m.get("avgVol", 0) or 0
        vol_ratio= round(best_vol / avg_vol, 2) if avg_vol > 0 else None

        sector   = m.get("sector", "Unknown")
        name     = m.get("name", tkr)
        mcap     = m.get("marketCap", 0)
        colors   = SECTOR_COLORS.get(sector, SECTOR_COLORS["Unknown"])

        if best_vol < 10_000:   # skip illiquid / bad data
            continue

        records.append({
            "ticker":      tkr,
            "name":        name,
            "sector":      sector,
            "price":       round(float(price), 2) if price else None,
            "changePct":   round(float(chg), 2),
            "volume":      int(best_vol),         # last completed session only
            "volDate":     m.get("volDate", ""),  # which date the volume is from
            "avgVolume":   int(avg_vol),           # 20-day baseline
            "volRatio":    vol_ratio,              # volume / avgVolume
            "volHistory":  m.get("volHistory", []),  # [{date, volume}] last 5 days
            "marketCap":   int(mcap),
            # Per-source breakdown
            "sourceVolumes": {k: v for k, v in vols.items() if v > 0},
            "bestSource":  best_src,
            "sourceCount": len(active_sources),
            "sectorColor":  colors["bg"],
            "sectorBorder": colors["border"],
            "sectorText":   colors["text"],
        })
    return records


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════

def run_scan() -> dict:
    t0 = time.time()

    # ── If Massive key available, use snapshot endpoint for full market coverage
    massive_snap = {}
    if get_key("massive"):
        log.info("Fetching Massive full-market snapshot (10,000+ tickers)…")
        massive_snap = fetch_massive_snapshot(get_key("massive"))

    # ── Get ticker universe ──────────────────────────────────────────────────
    if massive_snap:
        tickers = sorted(massive_snap.keys(), key=lambda t: massive_snap[t].get("vol", 0), reverse=True)[:500]
        log.info(f"Massive universe: {len(tickers)} tickers")
    else:
        tickers = get_most_active_tickers(250)
        log.info(f"Screener universe: {len(tickers)} tickers")

    # ── Parallel source fetching ─────────────────────────────────────────────
    log.info(f"Launching multi-source fetch for {len(tickers)} tickers…")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        f_yahoo   = pool.submit(lambda: _yahoo_full(tickers))
        f_stooq   = pool.submit(fetch_stooq_batch, tickers)
        f_massive = pool.submit(fetch_massive_batch,  tickers, get_key("massive"))
        f_finnhub = pool.submit(fetch_finnhub_batch, tickers, get_key("finnhub"))
        f_tiingo  = pool.submit(fetch_tiingo_batch,  tickers, get_key("tiingo"))

        yahoo_result  = f_yahoo.result()
        stooq_result  = f_stooq.result()
        massive_result= f_massive.result()
        finnhub_result= f_finnhub.result()
        tiingo_result = f_tiingo.result()

    # Massive: prefer snapshot data, fall back to batch if snapshot returned nothing
    massive_data = massive_snap if massive_snap else massive_result

    yahoo_vols = yahoo_result["vols"]
    meta_data  = yahoo_result["meta"]

    # ── Aggregate ────────────────────────────────────────────────────────────
    records = aggregate_sources(
        tickers, yahoo_vols, massive_data,
        finnhub_result, stooq_result, tiingo_result, meta_data
    )

    elapsed = round(time.time() - t0, 1)
    log.info(f"Scan done: {len(records)} records in {elapsed}s")

    # ── Build top lists ──────────────────────────────────────────────────────
    top_volume = sorted(records, key=lambda r: r["volume"], reverse=True)[:50]
    has_ratio  = [r for r in records if r["volRatio"] is not None]
    top_ratio  = sorted(has_ratio, key=lambda r: r["volRatio"], reverse=True)[:50]

    # Which sources actually contributed data
    active_sources = set()
    for r in records:
        active_sources.update(r.get("sourceVolumes", {}).keys())

    return {
        "topVolume":     top_volume,
        "topRatio":      top_ratio,
        "scannedCount":  len(records),
        "elapsed":       elapsed,
        "activeSources": sorted(active_sources),
        "sourceConfig": {
            "Yahoo":   True,
            "Stooq":   True,
            "Massive": bool(get_key("massive")),
            "Finnhub": bool(get_key("finnhub")),
            "Tiingo":  bool(get_key("tiingo")),
        }
    }


def _yahoo_full(tickers: list[str]) -> dict:
    """
    Fetch volume + metadata for all tickers in parallel.

    Each call uses history(period='5d') for volume and info['averageVolume']
    for the 3-month average — both are reliable on real machines.
    """
    meta = {}

    def get_meta(tkr):
        d = enrich_single_yahoo(tkr)
        if d:
            meta[tkr] = d

    with concurrent.futures.ThreadPoolExecutor(max_workers=ARGS.workers) as pool:
        list(pool.map(get_meta, tickers))

    # Count how many have a valid avg_vol (needed for vol_ratio)
    with_ratio = sum(1 for d in meta.values() if d.get("avgVol", 0) > 0)
    log.info(f"Yahoo: {len(meta)}/{len(tickers)} tickers returned data, "
             f"{with_ratio} have avg_vol (will appear in ratio list)")

    vols = {
        tkr: {
            "vol":       d.get("vol", 0),
            "price":     d.get("price", 0),
            "changePct": d.get("changePct", 0),
        }
        for tkr, d in meta.items()
    }
    return {"vols": vols, "meta": meta}


# ══════════════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════════════

def _do_scan_and_save() -> dict:
    """Run a live scan, persist to DB, update in-memory cache."""
    result = run_scan()
    result["fetchedAt"] = int(time.time())
    scan_id = db_save_scan(result)
    result["scanId"] = scan_id
    _cache["data"] = result
    _cache["ts"]   = time.time()
    return result

def _cached_scan(force: bool) -> dict:
    """
    Return scan data.

    force=True  → run a fresh live scan (only when user clicks the button)
    force=False → return in-memory cache or DB — NEVER auto-triggers a scan
    """
    now = time.time()

    # ── force=True: user explicitly requested a live scan ───────────────────
    if force:
        result = _do_scan_and_save()
        return {**result, "cached": False, "fromMemory": False,
                "age": 0, "nextRefresh": CACHE_TTL,
                "fetchedAt": result["fetchedAt"]}

    # ── force=False: serve existing data only, never scan automatically ─────

    # 1. In-memory cache still warm
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        age = int(now - _cache["ts"])
        return {**_cache["data"], "cached": True, "fromMemory": True,
                "age": age, "nextRefresh": int(CACHE_TTL - age)}

    # 2. Try database (server restart, stale cache, etc.)
    db_result = db_load_latest()
    if db_result:
        age = int(now - db_result["fetchedAt"])
        _cache["data"] = db_result   # warm the memory cache
        _cache["ts"]   = db_result["fetchedAt"]
        log.info(f"Serving from DB (age={age}s)")
        return {**db_result, "cached": True, "fromDb": True,
                "age": age, "nextRefresh": 0,
                "fetchedAt": db_result["fetchedAt"]}

    # 3. Nothing available — return empty payload; frontend shows "no data" state
    log.info("No data in cache or DB — waiting for manual scan")
    return {
        "topVolume":     [],
        "topRatio":      [],
        "scannedCount":  0,
        "activeSources": [],
        "sourceConfig":  {},
        "cached":        False,
        "fromDb":        False,
        "fromMemory":    False,
        "age":           0,
        "nextRefresh":   0,
        "fetchedAt":     int(now),
    }


@app.route("/api/scan")
def api_scan():
    force = request.args.get("force", "").lower() == "true"
    try:
        result = _cached_scan(force)
        return jsonify({"ok": True, **result})
    except Exception as e:
        log.error(f"Scan error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/top-volume")
def api_top_volume():
    force = request.args.get("force", "").lower() == "true"
    result = _cached_scan(force)
    return jsonify({"ok": True, "data": result["topVolume"],
                    "cached": result["cached"], "age": result.get("age", 0)})


@app.route("/api/top-ratio")
def api_top_ratio():
    force = request.args.get("force", "").lower() == "true"
    result = _cached_scan(force)
    return jsonify({"ok": True, "data": result["topRatio"],
                    "cached": result["cached"], "age": result.get("age", 0)})


@app.route("/api/sources")
def api_sources():
    return jsonify({
        "ok": True,
        "sources": [
            {"name": "Yahoo Finance", "active": True, "key": False,
             "coverage": "CTA+UTP consolidated — lit exchanges ~99%", "latency": "15 min"},
            {"name": "Stooq", "active": True, "key": False,
             "coverage": "US consolidated via their data partner", "latency": "EOD"},
            {"name": "Massive.com", "active": bool(get_key("massive")), "key": True,
             "coverage": "All 19 US exchanges + dark pools (FINRA/ATS) + OTC — 100%", "latency": "EOD (free) / 15-min delay (Starter) / real-time (Advanced)"},
            {"name": "Finnhub", "active": bool(get_key("finnhub")), "key": True,
             "coverage": "US consolidated + partial dark pool", "latency": "real-time"},
            {"name": "Tiingo", "active": bool(get_key("tiingo")), "key": True,
             "coverage": "US consolidated via IEX feed", "latency": "real-time"},
        ],
        "keysConfigured": {
            "massive": bool(get_key("massive")),
            "finnhub": bool(get_key("finnhub")),
            "tiingo":  bool(get_key("tiingo")),
        }
    })


@app.route("/api/rate-limits")
def api_rate_limits():
    """Live rate-limit stats for all throttled sources."""
    return jsonify({
        "ok": True,
        "limits": {
            "finnhub": {
                **FINNHUB_LIMITER.stats(),
                "type":        "token_bucket",
                "description": "60 requests per 60 seconds — token-bucket throttle",
            },
            "tiingo": {
                **TIINGO_LIMITER.stats(),
                "type":        "hourly_budget",
                "description": "50 tickers per clock hour — hard cap, resets on the hour",
            },
        }
    })


@app.route("/api/status")
def api_status():
    now = time.time()
    return jsonify({
        "ok": True,
        "server": "MarketPulse Multi-Source v3",
        "cacheAge": int(now - _cache["ts"]) if _cache["ts"] else None,
        "cacheTTL": CACHE_TTL,
        "scannedCount": len((_cache.get("data") or {}).get("topVolume", [])),
        "activeSources": (_cache.get("data") or {}).get("activeSources", []),
        "sourceConfig": {
            "Yahoo":   True,
            "Stooq":   True,
            "Massive": bool(get_key("massive")),
            "Finnhub": bool(get_key("finnhub")),
            "Tiingo":  bool(get_key("tiingo")),
        }
    })


# ══════════════════════════════════════════════════════════════════════════════
#  KEY MANAGEMENT ROUTES
# ══════════════════════════════════════════════════════════════════════════════

VALID_SOURCES = {"massive", "finnhub", "tiingo", "alphavantage"}

@app.route("/api/keys", methods=["GET"])
def api_keys_get():
    """Return which keys are configured (masked for security)."""
    def mask(v):
        if not v: return ""
        return v[:4] + "•" * max(0, len(v) - 8) + v[-4:] if len(v) > 8 else "•" * len(v)
    return jsonify({
        "ok": True,
        "keys": {k: {"set": bool(v), "masked": mask(v)} for k, v in _KEYS.items()}
    })


@app.route("/api/keys/<source>", methods=["POST"])
def api_keys_set(source: str):
    """Save an API key for a source. Body: {key: "..."}"""
    if source not in VALID_SOURCES:
        return jsonify({"ok": False, "error": f"Ukendt kilde: {source}"}), 400
    body = request.get_json(silent=True) or {}
    key  = (body.get("key") or "").strip()
    if not key:
        return jsonify({"ok": False, "error": "Nøgle må ikke være tom"}), 400

    # Validate key by making a test API call
    valid, msg = _validate_key(source, key)
    if not valid:
        return jsonify({"ok": False, "error": f"Nøgle afvist: {msg}"}), 400

    set_key(source, key)
    # Invalidate cache so next scan uses the new key
    _cache["data"] = None
    _cache["ts"]   = 0.0
    log.info(f"Key updated for {source}: {key[:4]}…")
    return jsonify({"ok": True, "message": f"{source} nøgle gemt ✓"})


@app.route("/api/keys/<source>", methods=["DELETE"])
def api_keys_delete(source: str):
    """Remove a stored key."""
    if source not in VALID_SOURCES:
        return jsonify({"ok": False, "error": f"Ukendt kilde: {source}"}), 400
    set_key(source, "")
    _cache["data"] = None
    _cache["ts"]   = 0.0
    return jsonify({"ok": True, "message": f"{source} nøgle fjernet"})


def _validate_key(source: str, key: str) -> tuple[bool, str]:
    """Quick validation ping to each API to verify the key works."""
    try:
        if source == "finnhub":
            r = requests.get(f"https://finnhub.io/api/v1/stock/profile2?symbol=AAPL&token={key}", timeout=6)
            if r.status_code == 401 or r.status_code == 403:
                return False, "Ugyldig Finnhub nøgle"
            if r.status_code == 200 and "error" in r.text.lower() and "api" in r.text.lower():
                return False, r.json().get("error","Ugyldig nøgle")
            return True, "ok"

        elif source == "massive":
            r = requests.get(f"{MASSIVE_BASE}/v2/aggs/ticker/AAPL/range/1/day/2024-01-02/2024-01-02?apiKey={key}", timeout=6)
            if r.status_code in (401, 403):
                return False, "Ugyldig Massive.com nøgle"
            return True, "ok"

        elif source == "tiingo":
            r = requests.get(f"https://api.tiingo.com/api/test/?token={key}", timeout=6)
            if r.status_code in (401, 403):
                return False, "Ugyldig Tiingo nøgle"
            return True, "ok"

        elif source == "alphavantage":
            r = requests.get(f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey={key}", timeout=6)
            data = r.json()
            if "Error Message" in data or "Invalid API call" in str(data):
                return False, "Ugyldig Alpha Vantage nøgle"
            return True, "ok"

    except requests.exceptions.ConnectionError:
        # Network blocked in some environments — accept key without validation
        log.warning(f"Cannot reach {source} for validation — accepting key without check")
        return True, "ok (uvalideret — netværk utilgængeligt)"
    except Exception as e:
        log.warning(f"Key validation error for {source}: {e}")
        return True, "ok (uvalideret)"

    return True, "ok"


@app.route("/api/history")
def api_history():
    """List metadata for the last 20 scans."""
    try:
        return jsonify({"ok": True, "scans": db_scan_history(20)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/history/<int:scan_id>")
def api_history_detail(scan_id: int):
    """Load a specific historical scan by ID."""
    result = db_load_scan_by_id(scan_id)
    if not result:
        return jsonify({"ok": False, "error": f"Scan #{scan_id} ikke fundet"}), 404
    return jsonify({"ok": True, **result})


@app.route("/")
def index():
    cfg = {"Yahoo": "✓ (gratis)", "Stooq": "✓ (gratis)",
           "Massive": "✓" if get_key("massive") else "✗ (ingen nøgle)",
           "Finnhub": "✓" if get_key("finnhub") else "✗ (ingen nøgle)",
           "Tiingo":  "✓" if get_key("tiingo")  else "✗ (ingen nøgle)"}
    rows = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k,v in cfg.items())
    return f"""<h2>MarketPulse Multi-Source v3 ✓</h2>
<h3>Aktive kilder:</h3><table border=1>{rows}</table>
<p>Tilføj nøgler: <code>python server_v3.py --massive KEY --finnhub KEY --tiingo KEY</code></p>
<ul>
  <li><a href='/api/scan'>/api/scan</a></li>
  <li><a href='/api/sources'>/api/sources</a></li>
  <li><a href='/api/status'>/api/status</a></li>
</ul>"""


if __name__ == "__main__":
    _init_keys()
    db_init()
    log.info("=" * 60)
    log.info("MarketPulse Multi-Source Volume Aggregator v3")
    log.info(f"  Yahoo Finance  : ✓ (altid aktiv)")
    log.info(f"  Stooq          : ✓ (altid aktiv)")
    log.info(f"  Massive.com    : {'✓ ' + get_key("massive")[:8] + '…' if get_key("massive") else '✗  (--massive KEY)'}")
    log.info(f"  Finnhub        : {'✓ ' + get_key("finnhub")[:8] + '…' if get_key("finnhub") else '✗  (--finnhub KEY)'}")
    log.info(f"  Tiingo         : {'✓ ' + get_key("tiingo")[:8]  + '…' if get_key("tiingo")  else '✗  (--tiingo KEY)'}")
    log.info(f"  Cache TTL      : {CACHE_TTL}s")
    log.info(f"  Database       : {DB_FILE}")
    log.info(f"  Workers        : {ARGS.workers}")
    log.info("=" * 60)
    app.run(host="0.0.0.0", port=ARGS.port, debug=False)
