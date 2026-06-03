"""
AI INFRA SCANNER v4.0 — PORT 6464
Lokal proxy: Yahoo Finance + FRED + Finnhub (59 kald/min) + API-nøgle persistens
"""
import json, time, threading, collections, os, urllib.request, urllib.parse, urllib.error
import http.cookiejar
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

PORT   = 6464
HOST   = 'localhost'
ROOT   = Path(__file__).resolve().parent
KEYS_FILE = ROOT / 'api_keys.json'
TICKERS_FILE = ROOT / 'tickers_db.json'
UA     = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36'

# ── persisted keys (loaded at startup, saved on change) ─────────────────────
_keys = {'finnhub': '', 'anthropic': ''}

def load_keys():
    global _keys
    if KEYS_FILE.exists():
        try:
            _keys.update(json.loads(KEYS_FILE.read_text('utf-8')))
        except Exception:
            pass

def save_keys():
    KEYS_FILE.write_text(json.dumps(_keys, indent=2), 'utf-8')

load_keys()

def load_ticker_db():
    if TICKERS_FILE.exists():
        try:
            data = json.loads(TICKERS_FILE.read_text('utf-8'))
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {'version': 1, 'tickers': [], 'addedTickers': [], 'updatedAt': None}

def _clean_ticker_entry(entry):
    if not isinstance(entry, dict):
        return None
    sym = str(entry.get('t') or entry.get('symbol') or '').upper().strip()
    sym = ''.join(ch for ch in sym if ch.isalnum() or ch in '.-')
    if not sym or len(sym) > 16:
        return None
    tier = str(entry.get('tier') or 'C').upper().strip()[:1]
    if tier not in ('S', 'A', 'B', 'C', 'D'):
        tier = 'C'
    try:
        score = int(entry.get('score', 50))
    except Exception:
        score = 50
    score = max(1, min(100, score))
    try:
        rank = int(entry.get('r', 0))
    except Exception:
        rank = 0
    return {
        'r': rank,
        't': sym,
        'n': str(entry.get('n') or entry.get('name') or sym)[:120],
        'cat': str(entry.get('cat') or 'US AKTIE')[:40],
        'score': score,
        'tier': tier,
        'mom': str(entry.get('mom') or '')[:80],
        'case': str(entry.get('case') or '')[:260],
        'isNew': bool(entry.get('isNew'))
    }

def save_ticker_db(payload):
    tickers = []
    seen = set()
    for raw in payload.get('tickers') or []:
        entry = _clean_ticker_entry(raw)
        if not entry or entry['t'] in seen:
            continue
        tickers.append(entry)
        seen.add(entry['t'])
    added = []
    for raw in payload.get('addedTickers') or []:
        sym = ''.join(ch for ch in str(raw).upper().strip() if ch.isalnum() or ch in '.-')
        if sym and sym in seen and sym not in added:
            added.append(sym)
    data = {
        'version': 1,
        'updatedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'tickers': tickers,
        'addedTickers': added
    }
    TICKERS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), 'utf-8')
    return data

# ── Yahoo session ────────────────────────────────────────────────────────────
_cookiejar = http.cookiejar.CookieJar()
_opener    = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_cookiejar))
_crumb, _crumb_time = None, 0

def http_get(url, timeout=20, accept='application/json,text/plain,*/*', extra_headers=None):
    hdrs = {'User-Agent': UA, 'Accept': accept,
            'Accept-Language': 'en-US,en;q=0.9,da;q=0.8', 'Connection': 'close'}
    if extra_headers:
        hdrs.update(extra_headers)
    req = urllib.request.Request(url, headers=hdrs)
    with _opener.open(req, timeout=timeout) as r:
        return r.status, r.read().decode('utf-8', errors='replace')

def ensure_yahoo_session():
    global _crumb, _crumb_time
    if _crumb and (time.time() - _crumb_time) < 21600:
        return _crumb
    for u in ['https://fc.yahoo.com', 'https://finance.yahoo.com/quote/NVDA']:
        try: http_get(u, timeout=12, accept='text/html,*/*')
        except: pass
    s, text = http_get('https://query1.finance.yahoo.com/v1/test/getcrumb',
                        timeout=15, accept='text/plain,*/*')
    c = text.strip()
    if s == 200 and c and '<html' not in c.lower() and len(c) < 200:
        _crumb, _crumb_time = c, time.time(); return _crumb
    raise RuntimeError('Kunne ikke hente Yahoo crumb')

def yahoo_quote(symbols):
    crumb = ensure_yahoo_session()
    params = {'symbols': symbols,
              'fields': 'regularMarketPrice,regularMarketChangePercent,'
                        'regularMarketPreviousClose,shortName,longName,currency,'
                        'regularMarketTime,symbol,marketCap,trailingPE,forwardPE,'
                        'priceToBook,dividendYield,fiftyTwoWeekHigh,fiftyTwoWeekLow,'
                        'epsTrailingTwelveMonths',
              'crumb': crumb}
    s, raw = http_get('https://query1.finance.yahoo.com/v7/finance/quote?' +
                       urllib.parse.urlencode(params), timeout=20)
    data = json.loads(raw)
    return {'ok': True, 'source': 'Yahoo',
            'requested': symbols.split(','),
            'results': data.get('quoteResponse', {}).get('result', [])}

def yahoo_chart_one(symbol):
    url = ('https://query1.finance.yahoo.com/v8/finance/chart/' +
           urllib.parse.quote(symbol) + '?' +
           urllib.parse.urlencode({'range': '1d', 'interval': '1d'}))
    s, raw = http_get(url, timeout=15)
    data   = json.loads(raw)
    result = (data.get('chart', {}).get('result') or [None])[0]
    if not result: return None
    meta = result.get('meta', {})
    price = meta.get('regularMarketPrice') or meta.get('chartPreviousClose')
    prev  = meta.get('previousClose') or meta.get('chartPreviousClose')
    chpct = None
    try:
        if price is not None and prev:
            chpct = (float(price) - float(prev)) / float(prev) * 100
    except: pass
    return {'symbol': meta.get('symbol', symbol),
            'shortName': meta.get('shortName') or symbol,
            'currency': meta.get('currency'),
            'regularMarketPrice': price,
            'regularMarketChangePercent': chpct,
            'regularMarketTime': meta.get('regularMarketTime')}

def yahoo_search(query, count=15):
    url = ('https://query1.finance.yahoo.com/v1/finance/search?' +
           urllib.parse.urlencode({'q': query, 'quotesCount': count,
                                   'newsCount': 0, 'enableFuzzyQuery': True}))
    s, raw = http_get(url, timeout=15)
    data = json.loads(raw)
    return [{'symbol': q.get('symbol'),
             'shortname': q.get('shortname') or q.get('longname'),
             'exchDisp': q.get('exchDisp'),
             'typeDisp': q.get('typeDisp')}
            for q in data.get('quotes', []) if q.get('symbol')]

def fred_series(series_id):
    url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=' + urllib.parse.quote(series_id)
    s, txt = http_get(url, timeout=20, accept='text/csv,text/plain,*/*')
    rows = []
    for line in txt.strip().splitlines()[1:]:
        if ',' not in line: continue
        d, v = line.split(',', 1)
        try: rows.append({'date': d, 'val': float(v)})
        except: pass
    return rows

def latest_by_year(rows):
    out = {}
    for r in rows: out[r['date'][:4]] = r['val']
    return out

def yahoo_chart_history(symbol, rng='10y', interval='1mo'):
    url = ('https://query1.finance.yahoo.com/v8/finance/chart/' +
           urllib.parse.quote(symbol) + '?' +
           urllib.parse.urlencode({'range': rng, 'interval': interval}))
    s, raw = http_get(url, timeout=20)
    data   = json.loads(raw)
    result = (data.get('chart', {}).get('result') or [None])[0]
    if not result: raise RuntimeError('Ingen Yahoo chart-data for ' + symbol)
    meta   = result.get('meta', {})
    ts     = result.get('timestamp') or []
    q      = (result.get('indicators', {}).get('quote') or [{}])[0]
    closes = q.get('close') or []
    rows   = [{'date': time.strftime('%Y-%m-%d', time.gmtime(int(t))), 'val': float(c)}
              for t, c in zip(ts, closes) if c is not None]
    price  = meta.get('regularMarketPrice') or (rows[-1]['val'] if rows else None)
    mt     = meta.get('regularMarketTime')
    mdate  = time.strftime('%Y-%m-%d', time.gmtime(int(mt))) if mt else (rows[-1]['date'] if rows else None)
    return {'symbol': meta.get('symbol', symbol),
            'price': float(price) if price is not None else None,
            'date': mdate, 'rows': rows, 'currency': meta.get('currency')}

# ── Finnhub rate-limiter  (max 59 calls / 60 s rolling window) ──────────────
_fh_lock      = threading.Lock()
_fh_timestamps = collections.deque()   # timestamps of recent calls
FH_MAX        = 59
FH_WINDOW     = 61   # seconds (slightly over 60 for safety)
_fh_cache     = {}   # symbol -> {data, ts}
FH_CACHE_TTL  = 55   # reuse cached data for 55 s

def _fh_wait():
    """Block until we can safely make one more Finnhub call."""
    with _fh_lock:
        now = time.time()
        # drop timestamps older than window
        while _fh_timestamps and now - _fh_timestamps[0] > FH_WINDOW:
            _fh_timestamps.popleft()
        if len(_fh_timestamps) >= FH_MAX:
            oldest = _fh_timestamps[0]
            wait   = FH_WINDOW - (now - oldest) + 0.1
            if wait > 0:
                time.sleep(wait)
            # re-purge after sleep
            now = time.time()
            while _fh_timestamps and now - _fh_timestamps[0] > FH_WINDOW:
                _fh_timestamps.popleft()
        _fh_timestamps.append(time.time())

def finnhub_quote(symbol, api_key):
    """Fetch a single quote from Finnhub with rate-limiting & caching."""
    cached = _fh_cache.get(symbol)
    if cached and (time.time() - cached['ts']) < FH_CACHE_TTL:
        return cached['data']
    _fh_wait()
    url = ('https://finnhub.io/api/v1/quote?symbol=' +
           urllib.parse.quote(symbol) + '&token=' + urllib.parse.quote(api_key))
    s, raw = http_get(url, timeout=15)
    data = json.loads(raw)
    result = {
        'symbol': symbol,
        'regularMarketPrice':         data.get('c'),
        'regularMarketChangePercent': data.get('dp'),
        'regularMarketPreviousClose': data.get('pc'),
        'high':  data.get('h'),
        'low':   data.get('l'),
        'open':  data.get('o'),
        'source': 'finnhub'
    }
    _fh_cache[symbol] = {'data': result, 'ts': time.time()}
    return result

def finnhub_batch(symbols, api_key):
    """Fetch multiple symbols respecting 59-call/min limit."""
    results, errors = [], {}
    for sym in symbols:
        try:
            r = finnhub_quote(sym, api_key)
            if r.get('regularMarketPrice') is not None:
                results.append(r)
            else:
                errors[sym] = 'Ingen pris'
        except Exception as e:
            errors[sym] = str(e)
    return results, errors

def fh_rate_status():
    now = time.time()
    active = [t for t in _fh_timestamps if now - t <= FH_WINDOW]
    return {'calls_last_60s': len(active), 'limit': FH_MAX, 'remaining': FH_MAX - len(active)}

# ── HTTP Handler ─────────────────────────────────────────────────────────────
class Handler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin',  '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma',  'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def json_response(self, payload, status=200):
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type',   'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args): pass   # suppress console noise

    # ── POST ────────────────────────────────────────────────────────────────
    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get('Content-Length', 0))
        body   = self.rfile.read(length)
        try: payload = json.loads(body) if body else {}
        except: payload = {}

        # ── save API keys ───────────────────────────────────────────────────
        if parsed.path == '/api/keys/save':
            changed = False
            for k in ('finnhub', 'anthropic'):
                v = (payload.get(k) or '').strip()
                if v and v != _keys.get(k):
                    _keys[k] = v
                    changed = True
            if changed:
                save_keys()
            return self.json_response({'ok': True, 'saved': list(_keys.keys()),
                                       'finnhub_set':   bool(_keys['finnhub']),
                                       'anthropic_set': bool(_keys['anthropic'])})

        if parsed.path == '/api/tickers/save':
            try:
                data = save_ticker_db(payload)
                return self.json_response({'ok': True,
                                           'saved': len(data.get('tickers', [])),
                                           'added': len(data.get('addedTickers', [])),
                                           'updatedAt': data.get('updatedAt')})
            except Exception as e:
                return self.json_response({'ok': False, 'error': str(e)}, 500)

        # ── Finnhub batch quotes ────────────────────────────────────────────
        if parsed.path == '/api/finnhub/quotes':
            api_key = _keys.get('finnhub') or payload.get('apiKey', '')
            if not api_key:
                return self.json_response({'ok': False,
                    'error': 'Ingen Finnhub API-nøgle. Gem den via Indstillinger.'}, 400)
            symbols = [s.strip() for s in (payload.get('symbols') or '').split(',') if s.strip()]
            if not symbols:
                return self.json_response({'ok': False, 'error': 'Ingen symbols'}, 400)
            results, errors = finnhub_batch(symbols, api_key)
            return self.json_response({'ok': True, 'source': 'finnhub',
                                       'results': results, 'errors': errors,
                                       'rate': fh_rate_status()})

        # ── Anthropic AI proxy ──────────────────────────────────────────────
        if parsed.path == '/api/ai':
            api_key = (payload.pop('apiKey', '') or '').strip() or \
                      _keys.get('anthropic') or \
                      os.environ.get('ANTHROPIC_API_KEY', '')
            if not api_key:
                return self.json_response({'ok': False,
                    'error': 'Ingen Anthropic API-nøgle fundet.\n\n'
                             'Løsning 1 (anbefalet): Åbn start_windows.bat i Notepad og erstat\n'
                             'DIN_NOGLE_HER med din nøgle fra console.anthropic.com\n\n'
                             'Løsning 2: Gem nøglen via ⚙ Indstillinger-panelet i browseren.'}, 400)
            try:
                req_body = json.dumps(payload).encode('utf-8')
                req = urllib.request.Request(
                    'https://api.anthropic.com/v1/messages',
                    data=req_body,
                    headers={'Content-Type': 'application/json',
                             'x-api-key': api_key,
                             'anthropic-version': '2023-06-01'},
                    method='POST')
                with urllib.request.urlopen(req, timeout=60) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return self.json_response({'ok': True, 'result': result})
            except urllib.error.HTTPError as e:
                err_body = e.read().decode('utf-8', errors='replace')
                try:    msg = json.loads(err_body).get('error', {}).get('message', err_body)
                except: msg = err_body
                return self.json_response({'ok': False,
                    'error': f'Anthropic {e.code}: {msg}'}, 502)
            except Exception as e:
                return self.json_response({'ok': False, 'error': str(e)}, 500)

        self.send_response(404); self.end_headers()

    # ── GET ─────────────────────────────────────────────────────────────────
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == '/favicon.ico':
            self.send_response(204); self.end_headers(); return

        if parsed.path == '/api/ping':
            return self.json_response({'ok': True, 'port': PORT,
                'finnhub_configured':   bool(_keys['finnhub']),
                'anthropic_configured': bool(_keys['anthropic']),
                'finnhub_rate': fh_rate_status()})

        # ── get current saved keys (masked) ────────────────────────────────
        if parsed.path == '/api/keys/status':
            def mask(s): return s[:8]+'…'+s[-4:] if len(s) > 14 else ('(sat)' if s else '')
            return self.json_response({'ok': True,
                'finnhub_set':   bool(_keys['finnhub']),
                'anthropic_set': bool(_keys['anthropic']),
                'finnhub_masked':   mask(_keys['finnhub']),
                'anthropic_masked': mask(_keys['anthropic']),
                'rate': fh_rate_status()})

        if parsed.path == '/api/tickers':
            data = load_ticker_db()
            return self.json_response({'ok': True,
                                       'version': data.get('version', 1),
                                       'updatedAt': data.get('updatedAt'),
                                       'tickers': data.get('tickers', []),
                                       'addedTickers': data.get('addedTickers', [])})

        if parsed.path == '/api/quotes':
            symbols = urllib.parse.parse_qs(parsed.query).get('symbols', [''])[0].strip()
            if not symbols:
                return self.json_response({'ok': False, 'error': 'Ingen symbols'}, 400)
            requested = [s.strip() for s in symbols.split(',') if s.strip()]
            # Try Yahoo first; fallback to Finnhub if key available
            try:
                payload = yahoo_quote(','.join(requested))
                if payload.get('results'):
                    return self.json_response(payload)
            except Exception as e:
                quote_error = str(e)
            else:
                quote_error = 'Yahoo gav 0 resultater'
            # Yahoo chart fallback
            results, errors = [], {}
            for sym in requested:
                try:
                    q = yahoo_chart_one(sym)
                    if q: results.append(q)
                    else: errors[sym] = 'Ingen data'
                except Exception as e:
                    errors[sym] = str(e)
            # If still missing and Finnhub key exists, fill gaps
            if _keys['finnhub']:
                missing = [sym for sym in requested
                           if not any(r.get('symbol') == sym for r in results)]
                if missing:
                    fh_results, fh_errors = finnhub_batch(missing, _keys['finnhub'])
                    results.extend(fh_results)
                    errors.update(fh_errors)
            return self.json_response({'ok': bool(results), 'source': 'Yahoo+Finnhub fallback',
                                       'quote_error': quote_error,
                                       'requested': requested,
                                       'results': results, 'errors': errors},
                                      200 if results else 502)

        if parsed.path == '/api/search':
            q = urllib.parse.parse_qs(parsed.query).get('q', [''])[0].strip()
            if not q:
                return self.json_response({'ok': False, 'error': 'Ingen søgeterm'}, 400)
            try:
                return self.json_response({'ok': True, 'query': q,
                                           'results': yahoo_search(q)})
            except Exception as e:
                return self.json_response({'ok': False, 'error': str(e)}, 502)

        if parsed.path == '/api/fred':
            sid = urllib.parse.parse_qs(parsed.query).get('series', [''])[0]
            if not sid:
                return self.json_response({'ok': False, 'error': 'Ingen series'}, 400)
            try:
                return self.json_response({'ok': True, 'series': sid,
                                           'results': fred_series(sid)})
            except Exception as e:
                return self.json_response({'ok': False, 'error': str(e)}, 502)

        if parsed.path == '/api/buffett':
            try:
                w5000 = yahoo_chart_history('^W5000', rng='10y', interval='1mo')
                gdp   = fred_series('GDP')
                if not w5000.get('price') or not gdp:
                    raise RuntimeError('Mangler data')
                lg       = gdp[-1]
                mcap_val = float(w5000['price'])
                gdp_val  = float(lg['val'])
                gY = latest_by_year(gdp)
                wY = latest_by_year(w5000.get('rows', []))
                hist = [{'y': y, 'v': round((wY[y]/gY[y])*100, 1)}
                        for y in sorted(wY) if int(y) >= 2015 and y in gY]
                cy = w5000['date'][:4] if w5000.get('date') else str(time.gmtime().tm_year)
                cp = {'y': cy, 'v': round((mcap_val/gdp_val)*100, 1)}
                if not hist or hist[-1]['y'] != cp['y']: hist.append(cp)
                else: hist[-1] = cp
                return self.json_response({'ok': True, 'source': 'Yahoo ^W5000 + FRED GDP',
                    'mcap': mcap_val, 'gdp': gdp_val,
                    'value': (mcap_val/gdp_val)*100,
                    'lastUpdated': w5000.get('date') or lg['date'],
                    'gdpUpdated': lg['date'], 'history': hist})
            except Exception as e:
                return self.json_response({'ok': False, 'error': str(e),
                    'mcap': 73730.65, 'gdp': 30767,
                    'value': 73730.65/30767*100,
                    'history': [{'y':'2024','v':196},{'y':'2025','v':221.4},{'y':'2026','v':239.7}]})

        return super().do_GET()


if __name__ == '__main__':
    os.chdir(ROOT)
    print(f'AI INFRA SCANNER v4.0  —  http://{HOST}:{PORT}/')
    print(f'Finnhub konfigureret:   {"JA [OK]" if _keys["finnhub"]   else "NEJ (sæt via UI)"}')
    print(f'Anthropic konfigureret: {"JA [OK]" if _keys["anthropic"] else "NEJ (sæt via UI)"}')
    print(f'Finnhub rate limit:     maks {FH_MAX} kald / {FH_WINDOW}s')
    print('Luk med Ctrl+C')
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
