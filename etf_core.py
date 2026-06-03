"""
ETF PRO v10 — Ren og simpel
Data sources (i prioritet):
  1. etf-database.json  — lokal database med 600 ETFs (ISIN, navn, TER, provider)
  2. Yahoo Finance       — pris, sektorer (US ETFs), returns via chart
  3. justETF Wicket      — lande + sektorer (VIRKER: testet)
  4. Alpha Vantage       — sektorer + top-10 holdings
  5. DWS/Xtrackers       — lande for Xtrackers ETFs
"""
import json, os, time, re, math, urllib.parse, urllib.request, http.cookiejar, csv
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

HOST  = "localhost"
PORT  = int(os.environ.get("PORT", "9002"))
ROOT  = Path(__file__).resolve().parent
CACHE = ROOT / ".etf_cache"
CACHE.mkdir(exist_ok=True)

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

_jar    = http.cookiejar.CookieJar()
_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_jar))
_crumb  = None
_crumb_ts = 0.0
_av_last  = 0.0

# ── Lokal ETF database (etf-database.json) ────────────────────────────────────
_db_by_isin   = {}   # ISIN → record
_db_by_ticker = {}   # ticker (upper) → record
_db_by_yahoo  = {}   # yahoo symbol (upper) → record

def _load_db():
    global _db_by_isin, _db_by_ticker, _db_by_yahoo
    db_path = ROOT / "etf-database.json"
    if not db_path.exists():
        print("  ADVARSEL: etf-database.json ikke fundet")
        return
    try:
        records = json.loads(db_path.read_text("utf-8"))
        for r in records:
            isin   = (r.get("isin") or "").strip().upper()
            ticker = (r.get("ticker") or "").strip().upper()
            yahoo  = (r.get("yahoo") or "").strip().upper()
            if isin:   _db_by_isin[isin]     = r
            if ticker: _db_by_ticker[ticker]  = r
            if yahoo:  _db_by_yahoo[yahoo]    = r
        print(f"  DB: {len(records)} ETFs indlæst "
              f"({len(_db_by_isin)} ISINs, {len(_db_by_ticker)} tickers)")
    except Exception as e:
        print(f"  DB fejl: {e}")

def db_lookup(symbol):
    """
    Find ETF i lokal database via ticker eller ISIN.
    Prøver: exact ticker → ticker+.DE → ticker+.L → ISIN → yahoo symbol
    """
    s = symbol.strip().upper()
    return (_db_by_ticker.get(s) or
            _db_by_isin.get(s) or
            _db_by_yahoo.get(s) or
            _db_by_ticker.get(s.split(".")[0]) or
            None)

# ── cache ─────────────────────────────────────────────────────────────────────
def _cp(n):
    s = "".join(c if c.isalnum() or c in "._-" else "_" for c in n)
    return CACHE / (s + ".json")

def rc(n, ttl):
    p = _cp(n)
    if not p.exists(): return None
    if time.time() - p.stat().st_mtime > ttl: return None
    try: return json.loads(p.read_text("utf-8"))
    except: return None

def wc(n, d):
    try: _cp(n).write_text(json.dumps(d, ensure_ascii=False), "utf-8")
    except: pass

# ── http ──────────────────────────────────────────────────────────────────────
def hget(url, timeout=20, accept="application/json,*/*", extra=None):
    h = {"User-Agent": UA, "Accept": accept,
         "Accept-Language": "en-US,en;q=0.9", "Connection": "close"}
    if extra: h.update(extra)
    req = urllib.request.Request(url, headers=h)
    with _opener.open(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", errors="replace")

def gjson(url, timeout=20, extra=None):
    _, t = hget(url, timeout=timeout, extra=extra)
    return json.loads(t)

def raw(v):
    return v.get("raw") if isinstance(v, dict) and "raw" in v else v

def to_float(v, default=None):
    v = raw(v)
    if v is None: return default
    try: return float(v)
    except: return default

def to_pct(v):
    f = to_float(v)
    if f is None: return None
    return f / 100 if abs(f) > 1 else f

def ysym(s):  return s.strip().upper()
def tvsym(s): return s.split(".")[0].upper()

def yahoo_sym(ticker, db_rec=None):
    """
    Byg korrekt Yahoo Finance symbol fra DB eller ticker.
    DB yahoo-feltet er autoritativt — sat korrekt per ETF.
    Fallback: .DE for de fleste UCITS ETFs.
    """
    # DB har korrekt suffix sat per ETF
    if db_rec and db_rec.get("yahoo"):
        return db_rec["yahoo"].strip().upper()
    t = ticker.strip().upper()
    if "." in t: return t   # allerede med suffix (fx QOMP.DE fra bruger)
    return t + ".DE"        # default: XETR Frankfurt

def yahoo_sym_fallbacks(ticker, db_rec=None):
    """
    Returnerer liste af Yahoo-symboler at prøve, i prioriteret rækkefølge.
    """
    primary = yahoo_sym(ticker, db_rec)
    base    = ticker.split(".")[0].upper()
    
    # Byg fallback liste
    fallbacks = [primary]
    for sfx in [".DE",".L",".PA",".AS",".MI",".SW",""]:
        sym = base + sfx
        if sym not in fallbacks:
            fallbacks.append(sym)
    return fallbacks

# ══════════════════════════════════════════════════════════════════════════════
#  Yahoo Finance
# ══════════════════════════════════════════════════════════════════════════════
def get_crumb():
    global _crumb, _crumb_ts
    if _crumb and time.time() - _crumb_ts < 3600: return _crumb
    for u in ["https://fc.yahoo.com", "https://finance.yahoo.com"]:
        try: hget(u, timeout=10, accept="text/html,*/*")
        except: pass
    for host in ["query1", "query2"]:
        try:
            st, txt = hget(
                f"https://{host}.finance.yahoo.com/v1/test/getcrumb",
                timeout=12, accept="text/plain,*/*")
            c = txt.strip()
            if st == 200 and c and "<html" not in c.lower() and 3 < len(c) < 200:
                _crumb = c; _crumb_ts = time.time(); return _crumb
        except: pass
    _crumb = ""; _crumb_ts = time.time(); return ""

def quote_summary(symbol, modules):
    c = get_crumb()
    p = {"modules": modules}
    if c: p["crumb"] = c
    sym = urllib.parse.quote(symbol)
    qs  = urllib.parse.urlencode(p)
    err = "no response"
    for host in ["query2", "query1"]:
        try:
            data = gjson(
                f"https://{host}.finance.yahoo.com/v10/finance/quoteSummary/{sym}?{qs}",
                timeout=22)
            res = (data.get("quoteSummary", {}).get("result") or [None])[0]
            if res: return res, None
            err = str(data.get("quoteSummary", {}).get("error") or "no result")
        except Exception as e: err = str(e)
    return None, err

def get_chart(symbol):
    sym = urllib.parse.quote(symbol)
    for host in ["query1", "query2"]:
        try:
            data = gjson(
                f"https://{host}.finance.yahoo.com/v8/finance/chart/{sym}"
                f"?range=1y&interval=1d", timeout=22)
            res = (data.get("chart", {}).get("result") or [None])[0]
            if res: return res
        except: pass
    return None

def v7quote(symbol):
    c = get_crumb()
    fields = ("regularMarketPrice,regularMarketChange,regularMarketChangePercent,"
              "longName,shortName,currency,fiftyTwoWeekHigh,fiftyTwoWeekLow,totalAssets")
    p = f"symbols={urllib.parse.quote(symbol)}&fields={fields}"
    if c: p += f"&crumb={urllib.parse.quote(c)}"
    try:
        data = gjson(f"https://query1.finance.yahoo.com/v7/finance/quote?{p}", timeout=15)
        rr = data.get("quoteResponse", {}).get("result") or []
        return rr[0] if rr else {}
    except: return {}

# ══════════════════════════════════════════════════════════════════════════════
#  justETF Wicket AJAX — lande + sektorer for ALLE ETFs
#
#  VIGTIGT: Wicket page ID (pid) er SESSION-SPECIFIK og ændres hver gang.
#  Vi må ALDRIG cache sessionen — hent altid en frisk session med nyt pid.
#  Vi cacher KUN succesfulde data-resultater.
#
#  Flow per ETF:
#    1. GET etf-profile.html?isin=XXXX → nye cookies + nyt pid
#    2. POST Wicket AJAX med de nye cookies → data
#    3. Parse HTML-tabeller → lande/sektorer dict
# ══════════════════════════════════════════════════════════════════════════════

JSE_BASE = "https://www.justetf.com"

def jse_get_session(isin):
    """
    Hent en FRISK justETF session for dette ISIN.
    Returnerer (cookies_dict, pid) — aldrig cached.
    """
    try:
        jar2 = http.cookiejar.CookieJar()
        op2  = urllib.request.build_opener(
                   urllib.request.HTTPCookieProcessor(jar2))
        req  = urllib.request.Request(
            f"{JSE_BASE}/en/etf-profile.html?isin={isin}",
            headers={
                "User-Agent":      UA,
                "Accept":          "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control":   "no-cache",
            })
        with op2.open(req, timeout=25) as r:
            html = r.read().decode("utf-8", errors="replace")

        cookies = {c.name: c.value for c in jar2}

        # Find Wicket page ID — bruges i alle AJAX-URL'er
        # Format: ?0-1.0-holdingsSection-... eller ?12-1.0-...
        m = re.search(r'etf-profile\.html\?(\d+)-1\.', html)
        pid = m.group(1) if m else "0"

        return cookies, pid, html
    except Exception as e:
        return {}, "0", ""

def jse_wicket_call(isin, cookies, pid, endpoint):
    """
    Kald et justETF Wicket AJAX endpoint med friske cookies og pid.
    """
    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
    url = (f"{JSE_BASE}/en/etf-profile.html"
           f"?{pid}-1.0-holdingsSection-{endpoint}"
           f"&isin={isin}&_wicket=1")
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent":          UA,
            "Accept":              "*/*",
            "Accept-Language":     "en-US,en;q=0.9",
            "Referer":             f"{JSE_BASE}/en/etf-profile.html?isin={isin}",
            "X-Requested-With":    "XMLHttpRequest",
            "Wicket-Ajax":         "true",
            "Wicket-Ajax-BaseURL": f"en/etf-profile.html?isin={isin}",
            "Cookie":              cookie_str,
        })
        with _opener.open(req, timeout=20) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception:
        return None

def jse_parse_table(xml_body):
    """
    Parse justETF Wicket AJAX XML → dict {name: weight_float}.
    XML indeholder CDATA med HTML-tabeller.
    """
    if not xml_body or "internal-error" in xml_body:
        return {}

    result = {}
    for cdata in re.findall(r'<!\[CDATA\[(.*?)\]\]>', xml_body, re.S):
        # Primær metode: data-testid attributter
        rows = re.findall(
            r'data-testid="[^"]*_value_name">([^<]+)</td>.*?'
            r'([\d]+[.,]\d+)\s*%',
            cdata, re.S)
        for name, pct in rows:
            try:
                result[name.strip()] = round(float(pct.replace(",", ".")) / 100, 4)
            except: pass

        # Fallback: find navn og % separat
        if not result:
            names = re.findall(
                r'data-testid="[^"]*_value_name">([^<]{2,50})<', cdata)
            pcts  = re.findall(r'(?<!\d)([\d]{1,3}[.,]\d{1,2})\s*%', cdata)
            for n, p in zip(names, pcts):
                try:
                    w = float(p.replace(",", ".")) / 100
                    if 0 < w < 1:
                        result[n.strip()] = round(w, 4)
                except: pass

    return result

def jse_data(isin):
    """
    Hent lande + sektorer fra justETF Wicket AJAX.
    Henter altid en frisk session (pid ændres per request).
    Cacher KUN succesfulde resultater.
    """
    if not isin: return None

    # Tjek cache — kun succesfulde resultater er cachet
    cached = rc(f"jse_ok_{isin}", 3600 * 6)
    if cached: return cached

    # Hent FRISK session (aldrig cached)
    cookies, pid, html = jse_get_session(isin)

    if not cookies:
        return None

    # Kald begge endpoints med de friske cookies
    countries_xml = jse_wicket_call(isin, cookies, pid, "countries-loadMoreCountries")
    sectors_xml   = jse_wicket_call(isin, cookies, pid, "sectors-loadMoreSectors")

    countries = jse_parse_table(countries_xml)
    sectors   = jse_parse_table(sectors_xml)

    # Hvis ingen data — prøv én gang til med ny session
    if not countries and not sectors:
        cookies2, pid2, _ = jse_get_session(isin)
        if cookies2 and pid2 != pid:
            countries = jse_parse_table(
                jse_wicket_call(isin, cookies2, pid2, "countries-loadMoreCountries"))
            sectors = jse_parse_table(
                jse_wicket_call(isin, cookies2, pid2, "sectors-loadMoreSectors"))

    if countries or sectors:
        out = {
            "countries": countries,
            "sectors":   sectors,
            "source":    "justETF",
            "isin":      isin,
        }
        wc(f"jse_ok_{isin}", out)  # cache KUN succesfulde resultater
        return out

    return None

def jse_find_isin(ticker):
    """Søg efter ISIN på justETF via ticker."""
    base = ticker.split(".")[0].upper()
    cached = rc(f"jse_isin_{base}", 3600 * 24)
    if cached: return cached
    try:
        url = (f"{JSE_BASE}/en/find-etf.html"
               f"?groupField=index&sortField=ter&sortOrder=asc"
               f"&search={urllib.parse.quote(base)}")
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Accept": "text/html,*/*",
            "Accept-Language": "en-US,en;q=0.9"})
        with _opener.open(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
        isins = re.findall(
            r'etf-profile\.html\?isin=([A-Z]{2}[A-Z0-9]{10})', html)
        if isins:
            wc(f"jse_isin_{base}", isins[0]); return isins[0]
    except: pass
    return None

# ══════════════════════════════════════════════════════════════════════════════
#  Alpha Vantage — sektorer + holdings
# ══════════════════════════════════════════════════════════════════════════════
AV_KEY  = os.environ.get("AV_KEY", "0ZSCF5ALLINJMQUE")
AV_BASE = "https://www.alphavantage.co/query"

def av_etf(symbol):
    av_sym = tvsym(symbol)
    cached = rc(f"av_{av_sym}", 3600 * 12)
    if cached: return cached
    global _av_last
    wait = 12.0 - (time.time() - _av_last)
    if wait > 0: time.sleep(wait)
    _av_last = time.time()
    try:
        st, body = hget(
            f"{AV_BASE}?function=ETF_PROFILE&symbol={av_sym}&apikey={AV_KEY}",
            timeout=20, extra={"Referer": "https://www.alphavantage.co/"})
        if st != 200: return None
        d = json.loads(body)
        if "Information" in d or "Note" in d: return None
        sectors  = {}
        holdings = []
        for item in (d.get("sector_weightings") or []):
            for s, pct in item.items():
                try:
                    wt = float(str(pct).replace("%","").strip()) / 100
                    if s and wt > 0: sectors[s] = round(wt, 4)
                except: pass
        for h in (d.get("holdings") or []):
            sym  = (h.get("symbol") or "").upper()
            name = h.get("description") or h.get("name") or sym
            try: wt = float(str(h.get("weight","0")).replace("%","").strip()) / 100
            except: wt = 0
            if (sym or name) and wt > 0:
                holdings.append({"ticker": sym, "name": name, "weight": wt})
        aa = {}
        for k, v in (d.get("asset_allocation") or {}).items():
            try:
                pct = float(str(v.get("Net Assets %","0")).replace("%","").strip()) / 100
                if k and abs(pct) > 0: aa[k] = round(pct, 4)
            except: pass
        out = {"sectors": sectors, "holdings": holdings, "asset_allocation": aa,
               "ter": d.get("net_expense_ratio"), "source": "Alpha Vantage"}
        if sectors or holdings:
            wc(f"av_{av_sym}", out)
            return out
    except: pass
    return None

# ══════════════════════════════════════════════════════════════════════════════
#  DWS/Xtrackers (309 ETFs fra AllProductData.xlsx)
# ══════════════════════════════════════════════════════════════════════════════
try:
    from xtrackers_data import XTRACKERS_LOOKUP, DWS_BASE, DWS_LANG
except ImportError:
    XTRACKERS_LOOKUP = {}; DWS_BASE = "https://etf.dws.com"; DWS_LANG = "en-gb"

DWS_H = {"Referer": f"{DWS_BASE}/{DWS_LANG}/",
          "Accept-Language": "en-GB,en;q=0.9",
          "X-Requested-With": "XMLHttpRequest"}

def dws_get(path, isin, timeout=18):
    url = f"{DWS_BASE}{path}?isin={isin}&language={DWS_LANG}"
    info = XTRACKERS_LOOKUP.get(isin, {})
    h = {**DWS_H, "Referer": info.get("url") or f"{DWS_BASE}/{DWS_LANG}/"}
    try:
        st, body = hget(url, timeout=timeout,
                        accept="application/json, text/plain, */*", extra=h)
        if st == 200 and body.strip().startswith(("{","[")):
            return json.loads(body)
    except: pass
    return None

def dws_countries(isin):
    cached = rc(f"dws_c_{isin}", 3600 * 8)
    if cached: return cached
    d = dws_get("/umbraco/api/FundData/GetFundCountries", isin)
    if not d: return {}
    out = {}
    for r in (d if isinstance(d,list) else d.get("countries") or d.get("data") or []):
        n = r.get("country") or r.get("name") or r.get("label") or ""
        wt = to_pct(r.get("weight") or r.get("value") or r.get("percentage"))
        if n and wt and wt > 0: out[n] = round(wt, 4)
    if out: wc(f"dws_c_{isin}", out)
    return out

def dws_sectors(isin):
    cached = rc(f"dws_s_{isin}", 3600 * 8)
    if cached: return cached
    d = dws_get("/umbraco/api/FundData/GetFundSectors", isin)
    if not d: return {}
    out = {}
    for r in (d if isinstance(d,list) else d.get("sectors") or d.get("data") or []):
        n = r.get("sector") or r.get("name") or r.get("label") or ""
        wt = to_pct(r.get("weight") or r.get("value") or r.get("percentage"))
        if n and wt and wt > 0: out[n] = round(wt, 4)
    if out: wc(f"dws_s_{isin}", out)
    return out

# ══════════════════════════════════════════════════════════════════════════════
#  Hype score per holding
# ══════════════════════════════════════════════════════════════════════════════
def hype_score(m1, m3, m6, m12, price, hi52, lo52, mcap):
    def f(x):
        try: return float(x) if x is not None else None
        except: return None
    def cl(x, lo=0, hi=100): return max(lo, min(hi, x))
    mom = 25.0
    for val, w in [(f(m1),.10),(f(m3),.15),(f(m6),.13),(f(m12),.12)]:
        if val is not None: mom += val * 1000 * w
    mom = cl(mom, 0, 50)
    trend = 15.0
    hi, lo, pr = f(hi52), f(lo52), f(price)
    if hi and lo and pr and hi > lo:
        trend = cl((pr-lo)/(hi-lo)*30, 0, 30)
    size = 10.0
    mc = f(mcap)
    if mc and mc > 0:
        size = cl((math.log10(max(mc,1e8))-8)/(12.5-8)*20, 0, 20)
    return {"hypeScore": cl(round(mom+trend+size)),
            "hype_momentum": round(mom), "hype_trend": round(trend),
            "hype_size": round(size)}

def holding_data(symbol):
    symbol = ysym(symbol)
    if not symbol: return None
    hit = rc(f"hd10_{symbol}", 300)
    if hit: return hit
    out = {"ticker": symbol, "name": symbol, "price": None, "change": None,
           "sector": "", "industry": "", "marketCap": None,
           "week52High": None, "week52Low": None,
           "m1": None, "m3": None, "m6": None, "m12": None,
           "hypeScore": 0, "hype_momentum": 0, "hype_trend": 0, "hype_size": 0}
    res, _ = quote_summary(symbol, "price,summaryDetail,assetProfile")
    if res:
        pm = res.get("price",{}) or {}; sd = res.get("summaryDetail",{}) or {}
        ap = res.get("assetProfile",{}) or {}
        out["name"]      = raw(pm.get("longName")) or raw(pm.get("shortName")) or symbol
        out["price"]     = to_float(pm.get("regularMarketPrice"))
        out["change"]    = to_float(pm.get("regularMarketChangePercent"))
        out["marketCap"] = to_float(pm.get("marketCap"))
        out["week52High"]= to_float(sd.get("fiftyTwoWeekHigh"))
        out["week52Low"] = to_float(sd.get("fiftyTwoWeekLow"))
        out["sector"]    = raw(ap.get("sector")) or ""
        out["industry"]  = raw(ap.get("industry")) or ""
    ch = get_chart(symbol)
    if ch:
        meta = ch.get("meta",{}); q = (ch.get("indicators",{}).get("quote") or [{}])[0]
        closes = [x for x in (q.get("close") or []) if x is not None]
        pr = to_float(meta.get("regularMarketPrice")) or (closes[-1] if closes else None)
        if out["price"] is None: out["price"] = pr
        if not out["name"] or out["name"] == symbol:
            out["name"] = meta.get("shortName") or meta.get("longName") or symbol
        if not out["week52High"]:
            out["week52High"] = to_float(meta.get("fiftyTwoWeekHigh")) or (max(closes) if closes else None)
        if not out["week52Low"]:
            out["week52Low"]  = to_float(meta.get("fiftyTwoWeekLow"))  or (min(closes) if closes else None)
        if closes and out["price"]:
            c0 = float(out["price"])
            def ret(d, cl=closes):
                if len(cl)>d and cl[-d]:
                    try: return c0/float(cl[-d])-1
                    except: return None
                return None
            out["m1"]=ret(21); out["m3"]=ret(63); out["m6"]=ret(126); out["m12"]=ret(252)
    out.update(hype_score(out["m1"],out["m3"],out["m6"],out["m12"],
                          out["price"],out["week52High"],out["week52Low"],out["marketCap"]))
    if out["price"] is not None: wc(f"hd10_{symbol}", out)
    return out

def holdings_batch(symbols):
    syms = [ysym(s) for s in symbols if (s or "").strip()]
    if not syms: return []
    out = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for f in as_completed({ex.submit(holding_data,s):s for s in syms}):
            try:
                r = f.result()
                if r: out.append(r)
            except: pass
    return out

def etf_scores(holdings, sw, cw):
    def cl(x,lo=0,hi=100): return max(lo,min(hi,x))
    scored = [h for h in holdings if (h.get("hypeScore") or 0)>0]
    tw = sum(h.get("weight") or 0 for h in scored) or 1
    wg = lambda k: round(sum(h.get(k,0)*(h.get("weight") or 0) for h in scored)/tw) if scored else 0
    if not sw:
        for h in holdings:
            s = h.get("sector") or "Other"; sw[s] = sw.get(s,0)+(h.get("weight") or 0)
    tot = sum(sw.values()) or 1; hhi = sum((v/tot)**2 for v in sw.values())
    n   = len(holdings)
    t5  = sum(h.get("weight") or 0 for h in
              sorted(holdings,key=lambda x:x.get("weight") or 0,reverse=True)[:5])
    return {
        "etfHype":wg("hypeScore"),"etf_momentum":wg("hype_momentum"),
        "etf_trend":wg("hype_trend"),"etf_size":wg("hype_size"),
        "diversification":cl(round(cl((1-hhi)*40,0,40)+
                                   (cl((math.log10(max(n,1))/math.log10(500))*25,0,25) if n else 0)+
                                   cl((1-t5/0.8)*20,0,20)+cl(min(len(sw)-1,7)/7*15,0,15))),
        "div_hhi":round(cl((1-hhi)*40,0,40)),
        "div_holdings":round(cl((math.log10(max(n,1))/math.log10(500))*25,0,25) if n else 0),
        "div_top5":round(cl((1-t5/0.8)*20,0,20)),
        "div_sectors":round(cl(min(len(sw)-1,7)/7*15,0,15)),
        "hhi":round(hhi,3),"top5Weight":round(t5*100,1),
        "numHoldings":n,"numSectors":len(sw),"numCountries":len(cw) if cw else 0,
    }

SECTOR_COLORS = {
    "Technology":"#38bdf8","Communication Services":"#a78bfa",
    "Consumer Cyclical":"#f59e0b","Consumer Defensive":"#22c55e",
    "Healthcare":"#34d399","Financial Services":"#ff9d00",
    "Industrials":"#6366f1","Energy":"#ef4444","Basic Materials":"#f97316",
    "Real Estate":"#e879f9","Utilities":"#4ade80","Other":"#94a3b8",
}
COUNTRY_COLORS = {
    "United States":"#38bdf8","USA":"#38bdf8","United Kingdom":"#a78bfa",
    "Germany":"#f59e0b","Japan":"#22c55e","France":"#ff9d00","China":"#ef4444",
    "Switzerland":"#34d399","Canada":"#6366f1","South Korea":"#e879f9",
    "Netherlands":"#f97316","Sweden":"#4ade80","Australia":"#94a3b8",
    "Taiwan":"#38bdf8","India":"#f59e0b","Denmark":"#a78bfa",
    "Norway":"#22c55e","Ireland":"#34d399","Spain":"#f97316",
    "Italy":"#6366f1","Finland":"#4ade80","Brazil":"#ef4444",
    "Singapore":"#34d399","Hong Kong":"#a78bfa","Belgium":"#6366f1",
}


# ══════════════════════════════════════════════════════════════════════════════
#  Full Holdings Fetcher
#
#  Henter ALLE holdings fra provider-specifikke CSV/XLSX filer.
#  Testet og virker:
#    iShares CSV: /uk/individual/en/literature/spreadsheet/{id}-fund-download-spreadsheet.csv
#    SPDR XLSX:   /us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker}.xlsx
#
#  iShares localSiteId lookup (fra produktsiden URL):
#    Mønster: ishares.com/uk/individual/en/products/{productId}/{localSiteId}/
# ══════════════════════════════════════════════════════════════════════════════

# iShares: ISIN → localSiteId (til CSV download URL)
ISHARES_CSV_IDS = {
    # Europæiske UCITS (uk/individual/en)
    "IE000C6ITGC8": "1478372549651",   # QOMP - Quantum Computing
    "IE00B4L5Y983": "1478372549651",   # MSCI World
    "IE00B52MJY50": "1478372549651",   # Core S&P 500
    "IE00B3XXRP09": "1478372549651",   # Core MSCI Emerging Markets
    "IE00BK5BQT80": "1478372549651",   # Core MSCI World
    "IE00BKM4GZ66": "1478372549651",   # Core MSCI EM IMI
    "IE00B5BMR087": "1467271812596",   # Core S&P 500 US
    # US iShares (us/products)
    "US4642874329": "1467271812596",   # IVV Core S&P 500
}

# SPDR: ticker → XLSX URL
SPDR_XLSX = {
    "SPY":  "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx",
    "IVV":  "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-ivv.xlsx",
    "GLD":  "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-gld.xlsx",
    "XLK":  "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlk.xlsx",
    "XLF":  "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlf.xlsx",
    "XLE":  "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xle.xlsx",
    "SPYD": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spyd.xlsx",
}

def parse_ishares_csv(body):
    """
    Parse iShares holdings CSV.
    Format:
      Linje 1-5: metadata
      Linje 6+:  Ticker,Name,Asset Class,Weight (%),Price,...,Country of Risk,...
    """
    holdings = []
    countries = {}
    lines = body.replace("\r\n","\n").replace("\r","\n").split("\n")

    # Find header-linje (indeholder "Ticker" eller "Name")
    header_idx = -1
    for i, line in enumerate(lines):
        if re.search(r"ticker|name|weight|asset.class", line, re.I) and "," in line:
            header_idx = i; break
    if header_idx < 0: return holdings, countries

    import csv as csv_mod
    reader = csv_mod.reader(lines[header_idx:])
    headers = [h.strip().lower() for h in next(reader, [])]

    def ci(pats):
        for pat in pats:
            for j, h in enumerate(headers):
                if pat.lower() in h: return j
        return -1

    ti  = ci(["ticker","symbol"])
    ni  = ci(["name"])
    wi  = ci(["weight (%)","weight"])
    coi = ci(["country of risk","country"])
    aci = ci(["asset class"])

    for row in reader:
        if not row or len(row) < 3: continue
        ac = row[aci].strip() if aci >= 0 and aci < len(row) else ""
        if ac.lower() in ("cash","futures","options","money market","other",""): continue
        ticker  = row[ti].strip()  if ti  >= 0 and ti  < len(row) else ""
        name    = row[ni].strip()  if ni  >= 0 and ni  < len(row) else ticker
        country = row[coi].strip() if coi >= 0 and coi < len(row) else ""
        try:
            wt_raw = row[wi].strip().replace(",",".") if wi >= 0 and wi < len(row) else "0"
            wt = float(wt_raw)
            if wt > 1: wt /= 100
        except: continue
        if wt <= 0: continue
        if ticker or name:
            holdings.append({"ticker": ticker.upper(), "name": name, "weight": wt})
        if country:
            countries[country] = countries.get(country, 0) + wt

    # Sorter efter vægt
    holdings.sort(key=lambda x: x["weight"], reverse=True)
    return holdings, countries

def parse_spdr_xlsx(body_bytes):
    """
    Parse SPDR XLSX holdings fil.
    Kræver openpyxl. Returnerer (holdings, countries).
    """
    holdings = []; countries = {}
    try:
        import openpyxl, io
        wb = openpyxl.load_workbook(io.BytesIO(body_bytes), read_only=True, data_only=True)
        ws = wb.active

        rows = list(ws.iter_rows(values_only=True))
        # Find header row
        header_idx = -1; headers = []
        for i, row in enumerate(rows):
            row_s = [str(c or "").lower() for c in row]
            if any("weight" in c or "ticker" in c or "name" in c for c in row_s):
                header_idx = i; headers = row_s; break
        if header_idx < 0: return holdings, countries

        def ci(pats):
            for pat in pats:
                for j, h in enumerate(headers):
                    if pat in h: return j
            return -1

        ti  = ci(["ticker","symbol"])
        ni  = ci(["name","security"])
        wi  = ci(["weight"])
        coi = ci(["country"])

        for row in rows[header_idx+1:]:
            if not row or not any(row): continue
            ticker  = str(row[ti] or "").strip() if ti  >= 0 and ti  < len(row) else ""
            name    = str(row[ni] or "").strip() if ni  >= 0 and ni  < len(row) else ""
            country = str(row[coi]or "").strip() if coi >= 0 and coi < len(row) else ""
            try:
                wt_raw = row[wi] if wi >= 0 and wi < len(row) else 0
                wt = float(str(wt_raw).replace("%","").replace(",",".").strip())
                if wt > 1: wt /= 100
            except: continue
            if wt <= 0 or wt > 1: continue
            if ticker or name:
                holdings.append({"ticker": ticker.upper(), "name": name, "weight": wt})
            if country:
                countries[country] = countries.get(country, 0) + wt
        holdings.sort(key=lambda x: x["weight"], reverse=True)
    except ImportError:
        pass  # openpyxl not installed - skip SPDR
    except Exception:
        pass
    return holdings, countries

def fetch_full_holdings(ticker, isin="", provider=""):
    """
    Hent alle holdings for en ETF fra provider-specifikke kilder.
    Returnerer (holdings_list, countries_dict) eller ([], {})
    """
    base = tvsym(ticker)  # strip .DE etc
    cached = rc(f"full_hold_{isin or base}", 3600 * 12)
    if cached: return cached.get("holdings",[]), cached.get("countries",{})

    holdings = []; countries = {}

    # 1. iShares CSV (virker! 372KB med alle holdings + Country of Risk)
    csv_id = ISHARES_CSV_IDS.get(isin)
    if not csv_id and provider.lower() in ("ishares","blackrock"):
        # Søg efter localSiteId ved at hente produktsiden
        csv_id = _find_ishares_csv_id(isin)
    if csv_id:
        try:
            url = (f"https://www.ishares.com/uk/individual/en/literature/spreadsheet/"
                   f"{csv_id}-fund-download-spreadsheet.csv")
            st, body = hget(url, timeout=30,
                           accept="text/csv,*/*",
                           extra={"Referer":"https://www.ishares.com/uk/"})
            if st == 200 and len(body) > 1000 and "," in body[:500]:
                holdings, countries = parse_ishares_csv(body)
        except: pass

    # 2. SPDR XLSX (virker! binær Excel fil)
    if not holdings and base in SPDR_XLSX:
        try:
            url = SPDR_XLSX[base]
            req = urllib.request.Request(url, headers={
                "User-Agent": UA, "Accept": "*/*",
                "Referer": "https://www.ssga.com/"})
            with _opener.open(req, timeout=30) as r:
                body_bytes = r.read()
            if r.status == 200 and len(body_bytes) > 10000:
                holdings, countries = parse_spdr_xlsx(body_bytes)
        except: pass

    # 3. Invesco QQQ
    if not holdings and base == "QQQ":
        try:
            url = "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0/?audienceType=Investor&action=download&ticker=QQQ"
            st, body = hget(url, timeout=20, accept="text/csv,*/*",
                           extra={"Referer":"https://www.invesco.com/"})
            if st == 200 and "," in body[:200]:
                holdings, _ = parse_ishares_csv(body)  # similar CSV format
        except: pass

    if holdings:
        wc(f"full_hold_{isin or base}",
           {"holdings": holdings, "countries": countries})

    return holdings, countries

def _find_ishares_csv_id(isin):
    """Find iShares localSiteId ved at scrape produktsiden."""
    if not isin: return None
    cached = rc(f"ish_lid_{isin}", 3600*24*7)
    if cached: return cached
    try:
        # Søg via screener
        url = (f"https://www.ishares.com/uk/individual/en/product-screener/"
               f"product-screener-v3.1.jsn?dcrPath=/templatedata/config/product-screener-v3"
               f"&siteEntryPassthrough=true"
               f"&selectedFilters=productView~all|dataType~fund|isin~{isin}")
        st, body = hget(url, timeout=15, accept="application/json,*/*",
                       extra={"Referer":"https://www.ishares.com/uk/"})
        if st == 200:
            d = json.loads(body)
            funds = (d.get("data",{}).get("tableData",{}).get("funds") or
                     d.get("tableData",{}).get("funds") or [])
            for fund in funds:
                lid = str(fund.get("localSiteId") or "")
                if lid and lid.isdigit() and len(lid) >= 10:
                    wc(f"ish_lid_{isin}", lid); return lid
    except: pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  Lande-fallback data
#  Bruges når justETF er access-denied og iShares CSV ikke virker.
#  Kategorier → kendte lande-fordelinger (statisk, opdateres sjældent)
#  ISIN → US-listet ækvivalent → Yahoo har countryWeightings for disse
# ══════════════════════════════════════════════════════════════════════════════

# Statiske lande-fordelinger per ETF-kategori
CATEGORY_COUNTRIES = {
    "S&P 500":     {"United States": 1.0},
    "Nasdaq 100":  {"United States": 1.0},
    "USA":         {"United States": 1.0},
    "DAX": {"Germany": 1.0},
    "EURO STOXX 50": {
        "France":0.22,"Germany":0.20,"Netherlands":0.12,"Spain":0.10,
        "Italy":0.08,"Finland":0.06,"Belgium":0.05,"Ireland":0.04,"Other":0.13
    },
    "STOXX Europe 600": {
        "United Kingdom":0.22,"France":0.15,"Switzerland":0.13,"Germany":0.13,
        "Sweden":0.07,"Netherlands":0.06,"Denmark":0.04,"Spain":0.04,"Other":0.16
    },
    "Europe": {
        "United Kingdom":0.22,"France":0.15,"Switzerland":0.13,"Germany":0.13,
        "Sweden":0.07,"Netherlands":0.06,"Denmark":0.04,"Spain":0.04,"Other":0.16
    },
    "Japan/Pacific": {"Japan":0.60,"Australia":0.20,"South Korea":0.12,"Other":0.08},
    "Gold/Commodities": {"United States": 1.0},
    "Crypto/Blockchain": {"United States": 0.60,"Other": 0.40},
}

# ISIN → US-listet ækvivalent (Yahoo har fuld countryWeightings for disse)
ISIN_US_EQUIV = {
    "IE00B5BMR087": "IVV",   "IE00B6YX5C33": "SPY",
    "IE000XZSV718": "SPY",   "IE00B44Z5B48": "SPY",
    "IE00B3XXRP09": "VOO",   "IE00BFMXXD54": "VOO",
    "IE00BK5BQT80": "VTI",   "IE00B3RBWM25": "VT",
    "IE00B53SZB19": "QQQ",   "IE0032077012": "QQQ",
    "IE00B3WJKG14": "XLK",   "DE0005933956": "FEZ",
    "DE0005933931": "EWG",   "DE0005933923": "EWG",
    "IE00BKM4GZ66": "IEMG",  "IE00B0M63177": "EEM",
    "IE00B4L5Y983": "URTH",  "IE00B3YCGJ38": "IVV",
    "LU0908500753": "EZU",   "LU1135865084": "IVV",
    "IE00B6R52259": "ACWI",  "IE00BK5BQT80": "VTI",
    "IE00BHZPJ890": "ESGU",  "IE00BFNM3G45": "ESGU",
}

def country_fallback_for_etf(isin, category, name):
    """
    Returner lande-fordeling baseret på:
    1. ISIN → fetch Yahoo for US-ækvivalent
    2. Kategori → statisk tabel
    3. Navn-matching
    """
    # Statisk kategori-match
    if category in CATEGORY_COUNTRIES:
        return CATEGORY_COUNTRIES[category], "Kategori"

    # Navn-baseret match
    name_lower = (name or "").lower()
    if any(x in name_lower for x in ["s&p 500","sp 500","s&p500"]):
        return CATEGORY_COUNTRIES["S&P 500"], "Kategori"
    if any(x in name_lower for x in ["nasdaq","nasdaq-100","nasdaq 100"]):
        return CATEGORY_COUNTRIES["Nasdaq 100"], "Kategori"
    if "dax" in name_lower and "stoxx" not in name_lower:
        return CATEGORY_COUNTRIES["DAX"], "Kategori"
    if any(x in name_lower for x in ["euro stoxx 50","eurostoxx 50"]):
        return CATEGORY_COUNTRIES["EURO STOXX 50"], "Kategori"
    if any(x in name_lower for x in ["stoxx europe 600","stoxx 600"]):
        return CATEGORY_COUNTRIES["STOXX Europe 600"], "Kategori"

    return None, None

def fetch_yahoo_countries(us_symbol):
    """Fetch countryWeightings fra Yahoo for en US-listet ETF."""
    cached = rc(f"yc_{us_symbol}", 3600*24)
    if cached: return cached
    res, _ = quote_summary(us_symbol, "topHoldings")
    if not res: return {}
    th = res.get("topHoldings",{}) or {}
    countries = {}
    for cw in (th.get("countryWeightings") or []):
        name = raw(cw.get("country")) or ""
        wt   = to_float(cw.get("holdingPercent"))
        if name and wt: countries[name] = wt
    if countries: wc(f"yc_{us_symbol}", countries)
    return countries

# ══════════════════════════════════════════════════════════════════════════════
#  ETF oversigt — samler alle datakilder
# ══════════════════════════════════════════════════════════════════════════════
def etf_overview(symbol):
    symbol = ysym(symbol)
    hit = rc(f"ov10_{symbol}", 180)
    if hit: return hit

    # 1. Slå op i lokal DB
    db = db_lookup(symbol)
    isin = (db.get("isin") or "").strip().upper() if db else ""

    # Byg Yahoo-symbol — tjek cache for tidligere fundet symbol
    cached_sym = rc(f"ysym_{symbol}", 3600*24*7)
    ysymbol = cached_sym or yahoo_sym(symbol, db)

    out = {
        "ok": False, "ticker": symbol, "name": db.get("name", symbol) if db else symbol,
        "price": None, "change": None, "changeAbs": None,
        "currency": "USD", "aum": None,
        "ter": db.get("ter") if db else None,
        "week52High": None, "week52Low": None,
        "dividendYield": None, "beta": None,
        "category": db.get("category","") if db else "",
        "provider": db.get("provider","") if db else "",
        "distribution": db.get("distribution","") if db else "",
        "benchmark": "",
        "equityPct": None, "bondPct": None, "cashPct": None, "otherPct": None,
        "pe": None, "pb": None,
        "sectorWeights": {}, "countryWeights": {},
        "holdingsRaw": [],
        "m1": None, "m3": None, "m6": None, "m12": None,
        "isin": isin, "yahooSymbol": ysymbol,
        "countrySource": "", "sectorSource": "",
        "countryUrl": "", "errors": {},
        "dbFound": db is not None,
    }

    # 2. Yahoo Finance — prøv alle fallback-symboler til vi får data
    res = None; err = None
    tried_syms = yahoo_sym_fallbacks(symbol, db)
    for attempt_sym in tried_syms[:4]:  # max 4 forsøg
        res, err = quote_summary(attempt_sym,
            "price,summaryDetail,defaultKeyStatistics,fundProfile,topHoldings")
        if res:
            ysymbol = attempt_sym  # opdater til det der virkede
            out["yahooSymbol"] = ysymbol
            # Gem det fundne symbol i DB-cache
            if attempt_sym != tried_syms[0]:
                wc(f"ysym_{symbol}", attempt_sym)
            break
    if err: out["errors"]["qs"] = err

    if res:
        pm = res.get("price",{}) or {}; sd = res.get("summaryDetail",{}) or {}
        st = res.get("defaultKeyStatistics",{}) or {}
        fp = res.get("fundProfile",{}) or {}; th = res.get("topHoldings",{}) or {}
        out["name"]         = raw(pm.get("longName")) or raw(pm.get("shortName")) or out["name"]
        out["price"]        = to_float(pm.get("regularMarketPrice"))
        out["changeAbs"]    = to_float(pm.get("regularMarketChange"))
        out["change"]       = to_float(pm.get("regularMarketChangePercent"))
        out["currency"]     = raw(pm.get("currency")) or "USD"
        out["aum"]          = to_float(pm.get("totalAssets")) or to_float(sd.get("totalAssets"))
        out["week52High"]   = to_float(sd.get("fiftyTwoWeekHigh"))
        out["week52Low"]    = to_float(sd.get("fiftyTwoWeekLow"))
        out["dividendYield"]= to_float(sd.get("yield")) or to_float(sd.get("dividendYield"))
        out["beta"]         = to_float(st.get("beta3Year")) or to_float(sd.get("beta"))
        if not out["category"]: out["category"] = fp.get("categoryName","")
        if not out["benchmark"]: out["benchmark"] = (th.get("benchmarkName") or "").strip()
        fees = fp.get("feesExpensesInvestment") or {}
        if not out["ter"]: out["ter"] = to_float(fees.get("annualReportExpenseRatio"))
        for attr, key in [("equityPct","stockPosition"),("bondPct","bondPosition"),
                          ("cashPct","cashPosition"),("otherPct","otherPosition")]:
            out[attr] = to_float(th.get(key))
        eq = th.get("equityHoldings") or {}
        out["pe"] = to_float(eq.get("priceToEarnings"))
        out["pb"] = to_float(eq.get("priceToBook"))
        for sw in (th.get("sectorWeightings") or []):
            for k, v in sw.items():
                if k != "maxAge":
                    fv = to_float(v)
                    if fv is not None: out["sectorWeights"][k] = fv
        if out["sectorWeights"]: out["sectorSource"] = "Yahoo"
        for cw in (th.get("countryWeightings") or []):
            n = raw(cw.get("country")) or cw.get("country") or ""
            wt = to_float(cw.get("holdingPercent"))
            if n and wt is not None: out["countryWeights"][n] = wt
        if out["countryWeights"]: out["countrySource"] = "Yahoo"
        yahoo_holdings = [
            {"ticker": str(raw(h.get("symbol")) or "").upper(),
             "name":   str(raw(h.get("holdingName")) or raw(h.get("symbol")) or ""),
             "weight": to_float(h.get("holdingPercent")) or 0}
            for h in (th.get("holdings") or [])
        ]
        out["holdingsRaw"] = yahoo_holdings
        out["ok"] = True

    # v7/quote fallback for pris
    if out["price"] is None:
        q = v7quote(ysymbol)
        if q:
            out["price"]     = to_float(q.get("regularMarketPrice"))
            out["changeAbs"] = to_float(q.get("regularMarketChange"))
            out["change"]    = to_float(q.get("regularMarketChangePercent"))
            out["currency"]  = q.get("currency") or "USD"
            if not out["name"] or out["name"] == symbol:
                out["name"]  = q.get("longName") or q.get("shortName") or out["name"]
            out["aum"]       = to_float(q.get("totalAssets")) or out["aum"]
            if out["price"]: out["ok"] = True

    # chart for returns
    ch = get_chart(ysymbol)
    if ch:
        meta = ch.get("meta",{}); q = (ch.get("indicators",{}).get("quote") or [{}])[0]
        closes = [x for x in (q.get("close") or []) if x is not None]
        pr = to_float(meta.get("regularMarketPrice")) or (closes[-1] if closes else None)
        if out["price"] is None and pr: out["price"] = pr; out["ok"] = True
        if not out["currency"]: out["currency"] = meta.get("currency") or "USD"
        if not out["name"] or out["name"] == symbol:
            out["name"] = meta.get("shortName") or meta.get("longName") or out["name"]
        if not out["week52High"]:
            out["week52High"] = to_float(meta.get("fiftyTwoWeekHigh")) or (max(closes) if closes else None)
        if not out["week52Low"]:
            out["week52Low"]  = to_float(meta.get("fiftyTwoWeekLow"))  or (min(closes) if closes else None)
        if closes and out["price"]:
            c0 = float(out["price"])
            def ret(d, cl=closes):
                if len(cl)>d and cl[-d]:
                    try: return c0/float(cl[-d])-1
                    except: return None
                return None
            out["m1"]=ret(21); out["m3"]=ret(63); out["m6"]=ret(126); out["m12"]=ret(252)

    # 3. Fuld holdings fra provider CSV/XLSX
    # Kør parallelt med andre kald — henter alle holdings ikke kun top 10
    provider = (db.get("provider","") if db else out.get("provider","")).strip()
    full_holdings, csv_countries = fetch_full_holdings(symbol, isin, provider)
    if full_holdings and len(full_holdings) > len(out.get("holdingsRaw",[])):
        out["holdingsRaw"]   = full_holdings
        out["holdingsSource"]= "Provider CSV/XLSX"
        # Brug landedata fra CSV hvis vi ikke har det fra Yahoo
        if csv_countries and not out["countryWeights"]:
            out["countryWeights"] = {k: round(v,4) for k,v in csv_countries.items()}
            out["countrySource"]  = "Provider CSV"

    # 4. justETF Wicket — lande + sektorer (VIRKER)
    if not isin and not out["countryWeights"]:
        isin = jse_find_isin(symbol) or ""
    if isin:
        out["isin"] = isin
        jw = jse_data(isin)
        if jw:
            if not out["countryWeights"] and jw.get("countries"):
                out["countryWeights"] = jw["countries"]
                out["countrySource"]  = "justETF"
                out["countryUrl"]     = f"{JSE_BASE}/en/etf-profile.html?isin={isin}#countries"
            if not out["sectorWeights"] and jw.get("sectors"):
                out["sectorWeights"] = jw["sectors"]
                out["sectorSource"]  = "justETF"

    # 4. Lande-fallback: US ækvivalent (Yahoo) eller kategori-baseret
    if not out["countryWeights"]:
        # Prøv US-listet ækvivalent på Yahoo
        us_sym = ISIN_US_EQUIV.get(isin)
        if us_sym:
            us_countries = fetch_yahoo_countries(us_sym)
            if us_countries:
                out["countryWeights"] = us_countries
                out["countrySource"]  = f"Yahoo ({us_sym})"
        # Kategori-baseret fallback
        if not out["countryWeights"]:
            cat_countries, src_label = country_fallback_for_etf(
                isin, out.get("category",""), out.get("name",""))
            if cat_countries:
                out["countryWeights"] = cat_countries
                out["countrySource"]  = src_label or "Kategori"

    # 5. Alpha Vantage — sektorer + holdings fallback
    if not out["sectorWeights"] or not out["holdingsRaw"]:
        try:
            av = av_etf(symbol)
            if av:
                if not out["sectorWeights"] and av.get("sectors"):
                    out["sectorWeights"] = av["sectors"]
                    out["sectorSource"]  = "Alpha Vantage"
                if not out["holdingsRaw"] and av.get("holdings"):
                    out["holdingsRaw"] = av["holdings"]
                if out["equityPct"] is None:
                    aa = av.get("asset_allocation") or {}
                    out["equityPct"] = aa.get("Stocks") or aa.get("Stock")
                    out["bondPct"]   = aa.get("Bonds") or aa.get("Bond")
                    out["cashPct"]   = aa.get("Cash")
                if not out["ter"] and av.get("ter"):
                    try: out["ter"] = float(str(av["ter"]).replace("%","").strip()) / 100
                    except: pass
        except Exception as e: out["errors"]["av"] = str(e)

    # 5. DWS/Xtrackers — lande fallback
    if not out["countryWeights"] and isin and isin in XTRACKERS_LOOKUP:
        try:
            dc = dws_countries(isin)
            if dc:
                out["countryWeights"] = dc
                out["countrySource"]  = "DWS/Xtrackers"
                out["countryUrl"]     = XTRACKERS_LOOKUP[isin].get("url","")
            if not out["sectorWeights"]:
                ds = dws_sectors(isin)
                if ds:
                    out["sectorWeights"] = ds
                    out["sectorSource"]  = "DWS/Xtrackers"
        except Exception as e: out["errors"]["dws"] = str(e)

    # Normaliser TER fra string
    if isinstance(out.get("ter"), str):
        try: out["ter"] = float(out["ter"].replace("%","").strip()) / 100
        except: out["ter"] = None

    out["tvSymbol"] = tvsym(symbol)
    if out["ok"] or out["dbFound"]:
        wc(f"ov10_{symbol}", out)
    return out

# ══════════════════════════════════════════════════════════════════════════════
#  HTTP Handler
# ══════════════════════════════════════════════════════════════════════════════
class H(SimpleHTTPRequestHandler):
    def log_message(self, *a): pass

    def jsend(self, d, code=200):
        b = json.dumps(d, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length", len(b))
        self.send_header("Access-Control-Allow-Origin","*")
        self.end_headers(); self.wfile.write(b)

    def do_GET(self):
        p  = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(p.query)

        # /api/etf?ticker=QOMP.DE
        if p.path == "/api/etf":
            ticker = (qs.get("ticker",[""])[0] or "").strip().upper()
            if not ticker: return self.jsend({"ok":False,"error":"Missing ticker"},400)
            try:
                ov = etf_overview(ticker)
                if not ov.get("ok") and not ov.get("dbFound"):
                    msg = f"Kunne ikke hente '{ticker}'."
                    errs = ov.get("errors",{})
                    first = next(iter(errs.values()),"") if errs else ""
                    if "no result" in str(first).lower():
                        msg += f" Prøv '{tvsym(ticker)}' uden børssuffix."
                    elif first: msg += " " + str(first)[:150]
                    return self.jsend({"ok":False,"error":msg,"debug_errors":errs},502)

                raw_h    = ov.get("holdingsRaw") or []
                syms     = [h["ticker"] for h in raw_h if h.get("ticker")]
                stock_map = {s["ticker"]:s for s in holdings_batch(syms[:50]) if s}

                enriched = []
                for h in raw_h:
                    t = h.get("ticker") or ""; sd = stock_map.get(t,{})
                    wt = float(h.get("weight") or 0)
                    if wt > 1: wt /= 100
                    enriched.append({
                        "ticker":t,"name":h.get("name") or sd.get("name") or t,
                        "weight":wt,"sector":sd.get("sector",""),
                        "industry":sd.get("industry",""),
                        "price":sd.get("price"),"change":sd.get("change"),
                        "marketCap":sd.get("marketCap"),
                        "week52High":sd.get("week52High"),"week52Low":sd.get("week52Low"),
                        "m1":sd.get("m1"),"m3":sd.get("m3"),
                        "m6":sd.get("m6"),"m12":sd.get("m12"),
                        "hypeScore":sd.get("hypeScore",0),
                        "hype_momentum":sd.get("hype_momentum",0),
                        "hype_trend":sd.get("hype_trend",0),
                        "hype_size":sd.get("hype_size",0),
                    })
                enriched.sort(key=lambda x: x.get("weight") or 0, reverse=True)

                sw  = ov.get("sectorWeights") or {}
                cw  = ov.get("countryWeights") or {}
                sc2 = etf_scores(enriched, sw, cw)

                return self.jsend({
                    "ok":True,
                    "overview":{**ov,"etfScore":sc2},
                    "holdings":enriched,"etfScore":sc2,
                    "sectors":sw,"countries":cw,
                    "countrySource":ov.get("countrySource",""),
                    "sectorSource":ov.get("sectorSource",""),
                    "countryUrl":ov.get("countryUrl",""),
                    "hasYahooSectors":ov.get("sectorSource")=="Yahoo",
                    "sectorColors":SECTOR_COLORS,"countryColors":COUNTRY_COLORS,
                })
            except Exception as e:
                import traceback
                return self.jsend({"ok":False,"error":str(e),
                                   "trace":traceback.format_exc()[-800:]},502)

        # /api/search?q=
        if p.path == "/api/search":
            q = (qs.get("q",[""])[0] or "").strip()
            if not q: return self.jsend({"ok":True,"results":[]})
            results = []
            # Søg i lokal DB først
            q_up = q.upper()
            q_lo = q.lower()
            for r in list(_db_by_isin.values()):
                ticker = (r.get("ticker") or "").upper()
                name   = (r.get("name") or "").lower()
                isin_v = (r.get("isin") or "").upper()
                if (q_up in ticker or q_up in isin_v or q_lo in name):
                    results.append({
                        "ticker":  ticker + ".DE",
                        "name":    r.get("name",""),
                        "exchange":"XETR",
                        "type":    "ETF",
                        "isin":    isin_v,
                        "provider":r.get("provider",""),
                        "category":r.get("category",""),
                        "ter":     r.get("ter",""),
                    })
            if len(results) < 5:
                # Yahoo søgning
                try:
                    c  = get_crumb()
                    pm = {"q":q,"quotesCount":10,"newsCount":0,
                          "quotesQueryId":"tss_match_phrase_query"}
                    if c: pm["crumb"] = c
                    data = gjson("https://query1.finance.yahoo.com/v1/finance/search?"
                                 + urllib.parse.urlencode(pm), timeout=10)
                    for r in (data.get("quotes") or []):
                        if (r.get("quoteType") or "").upper() in ("ETF","MUTUALFUND","INDEX","FUND"):
                            ticker = r.get("symbol","")
                            if not any(x["ticker"]==ticker for x in results):
                                # Check if this ticker is in our DB
                                db_match = db_lookup(ticker.split(".")[0])
                                results.append({
                                    "ticker":ticker,
                                    "name":r.get("longname") or r.get("shortname") or ticker,
                                    "exchange":r.get("exchDisp") or r.get("exchange") or "",
                                    "type":"ETF",
                                    "isin":  db_match.get("isin","") if db_match else "",
                                    "provider":db_match.get("provider","") if db_match else "",
                                    "category":db_match.get("category","") if db_match else "",
                                    "ter":   db_match.get("ter","") if db_match else "",
                                })
                except: pass
            return self.jsend({"ok":True,"results":results[:12]})

        # /api/db — list all ETFs in database
        if p.path == "/api/db":
            q = (qs.get("q",[""])[0] or "").strip().lower()
            provider = (qs.get("provider",[""])[0] or "").strip()
            category = (qs.get("category",[""])[0] or "").strip()
            recs = list(_db_by_isin.values())
            if q:
                recs = [r for r in recs if q in (r.get("name","")).lower()
                        or q in (r.get("ticker","")).lower()
                        or q in (r.get("isin","")).lower()]
            if provider:
                recs = [r for r in recs if r.get("provider","").lower() == provider.lower()]
            if category:
                recs = [r for r in recs if r.get("category","").lower() == category.lower()]
            return self.jsend({
                "ok":True,"total":len(recs),
                "results":[{"ticker":r.get("ticker",""),
                             "name":r.get("name",""),
                             "isin":r.get("isin",""),
                             "provider":r.get("provider",""),
                             "category":r.get("category",""),
                             "ter":r.get("ter",""),
                             "distribution":r.get("distribution","")}
                           for r in recs[:200]]
            })

        # /api/debug?ticker=
        if p.path == "/api/debug":
            t = (qs.get("ticker",[""])[0] or "").strip().upper()
            if not t: return self.jsend({"error":"no ticker"})
            db = db_lookup(t)
            c  = get_crumb()
            return self.jsend({
                "ticker": t,
                "tvSymbol": tvsym(t),
                "yahooSymbol": yahoo_sym(t, db),
                "crumb_ok": bool(c),
                "db_found": db is not None,
                "db_record": db,
                "db_total": len(_db_by_isin),
            })

        return super().do_GET()

def _clear_broken_cache():
    """Ryd gamle justETF cache-entries der kan indeholde forkerte pids."""
    cleared = 0
    for p in CACHE.glob("jse_sess_*.json"):
        p.unlink(); cleared += 1
    for p in CACHE.glob("jse_data_*.json"):
        p.unlink(); cleared += 1
    if cleared:
        print(f"  Cache: slettet {cleared} gamle justETF-session cache-filer")

def main():
    os.chdir(ROOT)
    _load_db()
    _clear_broken_cache()
    print("="*60)
    print("ETF PRO v10")
    print(f"  http://{HOST}:{PORT}/")
    print(f"  Lokal DB: {len(_db_by_isin)} ETFs")
    print(f"  Lande:    justETF Wicket AJAX (virker)")
    print(f"  Sektorer: Yahoo → justETF → Alpha Vantage")
    print("="*60)
    ThreadingHTTPServer((HOST, PORT), H).serve_forever()

if __name__ == "__main__": main()
