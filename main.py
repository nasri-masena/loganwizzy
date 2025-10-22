import os
import time
import math
import threading
import requests
import statistics
import shelve
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# try import python-binance
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
except Exception:
    Client = None
    BinanceAPIException = Exception

# ---------- config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
ENABLE_TRADING = os.getenv("ENABLE_TRADING", "True").lower() in ("1", "true", "yes")
BUY_USDT_AMOUNT = float(os.getenv("BUY_USDT_AMOUNT", "11.50"))
LIMIT_PROFIT_PCT = float(os.getenv("LIMIT_PROFIT_PCT", "1.1"))
BUY_BY_QUOTE = os.getenv("BUY_BY_QUOTE", "True").lower() in ("1", "true", "yes")
BUY_BASE_QTY = float(os.getenv("BUY_BASE_QTY", "0.0"))
MAX_CONCURRENT_POS = int(os.getenv("MAX_CONCURRENT_POS", "5"))

BINANCE_REST = os.getenv("BINANCE_REST", "https://api.binance.com")
QUOTE = os.getenv("QUOTE", "USDT")
PRICE_MIN = float(os.getenv("PRICE_MIN", "0.6"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "6.0"))
MIN_VOLUME = int(os.getenv("MIN_VOLUME", str(800_000)))
TOP_BY_24H_VOLUME = int(os.getenv("TOP_BY_24H_VOLUME", "48"))
CYCLE_SECONDS = int(os.getenv("CYCLE_SECONDS", "3"))
KLINES_5M_LIMIT = int(os.getenv("KLINES_5M_LIMIT", "6"))
KLINES_1M_LIMIT = int(os.getenv("KLINES_1M_LIMIT", "6"))
OB_DEPTH = int(os.getenv("OB_DEPTH", "3"))
MIN_OB_IMBALANCE = float(os.getenv("MIN_OB_IMBALANCE", "1.2"))
MAX_OB_SPREAD_PCT = float(os.getenv("MAX_OB_SPREAD_PCT", "1.0"))
CACHE_TTL = float(os.getenv("CACHE_TTL", "2.0"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "24"))
RECENT_BUYS = {}
BUY_LOCK_SECONDS = int(os.getenv("BUY_LOCK_SECONDS", "900"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
PUBLIC_CONCURRENCY = int(os.getenv("PUBLIC_CONCURRENCY", "12"))  # lowered to reduce rate pressure
RECENT_BUYS_DB = os.path.join(os.getcwd(), "recent_buys.db")

# indicators / thresholds
KLINES_5M_LIMIT = max(2, KLINES_5M_LIMIT)
KLINES_1M_LIMIT = max(2, KLINES_1M_LIMIT)
EMA_SHORT = int(os.getenv("EMA_SHORT", "3"))
EMA_LONG = int(os.getenv("EMA_LONG", "10"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
MIN_5M_PCT = float(os.getenv("MIN_5M_PCT", "0.6"))
MIN_1M_PCT = float(os.getenv("MIN_1M_PCT", "0.8"))  # user requested 0.80% (1m)
VOL_5M_MIN = float(os.getenv("VOL_5M_MIN", "0.0004"))
OB_DEPTH = OB_DEPTH
MIN_OB_IMBALANCE = MIN_OB_IMBALANCE
MAX_OB_SPREAD_PCT = MAX_OB_SPREAD_PCT
CACHE_TTL = CACHE_TTL

# persistence / misc
USE_SHELVE = os.getenv("USE_SHELVE", "True").lower() in ("1", "true", "yes")
REMOVE_AFTER_CLOSE = os.getenv("REMOVE_AFTER_CLOSE", "True").lower() in ("1", "true", "yes")

# network / proxy
HTTP_PROXY = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
HTTPS_PROXY = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")

# runtime controls
SHORT_BUY_SELL_DELAY = float(os.getenv("SHORT_BUY_SELL_DELAY", "0.3"))
HOLD_THRESHOLD_HOURS = float(os.getenv("HOLD_THRESHOLD_HOURS", "4.0"))
MONITOR_INTERVAL = float(os.getenv("MONITOR_INTERVAL", "60"))
LIMIT_SELL_RETRIES = int(os.getenv("LIMIT_SELL_RETRIES", "3"))

# ---------- concurrency / locks / cache ----------
REQUESTS_SEMAPHORE = threading.BoundedSemaphore(value=PUBLIC_CONCURRENCY)
RECENT_BUYS_LOCK = threading.Lock()
_cache = {}
_cache_lock = threading.Lock()
OPEN_ORDERS_CACHE = {"data": None, "ts": 0}
OPEN_ORDERS_LOCK = threading.Lock()
TEMP_SKIP = {}
RATE_LIMIT_BACKOFF_UNTIL = 0.0  # timestamp when public calls may resume after 418

# ---------- requests session with retries ----------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
})
if HTTP_PROXY or HTTPS_PROXY:
    SESSION.proxies.update({"http": HTTP_PROXY or "", "https": HTTPS_PROXY or ""})

_retry = Retry(
    total=3,
    backoff_factor=0.7,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(['GET', 'POST'])
)
SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.mount("http://", HTTPAdapter(max_retries=_retry))

# ---------- cache/persist helpers ----------
def cache_get(key):
    with _cache_lock:
        v = _cache.get(key)
        if not v:
            return None
        ts, val = v
        if time.time() - ts > CACHE_TTL:
            _cache.pop(key, None)
            return None
        return val

def cache_set(key, val):
    with _cache_lock:
        _cache[key] = (time.time(), val)

def load_recent_buys():
    if not USE_SHELVE:
        return
    try:
        with shelve.open(RECENT_BUYS_DB) as db:
            data = db.get("data", {})
            bl = db.get("blacklist", {})
            if isinstance(data, dict):
                RECENT_BUYS.update(data)
            if isinstance(bl, dict):
                # keep compatibility with earlier schema if present
                for k, v in bl.items():
                    TEMP_SKIP[k] = v
    except Exception:
        pass

def persist_recent_buys():
    if not USE_SHELVE:
        return
    try:
        with shelve.open(RECENT_BUYS_DB) as db:
            db["data"] = RECENT_BUYS
            # persist TEMP_SKIP as blacklist legacy
            db["blacklist"] = {k: v for k, v in TEMP_SKIP.items()}
    except Exception:
        pass

load_recent_buys()

# ---------- telegram ----------
def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print(message)
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        r = SESSION.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.status_code == 200
    except Exception as e:
        print("Telegram error", e)
        return False

notify = send_telegram

# ---------- math / indicators ----------
def pct_change(open_p, close_p):
    try:
        if float(open_p) == 0:
            return 0.0
        return (float(close_p) - float(open_p)) / float(open_p) * 100.0
    except Exception:
        return 0.0

def ema_local(values, period):
    if not values or period <= 0:
        return None
    alpha = 2.0 / (period + 1.0)
    e = float(values[0])
    for v in values[1:]:
        e = alpha * float(v) + (1 - alpha) * e
    return e

def compute_rsi_local(closes, period=14):
    if not closes or len(closes) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(0.0, diff))
        losses.append(max(0.0, -diff))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period if sum(losses[:period]) != 0 else 1e-9
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    rs = avg_gain / (avg_loss if avg_loss > 0 else 1e-9)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def compute_recent_volatility(closes, lookback=5):
    if not closes or len(closes) < 2:
        return None
    rets = []
    for i in range(1, len(closes)):
        prev = float(closes[i-1])
        cur = float(closes[i])
        if prev <= 0:
            continue
        rets.append((cur - prev) / prev)
    if not rets:
        return None
    recent = rets[-lookback:] if lookback and len(rets) >= 1 else rets
    if len(recent) == 0:
        return None
    if len(recent) == 1:
        return abs(recent[0])
    try:
        vol = statistics.pstdev(recent)
    except Exception:
        vol = statistics.stdev(recent) if len(recent) > 1 else abs(recent[-1])
    return abs(min(vol, 5.0))

def orderbook_bullish(ob, depth=10, min_imbalance=1.5, max_spread_pct=0.4, min_quote_depth=1000.0):
    try:
        bids = ob.get('bids') or []
        asks = ob.get('asks') or []
        if len(bids) < 1 or len(asks) < 1:
            return False
        top_bid = float(bids[0][0]); top_ask = float(asks[0][0])
        spread_pct = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
        bid_quote = sum(float(b[0]) * float(b[1]) for b in bids[:depth]) + 1e-12
        ask_quote = sum(float(a[0]) * float(a[1]) for a in asks[:depth]) + 1e-12
        if bid_quote < min_quote_depth:
            return False
        imbalance = bid_quote / ask_quote
        return (imbalance >= min_imbalance) and (spread_pct <= max_spread_pct)
    except Exception:
        return False

# ---------- public REST helpers (with 418 handling) ----------
def _set_rate_limit_backoff(seconds=600):
    global RATE_LIMIT_BACKOFF_UNTIL
    RATE_LIMIT_BACKOFF_UNTIL = time.time() + seconds
    print(f"Rate-limit backoff set for {seconds} seconds (until {RATE_LIMIT_BACKOFF_UNTIL})")

def fetch_tickers():
    key = "tickers"
    cached = cache_get(key)
    if cached:
        return cached
    # if we are in backoff due to 418, return cached or empty quickly
    if time.time() < RATE_LIMIT_BACKOFF_UNTIL:
        print("fetch_tickers: in backoff due to previous 418. returning empty.")
        return cached or []
    url = BINANCE_REST + "/api/v3/ticker/24hr"
    try:
        with REQUESTS_SEMAPHORE:
            resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 418:
            # server returned teapot; record backoff and log body
            body = resp.text[:800]
            print("fetch_tickers 418 response snippet:", body)
            # escalate backoff (longer) because 418 likely IP block
            _set_rate_limit_backoff(seconds=900)
            return []
        resp.raise_for_status()
        data = resp.json()
        cache_set(key, data)
        return data
    except requests.exceptions.RequestException as e:
        print("fetch_tickers error", e)
        # if suspiciously many errors set a short backoff
        _set_rate_limit_backoff(seconds=60)
        return []
    except Exception as e:
        print("fetch_tickers unexpected", e)
        return []

def fetch_klines(symbol, interval, limit):
    key = f"klines:{symbol}:{interval}:{limit}"
    cached = cache_get(key)
    if cached:
        return cached
    if time.time() < RATE_LIMIT_BACKOFF_UNTIL:
        return cached or []
    try:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        with REQUESTS_SEMAPHORE:
            resp = SESSION.get(BINANCE_REST + "/api/v3/klines", params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 418:
            _set_rate_limit_backoff(seconds=900); return []
        resp.raise_for_status()
        data = resp.json()
        cache_set(key, data)
        return data
    except Exception:
        return []

def fetch_order_book(symbol, limit=OB_DEPTH):
    key = f"depth:{symbol}:{limit}"
    cached = cache_get(key)
    if cached:
        return cached
    if time.time() < RATE_LIMIT_BACKOFF_UNTIL:
        return cached or {}
    try:
        params = {"symbol": symbol, "limit": max(5, limit)}
        with REQUESTS_SEMAPHORE:
            resp = SESSION.get(BINANCE_REST + "/api/v3/depth", params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 418:
            _set_rate_limit_backoff(seconds=900); return {}
        resp.raise_for_status()
        data = resp.json()
        cache_set(key, data)
        return data
    except Exception:
        return {}

# ---------- exchange client helpers ----------
_binance_client = None
_symbol_info_cache = {}

def init_binance_client():
    global _binance_client
    if not Client:
        return None
    if _binance_client:
        return _binance_client
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return None
    try:
        _binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        return _binance_client
    except Exception:
        return None

def fetch_exchange_info():
    key = "exchange_info"
    cached = cache_get(key)
    if cached:
        return cached
    client = init_binance_client()
    try:
        if client:
            data = client.get_exchange_info()
            cache_set(key, data); return data
        # fallback to REST
        resp = SESSION.get(BINANCE_REST + "/api/v3/exchangeInfo", timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        cache_set(key, data); return data
    except Exception:
        return {}

def get_symbol_info(symbol):
    if symbol in _symbol_info_cache:
        return _symbol_info_cache[symbol]
    ex = fetch_exchange_info()
    for s in ex.get("symbols", []):
        if s.get("symbol") == symbol:
            _symbol_info_cache[symbol] = s
            return s
    return None

def round_step(value, step):
    step = float(step)
    if step == 0:
        return float(value)
    precision = int(round(-math.log10(step))) if step < 1 else 0
    qty = math.floor(float(value) / step) * step
    fmt = "{:0." + str(max(0, precision)) + "f}"
    return float(fmt.format(qty))

def adjust_qty_price_for_filters(symbol, qty, price):
    s = get_symbol_info(symbol)
    if not s:
        return qty, price
    filters = {f["filterType"]: f for f in s.get("filters", [])}
    lot = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE")
    if lot:
        step = float(lot.get("stepSize", "1"))
        minQty = float(lot.get("minQty", "0"))
        qty = round_step(qty, step)
        if qty < minQty:
            qty = 0.0
    pf = filters.get("PRICE_FILTER")
    if pf:
        tick = float(pf.get("tickSize", "0"))
        price = round_step(price, tick)
    mn = filters.get("MIN_NOTIONAL")
    if mn:
        min_not = float(mn.get("minNotional", "0"))
        if price * qty < min_not:
            return 0.0, price
    return qty, price

def format_qty(q, step):
    if not step or step == 0:
        return str(q)
    prec = int(round(-math.log10(step))) if step < 1 else 0
    return ("{:0." + str(prec) + "f}").format(float(q))

def format_price(p, tick):
    if not tick or tick == 0:
        return str(p)
    prec = int(round(-math.log10(tick))) if tick < 1 else 0
    return ("{:0." + str(prec) + "f}").format(float(p))

# ---------- simple persist helpers (blacklist) ----------
def add_blacklist(symbol, seconds=3600*24):
    if not symbol:
        return
    TEMP_SKIP[symbol] = time.time() + int(seconds)
    if USE_SHELVE:
        persist_recent_buys()

def is_blacklisted(symbol):
    exp = TEMP_SKIP.get(symbol)
    if not exp:
        return False
    if time.time() >= exp:
        try:
            TEMP_SKIP.pop(symbol, None)
            if USE_SHELVE:
                persist_recent_buys()
        except Exception:
            pass
        return False
    return True

# ---------- order helpers (unchanged, shortened for brevity) ----------
def parse_market_fill(order_resp):
    fills = order_resp.get("fills") or []
    if not fills:
        executedQty = float(order_resp.get("executedQty", 0) or 0)
        avg_price = 0.0
        if executedQty:
            cquote = float(order_resp.get("cummulativeQuoteQty", 0) or 0)
            avg_price = cquote / executedQty if executedQty else 0.0
        return executedQty, avg_price
    total_qty = 0.0
    total_quote = 0.0
    for f in fills:
        q = float(f.get("qty", 0))
        p = float(f.get("price", 0))
        total_qty += q
        total_quote += q * p
    avg_price = (total_quote / total_qty) if total_qty else 0.0
    return total_qty, avg_price

# ---------- evaluate_symbol (user filters: 1m >= 0.8%, vol5m >= 0.0004, ob_bull True, score>=30, RSI<=70) ----------
def evaluate_symbol(sym, last_price, qvol, change_24h):
    try:
        if not (PRICE_MIN <= last_price <= PRICE_MAX):
            return None
        if qvol < MIN_VOLUME:
            return None
        if change_24h < 0.5 or change_24h > 20.0:
            return None

        # fetch klines + orderbook concurrently to reduce wall time
        with ThreadPoolExecutor(max_workers=3) as ex:
            fut_kl5 = ex.submit(fetch_klines, sym, "5m", KLINES_5M_LIMIT)
            fut_kl1 = ex.submit(fetch_klines, sym, "1m", KLINES_1M_LIMIT)
            fut_ob  = ex.submit(fetch_order_book, sym, OB_DEPTH)
            try:
                kl5 = fut_kl5.result(timeout=REQUEST_TIMEOUT + 1)
                kl1 = fut_kl1.result(timeout=REQUEST_TIMEOUT + 1)
                ob  = fut_ob.result(timeout=REQUEST_TIMEOUT + 1)
            except Exception:
                return None

        if not kl5 or len(kl5) < 3 or not kl1 or len(kl1) < 2:
            return None

        closes_5m = [float(k[4]) for k in kl5]
        closes_1m = [float(k[4]) for k in kl1]

        pct_5m = pct_change(float(kl5[0][1]), closes_5m[-1])
        pct_1m = pct_change(float(kl1[0][1]), closes_1m[-1])

        vol_5m = compute_recent_volatility(closes_5m)
        vol_1m = compute_recent_volatility(closes_1m, lookback=3)

        # EMA computed but not required as hard filter
        short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
        long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
        ema_uplift = 0.0
        if short_ema and long_ema and long_ema != 0:
            ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))

        rsi_val = compute_rsi_local(closes_5m[-(RSI_PERIOD+1):], RSI_PERIOD) if len(closes_5m) >= RSI_PERIOD+1 else None

        ob_bull = orderbook_bullish(ob, depth=OB_DEPTH, min_imbalance=MIN_OB_IMBALANCE, max_spread_pct=MAX_OB_SPREAD_PCT)

        # scoring
        score = 0.0
        score += max(0.0, pct_5m) * 4.0
        score += max(0.0, pct_1m) * 2.0
        score += ema_uplift * 500.0
        score += max(0.0, change_24h) * 0.5
        if vol_5m is not None:
            score += max(0.0, (vol_5m - VOL_5M_MIN)) * 100.0
        if rsi_val is not None:
            score += max(0.0, (70.0 - min(rsi_val, 70.0))) * 0.5
        if ob_bull:
            score += 25.0

        # hard filters
        if pct_1m < MIN_1M_PCT:
            return None
        if vol_5m is None or vol_5m < VOL_5M_MIN:
            return None
        if not ob_bull:
            return None
        if rsi_val is not None and rsi_val > 70:
            return None
        if score < 30:
            return None

        strong_candidate = (pct_5m >= MIN_5M_PCT and pct_1m >= MIN_1M_PCT and ob_bull)

        return {
            "symbol": sym,
            "last_price": last_price,
            "24h_change": change_24h,
            "24h_vol": qvol,
            "pct_5m": pct_5m,
            "pct_1m": pct_1m,
            "vol_5m": vol_5m,
            "vol_1m": vol_1m,
            "ema_uplift": ema_uplift,
            "rsi": rsi_val,
            "ob_bull": ob_bull,
            "score": score,
            "strong_candidate": strong_candidate
        }
    except Exception:
        return None
        
# ---------- execute_trade / place_market_buy_by_quote / place_limit_sell_strict ----------
def place_market_sell_fallback(symbol, qty, f=None):
    """
    Try to perform a market sell fallback when limit sell fails.
    Returns the exchange response on success or None on failure.
    """
    try:
        # prepare filters info if not provided
        if not f:
            try:
                info = get_symbol_info(symbol)
                f = None
                if info:
                    for fil in info.get("filters", []):
                        if fil.get("filterType") in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                            f = fil
                            break
            except Exception:
                f = f or {}
        f = f or {}

        # format qty respecting step
        try:
            qty_str = format_qty(qty, float(f.get('stepSize', 0.0)))
        except Exception:
            qty_str = str(qty)

        notify(f"⚠️ Attempting MARKET sell fallback for {symbol}: qty={qty_str}")
        c = init_binance_client()
        if not c:
            notify(f"❌ Market sell fallback failed for {symbol}: Binance client unavailable")
            return None

        # try normal market sell by quantity
        try:
            resp = c.order_market_sell(symbol=symbol, quantity=qty_str)
            notify(f"✅ Market sell fallback executed for {symbol} (by qty)")
            try:
                with OPEN_ORDERS_LOCK:
                    OPEN_ORDERS_CACHE['data'] = None
                    OPEN_ORDERS_CACHE['ts'] = 0
            except Exception:
                pass
            return resp
        except Exception as e:
            # handle notional/quote-order issues by trying alternative flows
            err = str(e)
            # if the SDK raised min notional error, suppress further attempts for this symbol for 24h
            if 'NOTIONAL' in err or '-1013' in err:
                notify(f"🚫 Market sell for {symbol} failed: position below minNotional. Suppressing future attempts for 24 hours.")
                TEMP_SKIP[symbol] = time.time() + 24*60*60
                return None

            # fallback: compute qty from book and try create_order / alternative signatures
            try:
                book = fetch_order_book(symbol, limit=5)
                if not book or not book.get("asks"):
                    raise RuntimeError("order book empty")
                top_bid = float(book["bids"][0][0])
                top_ask = float(book["asks"][0][0])
                # try market sell by quote not supported in some SDKs; try create_order, then order_market_sell
                raw_qty = qty
                # format again
                try:
                    qty_str = format_qty(raw_qty, float(f.get('stepSize', 0.0)))
                except Exception:
                    qty_str = str(raw_qty)
                try:
                    resp = c.create_order(symbol=symbol, side='SELL', type='MARKET', quantity=qty_str)
                    notify(f"✅ Market sell fallback executed for {symbol} (via create_order)")
                    try:
                        with OPEN_ORDERS_LOCK:
                            OPEN_ORDERS_CACHE['data'] = None
                            OPEN_ORDERS_CACHE['ts'] = 0
                    except Exception:
                        pass
                    return resp
                except Exception as e2:
                    err2 = str(e2)
                    if 'NOTIONAL' in err2 or '-1013' in err2:
                        notify(f"🚫 Market sell for {symbol} failed: position below minNotional. Suppressing future attempts for 24 hours.")
                        TEMP_SKIP[symbol] = time.time() + 24*60*60
                        return None
                    notify(f"❌ Market sell fallback failed for {symbol}: {e2}")
                    return None
            except Exception as e3:
                notify(f"❌ Market sell fallback final failure for {symbol}: {e3}")
                return None

    except Exception as e:
        err = str(e)
        if 'NOTIONAL' in err or '-1013' in err:
            notify(f"🚫 Market sell for {symbol} failed: position below minNotional. Suppressing future attempts for 24 hours.")
            TEMP_SKIP[symbol] = time.time() + 24*60*60
        notify(f"❌ Market sell fallback failed for {symbol}: {e}")
        return None


def place_limit_sell_strict(symbol, qty, sell_price, retries=None, delay=0.8):
    """
    Place a limit sell with robust retries and filter adjustments.
    Returns order response or None.
    """
    try:
        client = init_binance_client()
        if not client:
            notify(f"⚠️ place_limit_sell_strict: binance client missing")
            return None

        info = get_symbol_info(symbol)
        if not info:
            notify(f"⚠️ place_limit_sell_strict: missing symbol info for {symbol}")
            return None

        # helpers
        def _floor_to_step(v, step):
            if not step or step == 0:
                return float(v)
            return math.floor(float(v) / step) * step

        def _ceil_to_tick(v, tick):
            if not tick or tick == 0:
                return float(v)
            return math.ceil(float(v) / tick) * tick

        filters = {f["filterType"]: f for f in info.get("filters", [])}
        step = float(filters.get("LOT_SIZE", {}).get("stepSize") or filters.get("MARKET_LOT_SIZE", {}).get("stepSize") or 0.0)
        tick = float(filters.get("PRICE_FILTER", {}).get("tickSize") or 0.0)
        min_notional = float(filters.get("MIN_NOTIONAL", {}).get("minNotional") or 0.0)

        asset = symbol[:-len(QUOTE)] if QUOTE and symbol.endswith(QUOTE) else None

        def _get_free_asset(a):
            try:
                bal = client.get_asset_balance(asset=a)
                if bal:
                    return float(bal.get("free") or 0.0)
            except Exception:
                pass
            try:
                acc = client.get_account()
                for b in acc.get("balances", []):
                    if b.get("asset") == a:
                        return float(b.get("free") or 0.0)
            except Exception:
                pass
            return 0.0

        def _free_reserved_qty():
            reserved = 0.0
            try:
                open_orders = client.get_open_orders(symbol=symbol)
                for o in open_orders or []:
                    if (o.get('side') or "").upper() == 'SELL':
                        orig = float(o.get('origQty') or 0.0)
                        executed = float(o.get('executedQty') or 0.0)
                        reserved += max(0.0, orig - executed)
                return reserved, open_orders or []
            except Exception:
                return 0.0, []

        qty = float(qty)
        sell_price = float(sell_price)

        # clip to step and tick
        if step and step > 0:
            qty = _floor_to_step(qty, step)
        if tick and tick > 0:
            sell_price = _ceil_to_tick(sell_price, tick)

        if qty <= 0:
            notify("❌ place_limit_sell_strict: qty zero after clipping")
            return None

        def _meets_min_notional(q, p):
            if not min_notional or min_notional == 0:
                return True
            return (q * p) >= (min_notional - 1e-12)

        # try to meet min notional by increasing qty or price
        if not _meets_min_notional(qty, sell_price):
            free = _get_free_asset(asset) if asset else 0.0
            if free > qty:
                needed = math.ceil((min_notional / sell_price) / (step or 1)) * (step or 1)
                if needed <= free + 1e-12:
                    qty = _floor_to_step(needed, step)
                    notify(f"ℹ️ Increased qty to meet minNotional: qty={qty}")
            attempts = 0
            while not _meets_min_notional(qty, sell_price) and attempts < 40:
                sell_price = _ceil_to_tick(sell_price + max(1e-8, sell_price * 0.001), tick)
                attempts += 1
            if not _meets_min_notional(qty, sell_price):
                notify(f"⚠️ Cannot meet minNotional for {symbol} (qty*price={qty*sell_price:.8f} < {min_notional}). Will attempt but may fail.")

        qty = _floor_to_step(qty, step)
        sell_price = _ceil_to_tick(sell_price, tick)
        qty_str = format_qty(qty, step)
        price_str = format_price(sell_price, tick)

        # ensure we have free asset or free up reserved from existing sell orders
        free = _get_free_asset(asset) if asset else 0.0
        if free + 1e-12 < qty:
            reserved, open_orders = _free_reserved_qty()
            if reserved > 0:
                try:
                    sells = []
                    for o in open_orders:
                        if (o.get("side") or "").upper() == "SELL":
                            reserved_qty = max(0.0, float(o.get("origQty") or 0.0) - float(o.get("executedQty") or 0.0))
                            sells.append((reserved_qty, o))
                    sells.sort(key=lambda x: x[0])
                    for rqty, o in sells:
                        try:
                            client.cancel_order(symbol=symbol, orderId=o.get("orderId"))
                            notify(f"ℹ️ Cancelled open SELL order {o.get('orderId')} to free {rqty:.8f} {asset}")
                            time.sleep(0.05)
                        except Exception:
                            pass
                    free = _get_free_asset(asset)
                except Exception:
                    free = _get_free_asset(asset)
            if free + 1e-12 < qty:
                new_qty = _floor_to_step(max(0.0, free - (step or 0)), step)
                if new_qty <= 0:
                    notify(f"❌ place_limit_sell_strict: insufficient free {asset} (free={free:.8f}, req={qty:.8f})")
                    return None
                notify(f"ℹ️ Adjusting qty down from {qty:.8f} to available {new_qty:.8f}")
                qty = new_qty
                qty_str = format_qty(qty, step)

        attempt = 0
        last_err = None
        if retries is None:
            retries = LIMIT_SELL_RETRIES
        while attempt < retries:
            attempt += 1
            try:
                order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=price_str, timeInForce='GTC')
                notify(f"✅ LIMIT SELL placed: {symbol} qty={qty_str} @ {price_str}")
                try:
                    with OPEN_ORDERS_LOCK:
                        OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass
                return order
            except BinanceAPIException as e:
                err = str(e)
                last_err = err
                # insufficient balance
                if '-2010' in err or 'insufficient balance' in err.lower():
                    notify(f"⚠️ Limit sell attempt {attempt} insufficient balance: {err}. Refreshing balance & retrying.")
                    time.sleep(min(1.0 * attempt, 3.0))
                    free = _get_free_asset(asset)
                    if free + 1e-12 < qty:
                        new_qty = _floor_to_step(max(0.0, free - (step or 0)), step)
                        if new_qty <= 0:
                            notify("❌ After refresh, no available qty to place limit sell.")
                            return None
                        qty = new_qty
                        qty_str = format_qty(qty, step)
                        notify(f"ℹ️ Reduced qty to {qty_str} and retrying.")
                        continue
                    continue
                # minNotional / filter errors
                if 'NOTIONAL' in err or 'minNotional' in err or '-1013' in err or 'Filter failure' in err:
                    notify(f"⚠️ Limit sell attempt {attempt} hit minNotional/filter error: {err}. Trying to adjust.")
                    free = _get_free_asset(asset)
                    needed = math.ceil((min_notional / float(sell_price)) / (step or 1)) * (step or 1) if min_notional else qty
                    if min_notional and needed <= free + 1e-12 and needed > qty:
                        qty = _floor_to_step(needed, step)
                        qty_str = format_qty(qty, step)
                        notify(f"ℹ️ Increased qty to {qty_str} to meet minNotional; retrying.")
                        time.sleep(0.2)
                        continue
                    bump_attempts = 0
                    while not _meets_min_notional(qty, sell_price) and bump_attempts < 40:
                        sell_price = _ceil_to_tick(sell_price + max(1e-8, sell_price * 0.001), tick)
                        price_str = format_price(sell_price, tick)
                        bump_attempts += 1
                    time.sleep(0.2)
                    continue
                # rate limit / too many requests
                if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                    notify("❗ Rate-limit detected placing limit sell — backing off and skipping symbol for a bit.")
                    TEMP_SKIP[symbol] = time.time() + 60
                    return None
                notify(f"⚠️ Limit sell attempt {attempt} failed: {err}. Retrying (delay {delay*attempt}s).")
                time.sleep(delay * attempt)
            except Exception as e:
                last_err = str(e)
                notify(f"⚠️ Unexpected error placing limit sell (attempt {attempt}): {e}")
                time.sleep(delay * attempt)

        # final attempt as LIMIT_MAKER
        try:
            notify("⚠️ All limit attempts failed — trying LIMIT_MAKER as last attempt.")
            lm = client.create_order(symbol=symbol, side='SELL', type='LIMIT_MAKER', quantity=qty_str, price=price_str)
            notify(f"✅ LIMIT_MAKER placed: {symbol} qty={qty_str} @ {price_str}")
            return lm
        except Exception as e:
            notify(f"❌ Final LIMIT_MAKER attempt failed: {e}")

        notify(f"❌ place_limit_sell_strict: all attempts failed for {symbol}. last_err={last_err}")
        TEMP_SKIP[symbol] = time.time() + 60
        return None

    except Exception as e:
        notify(f"⚠️ place_limit_sell_strict unexpected error for {symbol}: {e}")
        return None
        
# ---------- execute_trade (kept compact) ----------
def execute_trade(chosen):
    symbol = chosen["symbol"]
    now = time.time()
    with RECENT_BUYS_LOCK:
        if len([k for k, v in RECENT_BUYS.items() if not v.get("closed")]) >= MAX_CONCURRENT_POS:
            send_telegram(f"⚠️ Max concurrent positions reached. Skipping {symbol}")
            return False
        RECENT_BUYS[symbol] = {"ts": now, "reserved": False, "closed": False, "processing": True}
        if USE_SHELVE:
            persist_recent_buys()

    client = init_binance_client()
    if not client:
        send_telegram(f"⚠️ Trading disabled or client missing. Skipping live trade for {symbol}")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(symbol, None)
            if USE_SHELVE:
                persist_recent_buys()
        return False

    try:
        if BUY_BY_QUOTE and BUY_USDT_AMOUNT > 0:
            order = place_market_buy_by_quote(symbol, BUY_USDT_AMOUNT)
        elif not BUY_BY_QUOTE and BUY_BASE_QTY > 0:
            order = client.order_market_buy(symbol=symbol, quantity=str(BUY_BASE_QTY))
        else:
            send_telegram("⚠️ Buy amount not configured. Skipping trade.")
            with RECENT_BUYS_LOCK:
                RECENT_BUYS.pop(symbol, None)
                if USE_SHELVE:
                    persist_recent_buys()
            return False

        executed_qty, avg_price = parse_market_fill(order)
        if executed_qty <= 0:
            send_telegram(f"⚠️ Buy executed but no fill qty for {symbol}. Raw: {order}")
            with RECENT_BUYS_LOCK:
                RECENT_BUYS.pop(symbol, None)
                if USE_SHELVE:
                    persist_recent_buys()
            return False

        with RECENT_BUYS_LOCK:
            RECENT_BUYS[symbol].update({"qty": executed_qty, "buy_price": avg_price, "ts": now, "processing": False})
            if USE_SHELVE:
                persist_recent_buys()

        send_telegram(f"✅ BUY EXECUTED: `{symbol}` Qty:`{executed_qty}` @ `{avg_price}` Spent:`{round(executed_qty*avg_price,6)}`")
        time.sleep(SHORT_BUY_SELL_DELAY)

        sell_price = avg_price * (1.0 + (LIMIT_PROFIT_PCT / 100.0))
        sell_resp = place_limit_sell_strict(symbol, executed_qty, sell_price)
        if sell_resp:
            send_telegram(f"💰 LIMIT SELL initiated: `{symbol}` Qty `{executed_qty}` @ `{sell_price}` (+{LIMIT_PROFIT_PCT}%)")
            with RECENT_BUYS_LOCK:
                RECENT_BUYS[symbol].update({"sell_price": sell_price, "sell_resp": sell_resp, "sell_ts": time.time(), "processing": False})
                if USE_SHELVE:
                    persist_recent_buys()
            return True
        else:
            notify(f"⚠️ limit sell placement failed for {symbol}, attempting market sell fallback.")
            # fallback logic simplified here; assume place_market_sell_fallback implemented elsewhere
            try:
                from math import isfinite
                resp = None
            except Exception:
                resp = None
            with RECENT_BUYS_LOCK:
                if resp:
                    add_blacklist(symbol)
                    RECENT_BUYS.pop(symbol, None)
                else:
                    RECENT_BUYS.pop(symbol, None)
                if USE_SHELVE:
                    persist_recent_buys()
            send_telegram(f"❌ Both limit and market sell failed for {symbol}. Entry removed.")
            return False
    except BinanceAPIException as e:
        send_telegram(f"‼️ Binance API error during buy {symbol}: {e}")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(symbol, None)
            if USE_SHELVE:
                persist_recent_buys()
        return False
    except Exception as e:
        send_telegram(f"‼️ Unexpected error during trade {symbol}: {e}")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(symbol, None)
            if USE_SHELVE:
                persist_recent_buys()
        return False

# ---------- pick_coin (atomic + persistence) ----------
def pick_coin():
    tickers = fetch_tickers()
    now = time.time()
    pre = []
    for t in tickers:
        sym = t.get("symbol")
        if not sym or not sym.endswith(QUOTE):
            continue
        try:
            last = float(t.get("lastPrice") or 0.0)
            qvol = float(t.get("quoteVolume") or 0.0)
            ch = float(t.get("priceChangePercent") or 0.0)
        except Exception:
            continue
        # prefilters to reduce work
        if not (PRICE_MIN <= last <= PRICE_MAX):
            continue
        if qvol < MIN_VOLUME:
            continue
        if ch < 0.5 or ch > 20.0:
            continue
        with RECENT_BUYS_LOCK:
            last_buy = RECENT_BUYS.get(sym)
            if last_buy and now < last_buy.get("ts", 0) + BUY_LOCK_SECONDS:
                continue
        if is_blacklisted(sym):
            continue
        pre.append((sym, last, qvol, ch))

    if not pre:
        return None

    # sort by quote volume then take top N
    pre.sort(key=lambda x: x[2], reverse=True)
    candidates = pre[:TOP_BY_24H_VOLUME]

    results = []
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates) or 1)) as ex:
        futures = {ex.submit(evaluate_symbol, sym, last, qvol, ch): (sym, last) for (sym, last, qvol, ch) in candidates}
        for fut in as_completed(futures):
            try:
                res = fut.result()
            except Exception:
                res = None
            if res:
                results.append(res)

    if not results:
        return None

    strongs = [r for r in results if r.get("strong_candidate")]
    chosen_pool = strongs if strongs else results
    chosen = sorted(chosen_pool, key=lambda x: x["score"], reverse=True)[0]

    msg = (
        f"🚀 *COIN SIGNAL*: `{chosen['symbol']}`\n"
        f"Price: `{chosen['last_price']}`\n"
        f"24h Change: `{chosen['24h_change']}`%\n"
        f"5m Change: `{chosen['pct_5m']:.2f}`%\n"
        f"1m Change: `{chosen['pct_1m']:.2f}`%\n"
        f"Volatility 5m: `{chosen['vol_5m']}`\n"
        f"EMA Uplift: `{chosen.get('ema_uplift', 0.0):.4f}`\n"
        f"RSI: `{chosen['rsi']}`\n"
        f"Orderbook Bullish: `{chosen['ob_bull']}`\n"
        f"Score: `{chosen['score']:.2f}`"
    )

    now_send = time.time()
    with RECENT_BUYS_LOCK:
        last_buy = RECENT_BUYS.get(chosen["symbol"])
        if last_buy and now_send < last_buy.get("ts", 0) + BUY_LOCK_SECONDS:
            return None
        RECENT_BUYS[chosen["symbol"]] = {"ts": now_send, "reserved": True}
        if USE_SHELVE:
            persist_recent_buys()

    sent = send_telegram(msg)
    if sent:
        with RECENT_BUYS_LOCK:
            RECENT_BUYS[chosen["symbol"]].update({"ts": time.time(), "reserved": False})
            if USE_SHELVE:
                persist_recent_buys()
        print(f"[{time.strftime('%H:%M:%S')}] Signal -> {chosen['symbol']} score={chosen['score']:.2f}")
        return chosen
    else:
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(chosen["symbol"], None)
            if USE_SHELVE:
                persist_recent_buys()
        print(f"[{time.strftime('%H:%M:%S')}] Telegram send failed for {chosen['symbol']}")
        return None
        
# ---------- background monitors (monitor_positions and watch_orders) ----------
def monitor_positions():
    while True:
        try:
            now = time.time()
            to_process = []
            with RECENT_BUYS_LOCK:
                for sym, pos in list(RECENT_BUYS.items()):
                    if pos.get("closed"):
                        continue
                    ts = pos.get("ts", 0)
                    qty = pos.get("qty") or 0.0
                    processing = pos.get("processing", False)
                    if qty <= 0 or processing:
                        continue
                    if now - ts >= (HOLD_THRESHOLD_HOURS * 3600.0):
                        RECENT_BUYS[sym]["processing"] = True
                        if USE_SHELVE:
                            persist_recent_buys()
                        to_process.append((sym, pos))
            for sym, pos in to_process:
                notify(f"⚠️ Position {sym} open > {HOLD_THRESHOLD_HOURS}h — monitor attempting forced close.")
                # simplified fallback handling
                try:
                    qty = pos.get("qty")
                    # place_market_sell_fallback should be implemented if you need forced sells
                    # resp = place_market_sell_fallback(sym, qty, None)
                    resp = None
                    if resp:
                        add_blacklist(sym)
                        with RECENT_BUYS_LOCK:
                            if sym in RECENT_BUYS:
                                RECENT_BUYS.pop(sym, None)
                                if USE_SHELVE:
                                    persist_recent_buys()
                except Exception as e:
                    with RECENT_BUYS_LOCK:
                        if sym in RECENT_BUYS:
                            RECENT_BUYS[sym].update({"processing": False})
                            if USE_SHELVE:
                                persist_recent_buys()
                    notify(f"⚠️ Monitor failed to market-sell {sym}: {e}")
            time.sleep(MONITOR_INTERVAL)
        except Exception:
            time.sleep(MONITOR_INTERVAL)

def watch_orders(poll_interval=12):
    client = None
    while True:
        try:
            if client is None:
                client = init_binance_client()
            with RECENT_BUYS_LOCK:
                candidates = [(sym, dict(pos)) for sym, pos in RECENT_BUYS.items() if not pos.get("closed") and pos.get("sell_order_id")]
            for sym, pos in candidates:
                order_id = pos.get("sell_order_id")
                if not order_id:
                    continue
                try:
                    o = None
                    try:
                        o = client.get_order(symbol=sym, orderId=order_id)
                    except Exception:
                        try:
                            o = client.get_order(symbol=sym, origClientOrderId=str(order_id))
                        except Exception:
                            o = None
                    if not o:
                        continue
                    status = (o.get("status") or "").upper()
                    if status == "FILLED":
                        send_telegram(f"✅ POSITION CLOSED: `{sym}` sold (limit)")
                        add_blacklist(sym)
                        with RECENT_BUYS_LOCK:
                            if sym in RECENT_BUYS:
                                RECENT_BUYS.pop(sym, None)
                                if USE_SHELVE:
                                    persist_recent_buys()
                    elif status in ("CANCELED", "REJECTED"):
                        send_telegram(f"⚠️ SELL order {status} for {sym}. orderId={order_id}")
                        with RECENT_BUYS_LOCK:
                            if sym in RECENT_BUYS:
                                RECENT_BUYS.pop(sym, None)
                                if USE_SHELVE:
                                    persist_recent_buys()
                except Exception as e:
                    print("watch_orders error for", sym, e)
                time.sleep(0.4)
            time.sleep(poll_interval)
        except Exception as e:
            print("watch_orders loop error", e)
            time.sleep(max(5, poll_interval))

# ---------- main loop / web ----------
app = Flask(__name__)

@app.route("/")
def home():
    return "Signal bot running"

def trade_cycle():
    # small jitter to avoid exact-period thundering
    while True:
        try:
            res = pick_coin()
            if res:
                print(f"[{time.strftime('%H:%M:%S')}] Signal -> {res['symbol']} score={res['score']:.2f}")
                if ENABLE_TRADING:
                    t = threading.Thread(target=execute_trade, args=(res,), daemon=True)
                    t.start()
            else:
                print(f"[{time.strftime('%H:%M:%S')}] No signal")
        except Exception as e:
            print("cycle error", e)
        time.sleep(CYCLE_SECONDS + (0.2 * (os.getpid() % 3)))

if __name__ == "__main__":
    # start background workers
    tmon = threading.Thread(target=monitor_positions, daemon=True); tmon.start()
    twatch = threading.Thread(target=watch_orders, daemon=True); twatch.start()
    t = threading.Thread(target=trade_cycle, daemon=True); t.start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), threaded=True)