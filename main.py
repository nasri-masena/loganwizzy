import os
import time
import math
import threading
import random
import requests
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask

# try import python-binance
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
except Exception:
    Client = None
    BinanceAPIException = Exception

# -------------------------
# Config (tweak if needed)
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

ENABLE_TRADING = True
BUY_USDT_AMOUNT = 7.0
LIMIT_PROFIT_PCT = 0.8
BUY_BY_QUOTE = True
BUY_BASE_QTY = 0.0
MAX_CONCURRENT_POS = 8

QUOTE = "USDT"
BINANCE_REST = "https://api.binance.com"

PRICE_MIN = 0.1
PRICE_MAX = 15.0
MIN_VOLUME = 200000
TOP_BY_24H_VOLUME = 120

CYCLE_SECONDS = 3
KLINES_5M_LIMIT = 6
KLINES_1M_LIMIT = 6
OB_DEPTH = 3
MIN_OB_IMBALANCE = 1.2
MAX_OB_SPREAD_PCT = 1.0

CACHE_TTL = 1.0
REQUEST_TIMEOUT = 6.0
PUBLIC_CONCURRENCY = 12
MAX_WORKERS = 20

MIN_1M_PCT = 0.3
MIN_5M_PCT = 0.3
VOL_5M_MIN = 0.0002
RSI_PERIOD = 14
EMA_SHORT = 3
EMA_LONG = 10
MIN_SCORE = 12

RECENT_BUYS = {}
BUY_LOCK_SECONDS = 120
REMOVE_AFTER_CLOSE = True

SHORT_BUY_SELL_DELAY = 0.06
HOLD_THRESHOLD_HOURS = 5.0
MONITOR_INTERVAL = 6.0
LIMIT_SELL_RETRIES = 2
VOL_1M_THRESHOLD = 0.004

BLACKLIST_HOURS = 1.0
BLACKLIST_SECONDS = int(BLACKLIST_HOURS * 3600)
BLACKLIST = {}

SELL_DRAWDOWN_PCT = 5.0
SCAN_PAUSE_ON_OPEN = True

REQUESTS_SEMAPHORE = threading.BoundedSemaphore(value=PUBLIC_CONCURRENCY)
RECENT_BUYS_LOCK = threading.Lock()
_cache = {}
_cache_lock = threading.Lock()
OPEN_ORDERS_CACHE = {"data": None, "ts": 0}
OPEN_ORDERS_LOCK = threading.Lock()
TEMP_SKIP = {}
RATE_LIMIT_BACKOFF = None
ACTIVE_TRADE = threading.Event()

# -------------------------
# Cache helpers
# -------------------------
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

def sync_open_orders(force=False):
    client = init_binance_client()
    if not client:
        return None
    with OPEN_ORDERS_LOCK:
        if not force and OPEN_ORDERS_CACHE.get('data') and time.time() - OPEN_ORDERS_CACHE.get('ts', 0) < max(1.0, CACHE_TTL):
            orders = OPEN_ORDERS_CACHE['data']
        else:
            try:
                orders = client.get_open_orders()
            except Exception as e:
                print("sync_open_orders error", e)
                return None
            OPEN_ORDERS_CACHE['data'] = orders
            OPEN_ORDERS_CACHE['ts'] = time.time()

    any_open = False
    with RECENT_BUYS_LOCK:
        for o in (orders or []):
            sym = o.get('symbol')
            if not sym:
                continue
            any_open = True
            side = (o.get('side') or "").upper()
            if sym not in RECENT_BUYS or RECENT_BUYS.get(sym, {}).get("closed", True):
                RECENT_BUYS[sym] = {"ts": time.time(), "reserved": False, "closed": False, "processing": False}
            try:
                order_id = o.get("orderId") or o.get("clientOrderId")
                if side == "SELL":
                    RECENT_BUYS[sym].update({"sell_order_id": order_id, "sell_resp": o})
                else:
                    RECENT_BUYS[sym].update({"open_buy_order": o})
            except Exception:
                pass

    try:
        if any_open:
            ACTIVE_TRADE.set()
        else:
            with RECENT_BUYS_LOCK:
                if not any(not v.get("closed") for v in RECENT_BUYS.values()):
                    ACTIVE_TRADE.clear()
    except Exception:
        pass

    return orders

# -------------------------
# Telegram helper
# -------------------------
def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print(message)
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.status_code == 200
    except Exception as e:
        print("Telegram error", e)
        return False

notify = send_telegram


# -------------------------
# Math / indicators
# -------------------------
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

def orderbook_bullish(ob, depth=5, min_imbalance=1.1, max_spread_pct=0.6, min_quote_depth=200.0):
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

# -------------------------
# Binance public calls (cached)
# -------------------------
def fetch_tickers():
    key = "tickers"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        resp = safe_get("/api/v3/ticker/24hr")
        data = resp.json()
        cache_set(key, data)
        return data
    except RuntimeError:
        print("fetch_tickers: rate_limited")
        return []
    except Exception as e:
        print("fetch_tickers error", e)
        return []

def fetch_klines(symbol, interval, limit):
    key = f"klines:{symbol}:{interval}:{limit}"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        resp = safe_get("/api/v3/klines", params=params)
        data = resp.json()
        cache_set(key, data)
        return data
    except RuntimeError:
        return []
    except Exception:
        return []

def fetch_order_book(symbol, limit=OB_DEPTH):
    key = f"depth:{symbol}:{limit}"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        params = {"symbol": symbol, "limit": max(5, limit)}
        resp = safe_get("/api/v3/depth", params=params)
        data = resp.json()
        cache_set(key, data)
        return data
    except RuntimeError:
        return {}
    except Exception:
        return {}

def fetch_symbol_price(symbol):
    try:
        tickers = fetch_tickers()
        for t in tickers:
            if t.get("symbol") == symbol:
                return float(t.get("lastPrice") or 0.0)
    except Exception:
        pass
    try:
        kl = fetch_klines(symbol, "1m", 1)
        if kl:
            return float(kl[-1][4])
    except Exception:
        pass
    return None

def safe_get(path, params=None):
    global RATE_LIMIT_BACKOFF
    if RATE_LIMIT_BACKOFF and time.time() < RATE_LIMIT_BACKOFF:
        # quick fail to avoid more bans
        raise RuntimeError("rate_limited")
    try:
        with REQUESTS_SEMAPHORE:
            resp = requests.get(BINANCE_REST + path, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code in (429, 418):
            # conservative backoff
            RATE_LIMIT_BACKOFF = time.time() + max(10, REQUEST_TIMEOUT)
            raise RuntimeError("rate_limited")
        resp.raise_for_status()
        return resp
    except requests.exceptions.RequestException as e:
        try:
            code = getattr(e.response, "status_code", None)
            if code in (429, 418):
                RATE_LIMIT_BACKOFF = time.time() + max(10, REQUEST_TIMEOUT)
                raise RuntimeError("rate_limited")
        except Exception:
            pass
        raise

# -------------------------
# Client / exchange info
# -------------------------
_binance_client = None
_symbol_info_cache = {}

def init_binance_client():
    global _binance_client
    if not Client:
        print("python-binance not installed")
        return None
    if _binance_client:
        return _binance_client
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("Binance API keys not set")
        return None
    _binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    return _binance_client

def fetch_exchange_info():
    key = "exchange_info"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        client = init_binance_client()
        if not client:
            with REQUESTS_SEMAPHORE:
                r = requests.get(BINANCE_REST + "/api/v3/exchangeInfo", timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            cache_set(key, data)
            return data
        data = client.get_exchange_info()
        cache_set(key, data)
        return data
    except Exception as e:
        print("fetch_exchange_info error", e)
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

def get_tick_size(symbol):
    try:
        info = get_symbol_info(symbol) or {}
        filters = {f["filterType"]: f for f in info.get("filters", [])}
        tick = float(filters.get("PRICE_FILTER", {}).get("tickSize") or 0.0)
        return tick
    except Exception:
        return 0.0

def format_price_for_symbol(price, symbol):
    try:
        tick = get_tick_size(symbol)
        if not tick or tick == 0:
            return str(price)
        prec = int(round(-math.log10(tick))) if tick < 1 else 0
        adj = math.ceil(float(price) / tick) * tick
        return ("{:0." + str(max(0, prec)) + "f}").format(adj)
    except Exception:
        return str(price)
        
def ceil_step(v, step):
    if not step or step == 0:
        return v
    return math.ceil(v / step) * step

def get_free_asset(asset):
    client = init_binance_client()
    if not client:
        return 0.0
    try:
        b = client.get_asset_balance(asset=asset)
        if b:
            return float(b.get("free") or 0.0)
    except Exception:
        pass
    try:
        acc = client.get_account()
        for bal in acc.get("balances", []):
            if bal.get("asset") == asset:
                return float(bal.get("free") or 0.0)
    except Exception:
        pass
    return 0.0

# -------------------------
# Finalize close (clears ACTIVE_TRADE)
# -------------------------
def finalize_close(symbol, update_fields=None):
    with RECENT_BUYS_LOCK:
        if symbol in RECENT_BUYS:
            if REMOVE_AFTER_CLOSE:
                RECENT_BUYS.pop(symbol, None)
            else:
                if update_fields:
                    RECENT_BUYS[symbol].update(update_fields)
                RECENT_BUYS[symbol]["closed"] = True
    try:
        with RECENT_BUYS_LOCK:
            still_active = any(not v.get("closed") for v in RECENT_BUYS.values())
        if not still_active:
            ACTIVE_TRADE.clear()
    except Exception:
        pass
    
# -------------------------
# Blacklist helpers (in-memory)
# -------------------------
def add_blacklist(symbol, seconds=BLACKLIST_SECONDS):
    if not symbol:
        return
    BLACKLIST[symbol] = time.time() + int(seconds)

def is_blacklisted(symbol):
    exp = BLACKLIST.get(symbol)
    if not exp:
        return False
    if time.time() >= exp:
        try:
            BLACKLIST.pop(symbol, None)
        except Exception:
            pass
        return False
    return True

# -------------------------
# Market buy helpers
# -------------------------
def place_market_buy_by_quote(symbol, quote_qty):
    client = init_binance_client()
    if not client:
        raise RuntimeError("Binance client not available")
    try:
        order = client.order_market_buy(symbol=symbol, quoteOrderQty=str(quote_qty))
        return order
    except BinanceAPIException:
        book = fetch_order_book(symbol, limit=5)
        if not book:
            raise
        top_ask = float(book["asks"][0][0])
        raw_qty = quote_qty / top_ask
        qty, _ = adjust_qty_price_for_filters(symbol, raw_qty, top_ask)
        if qty <= 0:
            raise RuntimeError("Computed qty below symbol min after filters")
        order = client.order_market_buy(symbol=symbol, quantity=str(qty))
        return order

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

# -------------------------
# Market sell fallback (used for drawdown)
# -------------------------
def place_market_sell_fallback(symbol, qty, f=None):
    try:
        if not f:
            info = get_symbol_info(symbol)
            f = None
            if info:
                for fil in info.get("filters", []):
                    if fil.get("filterType") in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                        f = fil
                        break
            f = f or {}
    except Exception:
        f = f or {}
    try:
        try:
            qty_str = format_qty(qty, float(f.get('stepSize', 0.0)))
        except Exception:
            qty_str = str(qty)
        # notify(f"⚠️ Attempting MARKET sell fallback for {symbol}: qty={qty_str}")
        c = init_binance_client()
        if not c:
            notify(f"❌ Market sell fallback failed for {symbol}: Binance client unavailable")
            return None
        try:
            resp = c.order_market_sell(symbol=symbol, quantity=qty_str)
            notify(f"✅ Market sell fallback executed for {symbol}")
            try:
                with OPEN_ORDERS_LOCK:
                    OPEN_ORDERS_CACHE['data'] = None
                    OPEN_ORDERS_CACHE['ts'] = 0
            except Exception:
                pass
            return resp
        except Exception as e:
            if 'NOTIONAL' in str(e) or '-1013' in str(e):
                notify(f"🚫 Market sell for {symbol} failed: position below minNotional. Suppressing future attempts for 24 hours.")
                TEMP_SKIP[symbol] = time.time() + 24*60*60
                return None
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
                if 'NOTIONAL' in str(e2) or '-1013' in str(e2):
                    notify(f"🚫 Market sell for {symbol} failed: position below minNotional. Suppressing future attempts for 24 hours.")
                    TEMP_SKIP[symbol] = time.time() + 24*60*60
                    return None
                notify(f"❌ Market sell fallback failed for {symbol}: {e2}")
                return None
    except Exception as e:
        if 'NOTIONAL' in str(e) or '-1013' in str(e):
            notify(f"🚫 Market sell for {symbol} failed: position below minNotional. Suppressing future attempts for 24 hours.")
            TEMP_SKIP[symbol] = time.time() + 24*60*60
        notify(f"❌ Market sell fallback failed for {symbol}: {e}")
        return None

# -------------------------
# Limit sell strict
# -------------------------
def place_limit_sell_strict(symbol, qty, sell_price, retries=None, delay=0.8):
    if retries is None:
        retries = LIMIT_SELL_RETRIES
    try:
        client = init_binance_client()
        if not client:
            notify(f"⚠️ place_limit_sell_strict: binance client missing")
            return None

        info = get_symbol_info(symbol)
        if not info:
            notify(f"⚠️ place_limit_sell_strict: missing symbol info for {symbol}")
            return None

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

        if not _meets_min_notional(qty, sell_price):
            last_price = fetch_symbol_price(symbol) or 0.0
            if last_price and last_price > 0:
                needed = math.ceil((min_notional / float(sell_price)) / (step or 1)) * (step or 1)
                free = _get_free_asset(asset)
                if needed <= free + 1e-12 and needed > qty:
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
        price_str = format_price_for_symbol(sell_price, symbol)

        free = _get_free_asset(asset)
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
                qty = new_qty
                qty_str = format_qty(qty, step)

        attempt = 0
        last_err = None
        while attempt < retries:
            attempt += 1
            try:
                jitter = random.uniform(0.03, 0.12)
                time.sleep(min(0.2, SHORT_BUY_SELL_DELAY + jitter))
                order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=price_str, timeInForce='GTC')
                try:
                    with OPEN_ORDERS_LOCK:
                        OPEN_ORDERS_CACHE['data'] = None
                        OPEN_ORDERS_CACHE['ts'] = 0
                except Exception:
                    pass
                return order
            except BinanceAPIException as e:
                err = str(e)
                last_err = err
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
                        price_str = format_price_for_symbol(sell_price, symbol)
                        bump_attempts += 1
                    time.sleep(0.2)
                    continue
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

        try:
            notify("⚠️ All limit attempts failed — trying LIMIT_MAKER as last attempt.")
            lm = client.create_order(symbol=symbol, side='SELL', type='LIMIT_MAKER', quantity=qty_str, price=price_str)
            return lm
        except Exception as e:
            notify(f"❌ Final LIMIT_MAKER attempt failed: {e}")

        notify(f"❌ place_limit_sell_strict: all attempts failed for {symbol}. last_err={last_err}")
        TEMP_SKIP[symbol] = time.time() + 60
        return None

    except Exception as e:
        notify(f"⚠️ place_limit_sell_strict unexpected error for {symbol}: {e}")
        return None
        
# -------------------------
# Cancel open SELL orders for a symbol (safe)
# -------------------------
def cancel_open_sell_orders(symbol, client=None, sleep_between=0.08, timeout_wait=0.75):
    cancelled = []
    try:
        c = client or init_binance_client()
        if not c:
            notify(f"⚠️ cancel_open_sell_orders: binance client unavailable for {symbol}")
            return 0, []
        try:
            open_orders = c.get_open_orders(symbol=symbol) or []
        except Exception as e:
            # if cannot fetch open orders, bail but report
            notify(f"⚠️ cancel_open_sell_orders: failed to list open orders for {symbol}: {e}")
            return 0, []

        for o in open_orders:
            try:
                side = (o.get("side") or "").upper()
                if side != "SELL":
                    continue
                oid = o.get("orderId") or o.get("clientOrderId")
                # cancel by orderId if available
                if o.get("orderId") is not None:
                    try:
                        c.cancel_order(symbol=symbol, orderId=o.get("orderId"))
                    except Exception as e:
                        # try by clientOrderId fallback
                        try:
                            c.cancel_order(symbol=symbol, origClientOrderId=str(o.get("clientOrderId")))
                        except Exception as e2:
                            notify(f"⚠️ cancel_open_sell_orders: failed to cancel sell order {oid} for {symbol}: {e2}")
                            continue
                else:
                    try:
                        c.cancel_order(symbol=symbol, origClientOrderId=str(o.get("clientOrderId")))
                    except Exception as e:
                        notify(f"⚠️ cancel_open_sell_orders: failed to cancel sell order {o} for {symbol}: {e}")
                        continue
                cancelled.append(o)
                time.sleep(sleep_between)
            except Exception as e:
                # keep going even if single cancel fails
                notify(f"⚠️ cancel_open_sell_orders error for {symbol}: {e}")
                continue

        # small wait for exchange to process cancellations & release funds
        time.sleep(timeout_wait)
        # clear open orders cache so next call fetches fresh
        try:
            with OPEN_ORDERS_LOCK:
                OPEN_ORDERS_CACHE['data'] = None
                OPEN_ORDERS_CACHE['ts'] = 0
        except Exception:
            pass

    except Exception as e:
        notify(f"⚠️ cancel_open_sell_orders unexpected error for {symbol}: {e}")
    return len(cancelled), cancelled

# -------------------------
# Compute safe market-sell qty given desired qty and symbol (respect filters)
# -------------------------
def compute_market_sell_qty(symbol, desired_qty, client=None):
    """
    Compute qty we can market-sell given current free balance and symbol filters.
    Returns (qty_float, reason_str). qty_float may be 0.0 if cannot sell.
    """
    try:
        asset = symbol[:-len(QUOTE)] if QUOTE and symbol.endswith(QUOTE) else None
        if not asset:
            return 0.0, "asset_parse_failed"

        # refresh symbol filters
        info = get_symbol_info(symbol) or {}
        filters = {f["filterType"]: f for f in info.get("filters", [])}
        step = float(filters.get("LOT_SIZE", {}).get("stepSize") or filters.get("MARKET_LOT_SIZE", {}).get("stepSize") or 0.0)
        min_notional = float(filters.get("MIN_NOTIONAL", {}).get("minNotional") or 0.0)

        # get available free balance (after cancellations this should reflect freed funds)
        free = get_free_asset(asset)
        qty = float(desired_qty)

        # ensure we don't attempt to sell more than free
        if free + 1e-12 < qty:
            qty = free

        # apply step rounding down
        if step and step > 0:
            qty = math.floor(qty / step) * step

        # ensure min notional
        # if qty*approx_price < min_notional we might still try (market sell), but often fails.
        # To be safe, if we can't meet min_notional with available qty, return 0.
        if min_notional and qty > 0:
            # try to estimate price quickly from last ticker
            last_price = fetch_symbol_price(symbol) or 0.0
            if last_price > 0 and (qty * last_price) + 1e-12 < min_notional:
                return 0.0, f"below_min_notional (need {min_notional}, have {qty*last_price:.8f})"

        if qty <= 0:
            return 0.0, "qty_zero_after_adjust"

        return qty, "ok"
    except Exception as e:
        return 0.0, f"compute_error:{e}"

# -------------------------
# Cancel sells then market sell helper (robust)
# -------------------------
def cancel_then_market_sell(symbol, qty, max_retries=2):
    """
    Cancel open SELL orders for symbol, then attempt market sell for `qty`.
    Returns response on success, None on failure.
    """
    client = init_binance_client()
    if not client:
        notify(f"❌ cancel_then_market_sell: Binance client unavailable for {symbol}")
        return None

    # 1) cancel existing SELL orders (if any)
    try:
        ccount, cancelled = cancel_open_sell_orders(symbol, client=client)
        if ccount:
            pass
    # notify(f"ℹ️ Cancelled {ccount} existing SELL order(s) for {symbol} before market-sell.")
    except Exception as e:
        notify(f"⚠️ cancel_then_market_sell: cancel step failed for {symbol}: {e}")

    # 2) compute sellable qty (after cancellation balances should be freed)
    qty_adj, reason = compute_market_sell_qty(symbol, qty, client=client)
    if qty_adj <= 0:
        notify(f"🚫 Cannot market-sell {symbol}: {reason}")
        return None

    # format qty with step
    try:
        info = get_symbol_info(symbol) or {}
        step = 0.0
        for fil in info.get("filters", []):
            if fil.get("filterType") in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                step = float(fil.get("stepSize") or 0.0)
                break
        qty_str = format_qty(qty_adj, step)
    except Exception:
        qty_str = str(qty_adj)

    # 3) attempt market sell with a couple retries and fallback to create_order
    attempt = 0
    last_err = None
    while attempt < max_retries:
        attempt += 1
        try:
            # notify(f"⚠️ Attempting MARKET sell fallback for {symbol}: qty={qty_str} (attempt {attempt})")
            resp = client.order_market_sell(symbol=symbol, quantity=qty_str)
            # clear open orders cache
            try:
                with OPEN_ORDERS_LOCK:
                    OPEN_ORDERS_CACHE['data'] = None
                    OPEN_ORDERS_CACHE['ts'] = 0
            except Exception:
                pass
            notify(f"⭕ Coin ya {symbol} imeuzwa kwa hasara")
            return resp
        except Exception as e:
            last_err = str(e)
            # detect common errors
            if 'NOTIONAL' in last_err or '-1013' in last_err:
                notify(f"🚫 Market sell for {symbol} failed: position below minNotional or filter error: {last_err}. Suppressing future attempts for 24 hours.")
                TEMP_SKIP[symbol] = time.time() + 24*60*60
                return None
            if '-2010' in last_err or 'insufficient balance' in last_err.lower():
                # maybe exchange hasn't released reserved funds yet; wait short then retry
                notify(f"⚠️ Market sell insufficient balance for {symbol}: {last_err}. Retrying after short wait.")
                time.sleep(0.6 * attempt)
                # refresh balances and recompute qty
                qty_adj, reason = compute_market_sell_qty(symbol, qty, client=client)
                if qty_adj <= 0:
                    notify(f"🚫 After retry, cannot sell {symbol}: {reason}")
                    return None
                try:
                    # recompute qty_str
                    info = get_symbol_info(symbol) or {}
                    step = 0.0
                    for fil in info.get("filters", []):
                        if fil.get("filterType") in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                            step = float(fil.get("stepSize") or 0.0)
                            break
                    qty_str = format_qty(qty_adj, step)
                except Exception:
                    qty_str = str(qty_adj)
                continue
            # last resort try create_order
            try:
                notify(f"⚠️ order_market_sell failed for {symbol} (attempt {attempt}): {last_err}. Trying create_order fallback.")
                resp = client.create_order(symbol=symbol, side='SELL', type='MARKET', quantity=qty_str)
                try:
                    with OPEN_ORDERS_LOCK:
                        OPEN_ORDERS_CACHE['data'] = None
                        OPEN_ORDERS_CACHE['ts'] = 0
                except Exception:
                    pass
                notify(f"✅ Market sell executed (via create_order) for {symbol}")
                return resp
            except Exception as e2:
                last_err = str(e2)
                notify(f"⚠️ create_order market sell also failed for {symbol}: {last_err}")
                time.sleep(0.2 * attempt)
                continue

    notify(f"❌ cancel_then_market_sell: all attempts failed for {symbol}. last_err={last_err}")
    # if persistent failure, set a short TEMP_SKIP to avoid hot-loop retries
    TEMP_SKIP[symbol] = time.time() + 60
    return None

# -------------------------
# Evaluate symbol
# -------------------------
def evaluate_symbol(sym, last_price, qvol, change_24h):
    try:
        # basic filters
        if not (PRICE_MIN <= last_price <= PRICE_MAX):
            return None
        if qvol < MIN_VOLUME:
            return None
        if change_24h < 0.5 or change_24h > 20.0:
            return None

        # fetch klines + orderbook in parallel
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

        # require 1m volatility threshold (this is the important change)
        if vol_1m is None or vol_1m < VOL_1M_THRESHOLD:
            return None

        # EMAs / RSI
        short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
        long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
        ema_uplift = 0.0
        if short_ema and long_ema and long_ema != 0:
            ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))

        rsi_val = compute_rsi_local(closes_5m[-(RSI_PERIOD+1):], RSI_PERIOD) if len(closes_5m) >= RSI_PERIOD+1 else None

        ob_bull = orderbook_bullish(ob, depth=OB_DEPTH, min_imbalance=MIN_OB_IMBALANCE, max_spread_pct=MAX_OB_SPREAD_PCT)

        # scoring (added a small weight for vol_1m to prefer higher 1m activity)
        score = 0.0
        score += max(0.0, pct_5m) * 4.0
        score += max(0.0, pct_1m) * 2.0
        score += ema_uplift * 500.0
        score += max(0.0, change_24h) * 0.5
        if vol_5m is not None:
            score += max(0.0, (vol_5m - VOL_5M_MIN)) * 100.0
        # <-- key: use vol_1m directly (scaled) to prefer coins passing the threshold
        score += max(0.0, (vol_1m - VOL_1M_THRESHOLD)) * 100.0
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
        if score < MIN_SCORE:
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


def pick_coin():
    try:
        sync_open_orders()
    except Exception:
        pass

    if SCAN_PAUSE_ON_OPEN and ACTIVE_TRADE.is_set():
        return None

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
        if not (PRICE_MIN <= last <= PRICE_MAX):
            continue
        if qvol < MIN_VOLUME:
            continue
        if ch < 0.5 or ch > 20.0:
            continue

        # recent buys / locks
        with RECENT_BUYS_LOCK:
            last_buy = RECENT_BUYS.get(sym)
            if last_buy and not last_buy.get("closed"):
                continue
            if last_buy and now < last_buy.get("ts", 0) + BUY_LOCK_SECONDS:
                continue

        if is_blacklisted(sym):
            continue

        # candidate for deeper evaluation
        pre.append((sym, last, qvol, ch))

    if not pre:
        return None

    # sort by quote volume to prioritize liquid names
    pre.sort(key=lambda x: x[2], reverse=True)
    candidates = pre[:TOP_BY_24H_VOLUME]
    results = []

    # evaluate candidates in parallel (evaluate_symbol now enforces vol_1m threshold)
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates) or 1)) as ex:
        futures = {ex.submit(evaluate_symbol, sym, last, qvol, ch): sym for (sym, last, qvol, ch) in candidates}
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

    # final blacklist check
    if is_blacklisted(chosen["symbol"]):
        return None

    msg = (
        f"🔥 *COIN SIGNAL*: `{chosen['symbol']}`\n"
        f"Price:`{chosen['last_price']}`\n"
        f"24h:`{chosen['24h_change']}`%\n"
        f"1m:`{chosen['pct_1m']:.2f}`%\n"
        f"Vol1m:`{chosen['vol_1m']}`\n"
        f"Orderbook Bullish:`{chosen['ob_bull']}`\n"
        f"Score:`{chosen['score']:.2f}`"
    )

    with RECENT_BUYS_LOCK:
        last_buy = RECENT_BUYS.get(chosen["symbol"])
        if last_buy and not last_buy.get("closed"):
            return None
        if last_buy and time.time() < last_buy.get("ts", 0) + BUY_LOCK_SECONDS:
            return None
        if len([k for k, v in RECENT_BUYS.items() if not v.get("closed")]) >= MAX_CONCURRENT_POS:
            notify(f"⚠️ Max concurrent positions active ({MAX_CONCURRENT_POS}). Skipping new buy.")
            return None
        RECENT_BUYS[chosen["symbol"]] = {"ts": time.time(), "reserved": True, "closed": False, "processing": False}

    sent = send_telegram(msg)
    if sent:
        with RECENT_BUYS_LOCK:
            RECENT_BUYS[chosen["symbol"]].update({"ts": time.time(), "reserved": False})
        if ENABLE_TRADING:
            t = threading.Thread(target=execute_trade, args=(chosen,), daemon=True)
            t.start()
        return chosen
    else:
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(chosen["symbol"], None)
        return None
        
# -------------------------
# Execute trade
# -------------------------
def execute_trade(chosen):
    symbol = chosen["symbol"]
    now = time.time()
    with RECENT_BUYS_LOCK:
        if len([k for k, v in RECENT_BUYS.items() if not v.get("closed")]) >= MAX_CONCURRENT_POS:
            send_telegram(f"⚠️ Max concurrent positions reached. Skipping {symbol}")
            return False
        RECENT_BUYS[symbol] = {"ts": now, "reserved": False, "closed": False, "processing": True}
    client = init_binance_client()
    if not client:
        send_telegram(f"⚠️ Trading disabled or client missing. Skipping live trade for {symbol}")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(symbol, None)
        try: ACTIVE_TRADE.clear()
        except Exception: pass
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
            try: ACTIVE_TRADE.clear()
            except Exception: pass
            return False

        executed_qty, avg_price = parse_market_fill(order)
        if executed_qty <= 0:
            send_telegram(f"⚠️ Buy executed but no fill qty for {symbol}. Raw: {order}")
            with RECENT_BUYS_LOCK:
                RECENT_BUYS.pop(symbol, None)
            try: ACTIVE_TRADE.clear()
            except Exception: pass
            return False

        with RECENT_BUYS_LOCK:
            RECENT_BUYS[symbol].update({"qty": executed_qty, "buy_price": avg_price, "ts": now, "processing": False})

        try:
            ACTIVE_TRADE.set()
        except Exception:
            pass

        send_telegram(f"💸 Imenunuliwa `{symbol}` kwa:`{executed_qty}` @ `{avg_price}` jumla:`{round(executed_qty*avg_price,6)}`")

        time.sleep(SHORT_BUY_SELL_DELAY)

        sell_price = avg_price * (1.0 + (LIMIT_PROFIT_PCT / 100.0))
        sell_resp = place_limit_sell_strict(symbol, executed_qty, sell_price)
        if sell_resp:
            sell_order_id = None
            try:
                if isinstance(sell_resp, dict):
                    sell_order_id = sell_resp.get("orderId") or sell_resp.get("order_id") or sell_resp.get("clientOrderId")
                else:
                    sell_order_id = getattr(sell_resp, "orderId", None) or getattr(sell_resp, "order_id", None) or getattr(sell_resp, "clientOrderId", None)
            except Exception:
                sell_order_id = None

            with RECENT_BUYS_LOCK:
                RECENT_BUYS[symbol].update({
                    "sell_price": sell_price,
                    "sell_resp": sell_resp,
                    "sell_order_id": sell_order_id,
                    "sell_ts": time.time(),
                    "closed": False,
                    "processing": False,
                })

            send_telegram(f"📌 Order ya kuuzwa `{symbol}` imewekwa kwa `{executed_qty}` @ `{sell_price}` (+{LIMIT_PROFIT_PCT}%)")

            # short poll for immediate fills
            try:
                if sell_order_id:
                    filled_order = wait_for_order_fill(client, symbol, sell_order_id, timeout=8, poll=1.0)
                    if filled_order:
                        st = (filled_order.get("status") or "").upper()
                        if st == "FILLED":
                            filled_qty = 0.0
                            avg_price_fill = 0.0
                            try:
                                fills = filled_order.get("fills") or []
                                if fills:
                                    tq = 0.0; tq_quote = 0.0
                                    for f in fills:
                                        q = float(f.get("qty", 0)); p = float(f.get("price", 0))
                                        tq += q; tq_quote += q * p
                                    filled_qty = tq
                                    avg_price_fill = (tq_quote / tq) if tq else 0.0
                                else:
                                    filled_qty = float(filled_order.get("executedQty") or 0.0)
                                    cquote = float(filled_order.get("cummulativeQuoteQty") or 0.0)
                                    avg_price_fill = (cquote / filled_qty) if filled_qty else 0.0
                            except Exception:
                                filled_qty = executed_qty
                                avg_price_fill = sell_price

                            add_blacklist(symbol)
                            finalize_close(symbol, {"closed_ts": time.time(), "close_method": "limit_filled_immediate", "close_resp": filled_order, "sell_fill_qty": filled_qty, "sell_fill_price": avg_price_fill})
                            send_telegram(f"✔️ Coin ya `{symbol}` imeuzwa — {filled_qty} @ {avg_price} (limit)")
                            return True
            except Exception:
                pass

            return True
        else:
            notify(f"⚠️ limit sell placement failed for {symbol}, attempting market sell fallback.")
            fallback = cancel_then_market_sell(symbol, executed_qty)
            with RECENT_BUYS_LOCK:
                if fallback:
                    add_blacklist(symbol)
                    finalize_close(symbol, {"closed_ts": time.time(), "close_method": "market_fallback", "close_resp": fallback})
                else:
                    if symbol in RECENT_BUYS:
                        RECENT_BUYS.pop(symbol, None)
                    try:
                        ACTIVE_TRADE.clear()
                    except Exception:
                        pass
            if fallback:
                send_telegram(f"ℹ️ Market fallback sold {symbol}.")
            else:
                send_telegram(f"❌ Both limit and market sell failed for {symbol}. Entry removed to avoid blocking.")
            return False
    except BinanceAPIException as e:
        send_telegram(f"‼️ Binance API error during buy {symbol}: {e}")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(symbol, None)
        try: ACTIVE_TRADE.clear()
        except Exception: pass
        return False
    except Exception as e:
        send_telegram(f"‼️ Unexpected error during trade {symbol}: {e}")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(symbol, None)
        try: ACTIVE_TRADE.clear()
        except Exception: pass
        return False

# -------------------------
# wait_for_order_fill
# -------------------------
def wait_for_order_fill(client, symbol, order_id, timeout=10, poll=1.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            o = None
            try:
                o = client.get_order(symbol=symbol, orderId=order_id)
            except Exception:
                try:
                    o = client.get_order(symbol=symbol, origClientOrderId=str(order_id))
                except Exception:
                    o = None
            if not o:
                time.sleep(poll)
                continue
            status = (o.get("status") or "").upper()
            if status == "FILLED":
                return o
            if status in ("CANCELED", "REJECTED"):
                return o
        except Exception:
            pass
        time.sleep(poll)
    return None

# -------------------------
# Monitor positions (stale + drawdown -> market sell)
# -------------------------
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
                    buy_price = pos.get("buy_price") or 0.0
                    if qty <= 0 or processing:
                        continue
                    if now - ts >= (HOLD_THRESHOLD_HOURS * 3600.0):
                        RECENT_BUYS[sym]["processing"] = True
                        to_process.append((sym, pos, "stale"))
                        continue
                    try:
                        last = fetch_symbol_price(sym)
                        if last is not None and buy_price > 0:
                            pct = (last - buy_price) / buy_price * 100.0
                            if pct <= -abs(SELL_DRAWDOWN_PCT):
                                RECENT_BUYS[sym]["processing"] = True
                                to_process.append((sym, pos, "drawdown", last, pct))
                    except Exception:
                        pass

            for item in to_process:
                try:
                    if item[2] == "stale":
                        sym, pos = item[0], item[1]
                        notify(f"⚠️ Position {sym} open > {HOLD_THRESHOLD_HOURS}h — executing market sell fallback to close position.")
                        qty = pos.get("qty")
                        resp = cancel_then_market_sell(sym, qty)
                        if resp:
                            add_blacklist(sym)
                            notify(f"ℹ️ Position {sym} force-sold by monitor.")
                            finalize_close(sym, {"closed_ts": time.time(), "close_method": "monitor_market_fallback", "close_resp": resp})
                        else:
                            with RECENT_BUYS_LOCK:
                                if sym in RECENT_BUYS:
                                    RECENT_BUYS[sym].update({"processing": False})
                            notify(f"⚠️ Monitor failed to market-sell {sym}. Will retry later.")
                    elif item[2] == "drawdown":
                        sym, pos, _, last, pct = item[0], item[1], item[2], item[3], item[4]
                        # notify(f"⚠️ Position {sym} drawdown detected {pct:.2f}% (last={last}, buy={pos.get('buy_price')}). Selling to limit loss.")
                        qty = pos.get("qty")
                        resp = cancel_then_market_sell(sym, qty)
                        if resp:
                            add_blacklist(sym)
                            # notify(f"ℹ️ Position {sym} sold due to drawdown ({pct:.2f}%).")
                            finalize_close(sym, {"closed_ts": time.time(), "close_method": "monitor_drawdown_market", "close_resp": resp})
                        else:
                            with RECENT_BUYS_LOCK:
                                if sym in RECENT_BUYS:
                                    RECENT_BUYS[sym].update({"processing": False})
                            notify(f"⚠️ Monitor failed to market-sell {sym} for drawdown. Will retry later.")
                except Exception as e:
                    try:
                        sym = item[0]
                        with RECENT_BUYS_LOCK:
                            if sym in RECENT_BUYS:
                                RECENT_BUYS[sym].update({"processing": False})
                    except Exception:
                        pass
                    notify(f"⚠️ Monitor processing failed for {item}: {e}")

            time.sleep(MONITOR_INTERVAL)
        except Exception as e:
            print("monitor_positions loop error", e)
            time.sleep(max(5, MONITOR_INTERVAL))

# -------------------------
# Watch orders (poll for fills)
# -------------------------
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
                        filled_qty = 0.0; avg_price = 0.0
                        try:
                            fills = o.get("fills") or []
                            if fills:
                                tq = 0.0; tq_quote = 0.0
                                for f in fills:
                                    q = float(f.get("qty", 0)); p = float(f.get("price", 0))
                                    tq += q; tq_quote += q * p
                                filled_qty = tq
                                avg_price = (tq_quote / tq) if tq else 0.0
                            else:
                                filled_qty = float(o.get("executedQty") or 0.0)
                                cquote = float(o.get("cummulativeQuoteQty") or 0.0)
                                avg_price = (cquote / filled_qty) if filled_qty else 0.0
                        except Exception:
                            filled_qty = pos.get("qty") or 0.0
                            avg_price = pos.get("sell_price") or 0.0

                        send_telegram(f"✅ Trade ya `{sym}` imefungwa — {filled_qty} @ {avg_price} (limit)")
                        add_blacklist(sym)
                        finalize_close(sym, {"closed_ts": time.time(), "close_method": "limit_filled_watch", "close_resp": o, "sell_fill_qty": filled_qty, "sell_fill_price": avg_price})
                    elif status in ("CANCELED", "REJECTED"):
                        send_telegram(f"⚠️ SELL order {status} for {sym}. orderId={order_id}")
                        finalize_close(sym, {"closed_ts": time.time(), "close_method": f"sell_{status.lower()}", "close_resp": o})
                except Exception as e:
                    print("watch_orders error for", sym, e)
                time.sleep(0.4)
            time.sleep(poll_interval)
        except Exception as e:
            print("watch_orders loop error", e)
            time.sleep(max(5, poll_interval))

# -------------------------
# Web / main loop
# -------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "Signal bot running (stateless)"

def trade_cycle():
    while True:
        try:
            res = pick_coin()
            if res:
                print(f"[{time.strftime('%H:%M:%S')}] Signal -> {res['symbol']} score={res['score']:.2f}")
            else:
                if SCAN_PAUSE_ON_OPEN and ACTIVE_TRADE.is_set():
                    print(f"[{time.strftime('%H:%M:%S')}] Scanning paused. Active trade in progress.")
                else:
                    print(f"[{time.strftime('%H:%M:%S')}] No signal")
        except Exception as e:
            print("cycle error", e)
        time.sleep(CYCLE_SECONDS)

if __name__ == "__main__":
    try:
        sync_open_orders(force=True)
    except Exception as e:
        print("initial sync_open_orders failed:", e)
    tmon = threading.Thread(target=monitor_positions, daemon=True); tmon.start()
    twatch = threading.Thread(target=watch_orders, daemon=True); twatch.start()
    t = threading.Thread(target=trade_cycle, daemon=True); t.start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), threaded=True)