import os
import time
import math
import threading
import random
import statistics
import shelve
import requests
from decimal import Decimal, ROUND_DOWN
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask
from typing import Tuple, Optional

# Optional binance client
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
except Exception:
    Client = None
    BinanceAPIException = Exception

# -------------------------
# Config (change via env)
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

ENABLE_TRADING = os.getenv("ENABLE_TRADING", "True").lower() in ("1", "true", "yes")
TRADE_USD = float(os.getenv("TRADE_USD", "8.0"))
LIMIT_PROFIT_PCT = float(os.getenv("LIMIT_PROFIT_PCT", "1.0"))
QUOTE = "USDT"

PRICE_MIN = 1.0
PRICE_MAX = 4.0
MIN_VOLUME = 1_000_000
TOP_BY_24H_VOLUME = 6
CYCLE_SECONDS = 4
KLINES_5M_LIMIT = 6
KLINES_1M_LIMIT = 6
EMA_SHORT = 3
EMA_LONG = 10
MIN_5M_PCT = 0.6
MIN_1M_PCT = 0.3

RECENT_BUYS_DB = "recent_buys.db"
RECENT_BUYS = {}
RECENT_BUYS_LOCK = threading.Lock()

CACHE_TTL = 1.0
PRICE_CACHE_TTL = 3.0
_public_session = requests.Session()
PUBLIC_CONCURRENCY = 6
REQUEST_TIMEOUT = 6.0

BUY_LOCK_SECONDS = int(os.getenv("BUY_LOCK_SECONDS", "900"))
OCO_MAX_LIFE_SECONDS = 6 * 3600
RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_BASE_SLEEP = 2
RATE_LIMIT_BACKOFF_MAX = 300

BINANCE_REST = "https://api.binance.com"

_cache = {}
_cache_lock = threading.Lock()
_per_symbol_price_cache = {}
_ticker_cache = None
_ticker_cache_ts = 0

# -------------------------
# helpers
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

def load_recent_buys():
    try:
        with shelve.open(RECENT_BUYS_DB) as db:
            data = db.get("data", {})
            if isinstance(data, dict):
                RECENT_BUYS.update(data)
    except Exception:
        pass

def persist_recent_buys():
    try:
        with shelve.open(RECENT_BUYS_DB) as db:
            db["data"] = RECENT_BUYS
    except Exception:
        pass

load_recent_buys()

def send_telegram(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("TEL MSG:", text)
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        r = _public_session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.status_code == 200
    except Exception as e:
        print("Telegram error", e)
        return False

# -------------------------
# math / indicators
# -------------------------
def pct_change(open_p: float, close_p: float) -> float:
    try:
        if open_p == 0:
            return 0.0
        return (close_p - open_p) / open_p * 100.0
    except Exception:
        return 0.0

def ema_local(values, period):
    if not values or period <= 0 or len(values) < 1:
        return None
    alpha = 2.0 / (period + 1.0)
    e = float(values[0])
    for v in values[1:]:
        e = alpha * float(v) + (1 - alpha) * e
    return e

def compute_recent_volatility(closes, lookback=5):
    if not closes or len(closes) < 2:
        return None
    rets = []
    for i in range(1, len(closes)):
        prev = float(closes[i - 1]); cur = float(closes[i])
        if prev <= 0:
            continue
        rets.append((cur - prev) / prev)
    if not rets:
        return None
    recent = rets[-lookback:]
    if len(recent) == 1:
        return abs(recent[0])
    try:
        return abs(min(statistics.pstdev(recent), 5.0))
    except Exception:
        return abs(statistics.stdev(recent)) if len(recent) > 1 else abs(recent[-1])

# -------------------------
# orderbook balance
# -------------------------
def orderbook_bullish(ob, depth=5, min_imbalance=1.1, max_spread_pct=3.0, min_quote_depth=100.0):
    try:
        bids = ob.get("bids", [])[:depth]
        asks = ob.get("asks", [])[:depth]
        if not bids or not asks:
            return False
        top_bid = float(bids[0][0]); top_ask = float(asks[0][0])
        spread_pct = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
        bid_quote = sum(float(b[0]) * float(b[1]) for b in bids) + 1e-12
        ask_quote = sum(float(a[0]) * float(a[1]) for a in asks) + 1e-12
        if bid_quote < min_quote_depth:
            return False
        imbalance = bid_quote / ask_quote
        return (imbalance >= min_imbalance) and (spread_pct <= max_spread_pct)
    except Exception:
        return False

# -------------------------
# binance client helpers
# -------------------------
_binance_client = None
_symbol_info_cache = {}

def init_binance_client():
    global _binance_client
    if _binance_client:
        return _binance_client
    if not Client:
        print("python-binance not installed")
        return None
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("Binance keys not set")
        return None
    _binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    return _binance_client

def safe_api_call(fn):
    def wrapper(*a, **kw):
        global RATE_LIMIT_BACKOFF
        try:
            return fn(*a, **kw)
        except BinanceAPIException as e:
            err = str(e)
            send_telegram(f"⚠️ BinanceAPIException in {fn.__name__}: {err}")
            if any(x in err for x in ('-1003', 'Too much request weight', 'Way too much request weight', 'Request has been rejected')):
                prev = RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP
                RATE_LIMIT_BACKOFF = min(prev * 2, RATE_LIMIT_BACKOFF_MAX)
                send_telegram(f"❗ Rate-limit detected, backing off {RATE_LIMIT_BACKOFF}s.")
            raise
        except Exception as e:
            send_telegram(f"⚠️ Unexpected API error in {fn.__name__}: {e}")
            raise
    return wrapper

@safe_api_call
def fetch_exchange_info():
    client = init_binance_client()
    if client:
        return client.get_exchange_info()
    r = _public_session.get(BINANCE_REST + "/api/v3/exchangeInfo", timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_symbol_info(symbol):
    ent = _symbol_info_cache.get(symbol)
    if ent:
        return ent
    try:
        info = fetch_exchange_info()
        for s in info.get("symbols", []):
            if s.get("symbol") == symbol:
                _symbol_info_cache[symbol] = s
                return s
    except Exception:
        pass
    return None

def _normalize_ts(v):
    try:
        v = float(v)
        return v / 1000.0 if v > 1e11 else v
    except Exception:
        return None

def format_price(value, tick_size):
    try:
        tick = Decimal(str(tick_size))
        precision = max(0, -tick.as_tuple().exponent)
        q = Decimal(str(value)).quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN)
        return format(q, f'.{precision}f')
    except Exception:
        return f"{float(value):.8f}"

def format_qty(qty: float, step: float) -> str:
    try:
        if not step or float(step) == 0.0:
            return format(Decimal(str(qty)).normalize(), 'f')
        q = Decimal(str(qty)); s = Decimal(str(step))
        multiples = (q // s)
        quant = multiples * s
        precision = max(0, -s.as_tuple().exponent)
        if quant < Decimal('0'):
            quant = Decimal('0')
        return format(quant, f'.{precision}f')
    except Exception:
        return "0"

def round_step(n, step):
    try:
        if not step or step == 0:
            return n
        s = float(step)
        return math.floor(n / s) * s
    except Exception:
        return n

def ceil_step(n, step):
    try:
        if not step or step == 0:
            return n
        s = float(step)
        return math.ceil(n / s) * s
    except Exception:
        return n

def get_filters(symbol_info):
    fs = {f.get('filterType'): f for f in symbol_info.get('filters', [])} if symbol_info else {}
    lot = fs.get('LOT_SIZE') or fs.get('MARKET_LOT_SIZE')
    pricef = fs.get('PRICE_FILTER')
    mn = fs.get('MIN_NOTIONAL') or fs.get('NOTIONAL')
    min_notional = None
    if mn:
        # minNotional could be string
        try:
            min_notional = float(mn.get('minNotional') or mn.get('minNotional') or mn.get('minNotional', 0))
        except Exception:
            try:
                min_notional = float(mn.get('maxNotional', 0))
            except Exception:
                min_notional = None
    return {
        'stepSize': float(lot.get('stepSize')) if lot else 0.0,
        'minQty': float(lot.get('minQty')) if lot else 0.0,
        'tickSize': float(pricef.get('tickSize')) if pricef else 0.0,
        'minNotional': min_notional
    }

# -------------------------
# public data (cached)
# -------------------------
def fetch_tickers():
    global _ticker_cache, _ticker_cache_ts, RATE_LIMIT_BACKOFF
    now = time.time()
    if RATE_LIMIT_BACKOFF and now - _ticker_cache_ts < RATE_LIMIT_BACKOFF:
        return _ticker_cache or []
    if _ticker_cache is None or now - _ticker_cache_ts > CACHE_TTL:
        try:
            r = _public_session.get(BINANCE_REST + "/api/v3/ticker/24hr", timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            _ticker_cache = r.json()
            _ticker_cache_ts = now
        except Exception as e:
            print("fetch_tickers err", e)
            return _ticker_cache or []
    return _ticker_cache or []

def fetch_klines(symbol, interval, limit):
    key = f"klines:{symbol}:{interval}:{limit}"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        r = _public_session.get(BINANCE_REST + "/api/v3/klines", params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        cache_set(key, data)
        return data
    except Exception:
        return []

def fetch_order_book(symbol, limit=5):
    key = f"depth:{symbol}:{limit}"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        r = _public_session.get(BINANCE_REST + "/api/v3/depth", params={"symbol": symbol, "limit": max(5, limit)}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        d = r.json()
        cache_set(key, d)
        return d
    except Exception:
        return {}

# -------------------------
# evaluation
# -------------------------
def evaluate_symbol(sym, last_price, qvol, change_24h):
    try:
        if not (PRICE_MIN <= last_price <= PRICE_MAX):
            return None
        if qvol < MIN_VOLUME:
            return None
        if change_24h < 0.5 or change_24h > 20.0:
            return None
        kl5 = fetch_klines(sym, "5m", KLINES_5M_LIMIT)
        kl1 = fetch_klines(sym, "1m", KLINES_1M_LIMIT)
        ob = fetch_order_book(sym, limit=5)
        if not kl5 or len(kl5) < 3 or not kl1 or len(kl1) < 2:
            return None
        closes_5m = [float(k[4]) for k in kl5]
        closes_1m = [float(k[4]) for k in kl1]
        pct_5m = pct_change(float(kl5[0][1]), closes_5m[-1])
        pct_1m = pct_change(float(kl1[0][1]), closes_1m[-1])
        if pct_5m < MIN_5M_PCT or pct_1m < MIN_1M_PCT:
            return None
        vol_5m = compute_recent_volatility(closes_5m)
        short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
        long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
        ema_ok = bool(short_ema and long_ema and short_ema > long_ema * 1.0005)
        ema_uplift = (short_ema - long_ema) / (long_ema + 1e-12) if short_ema and long_ema else 0.0
        ob_bull = orderbook_bullish(ob, depth=5, min_imbalance=1.1, max_spread_pct=3.0)
        score = max(0.0, pct_5m) * 4.0 + max(0.0, pct_1m) * 2.0 + ema_uplift * 500.0 + max(0.0, change_24h) * 0.5
        if vol_5m is not None:
            score += max(0.0, (0.01 - min(vol_5m, 0.01))) * 100.0
        if ob_bull:
            score += 25.0
        strong_candidate = (pct_5m >= MIN_5M_PCT and pct_1m >= MIN_1M_PCT and ema_ok and ob_bull)
        return {"symbol": sym, "last_price": last_price, "24h_change": change_24h, "pct_5m": pct_5m, "pct_1m": pct_1m, "vol_5m": vol_5m, "ema_ok": ema_ok, "ema_uplift": ema_uplift, "ob_bull": ob_bull, "score": score, "strong_candidate": strong_candidate}
    except Exception:
        return None

# -------------------------
# pick_coin
# -------------------------
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
        pre.append((sym, last, qvol, ch))
    if not pre:
        return None
    pre.sort(key=lambda x: x[2], reverse=True)
    candidates = pre[:TOP_BY_24H_VOLUME]
    results = []
    max_workers = max(1, min(len(candidates), PUBLIC_CONCURRENCY))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(evaluate_symbol, sym, last, qvol, ch): sym for (sym, last, qvol, ch) in candidates}
        for fut in as_completed(futures):
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception:
                continue
    if not results:
        return None
    strongs = [r for r in results if r["strong_candidate"]]
    chosen_pool = strongs if strongs else results
    chosen = sorted(chosen_pool, key=lambda x: x["score"], reverse=True)[0]
    msg = (f"🚀 *COIN SIGNAL*: `{chosen['symbol']}`\n"
           f"Price: `{chosen['last_price']}`\n"
           f"24h Change: `{chosen['24h_change']}`%\n"
           f"5m Change: `{chosen['pct_5m']:.2f}`%\n"
           f"1m Change: `{chosen['pct_1m']:.2f}`%\n"
           f"Volatility 5m: `{chosen['vol_5m']}`\n"
           f"EMA OK: `{chosen['ema_ok']}` Uplift: `{chosen['ema_uplift']:.4f}`\n"
           f"Orderbook Bullish: `{chosen['ob_bull']}`\n"
           f"Score: `{chosen['score']:.2f}`")
    with RECENT_BUYS_LOCK:
        RECENT_BUYS[chosen["symbol"]] = {"ts": time.time(), "reserved": False, "closed": False}
        persist_recent_buys()
    send_telegram(msg)
    return chosen

# -------------------------
# price helpers
# -------------------------
def get_price_cached(symbol) -> Optional[float]:
    now = time.time()
    ent = _per_symbol_price_cache.get(symbol)
    if ent and now - ent[1] < PRICE_CACHE_TTL:
        return ent[0]
    try:
        tickers = fetch_tickers()
        for t in tickers:
            if t.get("symbol") == symbol:
                p = float(t.get("lastPrice") or t.get("price") or 0.0)
                _per_symbol_price_cache[symbol] = (p, now)
                return p
    except Exception:
        pass
    try:
        client = init_binance_client()
        if client:
            res = client.get_symbol_ticker(symbol=symbol)
            p = float(res.get("price") or 0.0)
            _per_symbol_price_cache[symbol] = (p, now)
            return p
    except Exception:
        pass
    return None

def get_free_usdt():
    try:
        client = init_binance_client()
        if not client:
            return 0.0
        bal = client.get_asset_balance(asset=QUOTE)
        return float(bal.get('free') or 0.0)
    except Exception:
        return 0.0

def get_free_asset(asset):
    try:
        client = init_binance_client()
        if not client:
            return 0.0
        bal = client.get_asset_balance(asset=asset)
        return float(bal.get('free') or 0.0)
    except Exception:
        return 0.0

# -------------------------
# order placement
# -------------------------
def _parse_market_buy_exec(order_resp) -> Tuple[Optional[float], Optional[float]]:
    try:
        if not order_resp:
            return None, None
        fills = order_resp.get("fills") or []
        if fills:
            total_q = 0.0; total_quote = 0.0
            for f in fills:
                q = float(f.get("qty") or 0); p = float(f.get("price") or 0)
                total_q += q; total_quote += q * p
            if total_q > 0:
                return total_q, total_quote / total_q
        exec_qty = order_resp.get("executedQty") or order_resp.get("executedQty")
        if exec_qty:
            exec_qty = float(exec_qty)
            cquote = float(order_resp.get("cummulativeQuoteQty") or order_resp.get("cumQuote") or 0)
            if exec_qty and cquote:
                return exec_qty, cquote / exec_qty
    except Exception:
        pass
    return None, None

def place_limit_sell(symbol, qty, price):
    client = init_binance_client()
    if not client:
        raise RuntimeError("client missing")
    info = get_symbol_info(symbol)
    f = get_filters(info) if info else {}
    qty_str = format_qty(qty, f.get('stepSize', 0.0))
    price_str = format_price(price, f.get('tickSize', 0.0))
    if not qty_str or float(qty_str) <= 0:
        raise RuntimeError("qty invalid after rounding")
    return client.create_order(symbol=symbol, side='SELL', type='LIMIT', timeInForce='GTC', quantity=qty_str, price=price_str)

def place_safe_market_buy(symbol, usd_amount, require_orderbook=False):
    client = init_binance_client()
    if not client:
        send_telegram(f"⚠️ Trading disabled or client missing. Skipping {symbol}")
        return None, None
    info = get_symbol_info(symbol)
    if not info:
        send_telegram(f"❌ No symbol info for {symbol}")
        return None, None
    f = get_filters(info)
    if require_orderbook:
        ob = fetch_order_book(symbol, limit=5)
        if not orderbook_bullish(ob, depth=5, min_imbalance=1.05, max_spread_pct=3.0, min_quote_depth=50.0):
            send_telegram(f"⚠️ Orderbook not bullish for {symbol}")
            return None, None
    price = get_price_cached(symbol) or None
    if price is None:
        try:
            price = float(client.get_symbol_ticker(symbol=symbol).get("price") or 0.0)
        except Exception as e:
            send_telegram(f"⚠️ Failed to fetch price for {symbol}: {e}")
            return None, None
    if price <= 0:
        send_telegram(f"❌ Invalid price for {symbol}: {price}")
        return None, None

    qty_target = usd_amount / price
    qty_target = max(qty_target, f.get('minQty', 0.0))
    qty_target = round_step(qty_target, f.get('stepSize', 0.0))

    min_notional = f.get('minNotional')
    if min_notional:
        notional = qty_target * price
        if notional < (min_notional - 1e-12):
            needed_qty = ceil_step(min_notional / price, f.get('stepSize', 0.0))
            free_usdt = get_free_usdt()
            if needed_qty * price <= free_usdt + 1e-8:
                qty_target = needed_qty
                send_telegram(f"ℹ️ Increasing qty to meet minNotional for {symbol}")
            else:
                send_telegram(f"⛔ Skipping {symbol}: order notional ${notional:.6f} < minNotional ${min_notional:.6f}")
                return None, None

    qty_str = format_qty(qty_target, f.get('stepSize', 0.0))
    if not qty_str or float(qty_str) <= 0:
        send_telegram(f"❌ Computed qty invalid for {symbol}")
        return None, None

    try:
        time.sleep(random.uniform(0.05, 0.2))
        order = client.create_order(symbol=symbol, side='BUY', type='MARKET', quoteOrderQty=str(round(usd_amount, 6)))
    except Exception:
        try:
            order = client.create_order(symbol=symbol, side='BUY', type='MARKET', quantity=qty_str)
        except Exception as e2:
            send_telegram(f"❌ Market buy failed for {symbol}: {e2}")
            return None, None

    executed_qty, avg_price = _parse_market_buy_exec(order)
    if executed_qty is None or executed_qty <= 0:
        asset = symbol[:-len(QUOTE)]
        free_after = get_free_asset(asset)
        executed_qty = round_step(free_after, f.get('stepSize', 0.0)) if free_after else None
        if not executed_qty:
            send_telegram(f"❌ No executed qty for {symbol} after buy")
            return None, None
        avg_price = avg_price or price

    executed_qty = round_step(executed_qty, f.get('stepSize', 0.0))
    if executed_qty < f.get('minQty', 0.0) or executed_qty <= 0:
        send_telegram(f"❌ Executed qty too small for {symbol}")
        return None, None

    send_telegram(f"✅ BUY {symbol}: qty={executed_qty} ~price={avg_price:.8f} notional≈${executed_qty * avg_price:.6f}")
    # clear tickers cache to reflect balance change
    global _ticker_cache, _ticker_cache_ts
    _ticker_cache = None
    _ticker_cache_ts = 0
    return executed_qty, avg_price

def place_market_sell_fallback(symbol, qty, f=None):
    client = init_binance_client()
    if not client:
        send_telegram("❌ Market sell fallback: client missing")
        return None
    try:
        info = get_symbol_info(symbol)
        f = f or get_filters(info) or {}
        qty_str = format_qty(qty, f.get('stepSize', 0.0))
        send_telegram(f"⚠️ MARKET SELL fallback for {symbol}: qty={qty_str}")
        resp = client.create_order(symbol=symbol, side='SELL', type='MARKET', quantity=qty_str)
        send_telegram(f"✅ Market sell executed for {symbol}")
        return resp
    except Exception as e:
        err = str(e)
        if 'NOTIONAL' in err or '-1013' in err:
            send_telegram(f"🚫 Market sell failed for {symbol}: below minNotional. Suppressing for 24h.")
            return None
        send_telegram(f"❌ Market sell fallback failed for {symbol}: {e}")
        return None

# -------------------------
# enforcement
# -------------------------
def get_open_orders_cached(symbol=None):
    try:
        client = init_binance_client()
        if not client:
            return []
        cache_key = f"open_orders:{symbol or 'all'}"
        cached = cache_get(cache_key)
        if cached:
            return [o for o in cached if (not symbol) or o.get('symbol') == symbol]
        if symbol:
            data = client.get_open_orders(symbol=symbol)
        else:
            data = client.get_open_orders()
        cache_set(cache_key, data)
        return data or []
    except Exception:
        return []

def cancel_all_open_orders(symbol):
    client = init_binance_client()
    if not client:
        return
    try:
        client.cancel_open_orders(symbol=symbol)
        send_telegram(f"ℹ️ Cancelled open orders for {symbol}")
    except Exception as e:
        send_telegram(f"⚠️ cancel_all_open_orders failed for {symbol}: {e}")

def enforce_oco_max_life_and_exit_if_needed():
    now_ts = time.time()
    open_orders = get_open_orders_cached()
    by_symbol = {}
    for o in open_orders:
        sym = o.get("symbol")
        if not sym:
            continue
        by_symbol.setdefault(sym, []).append(o)
    for sym, orders in by_symbol.items():
        oldest_ts = None
        for o in orders:
            ts = None
            for k in ('time', 'updateTime', 'transactTime', 'timestamp', 'createTime'):
                v = o.get(k)
                n = _normalize_ts(v) if v is not None else None
                if n is not None:
                    ts = n; break
            if ts:
                oldest_ts = ts if oldest_ts is None else min(oldest_ts, ts)
        if oldest_ts and (now_ts - oldest_ts) > OCO_MAX_LIFE_SECONDS:
            send_telegram(f"⚠️ Limit sell for {sym} older than {OCO_MAX_LIFE_SECONDS}s -> cancelling and market-selling remainder.")
            try:
                cancel_all_open_orders(sym)
            except Exception:
                pass
            time.sleep(0.3)
            try:
                asset = sym[:-len(QUOTE)]
                free_qty = get_free_asset(asset)
                f = get_filters(get_symbol_info(sym) or {}) or {}
                if free_qty and free_qty >= (f.get('minQty', 0.0) or 0.0):
                    place_market_sell_fallback(sym, free_qty, f)
            except Exception as e:
                send_telegram(f"⚠️ Market-sell after enforcement failed for {sym}: {e}")

def enforce_max_hold_positions():
    now = time.time()
    with RECENT_BUYS_LOCK:
        for sym, info in list(RECENT_BUYS.items()):
            ts = info.get('ts', 0)
            if now - ts > OCO_MAX_LIFE_SECONDS and not info.get('closed'):
                send_telegram(f"⚠️ Position {sym} held >6h -> forcing exit.")
                try:
                    cancel_all_open_orders(sym)
                except Exception:
                    pass
                asset = sym[:-len(QUOTE)]
                f = get_filters(get_symbol_info(sym) or {}) or {}
                free = get_free_asset(asset)
                if free and free >= (f.get('minQty', 0.0) or 0.0):
                    place_market_sell_fallback(sym, free, f)
                RECENT_BUYS.pop(sym, None)
                persist_recent_buys()

# -------------------------
# execute trade
# -------------------------
def execute_trade(chosen):
    sym = chosen["symbol"]
    with RECENT_BUYS_LOCK:
        active = len([k for k, v in RECENT_BUYS.items() if not v.get("closed")])
        if active >= 5:
            send_telegram(f"⚠️ Max concurrent positions reached. Skipping {sym}")
            return False
        RECENT_BUYS[sym] = {"ts": time.time(), "reserved": False, "closed": False}
        persist_recent_buys()

    client = init_binance_client()
    if not client:
        send_telegram("⚠️ Trading disabled or API keys missing.")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(sym, None); persist_recent_buys()
        return False

    try:
        qty, avg_price = place_safe_market_buy(sym, TRADE_USD, require_orderbook=False)
        if not qty or not avg_price:
            with RECENT_BUYS_LOCK:
                RECENT_BUYS.pop(sym, None); persist_recent_buys()
            return False

        target_price = avg_price * (1.0 + (LIMIT_PROFIT_PCT / 100.0))
        try:
            sell_order = place_limit_sell(sym, qty, target_price)
            price_used = float(sell_order.get("price") or target_price)
            with RECENT_BUYS_LOCK:
                RECENT_BUYS[sym].update({"qty": qty, "buy_price": avg_price, "sell_price": price_used, "order_id": sell_order.get("orderId")})
                persist_recent_buys()
            send_telegram(f"💰 LIMIT SELL PLACED: `{sym}` Qty `{qty}` @ `{price_used}` (+{LIMIT_PROFIT_PCT}%)")
            return True
        except Exception as e:
            send_telegram(f"⚠️ Failed to place limit sell for {sym}: {e}. Attempting market sell fallback.")
            place_market_sell_fallback(sym, qty, get_filters(get_symbol_info(sym) or {}))
            with RECENT_BUYS_LOCK:
                RECENT_BUYS.pop(sym, None); persist_recent_buys()
            return False

    except Exception as e:
        send_telegram(f"‼️ Unexpected error during trade {sym}: {e}")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(sym, None); persist_recent_buys()
        return False

# -------------------------
# main loop / web
# -------------------------
app = Flask(__name__)
@app.route("/")
def home():
    return "Trading bot running"

def trade_cycle():
    while True:
        try:
            enforce_oco_max_life_and_exit_if_needed()
            enforce_max_hold_positions()
            with RECENT_BUYS_LOCK:
                if any(v.get("reserved") for v in RECENT_BUYS.values()):
                    time.sleep(CYCLE_SECONDS); continue

            chosen = pick_coin()
            if not chosen:
                time.sleep(CYCLE_SECONDS); continue

            send_telegram(f"ℹ️ Attempting live trade for {chosen['symbol']}")
            if ENABLE_TRADING:
                t = threading.Thread(target=execute_trade, args=(chosen,), daemon=True)
                t.start()
            else:
                send_telegram("ℹ️ ENABLE_TRADING=false, simulated only.")
        except Exception as e:
            err = str(e)
            if any(x in err for x in ('-1003', 'Too much request weight')):
                send_telegram(f"❌ Rate limit reached: {err}")
                time.sleep(RATE_LIMIT_BASE_SLEEP)
            else:
                send_telegram(f"❌ trade_cycle error: {e}")
            time.sleep(CYCLE_SECONDS)
        time.sleep(CYCLE_SECONDS)

if __name__ == "__main__":
    t = threading.Thread(target=trade_cycle, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), threaded=True)