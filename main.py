import os
import math
import time
import random
import queue
import statistics
import threading
import requests
from datetime import datetime, timedelta, timezone
from threading import Thread, Lock
from decimal import Decimal, ROUND_DOWN, ROUND_UP, getcontext
from flask import Flask
from binance.client import Client
from binance.exceptions import BinanceAPIException

# -------------------------
# CONFIG
# -------------------------
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
QUOTE = "USDT"

PRICE_MIN = 0.8
PRICE_MAX = 3.0
MIN_VOLUME = 800_000

RECENT_PCT_MIN = 0.6
RECENT_PCT_MAX = 4.0
MAX_24H_RISE_PCT = 4.0
MAX_24H_CHANGE_ABS = 5.0
MOVEMENT_MIN_PCT = 1.0

EMA_UPLIFT_MIN_PCT = 0.001
SCORE_MIN_THRESHOLD = 15.0

TRADE_USD = 8.0
SLEEP_BETWEEN_CHECKS = 8
CYCLE_DELAY = 8
COOLDOWN_AFTER_EXIT = 10

BASE_TP_PCT = 3.0
BASE_SL_PCT = 2.0

TRAIL_START_PCT = 0.8
TRAIL_DOWN_PCT = 1.5

ROLL_ON_RISE_PCT = 0.2
ROLL_TRIGGER_PCT = 0.25
ROLL_TRIGGER_DELTA_ABS = 0.001
ROLL_TP_STEP_ABS = 0.010
ROLL_SL_STEP_ABS = 0.0015
ROLL_COOLDOWN_SECONDS = 30
MAX_ROLLS_PER_POSITION = 9999999
ROLL_POST_CANCEL_JITTER = (0.25, 0.45)

ROLL_FAIL_COUNTER = {}
FAILED_ROLL_THRESHOLD = 3
FAILED_ROLL_SKIP_SECONDS = 3600

NOTIFY_QUEUE_MAX = 1000
NOTIFY_RETRY = 2
NOTIFY_TIMEOUT = 4.0
NOTIFY_PRIORITY_SUPPRESS = 0.08

client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
start_balance_usdt = None

TEMP_SKIP = {}
SKIP_SECONDS_ON_MARKET_CLOSED = 3600

getcontext().prec = 28

METRICS = {}
METRICS_LOCK = Lock()

EAT_TZ = timezone(timedelta(hours=3))

RECENT_BUYS = {}
REBUY_COOLDOWN = 3600
LOSS_COOLDOWN = 14400
REBUY_MAX_RISE_PCT = 5.0

RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_BACKOFF_MAX = 300
RATE_LIMIT_BASE_SLEEP = 120
CACHE_TTL = 280

_NOTIFY_Q = queue.Queue(maxsize=globals().get('NOTIFY_QUEUE_MAX', 1000))
_NOTIFY_THREAD_STARTED = False
_NOTIFY_LOCK = Lock()
_LAST_NOTIFY_NONPRIO = 0.0

def _send_telegram(text: str):
    BOT_TOK = globals().get('BOT_TOKEN')
    CHAT_ID_LOCAL = globals().get('CHAT_ID')
    if not BOT_TOK or not CHAT_ID_LOCAL:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOK}/sendMessage"
    data = {"chat_id": CHAT_ID_LOCAL, "text": text}
    retries = int(globals().get('NOTIFY_RETRY', 2))
    timeout = float(globals().get('NOTIFY_TIMEOUT', 4.0))
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, data=data, timeout=timeout)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.25 * attempt)
    return False

def notify(msg: str, priority: bool = False, category: str = None):
    now_ts = time.time()
    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    try:
        print(text)
    except Exception:
        pass

    # send immediate if forced priority
    if priority:
        try:
            Thread(target=_send_telegram, args=(text,), daemon=True).start()
        except Exception:
            pass
        return

    # allow only selected prefixes or daily category
    allow = False
    try:
        if category == 'daily':
            allow = True
        else:
            if isinstance(msg, str):
                if msg.startswith("✅ BUY"):
                    allow = True
                elif msg.startswith("✅ Position closed"):
                    allow = True
                elif msg.startswith("🔁 Moved"):
                    allow = True
                elif msg.startswith("📌 OCO "):
                    allow = True
                elif msg.startswith("💸 Profit"):
                    allow = True
                elif msg.startswith("⚠️") or msg.startswith("❌"):
                    allow = True
    except Exception:
        allow = False

    if not allow:
        # suppressed for Telegram but printed locally
        return

    # throttle non-priority slightly
    suppress = float(globals().get('NOTIFY_PRIORITY_SUPPRESS', 0.08))
    global _LAST_NOTIFY_NONPRIO
    try:
        if now_ts - _LAST_NOTIFY_NONPRIO < suppress:
            return
        _LAST_NOTIFY_NONPRIO = now_ts
    except Exception:
        _LAST_NOTIFY_NONPRIO = now_ts

    try:
        _start_notify_thread()
        _NOTIFY_Q.put_nowait(text)
    except queue.Full:
        try:
            Thread(target=_send_telegram, args=(text,), daemon=True).start()
        except Exception:
            pass
    except Exception:
        try:
            Thread(target=_send_telegram, args=(text,), daemon=True).start()
        except Exception:
            pass

def format_price(value, tick_size):
    try:
        tick = Decimal(str(tick_size))
        precision = max(0, -tick.as_tuple().exponent)
        return format(
            Decimal(str(value)).quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN),
            f'.{precision}f'
        )
    except Exception:
        return f"{value:.8f}"

def format_qty(qty: float, step: float) -> str:
    try:
        if not step or float(step) == 0.0:
            return format(Decimal(str(qty)), 'f')
        q = Decimal(str(qty))
        s = Decimal(str(step))
        multiples = (q // s)
        quant = multiples * s
        precision = max(0, -s.as_tuple().exponent)
        if quant < Decimal('0'):
            quant = Decimal('0')
        return format(quant, f'.{precision}f')
    except Exception:
        try:
            return str(math.floor(qty))
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

# Decorator to centralize BinanceAPIException handling and set RATE_LIMIT_BACKOFF/TEMP_SKIP
def safe_api_call(func):
    def wrapper(*args, **kwargs):
        global RATE_LIMIT_BACKOFF
        try:
            if not rest_allowed():
                raise Exception("REST calls currently banned by GLOBAL_REST_BAN_UNTIL")
            return func(*args, **kwargs)
        except BinanceAPIException as e:
            err = str(e)
            notify(f"⚠️ BinanceAPIException in {func.__name__}: {err}")
            if 'Way too much request weight' in err or '-1003' in err or 'Too much request weight' in err:
                set_rest_ban_from_error(err)
            # existing rate-limit logic...
            raise
        except Exception as e:
            notify(f"⚠️ Unexpected API error in {func.__name__}: {e}")
            raise
    return wrapper
    
# -------------------------
# BALANCES / FILTERS
# -------------------------
def get_free_usdt():
    try:
        bal = client.get_asset_balance(asset=QUOTE)
        return float(bal['free'])
    except Exception:
        return 0.0

def get_free_asset(asset):
    try:
        bal = client.get_asset_balance(asset=asset)
        return float(bal['free'])
    except Exception:
        return 0.0

def get_filters(symbol_info):
    fs = {f.get('filterType'): f for f in symbol_info.get('filters', [])}
    lot = fs.get('LOT_SIZE')
    pricef = fs.get('PRICE_FILTER')
    min_notional = None
    if fs.get('MIN_NOTIONAL'):
        min_notional = fs.get('MIN_NOTIONAL', {}).get('minNotional')
    elif fs.get('NOTIONAL'):
        min_notional = fs.get('NOTIONAL', {}).get('minNotional')
    return {
        'stepSize': float(lot['stepSize']) if lot else 0.0,
        'minQty': float(lot['minQty']) if lot else 0.0,
        'tickSize': float(pricef['tickSize']) if pricef else 0.0,
        'minNotional': float(min_notional) if min_notional else None
    }
# --- Compatibility wrapper so trade_cycle can keep using get_trade_candidates() ---
def get_trade_candidates():
    """
    Compatibility wrapper: reuses pick_coin() (which exists in this file)
    and returns a list of (symbol, score_data) so existing trade_cycle can work unchanged.
    """
    try:
        candidate = pick_coin()
    except Exception:
        candidate = None

    if not candidate:
        return []

    # pick_coin returns (symbol, price, qvol, change, closes)
    try:
        symbol, price, qvol, change, closes = candidate
    except Exception:
        # fallback in case signature differs
        return []

    score_data = {
        'usd': globals().get('DEFAULT_USD_PER_TRADE', TRADE_USD if 'TRADE_USD' in globals() else 7.0),
        'price': price,
        'qvol': qvol,
        'change': change,
        'closes': closes,
    }
    return [(symbol, score_data)]

def get_current_position_qty(symbol) -> float:
    try:
        asset = symbol[:-len(QUOTE)]
        bal = client.get_asset_balance(asset=asset)
        if not bal:
            return 0.0
        free = float(bal.get('free') or 0.0)
        locked = float(bal.get('locked') or 0.0)
        return free + locked
    except Exception:
        try:
            # fallback: try get_free_asset only
            return float(get_free_asset(asset))
        except Exception:
            return 0.0

def get_last_fill_price(symbol: str):
    try:
        trades = client.get_my_trades(symbol=symbol, limit=10)
        if not trades:
            return None
        # trades usually ordered by time ascending; safe pick last element
        last = trades[-1]
        price = float(last.get('price') or 0.0)
        return price
    except Exception:
        return None
        
def round_price_step(price: float, symbol: str):
    try:
        info = get_symbol_info_cached(symbol)
        f = get_filters(info) if info else {}
        tick = f.get('tickSize') or 0.0
        if not tick or tick == 0:
            return float(price)
        # determine precision from tick (Decimal trick)
        t = Decimal(str(tick))
        precision = max(0, -t.as_tuple().exponent)
        dec = Decimal(str(price)).quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN)
        return float(dec)
    except Exception:
        try:
            return float(round(price, 8))
        except Exception:
            return price
            
# -------------------------
# TRANSFERS
# -------------------------
def send_profit_to_funding(amount, asset='USDT'):
    try:
        result = client.universal_transfer(
            type='MAIN_FUNDING' if hasattr(client, 'universal_transfer') else 'SPOT_TO_FUNDING',
            asset=asset,
            amount=str(round(amount, 6))
        )
        notify(f"💸 Profit ${amount:.6f} transferred to funding wallet.")
        return result
    except Exception as e:
        try:
            result = client.universal_transfer(
                type='SPOT_TO_FUNDING',
                asset=asset,
                amount=str(round(amount, 6))
            )
            notify(f"💸 Profit ${amount:.6f} transferred to funding wallet (fallback).")
            return result
        except Exception as e2:
            notify(f"❌ Failed to transfer profit: {e} | {e2}")
            return None

# -------------------------
# CACHES & UTIL
# -------------------------

TICKER_CACHE = None
LAST_FETCH = 0
SYMBOL_INFO_CACHE = {}
SYMBOL_INFO_TTL = 120
OPEN_ORDERS_CACHE = {'ts': 0, 'data': None}
OPEN_ORDERS_TTL = 80

def get_symbol_info_cached(symbol, ttl=SYMBOL_INFO_TTL):
    now = time.time()
    ent = SYMBOL_INFO_CACHE.get(symbol)
    if ent and now - ent[1] < ttl:
        return ent[0]
    try:
        info = client.get_symbol_info(symbol)
        SYMBOL_INFO_CACHE[symbol] = (info, now)
        return info
    except Exception as e:
        notify(f"⚠️ Failed to fetch symbol info for {symbol}: {e}")
        return None

# thread-safe OPEN_ORDERS cache + helper
OPEN_ORDERS_LOCK = threading.Lock()

def get_open_orders_cached(symbol=None):
    now = time.time()
    with OPEN_ORDERS_LOCK:
        if OPEN_ORDERS_CACHE.get('data') is not None and now - OPEN_ORDERS_CACHE.get('ts', 0) < OPEN_ORDERS_TTL:
            data = OPEN_ORDERS_CACHE['data']
            if symbol:
                return [o for o in (data or []) if o.get('symbol') == symbol]
            return data or []

    try:
        if symbol:
            data = client.get_open_orders(symbol=symbol)
        else:
            data = client.get_open_orders()
        with OPEN_ORDERS_LOCK:
            OPEN_ORDERS_CACHE['data'] = data
            OPEN_ORDERS_CACHE['ts'] = now
        return data or []
    except Exception as e:
        notify(f"⚠️ Failed to fetch open orders: {e}")
        # on failure return last cached (if any) to avoid breaking callers
        with OPEN_ORDERS_LOCK:
            return OPEN_ORDERS_CACHE.get('data') or []

# GLOBAL rest-ban guard + helper
GLOBAL_REST_BAN_UNTIL = 0.0
TICKER_CACHE_LOCK = threading.Lock()

def set_rest_ban_from_error(err_text: str):
    """
    Try to parse a Binance "banned until <timestamp>" message and set GLOBAL_REST_BAN_UNTIL.
    Returns True if parsed a timestamp, False if fallback used.
    """
    global GLOBAL_REST_BAN_UNTIL
    try:
        import re, time
        # try find a long integer timestamp (ms or s) after word 'until'
        m = re.search(r'until\s+(\d{10,13})', err_text)
        if m:
            ts = int(m.group(1))
            if ts > 1e12:  # ms
                ts = ts / 1000.0
            GLOBAL_REST_BAN_UNTIL = float(ts)
            notify(f"⚠️ GLOBAL_REST_BAN_UNTIL set to {GLOBAL_REST_BAN_UNTIL} ({datetime.utcfromtimestamp(GLOBAL_REST_BAN_UNTIL).isoformat()} UTC)")
            return True
    except Exception:
        pass
    # fallback: set a conservative short ban (5 minutes)
    try:
        GLOBAL_REST_BAN_UNTIL = time.time() + 300
    except Exception:
        GLOBAL_REST_BAN_UNTIL = time.time() + 300
    notify(f"⚠️ GLOBAL_REST_BAN_UNTIL fallback set to {GLOBAL_REST_BAN_UNTIL} (5m)")
    return False

def rest_allowed() -> bool:
    """
    Return True if it's OK to make REST calls (current time >= GLOBAL_REST_BAN_UNTIL).
    """
    try:
        return time.time() >= (GLOBAL_REST_BAN_UNTIL or 0.0)
    except Exception:
        return True

PER_SYMBOL_PRICE_CACHE = {}  # symbol -> (price, ts)
PRICE_CACHE_TTL = 2.0 

def get_tickers_cached():
    global TICKER_CACHE, LAST_FETCH, RATE_LIMIT_BACKOFF, GLOBAL_REST_BAN_UNTIL

    now = time.time()

    # if currently banned by global flag, return cached immediately and avoid REST calls
    if GLOBAL_REST_BAN_UNTIL and now < GLOBAL_REST_BAN_UNTIL:
        notify(f"⏸️ REST calls banned until {datetime.utcfromtimestamp(GLOBAL_REST_BAN_UNTIL).isoformat()} UTC; returning cached tickers.")
        with TICKER_CACHE_LOCK:
            return TICKER_CACHE or []

    # if we're in a RATE_LIMIT_BACKOFF window, avoid calling until it expires (use cached)
    if RATE_LIMIT_BACKOFF and (now - LAST_FETCH) < RATE_LIMIT_BACKOFF:
        with TICKER_CACHE_LOCK:
            return TICKER_CACHE or []

    # if cache still valid, return it
    with TICKER_CACHE_LOCK:
        if TICKER_CACHE is not None and (now - LAST_FETCH) < CACHE_TTL:
            return TICKER_CACHE or []

    # attempt fetching fresh tickers (guarded)
    try:
        tickers = client.get_ticker()
        now = time.time()
        with TICKER_CACHE_LOCK:
            TICKER_CACHE = tickers or []
            LAST_FETCH = now
            # reset rate-limit backoff on success
            RATE_LIMIT_BACKOFF = 0
        return TICKER_CACHE or []
    except BinanceAPIException as e:
        err = str(e)
        # detect explicit "way too much request weight" / ban message
        if 'Way too much request weight' in err or '-1003' in err or 'Too much request weight' in err:
            parsed = set_rest_ban_from_error(err)
            # escalate RATE_LIMIT_BACKOFF defensively
            prev = RATE_LIMIT_BACKOFF if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP
            RATE_LIMIT_BACKOFF = min(prev * 2 if prev else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
            notify(f"⚠️ Rate limit / ban detected in get_tickers_cached: {err}. Backing off {RATE_LIMIT_BACKOFF}s.")
            with TICKER_CACHE_LOCK:
                return TICKER_CACHE or []
        else:
            # other Binance error: increase backoff a bit to be safe
            prev = RATE_LIMIT_BACKOFF if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP
            RATE_LIMIT_BACKOFF = min(prev * 2 if prev else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
            notify(f"⚠️ BinanceAPIException while fetching tickers: {err} — backing off {RATE_LIMIT_BACKOFF}s.")
            with TICKER_CACHE_LOCK:
                return TICKER_CACHE or []
    except Exception as e:
        err = str(e)
        # generic network / requests error; set a modest backoff and return cached
        prev = RATE_LIMIT_BACKOFF if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP
        RATE_LIMIT_BACKOFF = min(prev * 2 if prev else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
        notify(f"⚠️ Failed to refresh tickers: {err} — backing off {RATE_LIMIT_BACKOFF}s.")
        with TICKER_CACHE_LOCK:
            return TICKER_CACHE or []
            
def get_price_cached(symbol):
    """
    First try per-symbol short TTL cache, then fall back to TICKER_CACHE; last fallback uses client.get_symbol_ticker.
    """
    now = time.time()
    ent = PER_SYMBOL_PRICE_CACHE.get(symbol)
    if ent and now - ent[1] < PRICE_CACHE_TTL:
        return ent[0]
    # try ticker cache first
    tickers = get_tickers_cached() or []
    if tickers:
        for t in tickers:
            if t.get('symbol') == symbol:
                try:
                    p = float(t.get('lastPrice') or t.get('price') or 0.0)
                    PER_SYMBOL_PRICE_CACHE[symbol] = (p, now)
                    return p
                except Exception:
                    break
    # last resort: direct symbol ticker
    try:
        res = client.get_symbol_ticker(symbol=symbol)
        price = float(res.get('price') or res.get('lastPrice') or 0.0)
        PER_SYMBOL_PRICE_CACHE[symbol] = (price, now)
        return price
    except Exception as e:
        notify(f"⚠️ get_price_cached fallback failed for {symbol}: {e}")
        return None

def cleanup_temp_skip():
    now = time.time()
    for s, until in list(TEMP_SKIP.items()):
        if now >= until:
            del TEMP_SKIP[s]

def cleanup_recent_buys():
    now = time.time()
    for s, info in list(RECENT_BUYS.items()):
        cd = info.get('cooldown', REBUY_COOLDOWN)
        if now >= info['ts'] + cd:
            del RECENT_BUYS[s]

def calculate_realized_profit(symbol: str, sold_amount: float = None) -> float:
    try:
        trades = client.get_my_trades(symbol=symbol)
    except Exception:
        return 0.0

    # convert trades to list of (side, qty, price, quote)
    sells = []
    buys = []
    for t in trades:
        try:
            qty = float(t.get('qty') or t.get('executedQty') or 0.0)
            price = float(t.get('price') or 0.0)
            quote = qty * price
            is_buyer = t.get('isBuyer') in (True, 'true', 'True', 1)
            if is_buyer:
                buys.append((qty, price, quote))
            else:
                sells.append((qty, price, quote))
        except Exception:
            continue

    # if no sells, profit 0
    if not sells:
        return 0.0

    # if user provided sold_amount, limit to that amount (sum earliest sells)
    total_sold = sum(q for q,_,_ in sells)
    if sold_amount and sold_amount > 0 and sold_amount < total_sold:
        remaining = sold_amount
        proceeds = 0.0
        proportion = 0.0
        for q, p, quote in sells:
            take = min(q, remaining)
            proceeds += take * p
            remaining -= take
            if remaining <= 1e-12:
                break
    else:
        proceeds = sum(q*p for q,p,_ in sells)

    # approximate cost basis: proportionally consume buys to match sold qty
    # We'll approximate average buy price by total buy quote / total buy qty.
    total_buy_qty = sum(q for q,_,_ in buys)
    total_buy_quote = sum(quote for _,_,quote in buys)
    if total_buy_qty > 0:
        avg_buy_price = (total_buy_quote / total_buy_qty)
    else:
        avg_buy_price = None

    # if avg_buy_price known, estimate profit = proceeds - sold_qty * avg_buy_price
    sold_qty_used = sold_amount if (sold_amount and sold_amount > 0) else sum(q for q,_,_ in sells)
    if avg_buy_price:
        profit = proceeds - (sold_qty_used * avg_buy_price)
        return float(profit)
    else:
        # fallback: use difference between sells and buys totals
        total_sell_quote = sum(q*p for q,p,_ in sells)
        profit_est = total_sell_quote - total_buy_quote
        return float(profit_est)
        
# ---------- REST ban guard (ensure placed near other global ban helpers) ----------
GLOBAL_REST_BAN_UNTIL = globals().get('GLOBAL_REST_BAN_UNTIL', 0.0)
KLINES_CACHE = {}           # symbol|interval|limit -> (klines, ts)
KLINES_CACHE_LOCK = threading.Lock()
KLINES_CACHE_TTL = 12.0     # seconds (tune: higher reduces REST weight)

def get_klines_cached(symbol, interval='5m', limit=6, force_refresh=False):
    global KLINES_CACHE, RATE_LIMIT_BACKOFF, GLOBAL_REST_BAN_UNTIL

    key = f"{symbol}:{interval}:{limit}"
    now = time.time()

    # if banned globally, avoid REST calls and return cached if any
    if GLOBAL_REST_BAN_UNTIL and now < GLOBAL_REST_BAN_UNTIL:
        notify(f"⏸️ REST banned until {datetime.utcfromtimestamp(GLOBAL_REST_BAN_UNTIL).isoformat()} UTC; using cached klines for {symbol} if available.")
        with KLINES_CACHE_LOCK:
            ent = KLINES_CACHE.get(key)
            return ent[0] if ent else None

    # if in RATE_LIMIT_BACKOFF window, return cache if available
    if RATE_LIMIT_BACKOFF and (now - globals().get('LAST_FETCH', 0)) < RATE_LIMIT_BACKOFF:
        with KLINES_CACHE_LOCK:
            ent = KLINES_CACHE.get(key)
            return ent[0] if ent else None

    # return cached if fresh unless force_refresh
    with KLINES_CACHE_LOCK:
        ent = KLINES_CACHE.get(key)
        if ent and not force_refresh and (now - ent[1]) < KLINES_CACHE_TTL:
            return ent[0]

    # attempt fetch with conservative retry
    try:
        klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
        now = time.time()
        with KLINES_CACHE_LOCK:
            KLINES_CACHE[key] = (klines or [], now)
            globals()['LAST_FETCH'] = now
            # reset rate-limit backoff on success
            globals()['RATE_LIMIT_BACKOFF'] = 0
        return klines or []
    except BinanceAPIException as e:
        err = str(e)
        # parse explicit ban / too many weight messages
        if 'Too much request weight' in err or 'Way too much request weight' in err or '-1003' in err:
            # try to parse ban timestamp (ms) then set GLOBAL_REST_BAN_UNTIL
            try:
                import re
                m = re.search(r'until\s+(\d{10,13})', err)
                if m:
                    ts = int(m.group(1))
                    if ts > 1e12:
                        ts = ts / 1000.0
                    globals()['GLOBAL_REST_BAN_UNTIL'] = float(ts)
                    notify(f"⚠️ Detected BINANCE BAN until {datetime.utcfromtimestamp(ts).isoformat()} UTC from kline error.")
                else:
                    # fallback short conservative ban
                    globals()['GLOBAL_REST_BAN_UNTIL'] = time.time() + 300
                    notify("⚠️ GLOBAL_REST_BAN_UNTIL fallback set to +5m due to kline rate limit.")
            except Exception:
                globals()['GLOBAL_REST_BAN_UNTIL'] = time.time() + 300
            # escalate local RATE_LIMIT_BACKOFF defensively
            prev = globals().get('RATE_LIMIT_BACKOFF') or globals().get('RATE_LIMIT_BASE_SLEEP', 120)
            globals()['RATE_LIMIT_BACKOFF'] = min(prev * 2 if prev else globals().get('RATE_LIMIT_BASE_SLEEP', 120), globals().get('RATE_LIMIT_BACKOFF_MAX', 300))
            notify(f"⚠️ Rate limit while fetching klines for {symbol}: {err}. Backing off {globals()['RATE_LIMIT_BACKOFF']}s.")
            with KLINES_CACHE_LOCK:
                ent = KLINES_CACHE.get(key)
                return ent[0] if ent else None
        else:
            # other Binance errors: backoff and return cached if available
            prev = globals().get('RATE_LIMIT_BACKOFF') or globals().get('RATE_LIMIT_BASE_SLEEP', 120)
            globals()['RATE_LIMIT_BACKOFF'] = min(prev * 2 if prev else globals().get('RATE_LIMIT_BASE_SLEEP', 120), globals().get('RATE_LIMIT_BACKOFF_MAX', 300))
            notify(f"⚠️ Error while fetching klines for {symbol}: {err}. Backing off {globals()['RATE_LIMIT_BACKOFF']}s.")
            with KLINES_CACHE_LOCK:
                ent = KLINES_CACHE.get(key)
                return ent[0] if ent else None
    except Exception as e:
        prev = globals().get('RATE_LIMIT_BACKOFF') or globals().get('RATE_LIMIT_BASE_SLEEP', 120)
        globals()['RATE_LIMIT_BACKOFF'] = min(prev * 2 if prev else globals().get('RATE_LIMIT_BASE_SLEEP', 120), globals().get('RATE_LIMIT_BACKOFF_MAX', 300))
        notify(f"⚠️ Generic failure fetching klines for {symbol}: {e}. Backing off {globals()['RATE_LIMIT_BACKOFF']}s.")
        with KLINES_CACHE_LOCK:
            ent = KLINES_CACHE.get(key)
            return ent[0] if ent else None
            
def compute_recent_volatility(closes):
    try:
        if not closes or len(closes) < 3:
            return None
        # simple pct returns between consecutive closes
        returns = []
        for i in range(1, len(closes)):
            prev = closes[i-1]
            if prev and prev != 0:
                returns.append((closes[i] - prev) / prev)
        if not returns:
            return None
        # population stddev is fine for short windows
        vol = statistics.pstdev(returns)
        # guard against tiny float noise
        if vol <= 0:
            return None
        return float(vol)
    except Exception:
        return None


def compute_trade_size_by_volatility(closes, base_usd=None, target_vol=0.01, min_usd=1.0, max_usd=None):
    try:
        if base_usd is None:
            base_usd = float(globals().get('TRADE_USD', 8.0))
        vol = compute_recent_volatility(closes)
        if vol is None or vol <= 0:
            return float(base_usd)
        scale = float(target_vol) / float(vol)
        # cap scaling to avoid extremes (tunable)
        scale = max(0.25, min(1.5, scale))
        size = float(base_usd) * scale
        if max_usd:
            size = min(size, float(max_usd))
        size = max(size, float(min_usd))
        # round to 2 decimals (USDT cents)
        return float(round(size, 2))
    except Exception:
        return float(base_usd)

def orderbook_bullish(symbol, depth=3, min_imbalance=1.02, max_spread_pct=1.0):
    try:
        ob = client.get_order_book(symbol=symbol, limit=depth)
        bids = ob.get('bids') or []
        asks = ob.get('asks') or []
        if not bids or not asks:
            return False
        top_bid_p, top_bid_q = float(bids[0][0]), float(bids[0][1])
        top_ask_p, top_ask_q = float(asks[0][0]), float(asks[0][1])
        spread_pct = (top_ask_p - top_bid_p) / (top_bid_p + 1e-12) * 100.0
        bid_sum = sum(float(b[1]) for b in bids[:depth]) + 1e-12
        ask_sum = sum(float(a[1]) for a in asks[:depth]) + 1e-12
        imbalance = bid_sum / ask_sum
        return (imbalance >= min_imbalance) and (spread_pct <= max_spread_pct)
    except Exception:
        return False

# -------------------------
# PICKER (tweaked)
# -------------------------
def pick_coin():
    global RATE_LIMIT_BACKOFF, TEMP_SKIP, RECENT_BUYS

    # Local fallbacks in case helper funcs are missing globally (keeps compatibility)
    def _compute_recent_volatility_local(closes):
        try:
            if not closes or len(closes) < 3:
                return None
            returns = []
            for i in range(1, len(closes)):
                prev = closes[i-1]
                if prev and prev != 0:
                    returns.append((closes[i] - prev) / prev)
            if not returns:
                return None
            return float(statistics.pstdev(returns))
        except Exception:
            return None

    def _compute_trade_size_by_volatility_local(closes, base_usd=None, target_vol=0.01, min_usd=1.0, max_usd=None):
        try:
            if base_usd is None:
                base_usd = float(globals().get('TRADE_USD', 7.0))
            vol = None
            # prefer global implementation if available
            if 'compute_recent_volatility' in globals() and callable(globals()['compute_recent_volatility']):
                try:
                    vol = globals()['compute_recent_volatility'](closes)
                except Exception:
                    vol = _compute_recent_volatility_local(closes)
            else:
                vol = _compute_recent_volatility_local(closes)
            if vol is None or vol <= 0:
                return float(base_usd)
            scale = float(target_vol) / float(vol)
            scale = max(0.25, min(1.5, scale))
            size = float(base_usd) * scale
            if max_usd:
                size = min(size, float(max_usd))
            size = max(size, float(min_usd))
            return float(round(size, 2))
        except Exception:
            return float(base_usd)

    try:
        t0 = time.time()
        now = t0

        # localizable tuning (can be overridden via globals())
        TOP_CANDIDATES = globals().get('TOP_CANDIDATES', 60)
        DEEP_EVAL = globals().get('DEEP_EVAL', 3)
        REQUEST_SLEEP = globals().get('REQUEST_SLEEP', 0.02)
        KLINES_LIMIT = globals().get('KLINES_LIMIT', 6)
        MIN_VOL_RATIO = globals().get('MIN_VOL_RATIO', 1.25)

        EMA_UPLIFT_MIN = globals().get('EMA_UPLIFT_MIN_PCT', globals().get('EMA_UPLIFT_MIN_PCT', 0.0001))
        SCORE_MIN = globals().get('SCORE_MIN_THRESHOLD', globals().get('SCORE_MIN_THRESHOLD', 13.0))

        REQUIRE_OB_IN_PICK = globals().get('REQUIRE_ORDERBOOK_BEFORE_BUY', False)

        # small breakout margin constant (if not present globally)
        PREBUY_BREAKOUT_MARGIN = globals().get('PREBUY_BREAKOUT_MARGIN', 0.0015)

        tickers = get_tickers_cached() or []
        prefiltered = []

        # quick prefilter using tickers (cheap)
        for t in tickers:
            sym = t.get('symbol')
            if not sym or not sym.endswith(QUOTE):
                continue

            # respect TEMP_SKIP
            skip_until = TEMP_SKIP.get(sym)
            if skip_until and now < skip_until:
                continue

            # basic fields
            try:
                price = float(t.get('lastPrice') or 0.0)
                qvol = float(t.get('quoteVolume') or 0.0)
                change_pct = float(t.get('priceChangePercent') or 0.0)
            except Exception:
                continue

            # price bounds
            price_min = globals().get('PRICE_MIN', PRICE_MIN)
            price_max = globals().get('PRICE_MAX', PRICE_MAX)
            if not (price_min <= price <= price_max):
                continue

            # volume baseline
            min_vol = max(globals().get('MIN_VOLUME', MIN_VOLUME), 300_000)
            if qvol < min_vol:
                continue

            # 24h guardrails
            if abs(change_pct) > globals().get('MAX_24H_CHANGE_ABS', MAX_24H_CHANGE_ABS):
                continue
            if change_pct > globals().get('MAX_24H_RISE_PCT', MAX_24H_RISE_PCT):
                continue

            # recent buy cooldown / rebuy protection
            last_buy = RECENT_BUYS.get(sym)
            if last_buy:
                cd = last_buy.get('cooldown', REBUY_COOLDOWN)
                if now < last_buy['ts'] + cd:
                    continue
                last_price = last_buy.get('price')
                if last_price and price > last_price * (1 + globals().get('REBUY_MAX_RISE_PCT', REBUY_MAX_RISE_PCT) / 100.0):
                    continue

            prefiltered.append((sym, price, qvol, change_pct))

        if not prefiltered:
            return None

        # sort by quote volume and pick top pool
        prefiltered.sort(key=lambda x: x[2], reverse=True)
        top_pool = prefiltered[:TOP_CANDIDATES]

        # sample DEEP_EVAL from pool for deeper analysis
        if len(top_pool) > DEEP_EVAL:
            sampled = random.sample(top_pool, DEEP_EVAL)
        else:
            sampled = list(top_pool)

        candidates = []

        # small local helpers
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
            return 100 - (100 / (1 + rs))

        # deep-evaluate sampled symbols
        for sym, last_price, qvol, change_pct in sampled:
            try:
                time.sleep(REQUEST_SLEEP)

                # fetch klines
                try:
                    klines = client.get_klines(symbol=sym, interval='5m', limit=KLINES_LIMIT)
                except Exception as e:
                    err = str(e)
                    if '-1003' in err or 'Too much request weight' in err or 'Way too much request weight' in err:
                        prev = RATE_LIMIT_BACKOFF if isinstance(RATE_LIMIT_BACKOFF, (int, float)) and RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP
                        RATE_LIMIT_BACKOFF = min(prev * 2 if prev else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                        notify(f"⚠️ Rate limit while fetching klines for {sym}: {err}. Backing off {RATE_LIMIT_BACKOFF}s.")
                        return None
                    continue

                if not klines or len(klines) < 4:
                    continue

                closes = []
                vols = []
                for k in klines:
                    try:
                        closes.append(float(k[4]))
                    except Exception:
                        closes.append(0.0)
                    try:
                        if len(k) > 7 and k[7] is not None:
                            vols.append(float(k[7]))
                        else:
                            # fallback: approximate quote vol = volume * close
                            vols.append(float(k[5]) * float(k[4]))
                    except Exception:
                        vols.append(0.0)

                if not closes or len(closes) < 3:
                    continue

                # compute recent volume ratio
                recent_vol = vols[-1]
                prev_avg = (sum(vols[:-1]) / max(1, len(vols[:-1]))) if len(vols) > 1 else recent_vol
                vol_ratio = recent_vol / (prev_avg + 1e-12)

                # recent percentage move over last 3 intervals (5m each)
                recent_pct = 0.0
                if len(closes) >= 4 and closes[-4] > 0:
                    recent_pct = (closes[-1] - closes[-4]) / (closes[-4] + 1e-12) * 100.0

                # volatility: prefer global helper if present
                if 'compute_recent_volatility' in globals() and callable(globals()['compute_recent_volatility']):
                    try:
                        vol_f = globals()['compute_recent_volatility'](closes) or 0.0
                    except Exception:
                        vol_f = _compute_recent_volatility_local(closes) or 0.0
                else:
                    vol_f = _compute_recent_volatility_local(closes) or 0.0

                # breakout requirement (price above recent maxima)
                if len(closes) >= 4:
                    if not (closes[-1] > max(closes[:-1]) * (1.0 + PREBUY_BREAKOUT_MARGIN)):
                        continue

                # require at least one of last 3 candles are up (momentum)
                last3 = closes[-3:]
                ups = 0
                if len(last3) >= 2 and last3[1] > last3[0]:
                    ups += 1
                if len(last3) >= 3 and last3[2] > last3[1]:
                    ups += 1
                if ups < 1:
                    continue

                # EMA uplift (short vs long)
                short_period = 3
                long_period = 10
                short_ema = ema_local(closes[-short_period:], short_period) if len(closes) >= short_period else None
                long_ema = ema_local(closes[-long_period:], long_period) if len(closes) >= long_period else ema_local(closes, max(len(closes), 1))
                if short_ema is None or long_ema is None:
                    continue
                ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
                if ema_uplift < EMA_UPLIFT_MIN * 1.2:
                    continue
                if not (short_ema > long_ema * 1.0005):
                    continue

                # RSI sanity check
                rsi_val = compute_rsi_local(closes, period=14)
                if rsi_val is not None and (rsi_val > 65 or rsi_val < 25):
                    continue

                # optional orderbook check if requested globally
                if REQUIRE_OB_IN_PICK:
                    try:
                        if not orderbook_bullish(sym, depth=5, min_imbalance=1.08, max_spread_pct=0.6):
                            continue
                    except Exception:
                        # on error, be conservative and skip
                        continue

                # scoring
                score = 0.0
                score += max(0.0, recent_pct) * 12.0
                try:
                    score += math.log1p(qvol) * 0.5
                except Exception:
                    score += 0.0
                score += max(0.0, (vol_ratio - 1.0)) * 8.0
                score += ema_uplift * 100.0
                if rsi_val is not None:
                    score += max(0.0, (60.0 - min(rsi_val, 60.0))) * 1.0
                score += max(0.0, change_pct) * 0.6
                # small dampening by volatility (prefer controlled moves)
                score -= min(vol_f, 5.0) * 0.5

                if score < SCORE_MIN:
                    continue

                candidates.append({
                    'symbol': sym,
                    'price': last_price,
                    'qvol': qvol,
                    'change': change_pct,
                    'recent_pct': recent_pct,
                    'vol_ratio': vol_ratio,
                    'ema_uplift': ema_uplift,
                    'rsi': rsi_val,
                    'volatility_pct': vol_f,
                    'score': score,
                    'closes': closes
                })

            except Exception as e:
                notify(f"⚠️ pick_coin deep-eval error for {sym}: {e}")
                continue

        # no candidates after deep-eval
        if not candidates:
            return None

        # pick best by score
        candidates.sort(key=lambda x: x['score'], reverse=True)
        best = candidates[0]

        # compute suggested trade size using volatility-aware sizing (prefer global func if present)
        try:
            closes_for_vol = best.get('closes') or []
            TARGET_VOL = globals().get('TRADE_VOL_TARGET', 0.01)
            max_usd_cap = globals().get('TRADE_USD_MAX_CAP', globals().get('TRADE_USD', TRADE_USD) * 2.0)
            if 'compute_trade_size_by_volatility' in globals() and callable(globals()['compute_trade_size_by_volatility']):
                try:
                    suggested_usd = globals()['compute_trade_size_by_volatility'](closes_for_vol, base_usd=globals().get('TRADE_USD', TRADE_USD), target_vol=TARGET_VOL, min_usd=1.0, max_usd=max_usd_cap)
                except Exception:
                    suggested_usd = _compute_trade_size_by_volatility_local(closes_for_vol, base_usd=globals().get('TRADE_USD', TRADE_USD), target_vol=TARGET_VOL, min_usd=1.0, max_usd=max_usd_cap)
            else:
                suggested_usd = _compute_trade_size_by_volatility_local(closes_for_vol, base_usd=globals().get('TRADE_USD', TRADE_USD), target_vol=TARGET_VOL, min_usd=1.0, max_usd=max_usd_cap)

            # publish to globals for trade_cycle to pick up without signature changes
            globals()['DEFAULT_USD_PER_TRADE'] = float(suggested_usd)
            # informational notify (non-spammy): use simple prefix; if you want Telegram, prepend emoji
            notify(f"ℹ️ Suggested trade size (vol adjusted): ${suggested_usd:.2f} (target_vol={TARGET_VOL})")
        except Exception:
            pass

        # return same tuple shape as your prior pick_coin so other code remains compatible
        return (best['symbol'], best['price'], best['qvol'], best['change'], best.get('closes'))

    except Exception as e:
        notify(f"⚠️ pick_coin unexpected error: {e}")
        return None

# -------------------------
# MARKET BUY helpers
# -------------------------
def _parse_market_buy_exec(order_resp):
    try:
        if not order_resp:
            return None, None
        # common: dict with 'fills'
        if isinstance(order_resp, dict):
            # direct executedQty / cummulativeQuoteQty
            try:
                exec_qty = order_resp.get('executedQty') or order_resp.get('executedQty') or None
                if exec_qty:
                    cumm = order_resp.get('cummulativeQuoteQty') or order_resp.get('cumQuote') or order_resp.get('cummulativeQuoteQty')
                    if cumm:
                        try:
                            avg = float(cumm) / float(exec_qty)
                            return float(exec_qty), float(avg)
                        except Exception:
                            pass
            except Exception:
                pass
            fills = order_resp.get('fills') or []
            if fills:
                total_qty = 0.0
                total_quote = 0.0
                for f in fills:
                    q = float(f.get('qty') or f.get('executedQty') or 0)
                    p = float(f.get('price') or 0)
                    total_qty += q
                    total_quote += q * p
                if total_qty > 0:
                    return total_qty, (total_quote / total_qty)
            # fallback: parse 'fills' if string-encoded
            if 'fills' in order_resp and not fills:
                try:
                    import json as _json
                    fills_js = _json.loads(order_resp.get('fills') or '[]')
                    total_qty = sum(float(x.get('qty',0)) for x in fills_js)
                    total_quote = sum(float(x.get('qty',0)) * float(x.get('price',0)) for x in fills_js)
                    if total_qty:
                        return total_qty, (total_quote / total_qty)
                except Exception:
                    pass
        # last-resort: common fields
        try:
            exec_qty = float(order_resp.get('executedQty') or order_resp.get('transactQty') or 0)
            quote = float(order_resp.get('cummulativeQuoteQty') or order_resp.get('cumQuote') or 0)
            if exec_qty and quote:
                return exec_qty, (quote / exec_qty)
        except Exception:
            pass
    except Exception:
        pass
    return None, None

ALLOW_TRADABLE_FALLBACK = True

def is_symbol_tradable(symbol):
    try:
        info = get_symbol_info_cached(symbol)
        if info:
            status = (info.get('status') or '').upper()
            if status in ('BREAK','HALT','DELISTED','PREP'):
                return False
            if status == 'TRADING' or info.get('isSpotTradingAllowed') is True:
                return True
            if info.get('filters'):
                return True

        if ALLOW_TRADABLE_FALLBACK:
            # fallback only if ticker present AND orderbook shows some liquidity
            tickers = get_tickers_cached() or []
            for t in tickers:
                if t.get('symbol') == symbol:
                    # quick liquidity check via orderbook_bullish (non-strict)
                    try:
                        if orderbook_bullish(symbol, depth=3, min_imbalance=1.01, max_spread_pct=3.0):
                            notify(f"ℹ️ is_symbol_tradable fallback: ticker + orderbook ok for {symbol}")
                            return True
                        else:
                            notify(f"ℹ️ is_symbol_tradable fallback: ticker ok but orderbook weak for {symbol}")
                            return False
                    except Exception:
                        # if orderbook fails, be conservative and reject fallback
                        return False
        return False
    except Exception as e:
        notify(f"⚠️ is_symbol_tradable error (balanced) for {symbol}: {e}")
        return False

def place_safe_market_buy(symbol, usd_amount, require_orderbook: bool = False):
    global TEMP_SKIP, RATE_LIMIT_BACKOFF
    now = time.time()
    skip_until = TEMP_SKIP.get(symbol)
    if skip_until and now < skip_until:
        notify(f"⏭️ Skipping {symbol} until {time.ctime(skip_until)} (recent failure).")
        return None, None

    if not is_symbol_tradable(symbol):
        notify(f"⛔ Symbol {symbol} not tradable / market closed. Skipping and blacklisting.")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None

    info = get_symbol_info_cached(symbol)
    if not info:
        notify(f"❌ place_safe_market_buy: couldn't fetch symbol info for {symbol}")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None
    f = get_filters(info)

    if require_orderbook:
        try:
            if not orderbook_bullish(symbol, depth=5, min_imbalance=1.1, max_spread_pct=0.6):
                notify(f"⚠️ Orderbook not bullish for {symbol}; aborting market buy.")
                return None, None
        except Exception as e:
            notify(f"⚠️ Orderbook check error for {symbol}: {e}")

    price = get_price_cached(symbol)
    if price is None:
        try:
            res = client.get_symbol_ticker(symbol=symbol)
            price = float(res.get('price') or res.get('lastPrice') or 0.0)
        except Exception as e:
            err = str(e)
            notify(f"⚠️ Failed to fetch ticker for {symbol}: {err}")
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                notify(f"❗ Rate-limit fetching price. Backing off {RATE_LIMIT_BACKOFF}s; TEMP skipping {symbol}.")
            return None, None

    try:
        price = float(price)
        if price <= 0:
            notify(f"❌ Invalid price for {symbol}: {price}")
            return None, None
    except Exception:
        notify(f"❌ Invalid price type for {symbol}: {price}")
        return None, None

    # compute target qty (take rounding & minNotional into account)
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
                notify(f"ℹ️ Increasing buy qty to meet minNotional: qty -> {qty_target}, notional -> {qty_target*price:.6f}")
            else:
                notify(f"⛔ Skipping market buy for {symbol}: computed order notional ${notional:.6f} < minNotional ${min_notional:.6f} and insufficient funds to top-up.")
                return None, None

    qty_str = format_qty(qty_target, f.get('stepSize', 0.0))
    if not qty_str or float(qty_target) <= 0:
        notify(f"❌ Computed qty invalid for {symbol}: qty_target={qty_target}, qty_str={qty_str}")
        return None, None

    time.sleep(random.uniform(0.05, 0.25))
    try:
        order_resp = client.order_market_buy(symbol=symbol, quantity=qty_str)
    except Exception as e:
        err = str(e)
        notify(f"❌ Market buy failed for {symbol}: {err}")
        if '-1013' in err or 'Market is closed' in err or 'MIN_NOTIONAL' in err or 'minNotional' in err:
            TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
            notify(f"⏸ TEMP skipping {symbol} due to order error.")
        if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
            RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                    RATE_LIMIT_BACKOFF_MAX)
            TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
            notify(f"❗ Rate-limit while placing market buy: backing off {RATE_LIMIT_BACKOFF}s and TEMP skipping {symbol}.")
        return None, None

    executed_qty, avg_price = _parse_market_buy_exec(order_resp)

    time.sleep(0.8)
    asset = symbol[:-len(QUOTE)]
    free_after = get_free_asset(asset)
    free_after_clip = round_step(free_after, f.get('stepSize', 0.0))

    if free_after_clip >= f.get('minQty', 0.0) and (executed_qty <= 0 or abs(free_after_clip - executed_qty) / (executed_qty + 1e-9) > 0.02):
        notify(f"ℹ️ Adjusting executed qty from parsed {executed_qty} to actual free balance {free_after_clip}")
        executed_qty = free_after_clip
        if not avg_price or avg_price == 0.0:
            avg_price = price

    executed_qty = round_step(executed_qty, f.get('stepSize', 0.0))
    if executed_qty < f.get('minQty', 0.0) or executed_qty <= 0:
        notify(f"❌ Executed quantity too small after reconciliation for {symbol}: {executed_qty}")
        return None, None

    if free_after_clip < max(1e-8, executed_qty * 0.5):
        notify(f"⚠️ After buy free balance {free_after_clip} is much smaller than expected executed {executed_qty}. Skipping symbol for a while.")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None

    notify(f"✅ BUY {symbol}: qty={executed_qty} ~price={avg_price:.8f} notional≈${executed_qty*avg_price:.6f}")
    # invalidate open orders cache
    try:
        with OPEN_ORDERS_LOCK:
            OPEN_ORDERS_CACHE['data'] = None
            OPEN_ORDERS_CACHE['ts'] = 0
    except Exception:
        pass
    return executed_qty, avg_price

# -------------------------
# SAFE SELL FALLBACK (market)
# -------------------------
def place_market_sell_fallback(symbol, qty=None, filters=None):
    try:
        # determine filters if not provided
        if filters is None:
            info = get_symbol_info_cached(symbol)
            filters = get_filters(info) if info else {}

        step = filters.get('stepSize') or None
        min_qty = filters.get('minQty') or 0.0

        # prefer actual position qty (locked+free) for sell
        try:
            pos_qty = float(get_current_position_qty(symbol))
        except Exception:
            pos_qty = None

        if qty is None:
            qty_to_sell = pos_qty if pos_qty is not None else float(get_free_asset(symbol[:-len(QUOTE)]))
        else:
            qty_to_sell = float(qty)

        # round down to step
        if step:
            qty_to_sell = math.floor(qty_to_sell / step) * step

        if qty_to_sell <= 0 or qty_to_sell < min_qty:
            notify(f"❌ Market sell fallback: qty too small for {symbol} after rounding ({qty_to_sell} < minQty {min_qty})")
            return None

        qty_str = format_qty(qty_to_sell, step)
        # place market sell
        order = client.create_order(symbol=symbol, side='SELL', type='MARKET', quantity=qty_str)
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        notify(f"⚠️ Emergency MARKET sell executed for {symbol}: qty={qty_str}")
        return order
    except Exception as e:
        notify(f"❌ Market sell fallback failed for {symbol}: {e}")
        # after failing, refresh balances and set TEMP_SKIP to avoid retry storm
        try:
            TEMP_SKIP[symbol] = time.time() + FAILED_ROLL_SKIP_SECONDS
        except Exception:
            pass
        return None
        
def place_oco_sell(symbol, qty, buy_price, tp_pct=2.5, sl_pct=0.9,
                   explicit_tp: float = None, explicit_sl: float = None,
                   retries=3, delay=1):
    global RATE_LIMIT_BACKOFF

    info = get_symbol_info_cached(symbol)
    if not info:
        notify(f"⚠️ place_oco_sell: couldn't fetch symbol info for {symbol}")
        return None
    f = get_filters(info)
    asset = symbol[:-len(QUOTE)]

    tp = explicit_tp if explicit_tp is not None else (buy_price * (1 + tp_pct / 100.0))
    sp = explicit_sl if explicit_sl is not None else (buy_price * (1 - sl_pct / 100.0))
    stop_limit = sp * 0.999

    def clip_floor(v, step):
        if not step or step == 0:
            return v
        return math.floor(v / step) * step

    def clip_ceil(v, step):
        if not step or step == 0:
            return v
        return math.ceil(v / step) * step

    # Make sure qty respects step
    qty = clip_floor(qty, f['stepSize'])
    tp = clip_ceil(tp, f['tickSize'])
    sp = clip_floor(sp, f['tickSize'])
    sl = clip_floor(stop_limit, f['tickSize'])

    if qty <= 0:
        notify("❌ place_oco_sell: quantity too small after clipping")
        return None

    # ensure enough free asset
    free_qty = get_free_asset(asset)
    safe_margin = f['stepSize'] if f['stepSize'] and f['stepSize'] > 0 else 0.0
    if free_qty + 1e-12 < qty:
        new_qty = clip_floor(max(0.0, free_qty - safe_margin), f['stepSize'])
        if new_qty <= 0:
            notify(f"❌ Not enough free {asset} to place sell. free={free_qty}, required={qty}")
            return None
        notify(f"ℹ️ Adjusting sell qty down from {qty} to available {new_qty} to avoid insufficient balance.")
        qty = new_qty

    # BEFORE trying OCO: ensure minNotional satisfied
    min_notional = f.get('minNotional')
    if min_notional:
        # try increasing qty first (safer) if holdings allow
        if qty * tp < min_notional - 1e-12:
            needed_qty = ceil_step(min_notional / tp, f['stepSize'])
            if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                notify(f"ℹ️ Increasing qty from {qty} to {needed_qty} to meet minNotional (qty*tp >= {min_notional}).")
                qty = needed_qty
            else:
                # attempt to bump TP to meet notional (but avoid crazy pumps)
                attempts = 0
                while attempts < 40 and qty * tp < min_notional - 1e-12:
                    if f.get('tickSize') and f.get('tickSize') > 0:
                        tp = clip_ceil(tp + f['tickSize'], f['tickSize'])
                    else:
                        tp = tp + max(1e-8, tp * 0.001)
                    attempts += 1
                if qty * tp < min_notional - 1e-12:
                    notify(f"⚠️ Cannot meet minNotional for OCO on {symbol} (qty*tp={qty*tp:.8f} < {min_notional}). Will attempt fallback flow.")

    qty_str = format_qty(qty, f['stepSize'])
    tp_str = format_price(tp, f['tickSize'])
    sp_str = format_price(sp, f['tickSize'])
    sl_str = format_price(sl, f['tickSize'])

    # Attempt 1: standard OCO
    for attempt in range(1, retries + 1):
        try:
            oco = client.create_oco_order(
                symbol=symbol,
                side='SELL',
                quantity=qty_str,
                price=tp_str,
                stopPrice=sp_str,
                stopLimitPrice=sl_str,
                stopLimitTimeInForce='GTC'
            )
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 OCO SELL placed (standard) TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
        except BinanceAPIException as e:
            err = str(e)
            code = getattr(e, 'code', None)
            notify(f"⚠️ OCO SELL attempt {attempt} (standard) failed: {err}")
            if 'aboveType' in err or '-1102' in err or 'Mandatory parameter' in err:
                notify("ℹ️ Detected 'aboveType' style requirement; will attempt alternative param names.")
                break
            if 'NOTIONAL' in err or 'minNotional' in err or '-1013' in err:
                # try to increase qty then retry
                if min_notional:
                    needed_qty = ceil_step(min_notional / float(tp), f['stepSize'])
                    if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                        qty = needed_qty
                        qty_str = format_qty(qty, f['stepSize'])
                        notify(f"ℹ️ Adjusted qty to {qty_str} to satisfy minNotional; retrying OCO.")
                        time.sleep(0.25)
                        continue
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                notify("❗ Rate-limit detected while placing OCO — backing off and TEMP skipping symbol.")
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                return None
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)
        except Exception as e:
            notify(f"⚠️ Unexpected error on OCO attempt: {e}")
            time.sleep(0.2)

    # Attempt 2: alternative param names (some clients require different param schema)
    for attempt in range(1, retries + 1):
        try:
            oco2 = client.create_oco_order(
                symbol=symbol,
                side='SELL',
                quantity=qty_str,
                aboveType="LIMIT_MAKER",
                abovePrice=tp_str,
                belowType="STOP_LOSS_LIMIT",
                belowStopPrice=sp_str,
                belowPrice=sl_str,
                belowTimeInForce="GTC"
            )
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 OCO SELL placed (alt params) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco_abovebelow', 'raw': oco2}
        except BinanceAPIException as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (alt) failed: {err}")
            if 'NOTIONAL' in err or 'minNotional' in err or 'Filter failure' in err:
                if min_notional:
                    needed_qty = ceil_step(min_notional / float(tp), f['stepSize'])
                    if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                        qty = needed_qty
                        qty_str = format_qty(qty, f['stepSize'])
                        notify(f"ℹ️ Adjusted qty to {qty_str} to attempt to satisfy minNotional for alt OCO.")
                        time.sleep(0.25)
                        continue
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                notify("❗ Rate-limit detected while placing OCO (alt) — backing off and TEMP skipping symbol.")
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                return None
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)
        except Exception as e:
            notify(f"⚠️ Unexpected error on OCO alt attempt: {e}")
            time.sleep(0.2)

    # Fallback: try placing TP limit and STOP_LOSS_LIMIT, else market sell
    notify("⚠️ All OCO attempts failed — falling back to separate TP (limit) + SL (stop-limit) or MARKET.")

    tp_order = None
    sl_order = None
    try:
        tp_order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=tp_str)
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        notify(f"📈 TP LIMIT placed (fallback): {tp_str}, qty={qty_str}")
    except BinanceAPIException as e:
        err = str(e)
        notify(f"❌ Fallback TP limit failed: {err}")
    except Exception as e:
        notify(f"❌ Fallback TP limit failed: {e}")

    try:
        # STOP_LOSS_LIMIT: requires stopPrice (trigger) and price (limit)
        sl_order = client.create_order(
            symbol=symbol,
            side="SELL",
            type="STOP_LOSS_LIMIT",
            stopPrice=sp_str,
            price=sl_str,
            timeInForce='GTC',
            quantity=qty_str
        )
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        notify(f"📉 SL STOP_LOSS_LIMIT placed (fallback): trigger={sp_str}, limit={sl_str}, qty={qty_str}")
    except BinanceAPIException as e:
        err = str(e)
        notify(f"❌ Fallback SL stop-limit failed: {err}")
    except Exception as e:
        notify(f"❌ Fallback SL stop-limit failed: {e}")

    if (tp_order or sl_order):
        return {'tp': tp, 'sl': sp, 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order}}

    # Final fallback: try market sell to avoid being stuck
    fallback_market = place_market_sell_fallback(symbol, qty, f)
    if fallback_market:
        return {'tp': tp, 'sl': sp, 'method': 'market_fallback', 'raw': fallback_market}

    notify("❌ All attempts to protect position failed (no TP/SL placed). TEMP skipping symbol.")
    TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
    return None


# -------------------------
# Targeted cancel helper
# -------------------------
def cancel_open_orders_for_roll(symbol,
                                max_cancel=6,
                                inter_delay=0.25,
                                types=None,
                                min_price=None,
                                max_price=None,
                                exclude_order_ids=None):
    result = {'cancelled': [], 'errors': [], 'partial': False, 'skipped': []}
    exclude_order_ids = set(exclude_order_ids or [])

    try:
        open_orders = get_open_orders_cached(symbol) or []
    except Exception as e:
        notify(f"⚠️ cancel_open_orders_for_roll: failed to fetch open orders: {e}")
        result['errors'].append(('fetch_open_orders', str(e)))
        return result

    cancelled_count = 0
    types_upper = [t.upper() for t in types] if types else None

    for o in open_orders:
        if cancelled_count >= max_cancel:
            notify(f"⚠️ Reached max_cancel ({max_cancel}) for {symbol}; leaving remaining orders.")
            result['partial'] = True
            break

        oid = o.get('orderId') or o.get('order_id') or o.get('clientOrderId')
        if oid in exclude_order_ids:
            result['skipped'].append(oid)
            continue

        otype = (o.get('type') or o.get('orderType') or '').upper()
        if types_upper and otype not in types_upper:
            result['skipped'].append(oid)
            continue

        # price filters
        price_val = None
        try:
            p = o.get('price')
            if p not in (None, '', '0'):
                price_val = float(p)
        except Exception:
            price_val = None

        if (min_price is not None or max_price is not None) and price_val is None:
            result['skipped'].append(oid)
            continue
        if min_price is not None and price_val is not None and price_val < min_price:
            result['skipped'].append(oid)
            continue
        if max_price is not None and price_val is not None and price_val > max_price:
            result['skipped'].append(oid)
            continue

        # attempt cancel
        try:
            client.cancel_order(symbol=symbol, orderId=oid)
            cancelled_count += 1
            result['cancelled'].append(oid)
            time.sleep(inter_delay)
        except BinanceAPIException as e:
            err = str(e)
            notify(f"⚠️ Cancel failed for {symbol} order {oid}: {err}")
            result['errors'].append((oid, err))
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                prev = RATE_LIMIT_BACKOFF if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP
                RATE_LIMIT_BACKOFF = min(prev * 2 if prev else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                notify(f"❗ Rate-limit on cancel — backing off {RATE_LIMIT_BACKOFF}s and TEMP skipping {symbol}.")
                result['partial'] = True
                break
        except Exception as e:
            notify(f"⚠️ Cancel failed for {symbol} order {oid}: {e}")
            result['errors'].append((oid, str(e)))

    # invalidate cache under lock
    try:
        with OPEN_ORDERS_LOCK:
            OPEN_ORDERS_CACHE['data'] = None
            OPEN_ORDERS_CACHE['ts'] = 0
    except Exception:
        pass

    if result['cancelled']:
        notify(f"❌ Cancelled {len(result['cancelled'])} open orders for {symbol}: {result['cancelled']}")
    return result


# ---------------------------
# monitor_and_roll (updated)
# ---------------------------
def monitor_and_roll(symbol, bought_qty, avg_buy_price, ctx=None):
    """
    Trailing/rolling manager for a single position.
    Returns (closed_bool, exit_price_or_None, total_profit_usd)
    """
    global TEMP_SKIP, RATE_LIMIT_BACKOFF, ROLL_FAIL_COUNTER, FAILED_ROLL_SKIP_SECONDS

    cfg = {
        'TRAIL_START_PCT': globals().get('TRAIL_START_PCT', 0.8),
        'TRAIL_DOWN_PCT': globals().get('TRAIL_DOWN_PCT', 1.5),
        'POLL_INTERVAL': 2.0,
        'MAX_POSITION_TIME_SECONDS': 4 * 3600,
        'INITIAL_SL_PCT': globals().get('BASE_SL_PCT', 2.0),
        'MIN_CANCEL_WAIT': 0.25,
        'MIN_TIME_BETWEEN_ROLLS': 3.0,
        'MIN_PRICE_DELTA_PCT': 0.05,
        'MAX_ROLLS': int(globals().get('MAX_ROLLS_PER_POSITION', 9999999)),
        'ROLL_POST_CANCEL_JITTER': globals().get('ROLL_POST_CANCEL_JITTER', (0.02, 0.08)),
        'CANCEL_TYPES': ['STOP_LOSS_LIMIT', 'LIMIT', 'TAKE_PROFIT_LIMIT']
    }
    if ctx:
        cfg.update(ctx)

    TRAIL_START_PCT = float(cfg['TRAIL_START_PCT'])
    TRAIL_DOWN_PCT = float(cfg['TRAIL_DOWN_PCT'])
    POLL_INTERVAL = float(cfg['POLL_INTERVAL'])
    MAX_POSITION_TIME_SECONDS = float(cfg['MAX_POSITION_TIME_SECONDS'])
    INITIAL_SL_PCT = float(cfg['INITIAL_SL_PCT'])
    MIN_CANCEL_WAIT = float(cfg['MIN_CANCEL_WAIT'])
    MIN_TIME_BETWEEN_ROLLS = float(cfg['MIN_TIME_BETWEEN_ROLLS'])
    MIN_PRICE_DELTA_PCT = float(cfg['MIN_PRICE_DELTA_PCT'])
    MAX_ROLLS = int(cfg['MAX_ROLLS'])
    ROLL_POST_CANCEL_JITTER = tuple(cfg['ROLL_POST_CANCEL_JITTER'])
    CANCEL_TYPES = cfg['CANCEL_TYPES']

    start_ts = time.time()
    highest_price = float(avg_buy_price or 0.0)
    qty_left = float(bought_qty or 0.0)
    current_stop_order = None
    current_stop_price = None
    rolls_done = 0
    last_roll_ts = 0.0
    total_profit_usd = 0.0
    exit_price = None

    info = get_symbol_info_cached(symbol)
    f = get_filters(info) if info else {}

    def _round_price(p):
        try:
            return round_price_step(p, symbol)
        except Exception:
            return p

    def _round_qty(q):
        try:
            return round_step(q, symbol)
        except Exception:
            return q

    # place an initial conservative stop near buy (best-effort)
    try:
        initial_stop_price = avg_buy_price * (1.0 - INITIAL_SL_PCT / 100.0)
        initial_stop_price = _round_price(initial_stop_price)
        if qty_left > 0 and initial_stop_price > 0:
            try:
                current_stop_order = place_stop_market_order(symbol, qty_left, initial_stop_price)
                current_stop_price = initial_stop_price
                notify(f"Initial STOP_MARKET placed for {symbol} qty {qty_left} stop {current_stop_price}")
            except Exception as e:
                notify(f"Initial stop placement failed for {symbol}: {e}")
                current_stop_order = None
                current_stop_price = None
    except Exception as e:
        notify(f"Failed to compute/place initial stop for {symbol}: {e}")

    # main loop
    while True:
        if RATE_LIMIT_BACKOFF and RATE_LIMIT_BACKOFF > 0:
            notify(f"⏸️ Respecting rate-limit backoff ({RATE_LIMIT_BACKOFF}s) before trailing for {symbol}")
            time.sleep(max(RATE_LIMIT_BACKOFF, POLL_INTERVAL))

        now = time.time()

        # safety: maximum hold time
        if now - start_ts > MAX_POSITION_TIME_SECONDS:
            notify(f"Max position time reached for {symbol}. Forcing market exit of remaining {qty_left}")
            try:
                place_market_sell_fallback(symbol, qty_left, f)
            except Exception as e:
                notify(f"Emergency market sell failed for {symbol}: {e}")
            try:
                total_profit_usd = calculate_realized_profit(symbol) or total_profit_usd
            except Exception:
                pass
            return True, exit_price, total_profit_usd

        # get latest price
        try:
            price = float(get_price_cached(symbol))
        except Exception as e:
            notify(f"Error fetching price for {symbol} while trailing: {e}")
            time.sleep(POLL_INTERVAL)
            continue

        if price > highest_price:
            highest_price = price

        # start trailing if threshold reached
        if current_stop_price is None:
            try:
                up_pct = (highest_price / avg_buy_price - 1.0) * 100.0
            except Exception:
                up_pct = 0.0

            if up_pct >= TRAIL_START_PCT:
                desired_stop = highest_price * (1.0 - TRAIL_DOWN_PCT / 100.0)
                desired_stop = _round_price(desired_stop)
                if desired_stop >= price:
                    desired_stop = _round_price(price * (1.0 - TRAIL_DOWN_PCT / 100.0))

                try:
                    actual_qty = float(get_current_position_qty(symbol))
                except Exception:
                    actual_qty = qty_left

                min_qty = (f.get('minQty') or 0.0)
                if actual_qty <= 0 or actual_qty < min_qty:
                    notify(f"⚠️ Not starting trailing for {symbol}: actual qty too small ({actual_qty})")
                    TEMP_SKIP[symbol] = time.time() + FAILED_ROLL_SKIP_SECONDS
                    try:
                        place_market_sell_fallback(symbol, actual_qty, f)
                    except Exception:
                        pass
                    try:
                        total_profit_usd = calculate_realized_profit(symbol) or total_profit_usd
                    except Exception:
                        pass
                    return True, exit_price, total_profit_usd

                try:
                    current_stop_order = place_stop_market_order(symbol, actual_qty, desired_stop)
                    current_stop_price = desired_stop
                    qty_left = actual_qty
                    notify(f"Started trailing for {symbol}: placed stop {current_stop_price} (peak {highest_price:.8f})")
                    last_roll_ts = time.time()
                except Exception as e:
                    notify(f"Failed to place initial trailing stop for {symbol}: {e}")
                    current_stop_order = None
                    current_stop_price = None

        else:
            desired_stop = highest_price * (1.0 - TRAIL_DOWN_PCT / 100.0)
            desired_stop = _round_price(desired_stop)

            if desired_stop > (current_stop_price or 0):
                if time.time() - last_roll_ts < MIN_TIME_BETWEEN_ROLLS:
                    time.sleep(POLL_INTERVAL)
                    continue

                try:
                    if current_stop_price and current_stop_price > 0:
                        pct_improvement = (desired_stop - current_stop_price) / current_stop_price * 100.0
                    else:
                        pct_improvement = 100.0
                except Exception:
                    pct_improvement = 100.0

                if pct_improvement < MIN_PRICE_DELTA_PCT:
                    time.sleep(POLL_INTERVAL)
                    continue

                try:
                    actual_qty = float(get_current_position_qty(symbol))
                except Exception:
                    actual_qty = qty_left
                min_qty = (f.get('minQty') or 0.0)
                if actual_qty <= 0 or actual_qty < min_qty:
                    notify(f"⚠️ In monitor_and_roll: actual position qty too small ({actual_qty}) for {symbol}; aborting rolling.")
                    TEMP_SKIP[symbol] = time.time() + FAILED_ROLL_SKIP_SECONDS
                    try:
                        place_market_sell_fallback(symbol, actual_qty, f)
                    except Exception:
                        pass
                    try:
                        total_profit_usd = calculate_realized_profit(symbol) or total_profit_usd
                    except Exception:
                        pass
                    return True, exit_price, total_profit_usd

                # cancel targeted open orders before placing new stop
                try:
                    exclude = set()
                    # protect current_stop_order if it has orderId
                    try:
                        if isinstance(current_stop_order, dict):
                            oid = current_stop_order.get('orderId') or current_stop_order.get('order_id')
                            if oid:
                                exclude.add(oid)
                    except Exception:
                        pass

                    cancel_res = cancel_open_orders_for_roll(symbol,
                                                            max_cancel=6,
                                                            inter_delay=0.12,
                                                            types=CANCEL_TYPES,
                                                            exclude_order_ids=exclude)
                    time.sleep(MIN_CANCEL_WAIT + random.uniform(*ROLL_POST_CANCEL_JITTER))
                except Exception as e:
                    notify(f"Warning: targeted cancel failed for {symbol}: {e}")

                # after cancel, confirm qty and place new stop
                try:
                    new_qty = float(get_current_position_qty(symbol))
                except Exception:
                    new_qty = actual_qty

                # detect if a sell happened during cancel
                if new_qty < actual_qty - 1e-12:
                    sold_amount = actual_qty - new_qty
                    try:
                        profit = calculate_realized_profit(symbol, sold_amount)
                    except Exception:
                        profit = None
                    notify(f"Stop/order filled during cancel for {symbol}: sold {sold_amount}, profit {profit}")
                    try:
                        if profit is not None:
                            send_profit_to_funding(symbol, profit)
                            total_profit_usd += profit
                    except Exception:
                        pass
                    qty_left = new_qty
                    if qty_left <= 0:
                        notify(f"All qty sold for {symbol} while moving stop. Exiting monitor.")
                        current_stop_order = None
                        current_stop_price = None
                        try:
                            total_profit_usd = calculate_realized_profit(symbol) or total_profit_usd
                        except Exception:
                            pass
                        return True, exit_price, total_profit_usd
                    actual_qty = qty_left

                # place the new stop for remaining qty
                try:
                    qty_to_place = _round_qty(actual_qty)
                    if qty_to_place <= 0:
                        notify(f"Rounded qty to zero for {symbol} when attempting to place new stop; exiting.")
                        try:
                            total_profit_usd = calculate_realized_profit(symbol) or total_profit_usd
                        except Exception:
                            pass
                        return True, exit_price, total_profit_usd

                    new_order = place_stop_market_order(symbol, qty_to_place, desired_stop)
                    current_stop_order = new_order
                    current_stop_price = desired_stop
                    qty_left = qty_to_place
                    rolls_done += 1
                    last_roll_ts = time.time()
                    notify(f"🔁 Moved stop for {symbol} up to {current_stop_price} for qty {qty_to_place} (roll #{rolls_done})")
                except Exception as e:
                    ROLL_FAIL_COUNTER[symbol] = ROLL_FAIL_COUNTER.get(symbol, 0) + 1
                    notify(f"❌ Failed to place moved stop for {symbol}: {e} (fail #{ROLL_FAIL_COUNTER[symbol]})")
                    if ROLL_FAIL_COUNTER.get(symbol, 0) >= globals().get('FAILED_ROLL_THRESHOLD', 3):
                        TEMP_SKIP[symbol] = time.time() + FAILED_ROLL_SKIP_SECONDS
                        notify(f"⚠️ Reached failed roll threshold for {symbol}. TEMP skipping for {FAILED_ROLL_SKIP_SECONDS}s.")
                    time.sleep(POLL_INTERVAL)
                    continue

                if rolls_done >= MAX_ROLLS:
                    notify(f"Reached MAX_ROLLS ({MAX_ROLLS}) for {symbol}. Will stop rolling further.")

        # reconciliation: detect if stop executed (market sell) by inspecting actual position qty
        try:
            latest_qty = float(get_current_position_qty(symbol))
        except Exception:
            latest_qty = qty_left

        if latest_qty < qty_left - 1e-12:
            sold = qty_left - latest_qty
            try:
                profit = calculate_realized_profit(symbol, sold)
            except Exception:
                profit = None
            notify(f"Detected sell for {symbol}: sold {sold}, profit {profit}")
            try:
                if profit is not None:
                    send_profit_to_funding(symbol, profit)
                    total_profit_usd += profit
            except Exception:
                pass

            qty_left = latest_qty
            if qty_left <= 0:
                try:
                    if 'get_last_fill_price' in globals():
                        exit_price = get_last_fill_price(symbol)
                    else:
                        if bought_qty and bought_qty > 0:
                            exit_price = avg_buy_price + (total_profit_usd / bought_qty)
                except Exception:
                    exit_price = None
                notify(f"Position fully closed for {symbol} by stop sell.")
                try:
                    total_profit_usd = calculate_realized_profit(symbol) or total_profit_usd
                except Exception:
                    pass
                return True, exit_price, total_profit_usd

        time.sleep(POLL_INTERVAL)

    # unreachable
    try:
        total_profit_usd = calculate_realized_profit(symbol) or total_profit_usd
    except Exception:
        pass
    return False, exit_price, total_profit_usd
    
def _update_metrics_for_profit(profit_usd: float, picked_symbol: str = None, was_win: bool = None):
    """
    Update METRICS for the current EAT date (so stats align with Africa/Dar_es_Salaam).
    Returns (date_key, metrics_entry)
    """
    try:
        now_eat = datetime.utcnow() + __import__('datetime').timedelta(hours=3)
        date_key = now_eat.date().isoformat()
        ent = METRICS.get(date_key)
        if ent is None:
            ent = {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0}
        if was_win is not None:
            if was_win:
                ent['wins'] += 1
            else:
                ent['losses'] += 1
        if profit_usd is not None:
            ent['profit'] = float(ent.get('profit', 0.0) + float(profit_usd))
        METRICS[date_key] = ent
        return date_key, ent
    except Exception:
        return None, None

def enforce_oco_max_life_and_exit_if_needed():
    return

_DAILY_REPORTER_STARTED = False
_DAILY_REPORTER_LOCK = Lock()

def _notify_daily_stats(date_key):
    with METRICS_LOCK:
        m = METRICS.get(date_key, {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0})
    profit_val = m['profit']
    profit_str = f"+{profit_val:.2f} USDT" if profit_val >= 0 else f"{profit_val:.2f} USDT"
    notify(
        f"📊 Stats ya {date_key}:\n\n"
        f"Coins zilizochaguliwa: {m['picks']}\n\n"
        f"Zilizofanikiwa (TP/Profit): {m['wins']}\n\n"
        f"Zilizopoteza: {m['losses']}\n\n"
        f"Jumla profit: {profit_str}",
        category='daily'
    )

def _daily_reporter_loop():
    while True:
        try:
            now = datetime.now(tz=EAT_TZ)
            target = now.replace(hour=22, minute=0, second=5, microsecond=0)
            if target <= now:
                target = target + timedelta(days=1)
            to_sleep = (target - now).total_seconds()
            time.sleep(max(1.0, to_sleep))

            # report for the previous EAT date (keeps same semantics as before)
            report_date = (datetime.now(tz=EAT_TZ) - timedelta(days=1)).date().isoformat()
            try:
                _notify_daily_stats(report_date)
            except Exception as e:
                notify(f"⚠️ Daily reporter failed to notify for {report_date}: {e}", priority=True)
        except Exception as e:
            notify(f"⚠️ Daily reporter loop error: {e}", priority=True)
            time.sleep(60)

def _start_daily_reporter_once():
    global _DAILY_REPORTER_STARTED
    with _DAILY_REPORTER_LOCK:
        if _DAILY_REPORTER_STARTED:
            return
        try:
            t = Thread(target=_daily_reporter_loop, daemon=True)
            t.start()
            _DAILY_REPORTER_STARTED = True
        except Exception as e:
            notify(f"⚠️ Failed to start daily reporter: {e}", priority=True)
 
# -------------------------
# UPDATED TRADE CYCLE
# -------------------------
ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0
BUY_LOCK_SECONDS = globals().get('BUY_LOCK_SECONDS', 30)

def trade_cycle():
    global start_balance_usdt, ACTIVE_SYMBOL, LAST_BUY_TS, RATE_LIMIT_BACKOFF

    if start_balance_usdt is None:
        try:
            start_balance_usdt = get_free_usdt()
            notify(f"🔰 Start balance snapshot: ${start_balance_usdt:.6f}")
        except Exception:
            start_balance_usdt = 0.0

    try:
        _start_daily_reporter_once()
    except Exception:
        pass

    while True:
        try:
            # housekeeping
            try:
                cleanup_recent_buys()
            except Exception:
                pass
            try:
                cleanup_temp_skip()
            except Exception:
                pass
            try:
                enforce_oco_max_life_and_exit_if_needed()
            except Exception:
                pass

            # quick open-orders check (if many open orders, wait a bit but re-check sooner)
            try:
                open_orders_global = get_open_orders_cached()
            except Exception:
                open_orders_global = None

            if open_orders_global:
                # if many or unknown, wait but not excessively long — re-check sooner
                notify(f"ℹ️ Skipping buy cycle: found {len(open_orders_global)} open orders.")
                time.sleep(min(60, globals().get('CYCLE_DELAY', 5) * 6))
                continue

            # skip if currently managing a symbol
            if ACTIVE_SYMBOL is not None:
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            now_ts = time.time()
            if now_ts - LAST_BUY_TS < BUY_LOCK_SECONDS:
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            # pick candidate
            candidate = pick_coin()
            if not candidate:
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            # normalize candidate
            if isinstance(candidate, dict):
                symbol = candidate.get('symbol') or candidate.get('s') or candidate.get('sym')
                entry_hint_price = candidate.get('price') or candidate.get('p')
            else:
                try:
                    symbol, entry_hint_price, *_ = candidate
                except Exception:
                    symbol = candidate
                    entry_hint_price = None

            if not symbol:
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            # skip if symbol temporarily blacklisted
            if symbol in TEMP_SKIP and TEMP_SKIP.get(symbol, 0) > time.time():
                notify(f"⚠️ Skipping {symbol} — TEMP_SKIP active.")
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            # available USD
            usd_suggested = float(globals().get('DEFAULT_USD_PER_TRADE',
                                                globals().get('TRADE_USD', globals().get('TRADE_USD', 10.0))))
            try:
                free_usdt = get_free_usdt()
            except Exception:
                free_usdt = 0.0

            # clamp trade size: don't try to use entire balance, leave safety buffer
            safety_buffer = float(globals().get('USDT_SAFETY_BUFFER', 1.0))
            usd_to_buy = min(usd_suggested, max(0.0, free_usdt - safety_buffer))
            min_trade_usd = float(globals().get('MIN_TRADE_USD', 1.0))
            if usd_to_buy < min_trade_usd:
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            # tradable check
            try:
                tradable = is_symbol_tradable(symbol)
            except Exception:
                tradable = False
            if not tradable:
                try:
                    TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
                except Exception:
                    pass
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            # attempt buy (with rate-limit handling)
            try:
                buy_res = place_safe_market_buy(symbol,
                                                usd_to_buy,
                                                require_orderbook=globals().get('REQUIRE_ORDERBOOK_BEFORE_BUY', False))
            except Exception as e:
                err = str(e)
                if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                    RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                             RATE_LIMIT_BACKOFF_MAX)
                    notify(f"❌ Rate limit detected during buy attempt: backing off for {RATE_LIMIT_BACKOFF}s.")
                    time.sleep(RATE_LIMIT_BACKOFF)
                else:
                    notify(f"❌ Exception during market buy attempt for {symbol}: {err}")
                    time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            if not buy_res:
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            # parse buy response robustly
            qty = 0.0
            entry_price = 0.0
            try:
                if isinstance(buy_res, dict):
                    qty = float(buy_res.get('executedQty') or buy_res.get('qty') or buy_res.get('executed_qty') or 0.0)
                    entry_price = float(buy_res.get('avgPrice') or buy_res.get('avg_price') or buy_res.get('price') or entry_hint_price or 0.0)
                else:
                    qty, entry_price = buy_res
                    qty = float(qty)
                    entry_price = float(entry_price)
            except Exception:
                notify(f"❌ Unexpected buy response for {symbol}: {buy_res}")
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            if qty <= 0 or entry_price <= 0:
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            notify(f"✅ BUY {symbol}: qty={qty} ~price={entry_price:.8f} notional≈${qty*entry_price:.6f}")

            # update daily metrics/picks
            try:
                # use local timezone offset +3 as you previously did
                now_datekey = (datetime.utcnow() + __import__('datetime').timedelta(hours=3)).date().isoformat()
                ent = METRICS.get(now_datekey) or {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0}
                ent['picks'] = ent.get('picks', 0) + 1
                METRICS[now_datekey] = ent
            except Exception:
                pass

            # register recent buy
            try:
                with RECENT_BUYS_LOCK:
                    RECENT_BUYS[symbol] = {'ts': time.time(), 'price': entry_price, 'profit': None, 'cooldown': REBUY_COOLDOWN}
            except Exception:
                pass

            # fetch filters
            try:
                info = get_symbol_info_cached(symbol)
                f = get_filters(info) if info else {}
            except Exception:
                f = {}

            if not f:
                notify(f"⚠️ Could not fetch filters for {symbol} after buy; attempting safety market-sell.")
                try:
                    place_market_sell_fallback(symbol, qty, f)
                except Exception:
                    pass
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            try:
                qty_remaining = round_step(qty, f.get('stepSize', 0.0))
            except Exception:
                qty_remaining = qty

            if qty_remaining <= 0 or qty_remaining < f.get('minQty', 0.0):
                try:
                    total_profit_usd = calculate_realized_profit(symbol) or 0.0
                except Exception:
                    total_profit_usd = 0.0
                LAST_BUY_TS = time.time()
                time.sleep(globals().get('CYCLE_DELAY', 1.0))
                continue

            # mark active
            ACTIVE_SYMBOL = symbol
            LAST_BUY_TS = time.time()
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            profit_usd = 0.0
            exit_price = None
            try:
                # call monitor_and_roll (it will handle stops, rolling and market fallback)
                res = monitor_and_roll(symbol, qty_remaining, entry_price, f)
                if isinstance(res, tuple) and len(res) >= 3:
                    closed_flag, exit_price, profit_usd = res[0], res[1], res[2]
                else:
                    profit_usd = calculate_realized_profit(symbol) or 0.0
                    if 'get_last_fill_price' in globals():
                        exit_price = get_last_fill_price(symbol)
                    else:
                        exit_price = (entry_price + (profit_usd / qty)) if qty > 0 else entry_price
            except Exception as e:
                notify(f"⚠️ monitor_and_roll raised for {symbol}: {e}")
                try:
                    place_market_sell_fallback(symbol, qty_remaining, f)
                except Exception:
                    pass
                try:
                    profit_usd = calculate_realized_profit(symbol) or 0.0
                except Exception:
                    profit_usd = 0.0
                exit_price = None
            finally:
                # always clear ACTIVE_SYMBOL even if monitor crashed
                ACTIVE_SYMBOL = None
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass

            # update RECENT_BUYS with outcome
            try:
                with RECENT_BUYS_LOCK:
                    ent = RECENT_BUYS.get(symbol, {}) or {}
                    ent['ts'] = time.time()
                    ent['price'] = entry_price
                    ent['profit'] = profit_usd
                    if profit_usd is None:
                        ent['cooldown'] = REBUY_COOLDOWN
                    elif profit_usd < 0:
                        ent['cooldown'] = LOSS_COOLDOWN
                    else:
                        ent['cooldown'] = REBUY_COOLDOWN
                    RECENT_BUYS[symbol] = ent
            except Exception:
                pass

            # metrics & funding
            try:
                was_win = (profit_usd > 0)
                date_key, m = _update_metrics_for_profit(profit_usd, picked_symbol=symbol, was_win=was_win)
                sign = "+" if profit_usd >= 0 else "-"
                notify(f"✅ Position closed for {symbol}: exit={(exit_price or 0):.8f}, profit≈{sign}${abs(profit_usd):.6f}")
            except Exception as e:
                notify(f"⚠️ Error updating metrics after position closed for {symbol}: {e}")

            try:
                if profit_usd and profit_usd > 0:
                    try:
                        send_profit_to_funding(max(0.0, float(profit_usd)))
                    except Exception:
                        pass
            except Exception:
                pass

            # reset backoff on successful cycle
            RATE_LIMIT_BACKOFF = 0

        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err or 'Way too much request weight' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                         RATE_LIMIT_BACKOFF_MAX)
                notify(f"❌ Rate limit reached in trade_cycle: backing off for {RATE_LIMIT_BACKOFF}s.")
                time.sleep(RATE_LIMIT_BACKOFF)
                continue
            notify(f"❌ Trade cycle unexpected error: {e}")
            time.sleep(globals().get('CYCLE_DELAY', 1.0))

        time.sleep(globals().get('CYCLE_DELAY', 1.0))
        
# # -------------------------
# FLASK KEEPALIVE
# -------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot running! ✅"

def start_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)

# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    bot_thread.start()
    start_flask()