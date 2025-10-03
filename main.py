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

EMA_UPLIFT_MIN_PCT = 0.0008
SCORE_MIN_THRESHOLD = 13.0

TRADE_USD = 8.0
SLEEP_BETWEEN_CHECKS = 8
CYCLE_DELAY = 8
COOLDOWN_AFTER_EXIT = 10

TRIGGER_PROXIMITY = 0.010
STEP_INCREMENT_PCT = 0.01
BASE_TP_PCT = 3.0
BASE_SL_PCT = 2.0

MICRO_TP_PCT = 0.5
MICRO_TP_FRACTION = 0.20
MICRO_MAX_WAIT = 8.0

ROLL_STEP_PCT = 0.2   
ROLL_STEP_ABS = 0.001 

ROLL_ON_RISE_PCT = 0.2
ROLL_TRIGGER_PCT = 0.25
ROLL_TRIGGER_DELTA_ABS = 0.001
ROLL_TP_STEP_ABS = 0.010
ROLL_SL_STEP_ABS = 0.0015
ROLL_COOLDOWN_SECONDS = 5
MAX_ROLLS_PER_POSITION = 9999999
ROLL_POST_CANCEL_JITTER = (0.5, 0.18)

ROLL_FAIL_COUNTER = {}
FAILED_ROLL_THRESHOLD = 3
FAILED_ROLL_SKIP_SECONDS = 60 * 60

NOTIFY_QUEUE_MAX = 1000
NOTIFY_RETRY = 2
NOTIFY_TIMEOUT = 4.0
NOTIFY_PRIORITY_SUPPRESS = 0.08

client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
start_balance_usdt = None

TEMP_SKIP = {}
SKIP_SECONDS_ON_MARKET_CLOSED = 60 * 60

getcontext().prec = 28

OCO_MAX_LIFE_SECONDS = int(float(os.environ.get('OCO_MAX_LIFE_SECONDS', 6 * 3600)))

METRICS = {}
METRICS_LOCK = Lock()

EAT_TZ = timezone(timedelta(hours=3))

ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0
BUY_LOCK_SECONDS = globals().get('BUY_LOCK_SECONDS', 30)

_LAST_MICRO_SOLD_QTY_FOR = {}      
_REPORTED_POSITION_CLOSES = set()

RECENT_BUYS = {}
REBUY_COOLDOWN = 60 * 60
LOSS_COOLDOWN = 60 * 60 * 4
REBUY_MAX_RISE_PCT = 5.0

RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_BACKOFF_MAX = 300
RATE_LIMIT_BASE_SLEEP = 90
CACHE_TTL = 300

# notify subsystem (replace existing notify/_send/_start thread block)
_NOTIFY_Q = queue.Queue(maxsize=globals().get('NOTIFY_QUEUE_MAX', 1000))
_NOTIFY_THREAD_STARTED = False
_NOTIFY_LOCK = Lock()
_LAST_NOTIFY_NONPRIO = 0.0
_NOTIFY_LAST_MSG_TS = {}    # text -> last send ts
_NOTIFY_MSG_LOCK = Lock()

def _send_telegram(text: str) -> bool:
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

def _start_notify_thread():
    global _NOTIFY_THREAD_STARTED
    with _NOTIFY_LOCK:
        if _NOTIFY_THREAD_STARTED:
            return
        def _worker():
            while True:
                text = None
                try:
                    text = _NOTIFY_Q.get()
                except Exception:
                    time.sleep(0.1)
                    continue
                if text is None:
                    break
                try:
                    ok = _send_telegram(text)
                    if not ok:
                        # final best-effort synchronous fallback
                        try:
                            requests.post(f"https://api.telegram.org/bot{globals().get('BOT_TOKEN')}/sendMessage",
                                          data={"chat_id": globals().get('CHAT_ID'), "text": text}, timeout=2)
                        except Exception:
                            pass
                finally:
                    try:
                        _NOTIFY_Q.task_done()
                    except Exception:
                        pass
            # thread exiting
        t = Thread(target=_worker, daemon=True)
        t.start()
        _NOTIFY_THREAD_STARTED = True

def notify(msg: str, priority: bool = False, category: str = None):
    now_ts = time.time()
    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    try:
        print(text)
    except Exception:
        pass

    # decide which messages are allowed to be sent to Telegram
    allowed = False
    try:
        if category == 'daily':
            allowed = True
        else:
            if isinstance(msg, str):
                if msg.startswith("✅ BUY"):
                    allowed = True
                elif msg.startswith("✅ Position closed"):
                    allowed = True
                elif msg.startswith("🔁 Rolled") or msg.startswith("🔁 Rolled OCO"):
                    allowed = True
                elif msg.startswith("📍 Micro"):
                    allowed = True
                elif msg.startswith("📌 OCO"):
                    allowed = True
                elif msg.startswith("💸"):
                    allowed = True
                elif msg.startswith("⚠️") or msg.startswith("❌"):
                    allowed = True
    except Exception:
        allowed = False

    if not allowed and not priority:
        return

    # immediate send for forced priority messages
    if priority:
        try:
            Thread(target=_send_telegram, args=(text,), daemon=True).start()
        except Exception:
            pass
        with _NOTIFY_MSG_LOCK:
            _NOTIFY_LAST_MSG_TS[text] = now_ts
        return

    # duplicate suppression window (avoid identical messages many times)
    dup_window = float(globals().get('NOTIFY_DUPLICATE_WINDOW', 300.0))  # seconds
    with _NOTIFY_MSG_LOCK:
        last = _NOTIFY_LAST_MSG_TS.get(text)
        if last and (now_ts - last) < dup_window:
            return
        # throttle short-term for non-priority messages
        suppress = float(globals().get('NOTIFY_PRIORITY_SUPPRESS', 0.08))
        global _LAST_NOTIFY_NONPRIO
        if now_ts - _LAST_NOTIFY_NONPRIO < suppress:
            return
        _LAST_NOTIFY_NONPRIO = now_ts
        _NOTIFY_LAST_MSG_TS[text] = now_ts

    # queue the message (worker will send). fallback to background thread if queue full
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
            return func(*args, **kwargs)
        except BinanceAPIException as e:
            err = str(e)
            notify(f"⚠️ BinanceAPIException in {func.__name__}: {err}")
            # common rate-limit detection
            if '-1003' in err or 'Too much request weight' in err or 'Way too much request weight' in err or 'Request has been rejected' in err:
                prev = RATE_LIMIT_BACKOFF if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP
                RATE_LIMIT_BACKOFF = min(prev * 2 if prev else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                notify(f"❗ Rate-limit detected, backing off {RATE_LIMIT_BACKOFF}s.")
            # re-raise for calling code to handle if necessary
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

def get_client():
    global client
    return client

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
OPEN_ORDERS_TTL = 300

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
        with OPEN_ORDERS_LOCK:
            return OPEN_ORDERS_CACHE.get('data') or []


# per-symbol price cache
PER_SYMBOL_PRICE_CACHE = {}  # symbol -> (price, ts)
PRICE_CACHE_TTL = 2.0  # seconds

def get_tickers_cached():
    global TICKER_CACHE, LAST_FETCH, RATE_LIMIT_BACKOFF
    now = time.time()
    if RATE_LIMIT_BACKOFF and now - LAST_FETCH < RATE_LIMIT_BACKOFF:
        return TICKER_CACHE or []
    if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
        try:
            TICKER_CACHE = client.get_ticker()
            LAST_FETCH = now
            RATE_LIMIT_BACKOFF = 0
        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                         RATE_LIMIT_BACKOFF_MAX)
                notify(f"⚠️ Rate limit detected in get_tickers_cached, backing off for {RATE_LIMIT_BACKOFF}s.")
                time.sleep(RATE_LIMIT_BACKOFF)
            else:
                notify(f"⚠️ Failed to refresh tickers: {e}")
            return TICKER_CACHE or []
    return TICKER_CACHE

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

def compute_recent_volatility(closes, lookback=5):
    try:
        if not closes or len(closes) < 2:
            return None
        # compute simple returns between consecutive closes
        rets = []
        for i in range(1, len(closes)):
            prev = float(closes[i-1])
            cur = float(closes[i])
            if prev <= 0:
                continue
            rets.append((cur - prev) / prev)
        if not rets:
            return None
        # use only last `lookback` returns to be responsive
        recent = rets[-lookback:] if lookback and len(rets) >= 1 else rets
        if len(recent) == 0:
            return None
        if len(recent) == 1:
            # single return -> treat absolute as a tiny volatility
            return abs(recent[0])
        try:
            vol = statistics.pstdev(recent)  # population stdev (fast)
        except Exception:
            vol = statistics.stdev(recent) if len(recent) > 1 else abs(recent[-1])
        # guard: ensure non-negative, clamp to reasonable ceiling
        if vol is None:
            return None
        vol = abs(vol)
        # cap to avoid absurd numbers (e.g., >1 means 100% in a single step)
        vol = min(vol, 5.0)  # keeps later math safe; interpret as fraction
        return vol
    except Exception:
        return None


def compute_trade_size_by_volatility(closes, base_usd=None, target_vol=None, min_usd=None, max_usd=None):
    try:
        base_usd = base_usd if base_usd is not None else globals().get('TRADE_USD', 7.0)
        target_vol = target_vol if target_vol is not None else globals().get('TARGET_VOL_FRACTION', 0.01)
        min_usd = min_usd if min_usd is not None else globals().get('MIN_TRADE_USD', 1.0)
        vol = compute_recent_volatility(closes)
        if vol is None or vol <= 0:
            return base_usd
        scale = target_vol / vol
        scale = max(0.25, min(1.5, scale))
        size = base_usd * scale
        if max_usd:
            size = min(size, max_usd)
        size = max(size, min_usd)
        return round(size, 2)
    except Exception:
        return base_usd

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
    try:
        t0 = time.time()
        now = t0
        TOP_CANDIDATES = globals().get('TOP_CANDIDATES', 60)
        DEEP_EVAL = globals().get('DEEP_EVAL', 3)
        REQUEST_SLEEP = globals().get('REQUEST_SLEEP', 0.04)
        KLINES_LIMIT = globals().get('KLINES_LIMIT', 6)
        EMA_UPLIFT_MIN = globals().get('EMA_UPLIFT_MIN_PCT', EMA_UPLIFT_MIN_PCT if 'EMA_UPLIFT_MIN_PCT' in globals() else 0.001)
        SCORE_MIN = globals().get('SCORE_MIN_THRESHOLD', SCORE_MIN_THRESHOLD if 'SCORE_MIN_THRESHOLD' in globals() else 14.0)
        REQUIRE_OB_IN_PICK = globals().get('REQUIRE_ORDERBOOK_BEFORE_BUY', True)
        PREBUY_BREAKOUT_MARGIN = globals().get('PREBUY_BREAKOUT_MARGIN', 0.0015)

        tickers = get_tickers_cached() or []
        prefiltered = []
        for t in tickers:
            sym = t.get('symbol')
            if not sym or not sym.endswith(QUOTE):
                continue
            skip_until = TEMP_SKIP.get(sym)
            if skip_until and now < skip_until:
                continue
            try:
                price = float(t.get('lastPrice') or 0.0)
                qvol = float(t.get('quoteVolume') or 0.0)
                change_pct = float(t.get('priceChangePercent') or 0.0)
            except Exception:
                continue
            if not (globals().get('PRICE_MIN', PRICE_MIN) <= price <= globals().get('PRICE_MAX', PRICE_MAX)):
                continue
            if qvol < max(globals().get('MIN_VOLUME', MIN_VOLUME), 300_000):
                continue
            if abs(change_pct) > globals().get('MAX_24H_CHANGE_ABS', MAX_24H_CHANGE_ABS):
                continue
            if change_pct > globals().get('MAX_24H_RISE_PCT', MAX_24H_RISE_PCT):
                continue
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

        prefiltered.sort(key=lambda x: x[2], reverse=True)
        top_pool = prefiltered[:TOP_CANDIDATES]
        if len(top_pool) > DEEP_EVAL:
            sampled = random.sample(top_pool, DEEP_EVAL)
        else:
            sampled = list(top_pool)

        candidates = []
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

        for sym, last_price, qvol, change_pct in sampled:
            try:
                time.sleep(REQUEST_SLEEP)
                c = get_client()
                if not c:
                    return None
                try:
                    klines = c.get_klines(symbol=sym, interval='5m', limit=KLINES_LIMIT)
                except Exception as e:
                    err = str(e)
                    if '-1003' in err or 'Too much request weight' in err:
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
                            vols.append(float(k[5]) * float(k[4]))
                    except Exception:
                        vols.append(0.0)
                if not closes or len(closes) < 3:
                    continue
                recent_vol = vols[-1]
                prev_avg = (sum(vols[:-1]) / max(1, len(vols[:-1]))) if len(vols) > 1 else recent_vol
                vol_ratio = recent_vol / (prev_avg + 1e-12)
                recent_pct = 0.0
                if len(closes) >= 4 and closes[-4] > 0:
                    recent_pct = (closes[-1] - closes[-4]) / (closes[-4] + 1e-12) * 100.0
                vol_f = compute_recent_volatility(closes) or 0.0
                if len(closes) >= 4:
                    if not (closes[-1] > max(closes[:-1]) * (1.0 + PREBUY_BREAKOUT_MARGIN)):
                        continue
                last3 = closes[-3:]
                ups = 0
                if len(last3) >= 2 and last3[1] > last3[0]:
                    ups += 1
                if len(last3) >= 3 and last3[2] > last3[1]:
                    ups += 1
                if ups < 1:
                    continue
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
                rsi_val = compute_rsi_local(closes, period=14)
                if rsi_val is not None and (rsi_val > 65 or rsi_val < 25):
                    continue
                if REQUIRE_OB_IN_PICK:
                    try:
                        if not orderbook_bullish(sym, depth=5, min_imbalance=1.08, max_spread_pct=0.6):
                            continue
                    except Exception:
                        continue
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

        if not candidates:
            return None
        candidates.sort(key=lambda x: x['score'], reverse=True)
        best = candidates[0]
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
# Micro TP helper (unchanged mostly)
# -------------------------
def place_micro_tp(symbol, qty, entry_price, f, pct=MICRO_TP_PCT, fraction=MICRO_TP_FRACTION):
    try:
        c = get_client()
        if not c:
            return None, 0.0, None

        min_notional = f.get('minNotional')
        step_size = f.get('stepSize', 0.0)
        tick_size = f.get('tickSize', 0.0)
        
        # Calculate micro sell qty
        sell_qty = round_step(qty * fraction, step_size)
        remainder = round_step(qty - sell_qty, step_size)

        # Check if remainder will be rollable
        if min_notional and remainder * entry_price < min_notional - 1e-12:
            # If not, sell whole position in micro TP
            sell_qty = round_step(qty, step_size)
            remainder = 0.0
            notify(f"⚠️ Micro TP remainder would be unrollable for {symbol}; selling full position instead.")

        # Check if sell_qty itself can be sold
        if sell_qty < f.get('minQty', 0.0) or (min_notional and sell_qty * entry_price < min_notional - 1e-12):
            notify(f"❌ Micro TP sell_qty too small for {symbol}; skipping micro TP.")
            return None, 0.0, None

        tp_price = float(entry_price) * (1.0 + float(pct) / 100.0)
        if tick_size and tick_size > 0:
            tp_price = math.ceil(tp_price / tick_size) * tick_size

        if min_notional and sell_qty * tp_price < min_notional - 1e-12:
            notify(f"❌ Micro TP at target price would be below minNotional for {symbol}; skipping micro TP.")
            return None, 0.0, None

        qty_str = format_qty(sell_qty, step_size)
        price_str = format_price(tp_price, tick_size)

        # Place limit sell
        try:
            order = c.order_limit_sell(symbol=symbol, quantity=qty_str, price=price_str)
        except Exception as e:
            try:
                order = c.create_order(symbol=symbol, side='SELL', type='LIMIT', quantity=qty_str, price=price_str, timeInForce='GTC')
            except Exception:
                return None, 0.0, None

        order_id = None
        if isinstance(order, dict):
            order_id = order.get('orderId') or order.get('orderId')
        if not order_id:
            return order, 0.0, tp_price

        poll_interval = 0.6
        max_wait = float(globals().get('MICRO_MAX_WAIT', MICRO_MAX_WAIT))
        waited = 0.0

        while waited < max_wait:
            try:
                status = c.get_order(symbol=symbol, orderId=order_id)
            except Exception:
                break

            executed_qty = 0.0
            avg_fill_price = None

            try:
                ex = status.get('executedQty')
                if ex is not None:
                    executed_qty = float(ex)
            except Exception:
                executed_qty = 0.0

            if executed_qty == 0.0:
                fills = status.get('fills') or []
                total_q = 0.0
                total_quote = 0.0
                for fll in fills:
                    try:
                        fq = float(fll.get('qty', 0.0) or 0.0)
                        fp = float(fll.get('price', 0.0) or 0.0)
                    except Exception:
                        fq = 0.0; fp = 0.0
                    total_q += fq
                    total_quote += fq * fp
                if total_q > 0:
                    executed_qty = total_q
                    avg_fill_price = (total_quote / total_q) if total_q > 0 else None

            if executed_qty and executed_qty > 0.0:
                if avg_fill_price is None:
                    cumm = status.get('cummulativeQuoteQty') or status.get('cumulativeQuoteQty') or status.get('cumQuote') or 0.0
                    try:
                        cumm = float(cumm)
                        if executed_qty > 0 and cumm > 0:
                            avg_fill_price = cumm / executed_qty
                    except Exception:
                        avg_fill_price = None
                if avg_fill_price is None:
                    avg_fill_price = tp_price
                executed_qty = round_step(executed_qty, f.get('stepSize', 0.0))
                return status, executed_qty, avg_fill_price

            time.sleep(poll_interval)
            waited += poll_interval

        try:
            c.cancel_order(symbol=symbol, orderId=order_id)
        except Exception:
            pass

        return None, 0.0, None

    except Exception:
        return None, 0.0, None


# -------------------------
# Monitor and roll (updated)
# -------------------------
def monitor_and_roll(symbol, qty, entry_price, f,
                     price_getter=None,
                     use_websocket_price=False):
    orig_qty = qty
    curr_tp = entry_price * (1 + BASE_TP_PCT / 100.0)
    curr_sl = entry_price * (1 - BASE_SL_PCT / 100.0)

    oco = place_oco_sell(symbol, qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
    if oco is None:
        notify(f"❌ Initial OCO failed for {symbol}, aborting monitor.")
        return False, entry_price, 0.0, 0.0

    tick = float(f.get('tickSize', 0.0) or 0.0)
    step_size = float(f.get('stepSize', 0.0) or 0.0)
    min_qty = float(f.get('minQty', 0.0) or 0.0)
    min_notional = float(f.get('minNotional', 0.0) or 0.0)

    if price_getter is None:
        price_getter = get_price_cached

    last_roll_price = entry_price
    last_roll_ts = 0.0
    roll_count = 0
    CHECK_BALANCE_EVERY = globals().get('CHECK_BALANCE_EVERY', 3)
    CHECK_OPEN_ORDERS_EVERY = globals().get('CHECK_OPEN_ORDERS_EVERY', 2)
    loop_count = 0

    last_free_qty = orig_qty
    last_open_orders = []
    asset = symbol[:-len(QUOTE)]

    while True:
        try:
            loop_count += 1
            time.sleep(SLEEP_BETWEEN_CHECKS)
            price_now = price_getter(symbol)
            if price_now is None:
                try:
                    price_now = float(client.get_symbol_ticker(symbol=symbol)['price'])
                except Exception as e:
                    notify(f"⚠️ Failed to fetch price in monitor (fallback): {e}")
                    continue

            if loop_count % CHECK_BALANCE_EVERY == 0:
                try:
                    free_qty = get_free_asset(asset)
                    last_free_qty = free_qty
                except Exception:
                    free_qty = last_free_qty
            else:
                free_qty = last_free_qty

            available_for_sell = min(round_step(free_qty, step_size), orig_qty)

            if loop_count % CHECK_OPEN_ORDERS_EVERY == 0:
                try:
                    open_orders = get_open_orders_cached(symbol)
                    last_open_orders = open_orders
                except Exception:
                    open_orders = last_open_orders
            else:
                open_orders = last_open_orders

            if available_for_sell < round_step(orig_qty * 0.05, step_size) and len(open_orders) == 0:
                exit_price = price_now
                profit_usd = (exit_price - entry_price) * orig_qty
                notify(f"✅ Position closed for {symbol}: exit={exit_price:.8f}, profit≈${profit_usd:.6f}")
                return True, exit_price, profit_usd, available_for_sell

            now_ts = time.time()
            can_roll = (now_ts - last_roll_ts) >= globals().get('ROLL_COOLDOWN_SECONDS', 1.0)
            roll_on_rise_pct = globals().get('ROLL_ON_RISE_PCT', None)
            if roll_count == 0 and roll_on_rise_pct is not None:
                allowed_to_start = price_now >= entry_price * (1 + roll_on_rise_pct / 100.0)
            else:
                allowed_to_start = True

            step_trigger = (
                price_now >= last_roll_price * (1 + globals().get('ROLL_STEP_PCT', 0.0) / 100.0) or
                price_now - last_roll_price >= globals().get('ROLL_STEP_ABS', 0.0)
            )

            # --- NEW: minNotional check before rolling ---
            if min_notional and available_for_sell * price_now < min_notional - 1e-12:
                notify(f"⚠️ Remaining qty for {symbol} too small to roll (qty*price={available_for_sell*price_now:.8f} < minNotional). Attempting market sell immediately.")
                resp = place_market_sell_fallback(symbol, available_for_sell, f)
                if resp:
                    notify(f"✅ Market sell fallback successful for {symbol}")
                    return True, price_now, (price_now-entry_price) * available_for_sell, available_for_sell
                else:
                    notify(f"❌ Market sell fallback failed for {symbol}. Manual intervention needed.")
                last_roll_ts = now_ts
                continue

            if step_trigger and available_for_sell >= min_qty and can_roll and allowed_to_start:
                if roll_count >= globals().get('MAX_ROLLS_PER_POSITION', 20):
                    notify(f"⚠️ Reached max rolls ({globals().get('MAX_ROLLS_PER_POSITION', 20)}) for {symbol}.")
                    last_roll_ts = now_ts
                    continue

                notify(f"🔎 Roll triggered for {symbol}: price={price_now:.8f}, last_roll_price={last_roll_price:.8f}, curr_tp={curr_tp:.8f}")

                candidate_tp = curr_tp + globals().get('ROLL_TP_STEP_ABS', 0.0)
                candidate_sl = curr_sl + globals().get('ROLL_SL_STEP_ABS', 0.0)
                if candidate_sl > entry_price:
                    candidate_sl = entry_price

                new_tp = math.ceil(candidate_tp / tick) * tick if tick else candidate_tp
                new_sl = math.floor(candidate_sl / tick) * tick if tick else candidate_sl
                tick_step = tick or 0.0

                if new_tp <= new_sl + (tick_step or 0.0):
                    new_tp = new_sl + tick_step * 2 if tick_step > 0 else new_sl + max(1e-8, globals().get('ROLL_TP_STEP_ABS', 1e-8))
                if new_tp <= curr_tp:
                    new_tp = math.ceil((curr_tp + tick_step) / tick_step) * tick_step if tick_step > 0 else curr_tp + max(1e-8, globals().get('ROLL_TP_STEP_ABS', 1e-8))

                sell_qty = round_step(available_for_sell, step_size)
                if sell_qty <= 0 or sell_qty < min_qty:
                    notify(f"⚠️ Roll skipped: sell_qty {sell_qty} too small or < minQty.")
                    last_roll_ts = now_ts
                    continue

                # single-pass minNotional adjustment
                if min_notional and sell_qty * new_tp < min_notional - 1e-12:
                    max_adj = 10
                    adj = 0
                    while adj < max_adj and sell_qty * new_tp < min_notional - 1e-12:
                        new_tp = math.ceil((new_tp + tick_step) / tick_step) * tick_step if tick_step > 0 else new_tp + max(1e-8, new_tp * 0.001)
                        adj += 1
                    if sell_qty * new_tp < min_notional - 1e-12:
                        needed_qty = ceil_step(min_notional / new_tp, step_size)
                        if needed_qty <= available_for_sell + 1e-12 and needed_qty > sell_qty:
                            notify(f"ℹ️ Increasing sell_qty to {needed_qty} to meet minNotional for roll.")
                            sell_qty = needed_qty
                        else:
                            notify(f"⚠️ Roll aborted: cannot meet minNotional for {symbol} even after TP bumps. Attempting market sell fallback...")
                            # TRY MARKET SELL if possible
                            if sell_qty * price_now >= min_notional:
                                notify(f"⚠️ Attempting market sell fallback on {symbol} for remaining qty={sell_qty}, price={price_now:.8f}")
                                resp = place_market_sell_fallback(symbol, sell_qty, f)
                                if resp:
                                    notify(f"✅ Market sell fallback successful for {symbol}")
                                    return True, price_now, (price_now-entry_price) * sell_qty, sell_qty
                                else:
                                    notify(f"❌ Market sell fallback failed for {symbol}. Manual intervention needed.")
                            else:
                                notify(f"⚠️ Remaining qty for {symbol} too small for any sell (even market). Position will be stuck until manually handled.")
                            last_roll_ts = now_ts
                            continue

                last_roll_ts = now_ts

                need_cancel = False
                try:
                    sell_orders = [o for o in (open_orders or []) if str(o.get('side','')).upper() == 'SELL']
                    if sell_orders:
                        for o in sell_orders:
                            ord_price = 0.0
                            for key in ('price', 'stopPrice', 'avgPrice'):
                                if o.get(key) is not None:
                                    try:
                                        ord_price = float(o.get(key))
                                        break
                                    except Exception:
                                        ord_price = 0.0
                            if ord_price <= 0.0 or abs(ord_price - new_tp) > (tick_step * 1.5 or 1e-8):
                                need_cancel = True
                                break
                except Exception:
                    need_cancel = True

                if need_cancel:
                    try:
                        cancel_all_open_orders(symbol)
                        time.sleep(random.uniform(0.05, 0.15))
                    except Exception as e:
                        notify(f"⚠️ cancel_all_open_orders failed: {e}")

                oco2 = place_oco_sell(symbol, sell_qty, entry_price, explicit_tp=new_tp, explicit_sl=new_sl)
                if oco2:
                    roll_count += 1
                    curr_tp = new_tp
                    curr_sl = new_sl
                    last_roll_price = price_now
                    notify(f"🔁 Rolled OCO (step): new TP={curr_tp:.8f}, new SL={curr_sl:.8f}, qty={sell_qty}")
                else:
                    notify("⚠️ Roll attempt failed; will try fallback OCO next loop")
                    time.sleep(0.35)
                    fallback = place_oco_sell(symbol, sell_qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
                    if fallback:
                        notify("ℹ️ Fallback OCO re-placed after failed roll.")
                    else:
                        notify("❌ Fallback OCO also failed; TEMP skipping symbol.")
                        TEMP_SKIP[symbol] = time.time() + globals().get('SKIP_SECONDS_ON_MARKET_CLOSED', 30)

        except Exception as e:
            notify(f"⚠️ Error in monitor_and_roll: {e}")
            return False, entry_price, 0.0, 0.0


# -------------------------
# SAFE SELL FALLBACK (market)
# -------------------------
def place_market_sell_fallback(symbol, qty, f):
    try:
        if not f:
            info = get_symbol_info_cached(symbol)
            f = get_filters(info) if info else {}
    except Exception:
        f = f or {}
    try:
        qty_str = format_qty(qty, f.get('stepSize', 0.0))
    except Exception:
        qty_str = str(qty)
    notify(f"⚠️ Attempting MARKET sell fallback for {symbol}: qty={qty_str}")
    c = get_client()
    if not c:
        notify(f"❌ Market sell fallback failed for {symbol}: Binance client unavailable")
        return None
    try:
        try:
            resp = c.order_market_sell(symbol=symbol, quantity=qty_str)
        except Exception:
            resp = c.create_order(symbol=symbol, side='SELL', type='MARKET', quantity=qty_str)
        notify(f"✅ Market sell fallback executed for {symbol}")
        try:
            with OPEN_ORDERS_LOCK:
                OPEN_ORDERS_CACHE['data'] = None
                OPEN_ORDERS_CACHE['ts'] = 0
        except Exception:
            pass
        return resp
    except Exception as e:
        notify(f"❌ Market sell fallback failed for {symbol}: {e}")
        return None

# -------------------------
# OCO SELL with robust fallbacks & minNotional & qty adjustment
# -------------------------
def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=0.9,
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
# CANCEL HELPERS (updated)
# -------------------------
def cancel_all_open_orders(symbol, max_cancel=6, inter_delay=0.25):
    """
    Cancel up to max_cancel open orders for symbol. Returns dict with counts and status.
    """
    result = {'cancelled': 0, 'errors': 0, 'partial': False}
    try:
        open_orders = get_open_orders_cached(symbol)
        cancelled = 0
        for o in open_orders:
            if cancelled >= max_cancel:
                notify(f"⚠️ Reached max_cancel ({max_cancel}) for {symbol}; leaving remaining orders.")
                result['partial'] = True
                break
            try:
                client.cancel_order(symbol=symbol, orderId=o.get('orderId'))
                cancelled += 1
                result['cancelled'] = cancelled
                time.sleep(inter_delay)
            except BinanceAPIException as e:
                err = str(e)
                notify(f"⚠️ Cancel failed for {symbol} order {o.get('orderId')}: {err}")
                result['errors'] += 1
                if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                    # escalate rate-limit backoff and temp-skip symbol
                    prev = RATE_LIMIT_BACKOFF if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP
                    RATE_LIMIT_BACKOFF = min(prev * 2 if prev else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                    TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                    notify(f"❗ Rate-limit on cancel — backing off {RATE_LIMIT_BACKOFF}s and TEMP skipping {symbol}.")
                    result['partial'] = True
                    break
                # otherwise continue attempting others
            except Exception as e:
                notify(f"⚠️ Cancel failed for {symbol} order {o.get('orderId')}: {e}")
                result['errors'] += 1
        # invalidate cache after attempts (under lock)
        try:
            with OPEN_ORDERS_LOCK:
                OPEN_ORDERS_CACHE['data'] = None
                OPEN_ORDERS_CACHE['ts'] = 0
        except Exception:
            pass
        if cancelled:
            notify(f"❌ Cancelled {cancelled} open orders for {symbol}")
    except Exception as e:
        notify(f"⚠️ Failed to cancel orders: {e}")
        result['errors'] += 1
    return result

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
    try:
        now_ts = time.time()
        open_orders = get_open_orders_cached() or []
        # group by symbol
        by_symbol = {}
        for o in open_orders:
            sym = o.get('symbol')
            if not sym:
                continue
            by_symbol.setdefault(sym, []).append(o)

        for sym, orders in by_symbol.items():
            oldest_ts = None
            for o in orders:
                # pick best timestamp we can find on order object
                # common fields: 'time', 'updateTime', 'transactTime', 'timestamp'
                ts = None
                for k in ('time', 'updateTime', 'transactTime', 'timestamp'):
                    try:
                        v = o.get(k)
                        if isinstance(v, (int, float)) and v > 1e9:
                            # Binance often returns ms; normalize to seconds
                            if v > 1e12:
                                ts = v / 1000.0
                            else:
                                ts = v / 1000.0 if v > 1e9 else v
                            break
                    except Exception:
                        continue
                if ts is None:
                    # try extracting from string fields (rare)
                    ts = None
                if ts is not None:
                    if oldest_ts is None or ts < oldest_ts:
                        oldest_ts = ts

            if oldest_ts and (now_ts - oldest_ts) > OCO_MAX_LIFE_SECONDS:
                notify(f"⚠️ OCO for {sym} older than {OCO_MAX_LIFE_SECONDS}s -> cancelling and market-selling remainder.")
                try:
                    cancel_all_open_orders(sym)
                except Exception as e:
                    notify(f"⚠️ Error cancelling orders for {sym} during OCO-age enforcement: {e}")
                # determine free asset and try market sell fallback
                try:
                    asset = sym[:-len(QUOTE)]
                    free_qty = get_free_asset(asset)
                    f = {}
                    info = get_symbol_info_cached(sym)
                    if info:
                        f = get_filters(info)
                    if free_qty and free_qty >= (f.get('minQty', 0.0) or 0.0) and free_qty > 0:
                        place_market_sell_fallback(sym, free_qty, f)
                except Exception as e:
                    notify(f"⚠️ Failed to market-sell {sym} after OCO-age enforcement: {e}")
    except Exception as e:
        notify(f"⚠️ enforce_oco_max_life error: {e}")

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
        except Exception:
            start_balance_usdt = 0.0
        notify(f"🔰 Start balance snapshot: ${start_balance_usdt:.6f}")

    # try to start daily reporter if present (non-fatal)
    try:
        _start_daily_reporter_once()
    except Exception:
        pass

    while True:
        try:
            cleanup_recent_buys()
            cleanup_temp_skip()

            try:
                enforce_oco_max_life_and_exit_if_needed()
            except Exception:
                pass

            open_orders_global = get_open_orders_cached()
            if open_orders_global:
                time.sleep(300)
                continue

            if ACTIVE_SYMBOL is not None:
                time.sleep(CYCLE_DELAY)
                continue

            now = time.time()
            if now - LAST_BUY_TS < BUY_LOCK_SECONDS:
                time.sleep(CYCLE_DELAY)
                continue

            candidate = pick_coin()
            if not candidate:
                time.sleep(CYCLE_DELAY)
                continue

            symbol, price, volume, change, closes = candidate
            usd_suggested = float(globals().get('DEFAULT_USD_PER_TRADE', globals().get('TRADE_USD', TRADE_USD)))
            free_usdt = get_free_usdt()
            usd_to_buy = min(usd_suggested, free_usdt)
            if usd_to_buy < 1.0:
                time.sleep(CYCLE_DELAY)
                continue

            info = get_symbol_info_cached(symbol)
            f = get_filters(info) if info else {}
            min_notional = f.get('minNotional', 0.0)
            # Ensure buy is large enough to allow micro TP and rolls
            min_safe_trade = min_notional * 2.5  # allow for both micro TP and roll
            if usd_to_buy < min_safe_trade:
                notify(f"⏭️ Skipping buy for {symbol}: not enough USDT to safely trade (need at least ${min_safe_trade:.2f})")
                time.sleep(CYCLE_DELAY)
                continue

            if not is_symbol_tradable(symbol):
                TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
                time.sleep(CYCLE_DELAY)
                continue

            try:
                buy_res = place_safe_market_buy(symbol, usd_to_buy, require_orderbook=globals().get('REQUIRE_ORDERBOOK_BEFORE_BUY', False))
            except Exception as e:
                err = str(e)
                if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                    RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                             RATE_LIMIT_BACKOFF_MAX)
                    notify(f"❌ Rate limit detected during buy attempt: backing off for {RATE_LIMIT_BACKOFF}s.")
                    time.sleep(RATE_LIMIT_BACKOFF)
                else:
                    notify(f"❌ Exception during market buy attempt for {symbol}: {err}")
                    time.sleep(CYCLE_DELAY)
                continue

            if not buy_res or buy_res == (None, None):
                time.sleep(CYCLE_DELAY)
                continue

            qty, entry_price = buy_res
            if qty is None or entry_price is None:
                time.sleep(CYCLE_DELAY)
                continue

            if globals().get('NOTIFY_ON_BUY', True):
                notify(f"✅ BUY {symbol}: qty={qty} ~price={entry_price:.8f} notional≈${qty*entry_price:.6f}")

            try:
                now_datekey = (datetime.utcnow() + __import__('datetime').timedelta(hours=3)).date().isoformat()
                ent = METRICS.get(now_datekey) or {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0}
                ent['picks'] = ent.get('picks', 0) + 1
                METRICS[now_datekey] = ent
            except Exception:
                pass

            RECENT_BUYS[symbol] = {'ts': time.time(), 'price': entry_price, 'profit': None, 'cooldown': REBUY_COOLDOWN}

            info = get_symbol_info_cached(symbol)
            f = get_filters(info) if info else {}
            if not f:
                notify(f"⚠️ Could not fetch filters for {symbol} after buy; aborting monitoring for safety.")
                try:
                    place_market_sell_fallback(symbol, qty, f)
                except Exception:
                    pass
                time.sleep(CYCLE_DELAY)
                continue

            micro_order, micro_sold_qty, micro_tp_price = None, 0.0, None
            try:
                micro_order, micro_sold_qty, micro_tp_price = place_micro_tp(symbol, qty, entry_price, f)
            except Exception as e:
                notify(f"⚠️ Micro TP placement error for {symbol}: {e}")

            qty_remaining = round_step(max(0.0, qty - micro_sold_qty), f.get('stepSize', 0.0))
            # NEW: If remainder is unrollable, do not proceed
            if qty_remaining <= 0 or qty_remaining < f.get('minQty', 0.0) or (f.get('minNotional') and qty_remaining * entry_price < f.get('minNotional') - 1e-12):
                # All sold in micro TP or remainder is unsellable/unrollable, close position
                total_profit_usd = 0.0
                try:
                    if micro_sold_qty and micro_tp_price:
                        total_profit_usd += (micro_tp_price - entry_price) * micro_sold_qty
                except Exception:
                    pass

                try:
                    was_win = total_profit_usd > 0
                    date_key, m = _update_metrics_for_profit(total_profit_usd, picked_symbol=symbol, was_win=was_win)
                    if globals().get('NOTIFY_ON_CLOSE', True):
                        notify(f"🔁 Position closed for {symbol}: profit≈${total_profit_usd:.6f}")
                except Exception:
                    pass
                LAST_BUY_TS = time.time()
                time.sleep(CYCLE_DELAY)
                continue

            ACTIVE_SYMBOL = symbol
            LAST_BUY_TS = time.time()

            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            try:
                # monitor_and_roll now returns filled_qty as 4th element
                closed, exit_price, profit_usd, filled_qty = monitor_and_roll(symbol, qty_remaining, entry_price, f)
            finally:
                ACTIVE_SYMBOL = None
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass

            # compute total profit including micro TP
            total_profit = 0.0
            try:
                total_profit = profit_usd or 0.0
                if micro_order and micro_sold_qty and micro_tp_price:
                    total_profit += (micro_tp_price - entry_price) * micro_sold_qty
            except Exception:
                pass

            # update RECENT_BUYS with computed profit (even if zero)
            try:
                ent = RECENT_BUYS.get(symbol, {})
                ent['ts'] = time.time()
                ent['price'] = entry_price
                ent['profit'] = total_profit
                ent['cooldown'] = LOSS_COOLDOWN if total_profit < 0 else REBUY_COOLDOWN
                RECENT_BUYS[symbol] = ent
            except Exception:
                pass

            try:
                was_win = (total_profit > 0)
                date_key, m = _update_metrics_for_profit(total_profit, picked_symbol=symbol, was_win=was_win)
                # Only send success notification if there was an actual executed sell
                if globals().get('NOTIFY_ON_CLOSE', True):
                    # there was either micro sells OR monitor sold some qty
                    if (micro_sold_qty and micro_sold_qty > 0) or (filled_qty and filled_qty > 0):
                        sign = "+" if total_profit >= 0 else "-"
                        notify(f"✅ Position closed for {symbol}: exit={exit_price:.8f}, profit≈{sign}${abs(total_profit):.6f}")
                    else:
                        # no executed sells detected: warn instead of sending misleading success
                        notify(f"⚠️ Position closed for {symbol} but no executed sell detected (filled_qty=0). profit≈${total_profit:.6f}")
            except Exception as e:
                if globals().get('NOTIFY_ON_ERROR', True):
                    notify(f"⚠️ Error updating metrics after position closed for {symbol}: {e}")

            try:
                if total_profit and total_profit > 0:
                    try:
                        send_profit_to_funding(max(0.0, float(total_profit)))
                    except Exception:
                        pass
            except Exception:
                pass

            RATE_LIMIT_BACKOFF = 0

        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err or 'Way too much request weight' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                         RATE_LIMIT_BACKOFF_MAX)
                notify(f"❌ Rate limit reached in trade_cycle: backing off for {RATE_LIMIT_BACKOFF}s.")
                time.sleep(RATE_LIMIT_BACKOFF)
                continue

            if globals().get('NOTIFY_ON_ERROR', True):
                notify(f"❌ Trade cycle unexpected error: {e}")
            time.sleep(CYCLE_DELAY)

        time.sleep(CYCLE_DELAY)
        
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