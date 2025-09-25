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
MIN_VOLUME = 2_000_000

RECENT_PCT_MIN = 0.6
RECENT_PCT_MAX = 4.0

MAX_24H_RISE_PCT = 4.0
MAX_24H_CHANGE_ABS = 5.0
MOVEMENT_MIN_PCT = 1.0

EMA_UPLIFT_MIN_PCT = 0.001
SCORE_MIN_THRESHOLD = 14.0

TRADE_USD = 7.0
MIN_TRADE_USD = 6.0
MAX_TRADE_USD = 10.0
TARGET_VOL_FRACTION = 0.01

SLEEP_BETWEEN_CHECKS = 8
CYCLE_DELAY = 8
COOLDOWN_AFTER_EXIT = 10

TRIGGER_PROXIMITY = 0.015
STEP_INCREMENT_PCT = 0.01
BASE_TP_PCT = 3.0
BASE_SL_PCT = 2.0

MICRO_TP_PCT = 1.0
MICRO_TP_FRACTION = 0.50
MICRO_MAX_WAIT = 12.0

ROLL_ON_RISE_PCT = 0.6
ROLL_TRIGGER_PCT = 0.75
ROLL_TRIGGER_DELTA_ABS = 0.006
ROLL_TP_STEP_ABS = 0.020
ROLL_SL_STEP_ABS = 0.003
ROLL_COOLDOWN_SECONDS = 60
MAX_ROLLS_PER_POSITION = 5
ROLL_POST_CANCEL_JITTER = (0.3, 0.8)

ROLL_FAIL_COUNTER = {}
FAILED_ROLL_THRESHOLD = 3
FAILED_ROLL_SKIP_SECONDS = 60 * 60

OCO_MAX_AGE_SECONDS = int(float(os.environ.get("OCO_MAX_AGE_SECONDS", 6 * 3600)))
INITIAL_TP_EXTRA_PCT = 0.5
OCO_TP_EXTRA_PCT = float(os.environ.get("OCO_TP_EXTRA_PCT", 0.35))

NOTIFY_QUEUE_MAX = 1000
NOTIFY_RETRY = 2
NOTIFY_TIMEOUT = 4.0
NOTIFY_PRIORITY_SUPPRESS = 0.08

NOTIFY_ON_BUY = True
NOTIFY_ON_CLOSE = True
NOTIFY_ON_ERROR = True
NOTIFY_ON_DAILY = True

RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_BACKOFF_MAX = 300
RATE_LIMIT_BASE_SLEEP = 90
CACHE_TTL = 240

TEMP_SKIP = {}
SKIP_SECONDS_ON_MARKET_CLOSED = 60 * 60

RECENT_BUYS = {}
REBUY_COOLDOWN = 60 * 60
LOSS_COOLDOWN = 60 * 60 * 4
REBUY_MAX_RISE_PCT = 5.0

METRICS = {}
METRICS_LOCK = Lock()
EAT_TZ = timezone(timedelta(hours=3))
DAILY_REPORT_HOUR_EAT = int(os.environ.get("DAILY_REPORT_HOUR_EAT", 22))

NOTIFY_THREAD_STARTED = False

# -------------------------
# HELPERS: formatting & rounding
# -------------------------
# improved notify system (safe defaults using globals().get)
_NOTIFY_Q = queue.Queue(maxsize=globals().get('NOTIFY_QUEUE_MAX', 1000))
_NOTIFY_THREAD_STARTED = False
_NOTIFY_LOCK = Lock()

# ----- replace existing notify() with this filtered version -----
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
                elif msg.startswith("🔁 Rolled"):
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

def compute_recent_volatility(closes, period=None):
    try:
        if not closes or len(closes) < 3:
            return None
        # choose period up to last N closes (e.g., 6)
        period = period or min(6, len(closes)-1)
        used = closes[-(period+1):]  # we need period returns
        returns = []
        for i in range(1, len(used)):
            a = used[i-1]
            b = used[i]
            if a <= 0:
                returns.append(0.0)
            else:
                returns.append(math.log((b + 1e-12) / (a + 1e-12)))
        if not returns:
            return None
        mean = sum(returns)/len(returns)
        var = sum((r-mean)**2 for r in returns) / len(returns)
        std = math.sqrt(var)
        # convert log-returns std to fractional approx
        return float(std)
    except Exception:
        return None

def compute_trade_size_by_volatility(closes, base_usd=TRADE_USD, target_vol=TARGET_VOL_FRACTION,
                                    min_usd=MIN_TRADE_USD, max_usd=MAX_TRADE_USD):
    try:
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
        MIN_VOL_RATIO = globals().get('MIN_VOL_RATIO', 1.25)
        MIN_QUOTE_VOL = globals().get('MIN_QUOTE_VOL', max(globals().get('MIN_VOLUME', MIN_VOLUME), 300_000))

        tickers = get_tickers_cached() or []
        prefiltered = []

        for t in tickers:
            sym = t.get('symbol')
            if not sym or not sym.endswith(QUOTE):
                continue

            skip_until = TEMP_SKIP.get(sym)
            if skip_until and now < skip_until:
                continue

            last_buy = RECENT_BUYS.get(sym)
            try:
                price = float(t.get('lastPrice') or 0.0)
                qvol = float(t.get('quoteVolume') or 0.0)
                change_pct = float(t.get('priceChangePercent') or 0.0)
            except Exception:
                continue

            if not (globals().get('PRICE_MIN', PRICE_MIN) <= price <= globals().get('PRICE_MAX', PRICE_MAX)):
                continue

            if qvol < MIN_QUOTE_VOL:
                continue

            if abs(change_pct) > globals().get('MAX_24H_CHANGE_ABS', MAX_24H_CHANGE_ABS):
                continue
            if change_pct > globals().get('MAX_24H_RISE_PCT', MAX_24H_RISE_PCT):
                continue

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

        # local fallback volatility calculator (fractional volatility)
        def _local_recent_volatility(closes):
            if not closes or len(closes) < 2:
                return 0.0
            rets = []
            for i in range(1, len(closes)):
                if closes[i-1] == 0:
                    rets.append(0.0)
                else:
                    rets.append((closes[i] - closes[i-1]) / closes[i-1])
            if not rets:
                return 0.0
            mean = sum(rets) / len(rets)
            var = sum((r - mean) ** 2 for r in rets) / len(rets)
            return math.sqrt(var)  # fractional (e.g., 0.01 = 1%)

        for sym, last_price, qvol, change_pct in sampled:
            try:
                time.sleep(REQUEST_SLEEP)

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
                            vols.append(float(k[5]) * float(k[4]))
                    except Exception:
                        vols.append(0.0)

                if not closes or len(closes) < 3:
                    continue

                recent_vol = vols[-1]
                prev_avg = (sum(vols[:-1]) / max(1, len(vols[:-1]))) if len(vols) > 1 else recent_vol
                vol_ratio = recent_vol / (prev_avg + 1e-12)

                # HARD filter: require volume uplift
                if vol_ratio < MIN_VOL_RATIO:
                    continue

                recent_pct = 0.0
                if len(closes) >= 4 and closes[-4] > 0:
                    recent_pct = (closes[-1] - closes[-4]) / (closes[-4] + 1e-12) * 100.0

                # volatility (try global function then fallback)
                try:
                    vol_f = compute_recent_volatility(closes) if 'compute_recent_volatility' in globals() else None
                    if vol_f is None:
                        vol_f = _local_recent_volatility(closes)
                except Exception:
                    vol_f = _local_recent_volatility(closes)

                # breakout requirement
                if len(closes) >= 4:
                    if not (closes[-1] > max(closes[:-1]) * (1.0 + PREBUY_BREAKOUT_MARGIN)):
                        continue

                # momentum last3
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

                # optional orderbook check
                if REQUIRE_OB_IN_PICK:
                    try:
                        if not orderbook_bullish(sym, depth=5, min_imbalance=1.08, max_spread_pct=0.6):
                            continue
                    except Exception:
                        continue

                # scoring
                score = 0.0
                score += max(0.0, recent_pct) * 12.0
                try:
                    score += math.log1p(qvol) * 0.5
                except Exception:
                    pass
                score += max(0.0, (vol_ratio - 1.0)) * 8.0
                score += ema_uplift * 100.0
                if rsi_val is not None:
                    score += max(0.0, (60.0 - min(rsi_val, 60.0))) * 1.0
                score += max(0.0, change_pct) * 0.6
                score -= min(vol_f, 5.0) * 0.5

                if score < SCORE_MIN:
                    continue

                # suggested trade sizing using compute_trade_size_by_volatility (if available)
                try:
                    if 'compute_trade_size_by_volatility' in globals():
                        suggested_usd = compute_trade_size_by_volatility(closes, base_usd=globals().get('TRADE_USD', TRADE_USD),
                                                                        target_vol=globals().get('TARGET_VOL_FRACTION', TARGET_VOL_FRACTION),
                                                                        min_usd=globals().get('MIN_TRADE_USD', MIN_TRADE_USD),
                                                                        max_usd=globals().get('MAX_TRADE_USD', MAX_TRADE_USD))
                    else:
                        suggested_usd = globals().get('TRADE_USD', TRADE_USD)
                except Exception:
                    suggested_usd = globals().get('TRADE_USD', TRADE_USD)

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
                    'closes': closes,
                    'suggested_usd': suggested_usd
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
        # sanitize inputs
        qty = float(qty)
        fraction = float(fraction)
        pct = float(pct)

        # compute intended micro sell qty (rounded down to step)
        intended = qty * fraction
        sell_qty = round_step(intended, f.get('stepSize', 0.0))

        # compute remainder left after micro sell
        remainder = round_step(qty - sell_qty, f.get('stepSize', 0.0))

        # decision logging
        notify(f"ℹ️ Micro TP decision for {symbol}: intended={intended:.8f}, sell_qty={sell_qty}, remainder={remainder}, fraction={fraction}")

        # If remainder would be a dust (non-zero but < minQty), prefer either:
        #  - increase sell_qty so remainder becomes zero (i.e., sell entire position), or
        #  - skip micro TP if that would violate minNotional.
        min_qty = f.get('minQty', 0.0)
        step = f.get('stepSize', 0.0)
        tick = f.get('tickSize', 0.0) or 0.0
        min_notional = f.get('minNotional')

        if remainder > 0 and remainder < min_qty:
            candidate_sell_all = round_step(qty, step)
            if candidate_sell_all >= min_qty:
                sell_qty = candidate_sell_all
                remainder = 0.0
                notify(f"ℹ️ Adjusting micro TP to sell entire position to avoid dust (sell_qty={sell_qty}).")
            else:
                notify(f"ℹ️ Skipping micro TP (would leave dust remainder={remainder} < minQty).")
                return None, 0.0, None

        # ensure sell_qty meets minQty
        if sell_qty <= 0 or sell_qty < min_qty:
            notify(f"ℹ️ Micro TP: sell_qty too small ({sell_qty}) for {symbol}, skipping micro TP.")
            return None, 0.0, None

        # compute TP price and round to tick
        tp_price = float(entry_price) * (1.0 + pct / 100.0)
        if tick and tick > 0:
            tp_price = math.ceil(tp_price / tick) * tick

        # re-check minNotional against rounded qty * rounded price
        if min_notional:
            # use rounded qty string (post-step) and tp_price (rounded) to evaluate notional
            qty_str_test = format_qty(sell_qty, step)
            try:
                qty_test = float(qty_str_test)
            except Exception:
                qty_test = sell_qty
            notional = qty_test * tp_price
            if notional < (min_notional - 1e-12):
                notify(f"⚠️ Micro TP would violate MIN_NOTIONAL for {symbol} (need {min_notional}, have {notional:.6f}). Skipping micro TP.")
                return None, 0.0, None

        qty_str = format_qty(sell_qty, step)
        price_str = format_price(tp_price, tick)

        # Place the limit sell
        try:
            order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=price_str)
            notify(f"📍 Micro TP placed for {symbol}: sell {qty_str} @ {price_str} (entry={entry_price:.8f})", priority=True)
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
        except Exception as e:
            notify(f"⚠️ Failed to place micro TP for {symbol}: {e}")
            return None, 0.0, None

        # if the client didn't return an orderId, just return the raw result and treat as not filled
        order_id = None
        if isinstance(order, dict):
            order_id = order.get('orderId') or order.get('orderId')
        if not order_id:
            # might be synchronous fill or ambiguous response — give caller the object but no fills tracked here
            return order, sell_qty, tp_price

        # Poll for fills up to MICRO_MAX_WAIT; use small adaptive interval to reduce load
        poll_interval = 0.6
        waited = 0.0
        max_wait = float(MICRO_MAX_WAIT or 20.0)
        filled_qty = 0.0
        avg_fill_price = None

        while waited < max_wait:
            try:
                status = client.get_order(symbol=symbol, orderId=order_id)
            except Exception:
                # if get_order fails, break and attempt to cancel later
                break

            # parse executedQty / fills
            executed_qty = 0.0
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
                # if avg_fill_price not set, attempt to compute from cumulative quote qty
                if avg_fill_price is None:
                    cumm = status.get('cummulativeQuoteQty') or status.get('cumulativeQuoteQty') or 0.0
                    try:
                        cumm = float(cumm)
                        if executed_qty > 0 and cumm > 0:
                            avg_fill_price = cumm / executed_qty
                    except Exception:
                        avg_fill_price = None
                if avg_fill_price is None:
                    avg_fill_price = tp_price

                filled_qty = round_step(executed_qty, step)
                # send profit to funding for micro portion (best-effort)
                profit_usd = (avg_fill_price - float(entry_price)) * filled_qty
                try:
                    profit_to_send = float(round(profit_usd, 6))
                except Exception:
                    profit_to_send = profit_usd
                if profit_to_send and profit_to_send > 0.0:
                    try:
                        send_profit_to_funding(profit_to_send)
                        notify(f"💸 Micro TP profit ${profit_to_send:.6f} for {symbol} sent to funding.", priority=True)
                    except Exception as e:
                        notify(f"⚠️ Failed to transfer micro profit for {symbol}: {e}")
                else:
                    notify(f"ℹ️ Micro TP filled but profit non-positive (${profit_usd:.6f}) — not sending.")
                return order, filled_qty, avg_fill_price

            # sleep then continue polling (adaptive increase)
            time.sleep(poll_interval)
            waited += poll_interval
            # gentle backoff to reduce load for long waits
            if waited > 6.0:
                poll_interval = min(1.2, poll_interval * 1.2)

        # if we reach here, micro TP not filled in time -> cancel order to free qty for rolling
        try:
            client.cancel_order(symbol=symbol, orderId=order_id)
            notify(f"⚠️ Micro TP timed out and was cancelled for {symbol} after {max_wait}s (sell {qty_str} @ {price_str}).")
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
        except Exception as e:
            notify(f"❌ Failed to cancel stale micro TP for {symbol}: {e}")

        # check if partial fill occurred after cancel (last attempt)
        try:
            status2 = None
            try:
                status2 = client.get_order(symbol=symbol, orderId=order_id)
            except Exception:
                status2 = None
            if status2:
                ex2 = status2.get('executedQty')
                if ex2:
                    ex2f = float(ex2)
                    filled_qty = round_step(ex2f, step)
                    # compute avg fill price if available
                    fills = status2.get('fills') or []
                    if fills:
                        total_q = sum(float(x.get('qty', 0.0) or 0.0) for x in fills)
                        total_quote = sum(float(x.get('qty', 0.0) or 0.0) * float(x.get('price', 0.0) or 0.0) for x in fills)
                        if total_q > 0:
                            avg_fill_price = total_quote / total_q
                    if filled_qty and filled_qty > 0:
                        notify(f"ℹ️ Micro TP partially filled after cancel: filled={filled_qty} @ {avg_fill_price or tp_price:.8f}")
                        # attempt to send funding for any profit (best-effort)
                        try:
                            profit_usd = (avg_fill_price - float(entry_price)) * filled_qty if avg_fill_price else (tp_price - float(entry_price)) * filled_qty
                            profit_to_send = float(round(profit_usd, 6))
                            if profit_to_send and profit_to_send > 0.0:
                                send_profit_to_funding(profit_to_send)
                        except Exception:
                            pass
                        return order, filled_qty, avg_fill_price
        except Exception:
            pass

        # nothing filled
        return None, 0.0, None

    except Exception as e:
        notify(f"⚠️ place_micro_tp error: {e}")
        return None, 0.0, None

def monitor_and_roll(symbol, qty, entry_price, f):

    orig_qty = qty
    curr_tp = entry_price * (1 + BASE_TP_PCT / 100.0) * (1 + INITIAL_TP_EXTRA_PCT / 100.0)
    curr_sl = entry_price * (1 - BASE_SL_PCT / 100.0)

    # place initial protection OCO
    oco = place_oco_sell(symbol, qty, entry_price, tp_pct=BASE_TP_PCT + INITIAL_TP_EXTRA_PCT, sl_pct=BASE_SL_PCT)
    if oco is None:
        # failed to protect: try safe market fallback to avoid being stuck
        notify(f"⚠️ Initial OCO failed for {symbol}, attempting market sell to avoid stuck position.")
        try:
            place_market_sell_fallback(symbol, qty, f)
        except Exception:
            pass
        return True, entry_price, 0.0, False

    start_ts = time.time()
    last_roll_ts = 0.0
    roll_count = 0

    def clip_tp(v, tick):
        if not tick or tick == 0:
            return v
        return math.ceil(v / tick) * tick

    def clip_sl(v, tick):
        if not tick or tick == 0:
            return v
        return math.floor(v / tick) * tick

    manual_detected = False

    while True:
        try:
            time.sleep(SLEEP_BETWEEN_CHECKS)
            now_ts = time.time()

            # Ageing: if OCO older than configured -> cancel + market-sell remainder
            if now_ts - start_ts >= OCO_MAX_AGE_SECONDS:
                notify(f"⚠️ OCO for {symbol} older than {OCO_MAX_AGE_SECONDS}s -> cancelling and market-selling remainder.")
                try:
                    cancel_all_open_orders(symbol, max_cancel=10, inter_delay=0.12)
                except Exception as e:
                    notify(f"⚠️ Cancel failed for {symbol}: {e}")
                # attempt market sell for remaining
                try:
                    place_market_sell_fallback(symbol, orig_qty, f)
                except Exception as e:
                    notify(f"⚠️ Market sell fallback failed for {symbol}: {e}")
                return True, get_price_cached(symbol) or entry_price, 0.0, False

            asset = symbol[:-len(QUOTE)]
            price_now = get_price_cached(symbol)
            if price_now is None:
                try:
                    price_now = float(client.get_symbol_ticker(symbol=symbol)['price'])
                except Exception:
                    continue

            free_qty = get_free_asset(asset)
            available_for_sell = min(round_step(free_qty, f.get('stepSize', 0.0)), orig_qty)
            open_orders = get_open_orders_cached(symbol)

            # If free quantity unexpectedly dropped drastically => manual sell or external interference
            if available_for_sell < round_step(orig_qty * 0.02, f.get('stepSize', 0.0)):
                # manual change detected
                manual_detected = True
                exit_price = price_now
                profit_usd = (exit_price - entry_price) * orig_qty
                return True, exit_price, profit_usd, True

            # Position appears closed if very little left and no open orders
            if available_for_sell < round_step(orig_qty * 0.05, f.get('stepSize', 0.0)) and len(open_orders) == 0:
                exit_price = price_now
                profit_usd = (exit_price - entry_price) * orig_qty
                return True, exit_price, profit_usd, manual_detected

            price_delta = price_now - entry_price
            rise_trigger_pct = price_now >= entry_price * (1 + ROLL_ON_RISE_PCT / 100.0)
            rise_trigger_abs = price_delta >= max(ROLL_TRIGGER_DELTA_ABS, entry_price * (ROLL_TRIGGER_PCT / 100.0))
            near_trigger = (price_now >= curr_tp * (1 - TRIGGER_PROXIMITY)) and (price_now < curr_tp * 1.05)
            tick = f.get('tickSize', 0.0) or 0.0
            minimal_move = max(entry_price * 0.004, ROLL_TRIGGER_DELTA_ABS * 0.4, tick)
            moved_enough = price_delta >= minimal_move
            can_roll = (now_ts - last_roll_ts) >= ROLL_COOLDOWN_SECONDS

            trigger_conditions = ((near_trigger and moved_enough) or rise_trigger_pct or rise_trigger_abs)
            if trigger_conditions and available_for_sell >= f.get('minQty', 0.0) and can_roll:
                if roll_count >= MAX_ROLLS_PER_POSITION:
                    last_roll_ts = now_ts
                    continue

                notify(f"🔎 Roll triggered for {symbol}: price={price_now:.8f}, entry={entry_price:.8f}, curr_tp={curr_tp:.8f}")

                candidate_tp = curr_tp + ROLL_TP_STEP_ABS
                candidate_sl = curr_sl + ROLL_SL_STEP_ABS
                if candidate_sl > entry_price:
                    candidate_sl = entry_price

                new_tp = clip_tp(candidate_tp, tick)
                new_sl = clip_sl(candidate_sl, tick)
                tick_step = tick or 0.0

                if new_tp <= new_sl + tick_step:
                    new_tp = new_sl + (tick_step * 2 if tick_step > 0 else max(1e-8, ROLL_TP_STEP_ABS))
                if new_tp <= curr_tp:
                    new_tp = curr_tp + (tick_step if tick_step > 0 else ROLL_TP_STEP_ABS)

                # cancel and recompute sell qty
                last_roll_ts = now_ts
                cancel_all_open_orders(symbol)
                OPEN_ORDERS_CACHE['data'] = None
                time.sleep(random.uniform(*ROLL_POST_CANCEL_JITTER))

                free_qty_after = get_free_asset(asset)
                available_for_sell = min(round_step(free_qty_after, f.get('stepSize', 0.0)), orig_qty)
                sell_qty = round_step(available_for_sell, f.get('stepSize', 0.0))

                if sell_qty <= 0 or sell_qty < f.get('minQty', 0.0):
                    continue

                # enforce minNotional
                min_notional = f.get('minNotional')
                if min_notional:
                    adjust_cnt = 0
                    max_adj = 40
                    while adjust_cnt < max_adj and sell_qty * new_tp < min_notional - 1e-12:
                        if tick_step > 0:
                            new_tp = math.ceil((new_tp + tick_step) / tick_step) * tick_step
                        else:
                            new_tp = new_tp + max(1e-8, new_tp * 0.001)
                        adjust_cnt += 1
                    if sell_qty * new_tp < min_notional - 1e-12:
                        needed_qty = ceil_step(min_notional / new_tp, f.get('stepSize'))
                        if needed_qty <= available_for_sell + 1e-12 and needed_qty > sell_qty:
                            sell_qty = needed_qty
                        else:
                            continue

                oco2 = place_oco_sell(symbol, sell_qty, entry_price, explicit_tp=new_tp, explicit_sl=new_sl)
                if oco2:
                    roll_count += 1
                    curr_tp = new_tp
                    curr_sl = new_sl
                    try:
                        ROLL_FAIL_COUNTER[symbol] = 0
                    except Exception:
                        pass
                    notify(f"🔁 Rolled OCO: new TP={curr_tp:.8f}, new SL={curr_sl:.8f}, qty={sell_qty}")
                else:
                    cnt = ROLL_FAIL_COUNTER.get(symbol, 0) + 1
                    ROLL_FAIL_COUNTER[symbol] = cnt
                    notify(f"⚠️ Roll attempt FAILED for {symbol} (count={cnt})")
                    time.sleep(0.4)
                    fallback = place_oco_sell(symbol, sell_qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
                    if fallback:
                        ROLL_FAIL_COUNTER[symbol] = 0
            # loop end
        except Exception as e:
            notify(f"⚠️ Error in monitor_and_roll: {e}")
            return False, entry_price, 0.0, manual_detected
            
# -------------------------
# SAFE SELL FALLBACK (market)
# -------------------------
def place_market_sell_fallback(symbol, qty, f):
    """Try market sell to ensure closing when all protective order attempts failed."""
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
    try:
        try:
            resp = client.order_market_sell(symbol=symbol, quantity=qty_str)
        except Exception:
            resp = client.create_order(symbol=symbol, side='SELL', type='MARKET', quantity=qty_str)
        notify(f"market sell fallback executed for {symbol}")
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        return resp
    except Exception as e:
        notify(f"❌ Market sell fallback failed for {symbol}: {e}")
        return None


# -------------------------
# SAFE SELL FALLBACK (market)
# -------------------------
def place_market_sell_fallback(symbol, qty, f):
    """Try market sell to ensure closing when all protective order attempts failed."""
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
    try:
        try:
            resp = client.order_market_sell(symbol=symbol, quantity=qty_str)
        except Exception:
            resp = client.create_order(symbol=symbol, side='SELL', type='MARKET', quantity=qty_str)
        notify(f"✅ Market sell fallback executed for {symbol}")
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        return resp
    except Exception as e:
        notify(f"❌ Market sell fallback failed for {symbol}: {e}")
        return None

# -------------------------
# OCO SELL with robust fallbacks & minNotional & qty adjustment
# -------------------------
def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=1.0,
                   explicit_tp: float = None, explicit_sl: float = None,
                   retries=3, delay=1):
    global RATE_LIMIT_BACKOFF, TEMP_SKIP

    try:
        info = get_symbol_info_cached(symbol)
        if not info:
            notify(f"⚠️ place_oco_sell: couldn't fetch symbol info for {symbol}")
            return None
        f = get_filters(info)
        asset = symbol[:-len(QUOTE)]

        # compute base TP/SL (before ticks)
        base_tp = explicit_tp if explicit_tp is not None else (buy_price * (1 + tp_pct / 100.0))
        base_sp = explicit_sl if explicit_sl is not None else (buy_price * (1 - sl_pct / 100.0))
        # apply initial extra to base TP when explicit_tp not provided (only for initial placement)
        if explicit_tp is None and globals().get('INITIAL_TP_EXTRA_PCT', 0):
            base_tp = base_tp * (1.0 + float(globals().get('INITIAL_TP_EXTRA_PCT', 0.0)) / 100.0)

        # compute stop-limit price (slightly below stop trigger)
        stop_limit_price = base_sp * 0.999

        def clip_floor(v, step):
            if not step or step == 0:
                return v
            return math.floor(v / step) * step

        def clip_ceil(v, step):
            if not step or step == 0:
                return v
            return math.ceil(v / step) * step

        # clip quantities and prices to symbol filters
        qty = clip_floor(qty, f.get('stepSize', 0.0))
        tp = clip_ceil(base_tp, f.get('tickSize', 0.0))
        sp = clip_floor(base_sp, f.get('tickSize', 0.0))
        sl = clip_floor(stop_limit_price, f.get('tickSize', 0.0))

        if qty <= 0:
            notify("❌ place_oco_sell: quantity too small after clipping")
            return None

        # ensure we really have enough free asset to sell
        free_qty = get_free_asset(asset)
        safe_margin = f.get('stepSize', 0.0) if f.get('stepSize') else 0.0
        if free_qty + 1e-12 < qty:
            new_qty = clip_floor(max(0.0, free_qty - safe_margin), f.get('stepSize', 0.0))
            if new_qty <= 0:
                notify(f"❌ Not enough free {asset} to place sell. free={free_qty}, required={qty}")
                return None
            notify(f"ℹ️ Adjusting sell qty down from {qty} to available {new_qty} to avoid insufficient balance.")
            qty = new_qty

        # handle minNotional: try increase qty first, then try lifting TP
        min_notional = f.get('minNotional')
        if min_notional:
            # if qty*tp < min_notional, try to increase qty if possible
            if qty * tp < min_notional - 1e-12:
                needed_qty = ceil_step(min_notional / tp, f.get('stepSize', 0.0))
                if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                    notify(f"ℹ️ Increasing qty from {qty} to {needed_qty} to meet minNotional (qty*tp >= {min_notional}).")
                    qty = needed_qty
                else:
                    # if not possible, try to raise TP (but cap attempts)
                    attempts = 0
                    max_attempts = 60
                    while attempts < max_attempts and qty * tp < min_notional - 1e-12:
                        if f.get('tickSize') and f.get('tickSize') > 0:
                            tp = clip_ceil(tp + f.get('tickSize'), f.get('tickSize'))
                        else:
                            tp = tp + max(1e-8, tp * 0.001)
                        attempts += 1
                    if qty * tp < min_notional - 1e-12:
                        notify(f"⚠️ Cannot meet minNotional for OCO on {symbol} (qty*tp={qty*tp:.8f} < {min_notional}). Will attempt fallback flow.")

        qty_str = format_qty(qty, f.get('stepSize', 0.0))
        tp_str = format_price(tp, f.get('tickSize', 0.0))
        sp_str = format_price(sp, f.get('tickSize', 0.0))
        sl_str = format_price(sl, f.get('tickSize', 0.0))

        # Helper to invalidate open orders cache safely
        def invalidate_open_orders_cache():
            try:
                with OPEN_ORDERS_LOCK:
                    OPEN_ORDERS_CACHE['data'] = None
                    OPEN_ORDERS_CACHE['ts'] = 0
            except Exception:
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                    OPEN_ORDERS_CACHE['ts'] = 0
                except Exception:
                    pass

        # ATTEMPT A: try alt param schema first (some client/server combos expect aboveType/belowType)
        for attempt in range(1, retries + 1):
            try:
                oco_alt = client.create_oco_order(
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
                invalidate_open_orders_cache()
                notify(f"📌 OCO SELL placed (alt params) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
                return {'tp': tp, 'sl': sp, 'method': 'oco_abovebelow', 'raw': oco_alt}
            except BinanceAPIException as e:
                err = str(e)
                notify(f"⚠️ OCO SELL attempt {attempt} (alt) failed: {err}")
                if 'NOTIONAL' in err or 'minNotional' in err:
                    # try adjust qty and retry (handled by outer loop)
                    if min_notional:
                        needed_qty = ceil_step(min_notional / float(tp), f.get('stepSize', 0.0))
                        if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                            qty = needed_qty
                            qty_str = format_qty(qty, f.get('stepSize', 0.0))
                            notify(f"ℹ️ Adjusted qty to {qty_str} to satisfy minNotional; retrying OCO (alt).")
                            time.sleep(0.25)
                            continue
                if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                    notify("❗ Rate-limit detected while placing OCO (alt) — backing off and TEMP skipping symbol.")
                    TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                    RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                    return None
                if attempt < retries:
                    time.sleep(delay)
                    continue
                # else fallthrough to next attempt type
            except Exception as e:
                notify(f"⚠️ Unexpected error on OCO (alt) attempt: {e}")
                time.sleep(0.2)
                # continue to next attempt type after loop

        # ATTEMPT B: try standard param create_oco_order
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
                invalidate_open_orders_cache()
                notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
                return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
            except BinanceAPIException as e:
                err = str(e)
                notify(f"⚠️ OCO SELL attempt {attempt} (standard) failed: {err}")
                if 'NOTIONAL' in err or 'minNotional' in err or '-1013' in err:
                    if min_notional:
                        needed_qty = ceil_step(min_notional / float(tp), f.get('stepSize', 0.0))
                        if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                            qty = needed_qty
                            qty_str = format_qty(qty, f.get('stepSize', 0.0))
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
                    continue
            except Exception as e:
                notify(f"⚠️ Unexpected error on OCO attempt: {e}")
                time.sleep(0.2)

        # Fallback: place separate TP (limit) and SL (stop-limit)
        notify("⚠️ All OCO attempts failed — falling back to separate TP (limit) + SL (stop-limit) or MARKET.")

        tp_order = None
        sl_order = None
        try:
            tp_order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=tp_str)
            invalidate_open_orders_cache()
            notify(f"📈 TP LIMIT placed (fallback): {tp_str}, qty={qty_str}")
        except BinanceAPIException as e:
            notify(f"❌ Fallback TP limit failed: {e}")
        except Exception as e:
            notify(f"❌ Fallback TP limit failed: {e}")

        try:
            sl_order = client.create_order(
                symbol=symbol,
                side="SELL",
                type="STOP_LOSS_LIMIT",
                stopPrice=sp_str,
                price=sl_str,
                timeInForce='GTC',
                quantity=qty_str
            )
            invalidate_open_orders_cache()
            notify(f"📉 SL STOP_LOSS_LIMIT placed (fallback): trigger={sp_str}, limit={sl_str}, qty={qty_str}")
        except BinanceAPIException as e:
            notify(f"❌ Fallback SL stop-limit failed: {e}")
        except Exception as e:
            notify(f"❌ Fallback SL stop-limit failed: {e}")

        if tp_order or sl_order:
            return {'tp': tp, 'sl': sp, 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order}}

        # Final fallback: market sell (to avoid being stuck)
        fallback_market = place_market_sell_fallback(symbol, qty, f)
        if fallback_market:
            return {'tp': tp, 'sl': sp, 'method': 'market_fallback', 'raw': fallback_market}

        notify("❌ All attempts to protect position failed (no TP/SL placed). TEMP skipping symbol.")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None

    except Exception as e:
        notify(f"⚠️ place_oco_sell unexpected error: {e}")
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
    
def start_daily_reporter():
    def _loop():
        while True:
            try:
                now = datetime.now(tz=EAT_TZ)
                # target today at 03:00:05 EAT
                target = now.replace(hour=22, minute=0, second=5, microsecond=0)
                if target <= now:
                    target = target + timedelta(days=1)
                to_sleep = (target - now).total_seconds()
                time.sleep(max(1.0, to_sleep))
                # on wake: report for yesterday (EAT date)
                yesterday = (datetime.now(tz=EAT_TZ) - timedelta(days=1)).strftime('%Y-%m-%d')
                _notify_daily_stats(yesterday)
            except Exception as e:
                notify(f"⚠️ Daily reporter error: {e}", priority=True)
                time.sleep(60)
    t = Thread(target=_loop, daemon=True)
    t.start()
try:
    start_daily_reporter()
except Exception:
    pass

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


# Daily reporter thread
def _daily_reporter_thread():
    """
    Sleeps until next 03:00 EAT (which is 00:00 UTC), then calls _notify_daily_stats for the previous day.
    Runs forever as daemon.
    """
    try:
        while True:
            now_utc = datetime.utcnow()
            # next 00:00 UTC
            next_midnight_utc = datetime(now_utc.year, now_utc.month, now_utc.day)  # today 00:00
            if now_utc >= next_midnight_utc:
                # move to next day midnight
                next_midnight_utc = next_midnight_utc + __import__('datetime').timedelta(days=1)
            sleep_seconds = (next_midnight_utc - now_utc).total_seconds()
            # sleep until 00:00 UTC (== 03:00 EAT)
            time.sleep(max(1.0, sleep_seconds + 1.0))

            # At this point it's 00:00 UTC => 03:00 EAT (the scheduled time).
            # Determine the date key for the day we just finished (previous EAT date).
            run_eat = datetime.utcnow() + __import__('datetime').timedelta(hours=22)
            report_date = (run_eat.date() - __import__('datetime').timedelta(days=1)).isoformat()
            try:
                # call the user's reporting function if present, else fallback to simple notify
                if '_notify_daily_stats' in globals() and callable(globals()['_notify_daily_stats']):
                    globals()['_notify_daily_stats'](report_date)
                else:
                    m = METRICS.get(report_date, {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0})
                    profit_val = m['profit']
                    profit_str = f"+{profit_val:.2f} USDT" if profit_val >= 0 else f"{profit_val:.2f} USDT"
                    notify(f"📊 Daily stats ({report_date}): picks={m['picks']} wins={m['wins']} losses={m['losses']} profit={profit_str}")
            except Exception as e:
                notify(f"⚠️ Daily reporter failed for {report_date}: {e}")

    except Exception as e:
        notify(f"⚠️ _daily_reporter_thread error: {e}")


# start daily reporter once
def _start_daily_reporter_once():
    global _DAILY_REPORTER_STARTED
    try:
        if _DAILY_REPORTER_STARTED:
            return
        t = Thread(target=_daily_reporter_thread, daemon=True)
        t.start()
        _DAILY_REPORTER_STARTED = True
    except Exception:
        pass

# -------------------------
# UPDATED TRADE CYCLE
# -------------------------
ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0
BUY_LOCK_SECONDS = 30

def trade_cycle():
    global start_balance_usdt, ACTIVE_SYMBOL, LAST_BUY_TS, RATE_LIMIT_BACKOFF

    # snapshot starting balance once
    if start_balance_usdt is None:
        try:
            start_balance_usdt = get_free_usdt()
        except Exception:
            start_balance_usdt = 0.0
        notify(f"🔰 Start balance snapshot: ${start_balance_usdt:.6f}")

    while True:
        try:
            # housekeeping
            cleanup_recent_buys()
            cleanup_temp_skip()

            # if any outstanding open orders globally, wait a bit (prevents overlapping state)
            try:
                open_orders_global = get_open_orders_cached()
            except Exception as e:
                notify(f"⚠️ get_open_orders_cached failed: {e}")
                open_orders_global = None

            if open_orders_global:
                notify("⏳ Open orders detected — waiting before new buy...")
                time.sleep(300)
                continue

            # prevent starting new buys while active symbol present
            if ACTIVE_SYMBOL is not None:
                notify(f"⏳ Active trade in progress for {ACTIVE_SYMBOL}, skipping new buys.")
                time.sleep(CYCLE_DELAY)
                continue

            # small buy lock between buys
            now = time.time()
            if now - LAST_BUY_TS < BUY_LOCK_SECONDS:
                time.sleep(CYCLE_DELAY)
                continue

            # obtain candidate: prefer pick_coin() but fall back to get_trade_candidates()
            candidate = None
            try:
                candidate = pick_coin()
            except Exception:
                try:
                    cands = get_trade_candidates()
                    candidate = cands[0] if cands else None
                except Exception:
                    candidate = None

            if not candidate:
                notify("⚠️ No eligible coin found. Sleeping...")
                if RATE_LIMIT_BACKOFF:
                    notify(f"⏸ Rate-limit backoff active: sleeping {RATE_LIMIT_BACKOFF}s")
                    time.sleep(RATE_LIMIT_BACKOFF)
                else:
                    time.sleep(30)
                continue

            # normalize candidate formats
            symbol = None
            price = None
            volume = None
            change = None
            try:
                if isinstance(candidate, (list, tuple)):
                    # pick_coin -> (symbol, price, qvol, change, closes)
                    if len(candidate) >= 4:
                        symbol = candidate[0]
                        price = candidate[1]
                        volume = candidate[2]
                        change = candidate[3]
                    elif len(candidate) >= 2:
                        symbol = candidate[0]
                        data = candidate[1]
                        if isinstance(data, dict):
                            price = data.get('price')
                            volume = data.get('qvol') or data.get('volume')
                            change = data.get('change')
                elif isinstance(candidate, dict):
                    symbol = candidate.get('symbol')
                    price = candidate.get('price')
                    volume = candidate.get('qvol') or candidate.get('volume')
                    change = candidate.get('change')
            except Exception as e:
                notify(f"⚠️ Candidate parsing error: {e}")
                symbol = None

            if not symbol:
                notify("⚠️ pick_coin returned unusable candidate. Sleeping...")
                time.sleep(CYCLE_DELAY)
                continue

            notify(f"🎯 Selected {symbol} for market buy (24h change={change}%, vol≈{volume})")

            # cooldown / rebuy protections
            last = RECENT_BUYS.get(symbol)
            if last:
                cooldown = last.get('cooldown', REBUY_COOLDOWN)
                if now < last['ts'] + cooldown:
                    notify(f"⏭️ Skipping {symbol}: recent buy cooldown active.")
                    time.sleep(CYCLE_DELAY)
                    continue
                last_price = last.get('price')
                if last_price and price and price > last_price * (1 + REBUY_MAX_RISE_PCT / 100.0):
                    notify(f"⏭️ Skipping {symbol}: price rose >{REBUY_MAX_RISE_PCT}% since last buy.")
                    time.sleep(CYCLE_DELAY)
                    continue

            # compute usd to use
            free_usdt = get_free_usdt()
            usd_to_buy = min(TRADE_USD, free_usdt)
            if usd_to_buy < 1.0:
                notify(f"⚠️ Not enough USDT ({free_usdt:.4f}) to place buy. Sleeping...")
                time.sleep(CYCLE_DELAY)
                continue

            # perform market buy (require orderbook confirmation for safety)
            try:
                buy_res = place_safe_market_buy(symbol, usd_to_buy, require_orderbook=True)
            except Exception as e:
                err = str(e)
                notify(f"❌ Exception during market buy for {symbol}: {err}")
                # detect rate-limit and set backoff similar to your existing style
                if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                    RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                             RATE_LIMIT_BACKOFF_MAX)
                    notify(f"❗ Rate-limit detected: backing off {RATE_LIMIT_BACKOFF}s.")
                    time.sleep(RATE_LIMIT_BACKOFF)
                else:
                    time.sleep(CYCLE_DELAY)
                continue

            if not buy_res or buy_res == (None, None):
                notify(f"ℹ️ Buy skipped/failed for {symbol}.")
                time.sleep(CYCLE_DELAY)
                continue

            qty, entry_price = buy_res
            if qty is None or entry_price is None:
                notify(f"⚠️ Unexpected buy result for {symbol}. Skipping.")
                time.sleep(CYCLE_DELAY)
                continue

            # record RECENT_BUYS to avoid rebuy spam
            RECENT_BUYS[symbol] = {'ts': time.time(), 'price': entry_price, 'profit': None, 'cooldown': REBUY_COOLDOWN}

            # get filters & try micro TP
            info = get_symbol_info_cached(symbol)
            f = get_filters(info) if info else {}
            if not f:
                notify(f"⚠️ Unable to fetch filters for {symbol} after buy. Aborting monitoring.")
                ACTIVE_SYMBOL = None
                time.sleep(CYCLE_DELAY)
                continue

            micro_order, micro_sold_qty, micro_tp_price = None, 0.0, None
            try:
                micro_order, micro_sold_qty, micro_tp_price = place_micro_tp(symbol, qty, entry_price, f)
            except Exception as e:
                notify(f"⚠️ place_micro_tp error for {symbol}: {e}")
                micro_order, micro_sold_qty, micro_tp_price = None, 0.0, None

            # qty left to monitor
            qty_remaining = round_step(max(0.0, qty - (micro_sold_qty or 0.0)), f.get('stepSize', 0.0))
            if qty_remaining <= 0 or qty_remaining < f.get('minQty', 0.0):
                notify(f"ℹ️ Nothing left to monitor for {symbol} after micro TP (qty_remaining={qty_remaining}).")
                # mark last buy ts and continue
                LAST_BUY_TS = time.time()
                # set cooldown/profit entry already handled
                time.sleep(CYCLE_DELAY)
                continue

            # mark active and invalidate open-orders cache
            ACTIVE_SYMBOL = symbol
            LAST_BUY_TS = time.time()
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            # monitor + roll (synchronous — same behavior as your original)
            try:
                closed, exit_price, profit_usd = monitor_and_roll(symbol, qty_remaining, entry_price, f)
            except Exception as e:
                notify(f"⚠️ monitor_and_roll error for {symbol}: {e}")
                closed, exit_price, profit_usd = False, None, 0.0
            finally:
                # always clear active symbol and invalidate open orders cache
                ACTIVE_SYMBOL = None
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass

            # include micro profit (if any)
            total_profit_usd = profit_usd or 0.0
            if micro_order and micro_sold_qty and micro_tp_price:
                try:
                    total_profit_usd += (micro_tp_price - entry_price) * micro_sold_qty
                except Exception:
                    pass

            # update RECENT_BUYS with profit & set cooldown depending on result
            try:
                ent = RECENT_BUYS.get(symbol, {})
                ent['ts'] = time.time()
                ent['price'] = entry_price
                ent['profit'] = total_profit_usd
                ent['cooldown'] = LOSS_COOLDOWN if (total_profit_usd < 0) else REBUY_COOLDOWN
                RECENT_BUYS[symbol] = ent
            except Exception:
                pass

            # if closed and profit positive, try send to funding (best-effort)
            try:
                if closed and total_profit_usd and total_profit_usd > 0:
                    send_profit_to_funding(total_profit_usd)
            except Exception as e:
                notify(f"⚠️ send_profit_to_funding failed: {e}")

            # successful cycle -> reset rate-limit backoff
            RATE_LIMIT_BACKOFF = 0

        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                         RATE_LIMIT_BACKOFF_MAX)
                notify(f"❌ Rate limit in trade_cycle: backing off {RATE_LIMIT_BACKOFF}s.")
                time.sleep(RATE_LIMIT_BACKOFF)
                continue
            notify(f"❌ trade_cycle unexpected error: {e}")
            time.sleep(CYCLE_DELAY)

        # short sleep between cycles
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