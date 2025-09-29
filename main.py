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
CACHE_TTL = 240

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
                elif msg.startswith("📌 STOP_MARKE"):
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

# ---------------------------
# place_stop_market_order
# ---------------------------
def place_stop_market_order(symbol, qty, stop_price, retries=3, delay=1):
    global RATE_LIMIT_BACKOFF, TEMP_SKIP, OPEN_ORDERS_CACHE

    try:
        info = get_symbol_info_cached(symbol)
    except Exception:
        info = None

    if not info:
        notify(f"⚠️ place_stop_market_order: couldn't fetch symbol info for {symbol}")
        return None

    f = get_filters(info)
    asset = symbol[:-len(QUOTE)]

    # rounding helpers
    tick = f.get('tickSize') or None
    step = f.get('stepSize') or None

    def clip_floor(v, s):
        if not s or s == 0:
            return v
        return math.floor(v / s) * s

    def clip_ceil(v, s):
        if not s or s == 0:
            return v
        return math.ceil(v / s) * s

    # sanitize qty and stop_price
    qty = clip_floor(qty, step)
    stop_price = clip_floor(stop_price, tick)

    if qty <= 0:
        notify("❌ place_stop_market_order: quantity too small after clipping")
        return None
    if stop_price <= 0:
        notify("❌ place_stop_market_order: invalid stop_price")
        return None

    # ensure enough free asset
    free_qty = get_free_asset(asset)
    safe_margin = step if step and step > 0 else 0.0
    if free_qty + 1e-12 < qty:
        new_qty = clip_floor(max(0.0, free_qty - safe_margin), step)
        if new_qty <= 0:
            notify(f"❌ Not enough free {asset} to place stop sell. free={free_qty}, required={qty}")
            return None
        notify(f"ℹ️ Adjusting stop qty down from {qty} to available {new_qty} to avoid insufficient balance.")
        qty = new_qty

    # minNotional check (approx using stop_price)
    min_notional = f.get('minNotional')
    approx_price = stop_price if stop_price and stop_price > 0 else 1.0
    if min_notional:
        if qty * approx_price < min_notional - 1e-12:
            try:
                needed_qty = ceil_step(min_notional / approx_price, step)
            except Exception:
                needed_qty = clip_ceil(min_notional / approx_price, step)
            if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                notify(f"ℹ️ Increasing qty from {qty} to {needed_qty} to meet minNotional.")
                qty = needed_qty
            else:
                # try bumping stop price a few ticks
                attempts = 0
                while attempts < 40 and qty * approx_price < min_notional - 1e-12:
                    if tick and tick > 0:
                        approx_price = clip_ceil(approx_price + tick, tick)
                    else:
                        approx_price = approx_price + max(1e-8, approx_price * 0.001)
                    attempts += 1
                if qty * approx_price < min_notional - 1e-12:
                    notify(f"⚠️ Cannot meet minNotional for stop market on {symbol} (qty*price={qty*approx_price:.8f} < {min_notional}). Will attempt fallback flows.")

    qty_str = format_qty(qty, step)
    stop_price_str = format_price(stop_price, tick)
    # for stop-limit fallback put limit slightly below stop (cushion)
    stop_limit_price = stop_price * 0.999 if stop_price and stop_price > 0 else stop_price
    stop_limit_str = format_price(clip_floor(stop_limit_price, tick), tick)

    # Attempt 1: STOP_MARKET / STOP_LOSS (trigger-market)
    for attempt in range(1, retries + 1):
        try:
            # Binance-style: type='STOP_LOSS' triggers a MARKET sell when stopPrice reached
            order = client.create_order(
                symbol=symbol,
                side='SELL',
                type='STOP_LOSS',
                quantity=qty_str,
                stopPrice=stop_price_str
            )
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 STOP_MARKET placed ✅ {symbol} stop={stop_price_str}, qty={qty_str}")
            return {'sl': stop_price, 'method': 'stop_market', 'raw': order}
        except BinanceAPIException as e:
            err = str(e)
            notify(f"STOP_MARKET attempt {attempt} failed for {symbol}: {err}")
            # handle notional errors
            if 'NOTIONAL' in err or 'minNotional' in err or '-1013' in err:
                if min_notional:
                    try:
                        needed_qty = ceil_step(min_notional / float(approx_price), step)
                    except Exception:
                        needed_qty = clip_ceil(min_notional / float(approx_price), step)
                    if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                        qty = needed_qty
                        qty_str = format_qty(qty, step)
                        notify(f"ℹ️ Adjusted qty to {qty_str} to satisfy minNotional; retrying STOP_MARKET.")
                        time.sleep(0.25)
                        continue
            # rate limit handling
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                notify("❗ Rate-limit detected while placing STOP_MARKET — backing off and TEMP skipping symbol.")
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                return None
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)
        except Exception as e:
            notify(f"⚠️ Unexpected error on STOP_MARKET attempt for {symbol}: {e}")
            time.sleep(0.2)

    # Attempt 2: STOP_LOSS_LIMIT fallback
    notify(f"⚠️ STOP_MARKET attempts failed for {symbol} — trying STOP_LOSS_LIMIT fallback.")
    for attempt in range(1, retries + 1):
        try:
            order2 = client.create_order(
                symbol=symbol,
                side='SELL',
                type='STOP_LOSS_LIMIT',
                quantity=qty_str,
                stopPrice=stop_price_str,
                price=stop_limit_str,
                timeInForce='GTC'
            )
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📉 STOP_LOSS_LIMIT placed (fallback) for {symbol}: trigger={stop_price_str}, limit={stop_limit_str}, qty={qty_str}")
            return {'sl': stop_price, 'method': 'stop_limit_fallback', 'raw': order2}
        except BinanceAPIException as e:
            err = str(e)
            notify(f"STOP_LOSS_LIMIT attempt {attempt} failed for {symbol}: {err}")
            if 'NOTIONAL' in err or 'minNotional' in err or 'Filter failure' in err:
                if min_notional:
                    try:
                        needed_qty = ceil_step(min_notional / float(approx_price), step)
                    except Exception:
                        needed_qty = clip_ceil(min_notional / float(approx_price), step)
                    if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                        qty = needed_qty
                        qty_str = format_qty(qty, step)
                        notify(f"ℹ️ Adjusted qty to {qty_str} to attempt to satisfy minNotional for stop-limit.")
                        time.sleep(0.25)
                        continue
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                notify("❗ Rate-limit detected while placing STOP_LOSS_LIMIT — backing off and TEMP skipping symbol.")
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                return None
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)
        except Exception as e:
            notify(f"⚠️ Unexpected error on STOP_LOSS_LIMIT attempt for {symbol}: {e}")
            time.sleep(0.2)

    # Final fallback: try emergency market sell to avoid being stuck
    notify(f"❌ All attempts to place STOP for {symbol} failed — attempting emergency MARKET sell to avoid stuck position.")
    fallback_market = place_market_sell_fallback(symbol, qty, f)
    if fallback_market:
        notify(f"⚠️ Emergency market sell executed for {symbol} as final fallback.")
        return {'sl': stop_price, 'method': 'market_fallback', 'raw': fallback_market}

    # If all fails, TEMP skip symbol
    notify(f"❌ All stop placement attempts failed for {symbol}. TEMP skipping symbol.")
    TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
    return None

# ---------------------------
# monitor_and_roll (slightly adjusted to use place_stop_market_order)
# ---------------------------
def monitor_and_roll(symbol, bought_qty, avg_buy_price, ctx=None):
    """
    Monitors a bought position and moves stop higher as price rises.
    Uses place_stop_market_order() for placing stops and cancel_order() to remove old stops.
    """
    global TEMP_SKIP, RATE_LIMIT_BACKOFF

    # defaults
    cfg = {
        'TRAIL_START_PCT': 0.8,
        'TRAIL_DOWN_PCT': 1.5,
        'POLL_INTERVAL': 2.0,
        'MAX_POSITION_TIME_SECONDS': 4 * 3600,
        'INITIAL_SL_PCT': 0.8,
        'MIN_CANCEL_WAIT': 0.25,
    }
    if ctx:
        cfg.update(ctx)

    TRAIL_START_PCT = float(cfg['TRAIL_START_PCT'])
    TRAIL_DOWN_PCT = float(cfg['TRAIL_DOWN_PCT'])
    POLL_INTERVAL = float(cfg['POLL_INTERVAL'])
    MAX_POSITION_TIME_SECONDS = float(cfg['MAX_POSITION_TIME_SECONDS'])
    INITIAL_SL_PCT = float(cfg['INITIAL_SL_PCT'])
    MIN_CANCEL_WAIT = float(cfg['MIN_CANCEL_WAIT'])

    start_ts = time.time()
    highest_price = float(avg_buy_price)
    qty_left = float(bought_qty)

    current_stop_order = None
    current_stop_price = None

    info = get_symbol_info_cached(symbol)
    f = get_filters(info) if info else {}
    price_tick = f.get('tickSize') or None
    qty_step = f.get('stepSize') or None

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

    # place initial conservative stop near buy (optional)
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

    # monitoring loop
    while True:
        now = time.time()
        if now - start_ts > MAX_POSITION_TIME_SECONDS:
            notify(f"Max position time reached for {symbol}. Forcing market exit of remaining {qty_left}")
            try:
                place_market_sell_fallback(symbol, qty_left, f)
            except Exception as e:
                notify(f"Emergency market sell failed for {symbol}: {e}")
            break

        # get latest price
        try:
            price = float(get_price_cached(symbol))
        except Exception as e:
            notify(f"Error fetching price for {symbol} while trailing: {e}")
            time.sleep(POLL_INTERVAL)
            continue

        if price > highest_price:
            highest_price = price

        # start trailing if not started and price moved up enough
        if current_stop_price is None:
            up_pct = (highest_price / avg_buy_price - 1.0) * 100.0
            if up_pct >= TRAIL_START_PCT:
                desired_stop = highest_price * (1.0 - TRAIL_DOWN_PCT / 100.0)
                desired_stop = _round_price(desired_stop)
                if desired_stop >= price:
                    desired_stop = _round_price(price * (1.0 - TRAIL_DOWN_PCT / 100.0))
                try:
                    try:
                        qty_left = float(get_current_position_qty(symbol))
                    except Exception:
                        pass
                    if qty_left <= 0:
                        notify(f"No remaining qty for {symbol} at trail start (qty_left={qty_left}). Exiting monitor.")
                        break
                    current_stop_order = place_stop_market_order(symbol, qty_left, desired_stop)
                    current_stop_price = desired_stop
                    notify(f"Started trailing for {symbol}: placed stop {current_stop_price} (peak {highest_price:.8f})")
                except Exception as e:
                    notify(f"Failed to place initial trailing stop for {symbol}: {e}")
                    current_stop_order = None
                    current_stop_price = None
        else:
            # already trailing: compute desired higher stop
            desired_stop = highest_price * (1.0 - TRAIL_DOWN_PCT / 100.0)
            desired_stop = _round_price(desired_stop)

            if desired_stop > (current_stop_price or 0):
                # refresh qty_left
                try:
                    qty_left = float(get_current_position_qty(symbol))
                except Exception:
                    pass

                if qty_left <= 0:
                    notify(f"Position for {symbol} closed while preparing to move stop.")
                    break

                # cancel old stop if exists
                if current_stop_order:
                    try:
                        cancel_order(current_stop_order)
                        time.sleep(MIN_CANCEL_WAIT)
                    except Exception as e:
                        notify(f"Warning: cancel order failed for {symbol}: {e}")

                    # reconcile after cancel: check whether partial fill happened
                    try:
                        new_qty = float(get_current_position_qty(symbol))
                    except Exception:
                        new_qty = qty_left
                    if new_qty < qty_left - 1e-12:
                        sold_amount = qty_left - new_qty
                        try:
                            profit = calculate_realized_profit(symbol, sold_amount)
                        except Exception:
                            profit = None
                        notify(f"Stop order filled during cancel for {symbol}: sold {sold_amount}, profit {profit}")
                        try:
                            if profit is not None:
                                send_profit_to_funding(symbol, profit)
                        except Exception:
                            pass
                        qty_left = new_qty
                        if qty_left <= 0:
                            notify(f"All qty sold for {symbol} while moving stop. Exiting monitor.")
                            current_stop_order = None
                            current_stop_price = None
                            break

                # place new stop at desired_stop for remaining qty
                try:
                    qty_to_place = _round_qty(qty_left)
                    if qty_to_place <= 0:
                        notify(f"Rounded qty to zero for {symbol} when attempting to place new stop; exiting.")
                        break
                    new_order = place_stop_market_order(symbol, qty_to_place, desired_stop)
                    current_stop_order = new_order
                    current_stop_price = desired_stop
                    notify(f"🔁 Moved stop for {symbol} up to {current_stop_price} for qty {qty_to_place}")
                except Exception as e:
                    notify(f"❌ Failed to place moved stop for {symbol}: {e}")
                    current_stop_order = None
                    time.sleep(POLL_INTERVAL)
                    continue

        # reconciliation: detect if some qty sold by stop
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
            except Exception:
                pass

            qty_left = latest_qty
            if qty_left <= 0:
                notify(f"Position fully closed for {symbol} by stop sell.")
                current_stop_order = None
                current_stop_price = None
                break

        time.sleep(POLL_INTERVAL)

    notify(f"monitor_and_roll ended for {symbol}.")
    return
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

            try:
                open_orders_global = get_open_orders_cached()
            except Exception:
                open_orders_global = None

            if open_orders_global:
                time.sleep(300)
                continue

            if ACTIVE_SYMBOL is not None:
                time.sleep(CYCLE_DELAY)
                continue

            now_ts = time.time()
            if now_ts - LAST_BUY_TS < BUY_LOCK_SECONDS:
                time.sleep(CYCLE_DELAY)
                continue

            candidate = pick_coin()
            if not candidate:
                time.sleep(CYCLE_DELAY)
                continue

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
                time.sleep(CYCLE_DELAY)
                continue

            usd_suggested = float(globals().get('DEFAULT_USD_PER_TRADE', globals().get('TRADE_USD', TRADE_USD)))
            try:
                free_usdt = get_free_usdt()
            except Exception:
                free_usdt = 0.0
            usd_to_buy = min(usd_suggested, free_usdt)

            if usd_to_buy < 1.0:
                time.sleep(CYCLE_DELAY)
                continue

            try:
                tradable = is_symbol_tradable(symbol)
            except Exception:
                tradable = False
            if not tradable:
                try:
                    TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
                except Exception:
                    pass
                time.sleep(CYCLE_DELAY)
                continue

            try:
                buy_res = place_safe_market_buy(symbol, usd_to_buy, require_orderbook=globals().get('REQUIRE_ORDERBOOK_BEFORE_BUY', False))
            except Exception as e:
                err = str(e)
                if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                    RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                    notify(f"❌ Rate limit detected during buy attempt: backing off for {RATE_LIMIT_BACKOFF}s.")
                    time.sleep(RATE_LIMIT_BACKOFF)
                else:
                    notify(f"❌ Exception during market buy attempt for {symbol}: {err}")
                    time.sleep(CYCLE_DELAY)
                continue

            if not buy_res:
                time.sleep(CYCLE_DELAY)
                continue

            if isinstance(buy_res, dict):
                qty = float(buy_res.get('executedQty') or buy_res.get('qty') or buy_res.get('executed_qty') or 0.0)
                entry_price = float(buy_res.get('avgPrice') or buy_res.get('avg_price') or buy_res.get('price') or entry_hint_price or 0.0)
            else:
                try:
                    qty, entry_price = buy_res
                    qty = float(qty)
                    entry_price = float(entry_price)
                except Exception:
                    notify(f"❌ Unexpected buy response for {symbol}: {buy_res}")
                    time.sleep(CYCLE_DELAY)
                    continue

            if qty is None or entry_price is None or qty <= 0 or entry_price <= 0:
                time.sleep(CYCLE_DELAY)
                continue

            notify(f"✅ BUY {symbol}: qty={qty} ~price={entry_price:.8f} notional≈${qty*entry_price:.6f}")

            try:
                now_datekey = (datetime.utcnow() + __import__('datetime').timedelta(hours=3)).date().isoformat()
                ent = METRICS.get(now_datekey) or {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0}
                ent['picks'] = ent.get('picks', 0) + 1
                METRICS[now_datekey] = ent
            except Exception:
                pass

            try:
                with RECENT_BUYS_LOCK:
                    RECENT_BUYS[symbol] = {'ts': time.time(), 'price': entry_price, 'profit': None, 'cooldown': REBUY_COOLDOWN}
            except Exception:
                pass

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
                time.sleep(CYCLE_DELAY)
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
                time.sleep(CYCLE_DELAY)
                continue

            ACTIVE_SYMBOL = symbol
            LAST_BUY_TS = time.time()

            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            profit_usd = 0.0
            exit_price = None
            try:
                res = monitor_and_roll(symbol, qty_remaining, entry_price, f)
                if isinstance(res, tuple) and len(res) >= 3:
                    _, exit_price, profit_usd = res[0], res[1], res[2]
                else:
                    try:
                        profit_usd = calculate_realized_profit(symbol) or 0.0
                    except Exception:
                        profit_usd = 0.0
                    try:
                        if 'get_last_fill_price' in globals():
                            exit_price = get_last_fill_price(symbol)
                        else:
                            if qty > 0:
                                exit_price = entry_price + (profit_usd / qty)
                            else:
                                exit_price = entry_price
                    except Exception:
                        exit_price = entry_price
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
                ACTIVE_SYMBOL = None
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass

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

            RATE_LIMIT_BACKOFF = 0

        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err or 'Way too much request weight' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                notify(f"❌ Rate limit reached in trade_cycle: backing off for {RATE_LIMIT_BACKOFF}s.")
                time.sleep(RATE_LIMIT_BACKOFF)
                continue
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