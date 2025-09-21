import os
import math
import time
import random
import queue
import statistics
import threading
import requests
from datetime import datetime
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

# price / liquidity filters
PRICE_MIN = 0.8
PRICE_MAX = 3.0
MIN_VOLUME = 500_000          # daily quote volume baseline

# require small recent move (we prefer coins that just started moving)
RECENT_PCT_MIN = 0.6
RECENT_PCT_MAX = 4.0            # require recent move between 1%..2%

# absolute 24h change guardrails (avoid extreme pump/dump)
MAX_24H_RISE_PCT = 5.0          # disallow > +5% 24h rise
MAX_24H_CHANGE_ABS = 5.0        # require abs(24h change) <= 5.0

MOVEMENT_MIN_PCT = 1.0

# picker tuning
EMA_UPLIFT_MIN_PCT = 0.0008        # fractional uplift threshold (0.001 = 0.1%)
SCORE_MIN_THRESHOLD = 10.0        # floor score required to accept a candidate

# runtime / pacing
TRADE_USD = 10.0
SLEEP_BETWEEN_CHECKS = 30
CYCLE_DELAY = 8
COOLDOWN_AFTER_EXIT = 10

# order / protection
TRIGGER_PROXIMITY = 0.06
STEP_INCREMENT_PCT = 0.02
BASE_TP_PCT = 2.5
BASE_SL_PCT = 2.0

# --- Pre-buy / orderbook confirmation config ---
REQUIRE_ORDERBOOK_BEFORE_BUY = False     # if True, picker requires OB bullish before accepting candidate
PREBUY_BREAKOUT_MARGIN = 0.0007          # fractional margin above recent highs (0.0007 = 0.07%)
PREBUY_OB_DEPTH = 5                      # number of orderbook levels to sample for pre-buy check
PREBUY_MIN_IMBALANCE = 1.08              # minimal bids/asks imbalance (bid_sum / ask_sum) required
PREBUY_MAX_SPREAD = 1.0                  # max allowed spread (%) at pre-buy confirmation

# micro-take profit
MICRO_TP_PCT = 1.0
MICRO_TP_FRACTION = 0.50
MICRO_MAX_WAIT = 20.0

# rolling config
ROLL_ON_RISE_PCT = 0.5
ROLL_TRIGGER_PCT = 0.75
ROLL_TRIGGER_DELTA_ABS = 0.007
ROLL_TP_STEP_ABS = 0.020
ROLL_SL_STEP_ABS = 0.003
ROLL_COOLDOWN_SECONDS = 60
MAX_ROLLS_PER_POSITION = 3
ROLL_POST_CANCEL_JITTER = (0.3, 0.8)

# Rolling failure tracking (prevents roll spam when OCO repeatedly fails)
ROLL_FAIL_COUNTER = {}             # symbol -> consecutive failed roll attempts
FAILED_ROLL_THRESHOLD = 3          # after this many failed roll attempts, pause symbol
FAILED_ROLL_SKIP_SECONDS = 60 * 60  # 1 hour skip when repeated roll attempts fail

# -------------------------
# INIT / GLOBALS
# -------------------------

client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
start_balance_usdt = None

TEMP_SKIP = {}  # symbol -> retry_unix_ts
SKIP_SECONDS_ON_MARKET_CLOSED = 60 * 60

getcontext().prec = 28

# REBUY / RECENT BUYS CONFIG
RECENT_BUYS = {}
REBUY_COOLDOWN = 60 * 60
LOSS_COOLDOWN = 60 * 60 * 4
REBUY_MAX_RISE_PCT = 5.0

# rate-limit/backoff
RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_BACKOFF_MAX = 300
RATE_LIMIT_BASE_SLEEP = 90
CACHE_TTL = 300

# -------------------------
# HELPERS: formatting & rounding
# -------------------------
_NOTIFY_Q = queue.Queue()
_NOTIFY_THREAD_STARTED = False
_NOTIFY_LOCK = Lock()

def _start_notify_thread():
    global _NOTIFY_THREAD_STARTED
    with _NOTIFY_LOCK:
        if _NOTIFY_THREAD_STARTED:
            return
        def _notify_worker():
            while True:
                text = _NOTIFY_Q.get()
                if text is None:
                    break
                try:
                    if BOT_TOKEN and CHAT_ID:
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                                data={"chat_id": CHAT_ID, "text": text},
                                timeout=6
                            )
                        except Exception:
                            # swallow network errors - we already printed locally
                            pass
                finally:
                    try:
                        _NOTIFY_Q.task_done()
                    except Exception:
                        pass
        t = Thread(target=_notify_worker, daemon=True)
        t.start()
        _NOTIFY_THREAD_STARTED = True

def notify(msg: str):
    global LAST_NOTIFY
    try:
        now_ts = time.time()
        if now_ts - LAST_NOTIFY < 0.5:
            return
        LAST_NOTIFY = now_ts
    except Exception:
        LAST_NOTIFY = time.time()

    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    try:
        print(text)
    except Exception:
        pass

    try:
        _start_notify_thread()
        _NOTIFY_Q.put_nowait(text)
    except Exception:
        # best-effort: if queue full / failing, ignore (we don't want notify to crash bot)
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

def compute_recent_volatility(closes):
    """
    Return fractional volatility (population stdev of returns) for given closes.
    """
    try:
        if not closes or len(closes) < 3:
            return None
        rets = []
        for i in range(1, len(closes)):
            p0 = closes[i-1]
            p1 = closes[i]
            if p0 <= 0:
                continue
            rets.append((p1 - p0) / p0)
        if not rets:
            return None
        return abs(statistics.pstdev(rets))
    except Exception:
        return None

def compute_trade_size_by_volatility(closes, base_usd=TRADE_USD, target_vol=0.01, min_usd=1.0, max_usd=None):
    """
    Scale trade size down when observed short-term volatility is higher than target_vol.
    - target_vol is fractional (0.01 = 1%).
    """
    try:
        vol = compute_recent_volatility(closes)
        if vol is None or vol <= 0:
            return base_usd
        scale = target_vol / vol
        # cap scaling to avoid extremes
        scale = max(0.25, min(1.5, scale))
        size = base_usd * scale
        if max_usd:
            size = min(size, max_usd)
        size = max(size, min_usd)
        # round to 0.5 USD to avoid tiny fractions
        return round(size, 2)
    except Exception:
        return base_usd

def pre_buy_confirmation(symbol, closes, require_breakout=True, require_orderbook=True):
    try:
        if require_breakout and closes and len(closes) >= 4:
            last_price = closes[-1]
            prev_max = max(closes[:-1])
            # require a small edge over previous highs
            if not (last_price > prev_max * 1.0007):
                return False

        if require_orderbook:
            try:
                ob = client.get_order_book(symbol=symbol, limit=5)
                bids = ob.get('bids') or []
                asks = ob.get('asks') or []
                if not bids or not asks:
                    return False
                top_bid = float(bids[0][0])
                top_ask = float(asks[0][0])
                spread_pct = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
                bid_sum = sum(float(b[1]) for b in bids[:5]) + 1e-12
                ask_sum = sum(float(a[1]) for a in asks[:5]) + 1e-12
                imbalance = bid_sum / ask_sum
                # stricter than orderbook_bullish
                if imbalance < 1.08 or spread_pct > 1.0:
                    return False
            except Exception:
                return False

        return True
    except Exception:
        return False

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
    """
    Cleaner picker (no per-symbol debug). Returns (symbol, price, qvol, change, closes) or None.
    """
    global RATE_LIMIT_BACKOFF, TEMP_SKIP, RECENT_BUYS

    try:
        t0 = time.time()
        now = t0

        TOP_CANDIDATES = globals().get('TOP_CANDIDATES', 60)
        DEEP_EVAL = globals().get('DEEP_EVAL', 3)
        REQUEST_SLEEP = globals().get('REQUEST_SLEEP', 0.02)
        KLINES_LIMIT = globals().get('KLINES_LIMIT', 6)
        MIN_VOL_RATIO = globals().get('MIN_VOL_RATIO', 1.25)

        EMA_UPLIFT_MIN = globals().get('EMA_UPLIFT_MIN_PCT', EMA_UPLIFT_MIN_PCT)
        SCORE_MIN = globals().get('SCORE_MIN_THRESHOLD', SCORE_MIN_THRESHOLD)

        REQUIRE_OB_IN_PICK = globals().get('REQUIRE_ORDERBOOK_BEFORE_BUY', False)

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

            if not (PRICE_MIN <= price <= PRICE_MAX):
                continue
            if qvol < max(MIN_VOLUME, 300_000):
                continue
            if abs(change_pct) > MAX_24H_CHANGE_ABS:
                continue
            if change_pct > MAX_24H_RISE_PCT:
                continue

            if last_buy:
                cooldown = last_buy.get('cooldown', REBUY_COOLDOWN)
                if now < last_buy['ts'] + cooldown:
                    continue
                last_price = last_buy.get('price')
                if last_price and price > last_price * (1 + REBUY_MAX_RISE_PCT / 100.0):
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

                recent_pct = 0.0
                if len(closes) >= 4 and closes[-4] > 0:
                    recent_pct = (closes[-1] - closes[-4]) / (closes[-4] + 1e-12) * 100.0

                vol_f = compute_recent_volatility(closes) or 0.0

                # require breakout (closes)
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

                # optional lightweight pre-buy confirmation inside picker
                try:
                    if not pre_buy_confirmation(sym, closes, require_breakout=True, require_orderbook=REQUIRE_OB_IN_PICK):
                        continue
                except Exception:
                    continue

                score = 0.0
                score += max(0.0, recent_pct) * 12.0
                score += math.log1p(qvol) * 0.5
                score += (vol_ratio - 1.0) * 8.0
                score += ema_uplift * 100.0
                if rsi_val is not None:
                    score += max(0.0, (60.0 - min(rsi_val, 60.0))) * 1.0
                score += max(0.0, change_pct) * 0.6

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

        took = time.time() - t0

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

def place_safe_market_buy(symbol, usd_amount, require_orderbook: bool = False, fee_buffer=0.003):
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

    # fetch current price
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

    # preflight: check free quote balance (USDT)
    free_quote = get_free_asset(QUOTE)
    needed_est = float(usd_amount) * (1.0 + fee_buffer)
    notify(f"[BUY_PREFLIGHT] {symbol} price={price:.8f} want_usd={usd_amount:.8f} free_{QUOTE}={free_quote:.8f} need≈{needed_est:.8f}")

    if free_quote + 1e-9 < needed_est:
        notify(f"❌ Aborting market buy for {symbol}: insufficient {QUOTE}. free={free_quote:.8f} need≈{needed_est:.8f}")
        return None, None

    # compute qty estimate (base asset) for minNotional enforcement if needed
    qty_est = float(usd_amount) / price
    # ensure >= minQty before rounding down (we'll use ceil if needed)
    if f.get('minQty'):
        qty_est = max(qty_est, f.get('minQty', 0.0))

    # enforce minNotional: if rounding down causes notional < minNotional, try to bump
    min_notional = f.get('minNotional')
    # First attempt: use quoteOrderQty (preferred) which avoids many rounding issues
    order_resp = None
    used_quote_order = False

    time.sleep(random.uniform(0.02, 0.12))
    try:
        # Try quoteOrderQty approach first (Binance calculates base qty)
        payload = {'symbol': symbol, 'side': 'BUY', 'type': 'MARKET', 'quoteOrderQty': str(round(float(usd_amount), 8))}
        notify(f"[BUY_ATTEMPT] trying quoteOrderQty payload={payload}")
        # Some clients provide client.create_order, some provide order_market_buy with quoteOrderQty. Try both.
        try:
            order_resp = client.create_order(**payload)
        except Exception:
            # fall back to order_market_buy if create_order not available
            order_resp = client.order_market_buy(symbol=symbol, quoteOrderQty=str(round(float(usd_amount), 8)))
        used_quote_order = True
    except Exception as e:
        err = str(e)
        notify(f"⚠️ quoteOrderQty market buy failed for {symbol}: {err}")
        # If error suggests quoteOrderQty unsupported or rejected, we'll fallback to base qty path below.
        if '-2010' in err and 'insufficient' in err.lower():
            # insufficient balance error — abort and mark temp skip
            TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
            notify(f"⏸ Market buy insufficient balance for {symbol}. TEMP skipping.")
            return None, None
        # handle rate limit
        if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
            RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                    RATE_LIMIT_BACKOFF_MAX)
            TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
            notify(f"❗ Rate-limit while placing market buy (quoteOrderQty): backing off {RATE_LIMIT_BACKOFF}s; TEMP skipping {symbol}.")
            return None, None
        order_resp = None

    # If quoteOrderQty succeeded, parse it. Otherwise fallback to base-qty flow:
    if order_resp is None:
        # fallback: compute base qty, round down to step, ensure minNotional satisfied
        step = f.get('stepSize', 0.0)
        qty_base = round_step(qty_est, step)
        # If this qty makes notional < minNotional, attempt to ceil up to meet minNotional (if funds available)
        if min_notional:
            notional = qty_base * price
            if notional + 1e-12 < min_notional:
                needed_qty = ceil_step(min_notional / price, step)
                if needed_qty * price <= free_quote * (1.0 + 1e-6):
                    qty_base = needed_qty
                    notify(f"ℹ️ Bumping qty to meet minNotional: qty={qty_base} notional≈{qty_base*price:.8f}")
                else:
                    notify(f"⛔ Skipping: computed notional {qty_base*price:.8f} < minNotional {min_notional:.8f} and insufficient funds to top-up.")
                    return None, None

        qty_str = format_qty(qty_base, step)
        if not qty_str or float(qty_base) <= 0:
            notify(f"❌ Computed qty invalid for {symbol}: qty_base={qty_base}, qty_str={qty_str}")
            return None, None

        # place market buy by base quantity
        try:
            notify(f"[BUY_ATTEMPT] trying base-qty market buy: symbol={symbol}, quantity={qty_str}")
            try:
                order_resp = client.create_order(symbol=symbol, side='BUY', type='MARKET', quantity=qty_str)
            except Exception:
                order_resp = client.order_market_buy(symbol=symbol, quantity=qty_str)
        except Exception as e:
            err = str(e)
            notify(f"❌ Market buy failed for {symbol}: {err}")
            if '-2010' in err and 'insufficient' in err.lower():
                TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
                notify(f"⏸ Insufficient balance for market buy (base qty). TEMP skipping {symbol}.")
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                notify(f"❗ Rate-limit while placing market buy (base qty): backing off {RATE_LIMIT_BACKOFF}s and TEMP skipping {symbol}.")
            return None, None

    # Parse executed qty and average price
    executed_qty, avg_price = None, None
    try:
        executed_qty, avg_price = _parse_market_buy_exec(order_resp)
    except Exception:
        # fallback parsing if helper fails
        try:
            fills = order_resp.get('fills') or []
            executed_qty = sum(float(f.get('qty') or f.get('executedQty') or 0) for f in fills)
            # compute average price via fills
            total_quote = sum(float(f.get('qty') or 0) * float(f.get('price') or 0) for f in fills)
            avg_price = (total_quote / executed_qty) if executed_qty else price
        except Exception:
            executed_qty = None
            avg_price = None

    # small wait to let balances update
    time.sleep(0.6)
    asset = symbol[:-len(QUOTE)]
    free_after = get_free_asset(asset)
    free_after_clip = round_step(free_after, f.get('stepSize', 0.0))

    # reconcile parsed executed_qty with actual free asset balance
    if free_after_clip >= f.get('minQty', 0.0) and (not executed_qty or executed_qty <= 0 or abs(free_after_clip - (executed_qty or 0)) / ((executed_qty or 1) + 1e-9) > 0.02):
        notify(f"ℹ️ Adjusting executed qty from parsed {executed_qty} to actual free balance {free_after_clip}")
        executed_qty = free_after_clip
        if not avg_price:
            avg_price = price

    # final rounding & validation
    executed_qty = round_step(executed_qty or 0.0, f.get('stepSize', 0.0))
    if executed_qty <= 0 or executed_qty < f.get('minQty', 0.0):
        notify(f"❌ Executed quantity too small after reconciliation for {symbol}: {executed_qty}")
        return None, None

    # very small leftover check
    if free_after_clip < max(1e-8, executed_qty * 0.5):
        notify(f"⚠️ After buy free balance {free_after_clip} is much smaller than expected executed {executed_qty}. Skipping symbol for a while.")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None

    notify(f"✅ BUY {symbol}: qty={executed_qty} ~price={avg_price:.8f} notional≈${executed_qty*avg_price:.6f} (method={'quoteOrderQty' if used_quote_order else 'base_qty'})")
    return executed_qty, avg_price

# -------------------------
# Micro TP helper (unchanged mostly)
# -------------------------
def place_micro_tp(symbol, qty, entry_price, f, pct=MICRO_TP_PCT, fraction=MICRO_TP_FRACTION):
    try:
        # compute intended micro sell qty
        sell_qty = float(qty) * float(fraction)
        # round down to step
        sell_qty = round_step(sell_qty, f.get('stepSize', 0.0))

        # compute remainder left after micro sell
        remainder = round_step(float(qty) - sell_qty, f.get('stepSize', 0.0))

        # If remainder would be a dust (non-zero but < minQty), prefer either:
        #  - increase sell_qty so remainder becomes zero (i.e., sell entire position), or
        #  - skip micro TP if that would violate minNotional.
        if remainder > 0 and remainder < f.get('minQty', 0.0):
            # Try to sell all (so no dust remains)
            candidate_sell_all = round_step(float(qty), f.get('stepSize', 0.0))
            if candidate_sell_all >= f.get('minQty', 0.0):
                sell_qty = candidate_sell_all
                remainder = 0.0
                notify(f"ℹ️ Adjusting micro TP to sell entire position to avoid dust (sell_qty={sell_qty}).")
            else:
                notify(f"ℹ️ Skipping micro TP (would leave dust remainder={remainder} < minQty).")
                return None, 0.0, None

        if sell_qty <= 0 or sell_qty < f.get('minQty', 0.0):
            notify(f"ℹ️ Micro TP: sell_qty too small ({sell_qty}) for {symbol}, skipping micro TP.")
            return None, 0.0, None

        tp_price = float(entry_price) * (1.0 + float(pct) / 100.0)
        tick = f.get('tickSize', 0.0) or 0.0
        if tick and tick > 0:
            tp_price = math.ceil(tp_price / tick) * tick

        if f.get('minNotional'):
            if sell_qty * tp_price < f['minNotional'] - 1e-12:
                notify(f"⚠️ Micro TP would violate MIN_NOTIONAL for {symbol} (need {f['minNotional']}). Skipping micro TP.")
                return None, 0.0, None

        qty_str = format_qty(sell_qty, f.get('stepSize', 0.0))
        price_str = format_price(tp_price, f.get('tickSize', 0.0))

        try:
            order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=price_str)
            notify(f"📍 Micro TP placed for {symbol}: sell {qty_str} @ {price_str}")
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            order_id = None
            if isinstance(order, dict):
                order_id = order.get('orderId') or order.get('orderId')
            if not order_id:
                return order, sell_qty, tp_price

            poll_interval = 0.6
            max_wait = MICRO_MAX_WAIT
            waited = 0.0

            while waited < max_wait:
                try:
                    status = client.get_order(symbol=symbol, orderId=order_id)
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
                        cumm = status.get('cummulativeQuoteQty') or status.get('cumulativeQuoteQty') or 0.0
                        try:
                            cumm = float(cumm)
                            if executed_qty > 0 and cumm > 0:
                                avg_fill_price = cumm / executed_qty
                        except Exception:
                            avg_fill_price = None
                    if avg_fill_price is None:
                        avg_fill_price = tp_price

                    profit_usd = (avg_fill_price - float(entry_price)) * executed_qty
                    try:
                        profit_to_send = float(round(profit_usd, 6))
                    except Exception:
                        profit_to_send = profit_usd

                    if profit_to_send and profit_to_send > 0.0:
                        try:
                            send_profit_to_funding(profit_to_send)
                            notify(f"💸 Micro TP profit ${profit_to_send:.6f} for {symbol} sent to funding.")
                        except Exception as e:
                            notify(f"⚠️ Failed to transfer micro profit for {symbol}: {e}")
                    else:
                        notify(f"ℹ️ Micro TP filled but profit non-positive (${profit_to_send:.6f}) — not sending.")
                    break

                time.sleep(poll_interval)
                waited += poll_interval

        except Exception as e:
            notify(f"⚠️ Failed to place micro TP for {symbol}: {e}")
            return None, 0.0, None

        return order, sell_qty, tp_price

    except Exception as e:
        notify(f"⚠️ place_micro_tp error: {e}")
        return None, 0.0, None


def monitor_and_roll(symbol, qty, entry_price, f):
    orig_qty = qty
    curr_tp = entry_price * (1 + BASE_TP_PCT / 100.0)
    curr_sl = entry_price * (1 - BASE_SL_PCT / 100.0)

    # place initial protection OCO
    oco = place_oco_sell(symbol, qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
    if oco is None:
        notify(f"❌ Initial OCO failed for {symbol}, aborting monitor.")
        return False, entry_price, 0.0

    last_tp = None
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

    while True:
        try:
            time.sleep(SLEEP_BETWEEN_CHECKS)
            asset = symbol[:-len(QUOTE)]
            price_now = get_price_cached(symbol)
            if price_now is None:
                try:
                    price_now = float(client.get_symbol_ticker(symbol=symbol)['price'])
                except Exception as e:
                    notify(f"⚠️ Failed to fetch price in monitor (fallback): {e}")
                    continue

            free_qty = get_free_asset(asset)
            available_for_sell = min(round_step(free_qty, f.get('stepSize', 0.0)), orig_qty)
            open_orders = get_open_orders_cached(symbol)

            # Position appears closed if very little left and no open orders
            if available_for_sell < round_step(orig_qty * 0.05, f.get('stepSize', 0.0)) and len(open_orders) == 0:
                exit_price = price_now
                profit_usd = (exit_price - entry_price) * orig_qty
                notify(f"✅ Position closed for {symbol}: exit={exit_price:.8f}, profit≈${profit_usd:.6f}")
                return True, exit_price, profit_usd

            price_delta = price_now - entry_price
            rise_trigger_pct = price_now >= entry_price * (1 + ROLL_ON_RISE_PCT / 100.0)
            rise_trigger_abs = price_delta >= max(ROLL_TRIGGER_DELTA_ABS, entry_price * (ROLL_TRIGGER_PCT / 100.0))
            near_trigger = (price_now >= curr_tp * (1 - TRIGGER_PROXIMITY)) and (price_now < curr_tp * 1.05)
            tick = f.get('tickSize', 0.0) or 0.0
            minimal_move = max(entry_price * 0.004, ROLL_TRIGGER_DELTA_ABS * 0.4, tick)
            moved_enough = price_delta >= minimal_move
            now_ts = time.time()
            can_roll = (now_ts - last_roll_ts) >= ROLL_COOLDOWN_SECONDS

            trigger_conditions = ((near_trigger and moved_enough) or rise_trigger_pct or rise_trigger_abs)
            if trigger_conditions and available_for_sell >= f.get('minQty', 0.0) and can_roll:
                if roll_count >= MAX_ROLLS_PER_POSITION:
                    notify(f"⚠️ Reached max rolls ({MAX_ROLLS_PER_POSITION}) for {symbol}, will not roll further.")
                    last_roll_ts = now_ts
                    continue

                notify(
                    f"🔎 Roll triggered for {symbol}: price={price_now:.8f}, entry={entry_price:.8f}, "
                    f"curr_tp={curr_tp:.8f}, delta={price_delta:.6f} (near={near_trigger}, pct={rise_trigger_pct}, abs={rise_trigger_abs})"
                )

                candidate_tp = curr_tp + ROLL_TP_STEP_ABS
                candidate_sl = curr_sl + ROLL_SL_STEP_ABS
                if candidate_sl > entry_price:
                    candidate_sl = entry_price

                new_tp = clip_tp(candidate_tp, tick)
                new_sl = clip_sl(candidate_sl, tick)
                tick_step = tick or 0.0

                # Ensure TP > SL gap
                if new_tp <= new_sl + tick_step:
                    new_tp = new_sl + (tick_step * 2 if tick_step > 0 else max(1e-8, ROLL_TP_STEP_ABS))
                if new_tp <= curr_tp:
                    new_tp = math.ceil((curr_tp + tick_step) / tick_step) * tick_step if tick_step > 0 else curr_tp + max(1e-8, ROLL_TP_STEP_ABS)

                # recompute sell qty AFTER cancelling orders (fix race/stale qty)
                last_roll_ts = now_ts
                cancel_all_open_orders(symbol)
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass
                time.sleep(random.uniform(*ROLL_POST_CANCEL_JITTER))

                # refresh free quantity AFTER cancellations
                try:
                    free_qty_after = get_free_asset(asset)
                except Exception:
                    free_qty_after = get_free_asset(asset)

                available_for_sell = min(round_step(free_qty_after, f.get('stepSize', 0.0)), orig_qty)
                sell_qty = round_step(available_for_sell, f.get('stepSize', 0.0))

                if sell_qty <= 0 or sell_qty < f.get('minQty', 0.0):
                    notify(f"⚠️ Roll skipped after cancel: recomputed sell_qty {sell_qty} too small (<minQty).")
                    continue

                # enforce minNotional by increasing TP first, then qty if possible
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
                        # try increasing sell_qty if free asset available
                        needed_qty = ceil_step(min_notional / new_tp, f.get('stepSize'))
                        if needed_qty <= available_for_sell + 1e-12 and needed_qty > sell_qty:
                            notify(f"ℹ️ Increasing sell_qty to {needed_qty} to meet minNotional for roll.")
                            sell_qty = needed_qty
                        else:
                            notify(f"⚠️ Roll aborted: cannot meet minNotional for {symbol} even after recompute.")
                            continue

                # attempt to place rolled OCO
                oco2 = place_oco_sell(symbol, sell_qty, entry_price, explicit_tp=new_tp, explicit_sl=new_sl)
                if oco2:
                    roll_count += 1
                    last_tp = curr_tp
                    curr_tp = new_tp
                    curr_sl = new_sl
                    try:
                        ROLL_FAIL_COUNTER[symbol] = 0
                    except Exception:
                        pass
                    notify(f"🔁 Rolled OCO (abs-step): new TP={curr_tp:.8f}, new SL={curr_sl:.8f}, qty={sell_qty}")
                else:
                    # failed roll handling
                    cnt = ROLL_FAIL_COUNTER.get(symbol, 0) + 1
                    ROLL_FAIL_COUNTER[symbol] = cnt
                    notify(f"⚠️ Roll attempt FAILED (place_oco_sell returned None). fail_count={cnt}.")
                    notify(f"    TEMP_SKIP[{symbol}]={TEMP_SKIP.get(symbol)} RATE_LIMIT_BACKOFF={RATE_LIMIT_BACKOFF}")

                    # small delay then fallback attempt with base percentages
                    time.sleep(0.4)
                    fallback = place_oco_sell(symbol, sell_qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
                    if fallback:
                        notify("ℹ️ Fallback OCO re-placed after failed roll.")
                        ROLL_FAIL_COUNTER[symbol] = 0
                    else:
                        notify(f"❌ Fallback OCO also failed for {symbol};")
                        if cnt >= FAILED_ROLL_THRESHOLD:
                            TEMP_SKIP[symbol] = time.time() + FAILED_ROLL_SKIP_SECONDS
                            notify(f"⏸ Pausing attempts for {symbol} for {FAILED_ROLL_SKIP_SECONDS//60} minutes after {cnt} failed roll attempts.")
            # end trigger handling

        except Exception as e:
            notify(f"⚠️ Error in monitor_and_roll: {e}")
            return False, entry_price, 0.0

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
            notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
        except BinanceAPIException as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (standard) failed: {err}")
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

# =========================
# DAILY METRICS (unchanged)
# =========================

METRICS = {}

def _update_metrics_for_profit(profit: float):
    date_key = datetime.now().date().isoformat()
    m = METRICS.get(date_key)
    if m is None:
        m = {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0}
        METRICS[date_key] = m
    m['picks'] += 1
    try:
        p = float(profit or 0.0)
    except Exception:
        p = 0.0
    if p > 0:
        m['wins'] += 1
    else:
        m['losses'] += 1
    m['profit'] += p
    return date_key, m

def _notify_daily_stats(date_key):
    m = METRICS.get(date_key, {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0})
    profit_val = m['profit']
    profit_str = f"+{profit_val:.2f} USDT" if profit_val >= 0 else f"{profit_val:.2f} USDT"
    notify(
        f"📊 Stats ya leo ({date_key}):\n\n"
        f"Coins zilizochaguliwa leo: {m['picks']}\n\n"
        f"Zilizofanikiwa (TP/Profit): {m['wins']}\n\n"
        f"Zilizopoteza: {m['losses']}\n\n"
        f"Jumla profit: {profit_str}"
    )

# -------------------------
# MAIN TRADE CYCLE (unchanged structure)
# -------------------------
ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0
BUY_LOCK_SECONDS = 30

def trade_cycle():
    global start_balance_usdt, ACTIVE_SYMBOL, LAST_BUY_TS, RATE_LIMIT_BACKOFF

    if start_balance_usdt is None:
        try:
            start_balance_usdt = get_free_usdt()
        except Exception as e:
            start_balance_usdt = 0.0
            notify(f"⚠️ Could not snapshot start balance: {e}")
        notify(f"🔰 Start balance snapshot: ${start_balance_usdt:.6f}")

    while True:
        try:
            cleanup_recent_buys()

            open_orders_global = get_open_orders_cached()
            if open_orders_global:
                notify("⏳ Still waiting for previous trade(s) to finish (open orders present)...")
                time.sleep(300)
                continue

            if ACTIVE_SYMBOL is not None:
                notify(f"⏳ Active trade in progress for {ACTIVE_SYMBOL}, skipping new buys.")
                time.sleep(CYCLE_DELAY)
                continue

            now = time.time()
            if now - LAST_BUY_TS < BUY_LOCK_SECONDS:
                time.sleep(CYCLE_DELAY)
                continue

            candidate = None
            try:
                candidate = pick_coin()
            except Exception as e:
                notify(f"⚠️ pick_coin() error: {e}")
                candidate = None

            if not candidate:
                notify("⚠️ No eligible coin found. Sleeping...")
                if RATE_LIMIT_BACKOFF:
                    notify(f"⏸ Backing off due to prior rate-limit for {RATE_LIMIT_BACKOFF}s.")
                    time.sleep(RATE_LIMIT_BACKOFF)
                else:
                    time.sleep(30)
                continue

            # Unpack candidate (handle 4-tuple or 5-tuple)
            symbol = price = volume = change = None
            closes = None
            try:
                if isinstance(candidate, (list, tuple)):
                    if len(candidate) == 5:
                        symbol, price, volume, change, closes = candidate
                    elif len(candidate) == 4:
                        symbol, price, volume, change = candidate
                    else:
                        symbol, price, volume, change = tuple(candidate)[:4]
                else:
                    notify(f"⚠️ Unexpected candidate format: {candidate}")
                    time.sleep(CYCLE_DELAY)
                    continue
            except Exception as e:
                notify(f"⚠️ Failed to unpack candidate: {e}")
                time.sleep(CYCLE_DELAY)
                continue

            notify(f"🎯 Selected {symbol} for potential buy (24h change={change}%, vol≈{volume})")

            # compute usd_to_buy
            usd_to_buy = TRADE_USD if 'TRADE_USD' in globals() else globals().get('DEFAULT_USD_PER_TRADE', 5.0)
            try:
                if closes:
                    usd_to_buy = compute_trade_size_by_volatility(closes, base_usd=usd_to_buy)
            except Exception:
                usd_to_buy = usd_to_buy

            # PRE-BUY: quick confirmation (breakout/orderbook) - keep original behaviour
            try:
                pre_ok = pre_buy_confirmation(symbol, closes, require_breakout=True, require_orderbook=True)
            except Exception as e:
                pre_ok = False
                notify(f"[DEBUG] pre_buy_confirmation threw: {e}")

            notify(f"[DEBUG] pre_buy_confirmation for {symbol} => {pre_ok}")

            if not pre_ok:
                notify(f"⏭️ Pre-buy confirmation failed for {symbol} (stale/orderbook). Skipping.")
                time.sleep(CYCLE_DELAY)
                continue

            # cooldown checks based on RECENT_BUYS (original logic)
            last = RECENT_BUYS.get(symbol)
            if last:
                if now < last['ts'] + last.get('cooldown', REBUY_COOLDOWN):
                    notify(f"⏭️ Skipping {symbol} due to recent buy cooldown.")
                    time.sleep(CYCLE_DELAY)
                    continue
                if last.get('price') and price and price > last['price'] * (1 + REBUY_MAX_RISE_PCT / 100.0):
                    notify(f"⏭️ Skipping {symbol} bc price rose >{REBUY_MAX_RISE_PCT}% since last buy.")
                    time.sleep(CYCLE_DELAY)
                    continue

            # balance check
            try:
                free_usdt = get_free_usdt()
            except Exception as e:
                notify(f"⚠️ get_free_usdt() error: {e}")
                free_usdt = 0.0

            usd_to_buy = min(usd_to_buy, free_usdt)
            if usd_to_buy < 1.0:
                notify(f"⚠️ Not enough USDT to buy (free={free_usdt:.4f}). Sleeping...")
                time.sleep(CYCLE_DELAY)
                continue

            # EXTRA DEBUG: symbol filters (minNotional/minQty/stepSize) before buy
            f = {}
            try:
                info = get_symbol_info_cached(symbol)
                f = get_filters(info) if info else {}
            except Exception as e:
                notify(f"[DEBUG] get_symbol_info/get_filters error for {symbol}: {e}")
                f = {}

            # estimate qty by price (best-effort; place_safe_market_buy may compute internally)
            try:
                estimated_price = price or get_price_cached(symbol) or 0.0
            except Exception:
                estimated_price = 0.0

            qty_est = None
            qty_rounded = None
            notional = None
            try:
                if estimated_price and usd_to_buy:
                    qty_est = usd_to_buy / estimated_price
                    # round using round_step if available
                    try:
                        qty_rounded = round_step(qty_est, float(f.get('stepSize')) if f.get('stepSize') else 0.0)
                    except Exception:
                        # fallback: floor to 8 decimals by default
                        qty_rounded = float("{:.8f}".format(qty_est))
                    notional = qty_rounded * estimated_price if qty_rounded and estimated_price else 0.0
            except Exception as e:
                notify(f"[DEBUG] qty estimation error: {e}")

            # parse minNotional/minQty for debug
            min_notional = None
            min_qty = None
            try:
                if f:
                    min_notional = float(f.get('minNotional')) if f.get('minNotional') is not None else None
                    min_qty = float(f.get('minQty')) if f.get('minQty') is not None else None
            except Exception:
                min_notional = min_notional
                min_qty = min_qty

            notify(f"[DEBUG] pre-buy check details for {symbol}: estimated_price={estimated_price}, usd_to_buy={usd_to_buy}, qty_est={qty_est}, qty_rounded={qty_rounded}, notional={notional}, min_notional={min_notional}, min_qty={min_qty}, filters_present={bool(f)}")

            # enforce minNotional / minQty checks (avoid Binance rejections)
            if min_notional and (notional is None or notional < min_notional):
                notify(f"⚠️ Notional too small for {symbol}: {notional} < minNotional({min_notional}). Skipping.")
                time.sleep(CYCLE_DELAY)
                continue
            if min_qty and (qty_rounded is None or qty_rounded < min_qty):
                notify(f"⚠️ Qty too small for {symbol}: {qty_rounded} < minQty({min_qty}). Skipping.")
                time.sleep(CYCLE_DELAY)
                continue

            # orderbook debug (best-effort check)
            try:
                ob_ok = orderbook_bullish(symbol, depth=5, min_imbalance=1.02, max_spread_pct=0.6)
            except Exception as e:
                ob_ok = f"ERR:{e}"
            notify(f"[DEBUG] orderbook_bullish for {symbol} => {ob_ok}")

            # Attempt the market buy (original call kept)
            try:
                buy_res = place_safe_market_buy(symbol, usd_to_buy, require_orderbook=True)
            except Exception as e:
                err = str(e)
                notify(f"❌ Exception during market buy attempt for {symbol}: {err}")
                if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                    RATE_LIMIT_BACKOFF = min(
                        RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                        RATE_LIMIT_BACKOFF_MAX
                    )
                    notify(f"❌ Rate limit detected during buy attempt: backing off for {RATE_LIMIT_BACKOFF}s.")
                    time.sleep(RATE_LIMIT_BACKOFF)
                else:
                    time.sleep(CYCLE_DELAY)
                continue

            if not buy_res or buy_res == (None, None):
                notify(f"ℹ️ Buy skipped/failed for {symbol}. (place_safe_market_buy returned nothing)")
                time.sleep(CYCLE_DELAY)
                continue

            qty, entry_price = buy_res
            if qty is None or entry_price is None:
                notify(f"⚠️ Unexpected buy result for {symbol}, skipping.")
                time.sleep(CYCLE_DELAY)
                continue

            notify(f"🟢 Bought {symbol}: qty={qty} avg_price={entry_price}")

            # record recent buy & continue with rest of original flow
            RECENT_BUYS[symbol] = {'ts': time.time(), 'price': entry_price, 'profit': None, 'cooldown': REBUY_COOLDOWN}

            info = get_symbol_info_cached(symbol)
            f = get_filters(info) if info else {}
            if not f:
                notify(f"⚠️ Could not fetch filters for {symbol} after buy; aborting monitoring for safety.")
                ACTIVE_SYMBOL = None
                time.sleep(CYCLE_DELAY)
                continue

            micro_order, micro_sold_qty, micro_tp_price = None, 0.0, None
            try:
                micro_order, micro_sold_qty, micro_tp_price = place_micro_tp(symbol, qty, entry_price, f)
            except Exception as e:
                notify(f"⚠️ Micro TP placement error: {e}")
                micro_order, micro_sold_qty, micro_tp_price = None, 0.0, None

            qty_remaining = round_step(max(0.0, qty - micro_sold_qty), f.get('stepSize', 0.0))
            if qty_remaining <= 0 or qty_remaining < f.get('minQty', 0.0):
                notify(f"ℹ️ Nothing left to monitor after micro TP for {symbol} (qty_remaining={qty_remaining}).")
                ACTIVE_SYMBOL = symbol
                LAST_BUY_TS = time.time()
                ACTIVE_SYMBOL = None
                total_profit_usd = 0.0
                date_key, m = _update_metrics_for_profit(total_profit_usd)
                _notify_daily_stats(date_key)
                continue

            ACTIVE_SYMBOL = symbol
            LAST_BUY_TS = time.time()

            # invalidate OPEN_ORDERS_CACHE safely
            try:
                with OPEN_ORDERS_LOCK:
                    OPEN_ORDERS_CACHE['data'] = None
                    OPEN_ORDERS_CACHE['ts'] = 0
            except Exception:
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass

            try:
                closed, exit_price, profit_usd = monitor_and_roll(symbol, qty_remaining, entry_price, f)
            finally:
                ACTIVE_SYMBOL = None
                try:
                    with OPEN_ORDERS_LOCK:
                        OPEN_ORDERS_CACHE['data'] = None
                        OPEN_ORDERS_CACHE['ts'] = 0
                except Exception:
                    try:
                        OPEN_ORDERS_CACHE['data'] = None
                    except Exception:
                        pass

            total_profit_usd = profit_usd or 0.0
            if micro_order and micro_sold_qty and micro_tp_price:
                try:
                    micro_profit = (micro_tp_price - entry_price) * micro_sold_qty
                    total_profit_usd += micro_profit
                except Exception:
                    pass

            now2 = time.time()
            ent = RECENT_BUYS.get(symbol, {})
            ent['ts'] = now2
            ent['price'] = entry_price
            ent['profit'] = total_profit_usd
            if ent['profit'] is None:
                ent['cooldown'] = REBUY_COOLDOWN
            elif ent['profit'] < 0:
                ent['cooldown'] = LOSS_COOLDOWN
            else:
                ent['cooldown'] = REBUY_COOLDOWN
            RECENT_BUYS[symbol] = ent

            date_key, m = _update_metrics_for_profit(total_profit_usd)
            _notify_daily_stats(date_key)

            if closed and total_profit_usd and total_profit_usd > 0:
                send_profit_to_funding(total_profit_usd)

            RATE_LIMIT_BACKOFF = 0

        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err:
                RATE_LIMIT_BACKOFF = min(
                    RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                    RATE_LIMIT_BACKOFF_MAX
                )
                notify(f"❌ Rate limit reached in trade_cycle: backing off for {RATE_LIMIT_BACKOFF}s.")
                time.sleep(RATE_LIMIT_BACKOFF)
                continue

            notify(f"❌ Trade cycle unexpected error: {e}")
            time.sleep(CYCLE_DELAY)

        time.sleep(CYCLE_DELAY)
        
# -------------------------
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