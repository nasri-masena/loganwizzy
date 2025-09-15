import os
import math
import time
import random
import threading
import requests
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP, getcontext
from flask import Flask
from binance.client import Client
from binance.exceptions import BinanceAPIException

# -------------------------
# CONFIG (general)
# -------------------------
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
QUOTE = "USDT"

PRICE_MIN = 0.9
PRICE_MAX = 3.0
MIN_VOLUME = 2_000_000

RECENT_PCT_MIN = 1.0
RECENT_PCT_MAX = 2.0

MAX_24H_RISE_PCT = 5.0
MAX_24H_CHANGE_ABS = 5.0
MOVEMENT_MIN_PCT = 1.0

TRADE_USD = 8.0
SLEEP_BETWEEN_CHECKS = 30
CYCLE_DELAY = 15
COOLDOWN_AFTER_EXIT = 10

TRIGGER_PROXIMITY = 0.06
STEP_INCREMENT_PCT = 0.02
BASE_TP_PCT = 2.5
BASE_SL_PCT = 2.0

MICRO_TP_PCT = 1.0
MICRO_TP_FRACTION = 0.50
MICRO_MAX_WAIT = 20.0

ROLL_ON_RISE_PCT = 0.5
ROLL_TRIGGER_PCT = 0.75
ROLL_TRIGGER_DELTA_ABS = 0.007
ROLL_TP_STEP_ABS = 0.020
ROLL_SL_STEP_ABS = 0.003
ROLL_COOLDOWN_SECONDS = 60
MAX_ROLLS_PER_POSITION = 3
ROLL_POST_CANCEL_JITTER = (0.3, 0.8)

MAX_HOLD_SECONDS = 6 * 3600  # emergency close after 6h

# -------------------------
# RATE LIMIT / WEIGHT CONFIG
# -------------------------
REQUEST_WEIGHT_LIMIT = 6000
RATE_LIMIT_WINDOW = 60
WEIGHT_SAFETY_MARGIN = 0.85

W_WEIGHT_TICKER = 1
W_WEIGHT_KLINES = 2
W_WEIGHT_ORDERBOOK = 2
W_WEIGHT_SYMBOL_INFO = 1
W_WEIGHT_OPEN_ORDERS = 2
W_WEIGHT_ORDER = 5

# -------------------------
# PICKER CONFIG (used by your pick_coin)
# -------------------------
# tuning knobs used by the pick_coin function (your version)
REQUEST_SLEEP = 0.12
TOP_EVAL_RANDOM_POOL = 80
FINAL_CHOICES = 3
MIN_VOL_RATIO = 1.4
MIN_VOL_RATIO_1M = 1.2
MIN_EMA_LIFT = 0.0010
MIN_OB_LIQUIDITY = 2000.0
FAST_OB_DEPTH = 2
KLINES_5M_LIMIT = 8
KLINES_1M_LIMIT = 8
OB_DEPTH = 5
EMA_SHORT = 3
EMA_LONG = 10
RSI_PERIOD = 14
KLINES_5M_LIMIT = 8
KLINES_1M_LIMIT = 8
TOP_POOL = TOP_EVAL_RANDOM_POOL
FINAL_CHOICES = FINAL_CHOICES
MIN_OB_IMBALANCE = 1.05
MAX_OB_SPREAD_PCT = 1.2

# -------------------------
# INIT / GLOBALS
# -------------------------
client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
start_balance_usdt = None

TEMP_SKIP = {}
SKIP_SECONDS_ON_MARKET_CLOSED = 60 * 60

getcontext().prec = 28

RECENT_BUYS = {}
REBUY_COOLDOWN = 60 * 60
LOSS_COOLDOWN = 60 * 60 * 4
REBUY_MAX_RISE_PCT = 5.0

RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_BACKOFF_MAX = 300
RATE_LIMIT_BASE_SLEEP = 30   # lowered to 30s initial
CACHE_TTL = 300

weight_counter = {'start_ts': time.time(), 'used': 0}
RATE_LIMIT_RESET_TS = 0.0

_LOCAL_CACHE = {}
METRICS = {}

# -------------------------
# HELPERS: notify, cache, rounding
# -------------------------
def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 0.9:
        return
    LAST_NOTIFY = now_ts
    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(text)
    if BOT_TOKEN and CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data={"chat_id": CHAT_ID, "text": text},
                timeout=8
            )
        except Exception:
            pass

def cache_set(key, value, ttl):
    _LOCAL_CACHE[key] = (time.time(), ttl, value)

def cache_get(key):
    ent = _LOCAL_CACHE.get(key)
    if not ent:
        return None
    ts, ttl, val = ent
    if time.time() - ts > ttl:
        try:
            del _LOCAL_CACHE[key]
        except KeyError:
            pass
        return None
    return val

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

# -------------------------
# SAFE API CALL wrapper (rate-limit aware)
# -------------------------
def safe_sleep(seconds):
    time.sleep(seconds)

def safe_api_call(func_name, func, args=None, kwargs=None, weight=1, cache_ttl=0, allow_fail=False):
    """
    func_name: string (for logging/cache key)
    func: callable to invoke
    weight: estimated request weight
    cache_ttl: seconds
    """
    global weight_counter, RATE_LIMIT_BACKOFF, RATE_LIMIT_RESET_TS

    args = args or []
    kwargs = kwargs or {}

    now = time.time()
    # reset per-minute counter if window expired
    if now - weight_counter['start_ts'] >= RATE_LIMIT_WINDOW:
        weight_counter['start_ts'] = now
        weight_counter['used'] = 0

    # if we are in a global reset window set by a previous rate-limit, sleep until it finishes
    if RATE_LIMIT_RESET_TS and time.time() < RATE_LIMIT_RESET_TS:
        wait = RATE_LIMIT_RESET_TS - time.time()
        jitter = random.uniform(0.08, 0.28)
        wait_sleep = max(0.5, min(wait + jitter, RATE_LIMIT_BACKOFF + 5))
        notify(f"⏸ Backoff active ({int(wait)}s left) before calling {func_name}. Sleeping {wait_sleep:.1f}s.")
        time.sleep(wait_sleep)

    # check cache
    cache_key = None
    if cache_ttl and func_name:
        cache_key = f"cache::{func_name}::{str(args)}::{str(kwargs)}"
        val = cache_get(cache_key)
        if val is not None:
            return val

    # avoid approaching local soft limit
    projected = weight_counter['used'] + weight
    soft_limit = int(REQUEST_WEIGHT_LIMIT * WEIGHT_SAFETY_MARGIN)
    if projected >= soft_limit:
        wait = max(1, int(RATE_LIMIT_WINDOW - (now - weight_counter['start_ts'])) + 1)
        notify(f"⚠️ Approaching local request-weight budget ({projected}/{REQUEST_WEIGHT_LIMIT}). Sleeping {wait}s.")
        time.sleep(wait)
        weight_counter['start_ts'] = time.time()
        weight_counter['used'] = 0

    attempts = 0
    max_attempts = 3
    while attempts < max_attempts:
        try:
            res = func(*args, **kwargs)
            weight_counter['used'] += weight
            if cache_key:
                cache_set(cache_key, res, cache_ttl)
            return res
        except BinanceAPIException as e:
            err = str(e)
            attempts += 1
            # detect request weight error
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                # increase backoff exponentially
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
                RATE_LIMIT_RESET_TS = time.time() + RATE_LIMIT_BACKOFF
                notify(f"⚠️ Rate limit detected in safe_api_call: backing off {RATE_LIMIT_BACKOFF}s (err={err}).")
                # reset local counters to avoid mistaken projections
                weight_counter['start_ts'] = time.time()
                weight_counter['used'] = 0
                # short sleep here (do not block for full backoff - other callers use RATE_LIMIT_RESET_TS)
                time.sleep(min(2.0, RATE_LIMIT_BACKOFF))
                if attempts >= max_attempts and allow_fail:
                    return None
                continue
            notify(f"⚠️ BinanceAPIException in {func_name}: {err} (attempt {attempts}/{max_attempts})")
            time.sleep(0.5 + attempts * 0.5)
            if attempts >= max_attempts and allow_fail:
                return None
            continue
        except Exception as e:
            attempts += 1
            notify(f"⚠️ Exception in safe_api_call for {func_name}: {e} (attempt {attempts}/{max_attempts})")
            time.sleep(0.3 + attempts * 0.4)
            if attempts >= max_attempts and allow_fail:
                return None
            continue

    if allow_fail:
        return None
    raise RuntimeError(f"safe_api_call failed for {func_name}")

# -------------------------
# WRAPPERS: commonly used endpoints
# -------------------------
def get_tickers_cached():
    def _fetch():
        return client.get_ticker()
    res = safe_api_call('ticker_all', _fetch, weight=W_WEIGHT_TICKER * 2, cache_ttl=CACHE_TTL, allow_fail=True)
    return res or []

def get_price_cached(symbol):
    tickers = get_tickers_cached() or []
    for t in tickers:
        if t.get('symbol') == symbol:
            try:
                return float(t.get('lastPrice') or t.get('price') or 0.0)
            except Exception:
                return None
    def _fetch(sym):
        return client.get_symbol_ticker(symbol=sym)
    res = safe_api_call('symbol_ticker_' + symbol, _fetch, args=[symbol], weight=W_WEIGHT_TICKER, cache_ttl=3, allow_fail=True)
    if not res:
        return None
    try:
        return float(res.get('price') or res.get('lastPrice') or 0.0)
    except Exception:
        return None

def get_klines_safe(symbol, interval='5m', limit=8):
    cache_key = f"klines::{symbol}::{interval}::{limit}"
    val = cache_get(cache_key)
    if val is not None:
        return val
    def _fetch(sym, intv, lim):
        return client.get_klines(symbol=sym, interval=intv, limit=lim)
    res = safe_api_call('klines_' + symbol, _fetch, args=[symbol, interval, limit], weight=W_WEIGHT_KLINES, cache_ttl=10, allow_fail=True)
    if res:
        cache_set(cache_key, res, 10)
    return res

def get_order_book_safe(symbol, limit=5):
    def _fetch(sym, lim):
        return client.get_order_book(symbol=sym, limit=lim)
    return safe_api_call('orderbook_' + symbol, _fetch, args=[symbol, limit], weight=W_WEIGHT_ORDERBOOK, cache_ttl=2, allow_fail=True)

def get_symbol_info_cached(symbol, ttl=120):
    cache_key = f"symbol_info::{symbol}"
    val = cache_get(cache_key)
    if val is not None:
        return val
    def _fetch(sym):
        return client.get_symbol_info(sym)
    res = safe_api_call('symbol_info_' + symbol, _fetch, args=[symbol], weight=W_WEIGHT_SYMBOL_INFO, cache_ttl=ttl, allow_fail=True)
    if res:
        cache_set(cache_key, res, ttl)
    return res

def get_open_orders_cached(symbol=None):
    cache_key = f"open_orders::{symbol or 'all'}"
    val = cache_get(cache_key)
    if val is not None:
        if symbol:
            return [o for o in val if o.get('symbol') == symbol]
        return val
    def _fetch(sym=None):
        if sym:
            return client.get_open_orders(symbol=sym)
        return client.get_open_orders()
    args = [symbol] if symbol else []
    res = safe_api_call('open_orders_' + (symbol or 'all'), _fetch, args=args, weight=W_WEIGHT_OPEN_ORDERS, cache_ttl=10, allow_fail=True)
    if res is None:
        return []
    cache_set(cache_key, res, 10)
    if symbol:
        return [o for o in res if o.get('symbol') == symbol]
    return res

# -------------------------
# BALANCE / FILTER HELPERS
# -------------------------
def get_free_usdt():
    try:
        bal = safe_api_call('balance_usdt', lambda: client.get_asset_balance(asset=QUOTE), weight=1, cache_ttl=2, allow_fail=True)
        return float(bal['free']) if bal else 0.0
    except Exception:
        return 0.0

def get_free_asset(asset):
    try:
        bal = safe_api_call(f"balance_{asset}", lambda: client.get_asset_balance(asset=asset), weight=1, cache_ttl=2, allow_fail=True)
        return float(bal['free']) if bal else 0.0
    except Exception:
        return 0.0

def get_filters(symbol_info):
    if not symbol_info:
        return {'stepSize': 0.0, 'minQty': 0.0, 'tickSize': 0.0, 'minNotional': None}
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

def is_symbol_tradable(symbol):
    try:
        info = get_symbol_info_cached(symbol)
        if not info:
            return False
        status = (info.get('status') or "").upper()
        if status != 'TRADING':
            return False
        perms = info.get('permissions') or []
        if isinstance(perms, list) and any(str(p).upper() == 'SPOT' for p in perms):
            return True
        order_types = info.get('orderTypes') or []
        if isinstance(order_types, list):
            for ot in order_types:
                if str(ot).upper() in ('MARKET', 'LIMIT', 'LIMIT_MAKER', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT_LIMIT'):
                    return True
        return False
    except Exception as e:
        notify(f"⚠️ is_symbol_tradable check failed for {symbol}: {e}")
        return False

# -------------------------
# ORDER helpers (market buy, micro TP, oco, fallback)
# -------------------------
def _parse_market_buy_exec(order_resp):
    executed_qty = 0.0
    avg_price = 0.0
    try:
        if not order_resp:
            return 0.0, 0.0
        ex = order_resp.get('executedQty') or order_resp.get('executedQty')
        if ex:
            executed_qty = float(ex)
        if executed_qty > 0:
            cumm = order_resp.get('cummulativeQuoteQty') or order_resp.get('cumulativeQuoteQty') or 0.0
            try:
                cumm = float(cumm)
            except Exception:
                cumm = 0.0
            if cumm > 0:
                avg_price = cumm / executed_qty
        if executed_qty == 0.0:
            fills = order_resp.get('fills') or []
            total_qty = 0.0
            total_quote = 0.0
            for f in fills:
                try:
                    q = float(f.get('qty', 0.0) or 0.0)
                    p = float(f.get('price', 0.0) or 0.0)
                except Exception:
                    q = 0.0; p = 0.0
                total_qty += q
                total_quote += q * p
            if total_qty > 0:
                executed_qty = total_qty
                avg_price = total_quote / total_qty if total_qty > 0 else 0.0
    except Exception:
        return 0.0, 0.0
    return executed_qty, avg_price

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
        notify(f"⚠️ Failed to fetch price for {symbol} (cached). Skipping buy.")
        return None, None

    try:
        price = float(price)
        if price <= 0:
            notify(f"❌ Invalid price for {symbol}: {price}")
            return None, None
    except Exception:
        notify(f"❌ Invalid price type for {symbol}: {price}")
        return None, None

    qty_target = usd_amount / price
    qty_target = max(qty_target, f.get('minQty', 0.0))
    qty_target = round_step(qty_target, f.get('stepSize', 0.0))

    min_notional = f.get('minNotional')
    if min_notional:
        notional = qty_target * price
        if notional < min_notional - 1e-12:
            needed_qty = ceil_step(min_notional / price, f.get('stepSize', 0.0))
            free_usdt = get_free_usdt()
            if needed_qty * price > free_usdt + 1e-8:
                notify(f"❌ Not enough funds for MIN_NOTIONAL on {symbol} (need ${needed_qty*price:.6f}, free=${free_usdt:.6f}).")
                return None, None
            qty_target = needed_qty

    qty_str = format_qty(qty_target, f.get('stepSize', 0.0))
    if not qty_str or float(qty_target) <= 0:
        notify(f"❌ Computed qty invalid for {symbol}: qty_target={qty_target}, qty_str={qty_str}")
        return None, None

    time.sleep(random.uniform(0.05, 0.18))

    def _order(sym, q):
        return client.order_market_buy(symbol=sym, quantity=q)
    order_resp = safe_api_call('market_buy_' + symbol, _order, args=[symbol, qty_str], weight=W_WEIGHT_ORDER, allow_fail=True)
    if not order_resp:
        notify(f"❌ Market buy returned no response for {symbol}")
        return None, None

    executed_qty, avg_price = _parse_market_buy_exec(order_resp)

    time.sleep(0.6)
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
    return executed_qty, avg_price

def place_micro_tp(symbol, qty, entry_price, f, pct=MICRO_TP_PCT, fraction=MICRO_TP_FRACTION):
    try:
        sell_qty = float(qty) * float(fraction)
        sell_qty = round_step(sell_qty, f.get('stepSize', 0.0))
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

        def _limit_sell(sym, q, p):
            return client.order_limit_sell(symbol=sym, quantity=q, price=p)
        order = safe_api_call('limit_sell_' + symbol, _limit_sell, args=[symbol, qty_str, price_str], weight=W_WEIGHT_ORDER, allow_fail=True)
        if not order:
            notify(f"⚠️ Micro TP placement failed for {symbol}")
            return None, 0.0, None
        notify(f"📍 Micro TP placed for {symbol}: sell {qty_str} @ {price_str}")

        order_id = None
        if isinstance(order, dict):
            order_id = order.get('orderId') or order.get('orderId')
        if not order_id:
            return order, sell_qty, tp_price

        poll_interval = 0.6
        max_wait = MICRO_MAX_WAIT
        waited = 0.0

        while waited < max_wait:
            status = safe_api_call('get_order_'+str(order_id), lambda sym, oid: client.get_order(symbol=sym, orderId=oid), args=[symbol, order_id], weight=1, allow_fail=True)
            if not status:
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

        return order, sell_qty, tp_price

    except Exception as e:
        notify(f"⚠️ place_micro_tp error: {e}")
        return None, 0.0, None

def place_market_sell_fallback(symbol, qty, f):
    try:
        qty_str = format_qty(qty, f.get('stepSize', 0.0))
        notify(f"⚠️ Attempting MARKET sell fallback for {symbol}: qty={qty_str}")
        def _mkt_sell(sym, q):
            return client.order_market_sell(symbol=sym, quantity=q)
        resp = safe_api_call('market_sell_' + symbol, _mkt_sell, args=[symbol, qty_str], weight=W_WEIGHT_ORDER, allow_fail=True)
        if resp:
            notify(f"✅ Market sell fallback executed for {symbol}")
        return resp
    except Exception as e:
        notify(f"❌ Market sell fallback failed for {symbol}: {e}")
        return None

def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=1.0,
                   explicit_tp: float = None, explicit_sl: float = None,
                   retries=3, delay=1):
    global RATE_LIMIT_BACKOFF, RATE_LIMIT_RESET_TS

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

    qty = clip_floor(qty, f['stepSize'])
    tp = clip_ceil(tp, f['tickSize'])
    sp = clip_floor(sp, f['tickSize'])
    sl = clip_floor(stop_limit, f['tickSize'])

    if qty <= 0:
        notify("❌ place_oco_sell: quantity too small after clipping")
        return None

    free_qty = get_free_asset(asset)
    safe_margin = f['stepSize'] if f['stepSize'] and f['stepSize'] > 0 else 0.0
    if free_qty + 1e-12 < qty:
        new_qty = clip_floor(max(0.0, free_qty - safe_margin), f['stepSize'])
        if new_qty <= 0:
            notify(f"❌ Not enough free {asset} to place sell. free={free_qty}, required={qty}")
            return None
        notify(f"ℹ️ Adjusting sell qty down from {qty} to available {new_qty} to avoid insufficient balance.")
        qty = new_qty

    min_notional = f.get('minNotional')
    if min_notional:
        if qty * tp < min_notional - 1e-12:
            needed_qty = ceil_step(min_notional / tp, f['stepSize'])
            if needed_qty <= free_qty + 1e-12 and needed_qty > qty:
                notify(f"ℹ️ Increasing qty from {qty} to {needed_qty} to meet minNotional.")
                qty = needed_qty
            else:
                attempts = 0
                while attempts < 40 and qty * tp < min_notional - 1e-12:
                    if f.get('tickSize') and f.get('tickSize') > 0:
                        tp = clip_ceil(tp + f['tickSize'], f['tickSize'])
                    else:
                        tp = tp + max(1e-8, tp * 0.001)
                    attempts += 1
                if qty * tp < min_notional - 1e-12:
                    notify(f"⚠️ Cannot meet minNotional for OCO on {symbol}. Will attempt fallback flow.")

    qty_str = format_qty(qty, f['stepSize'])
    tp_str = format_price(tp, f['tickSize'])
    sp_str = format_price(sp, f['tickSize'])
    sl_str = format_price(sl, f['tickSize'])

    # Attempt standard OCO (wrapped)
    for attempt in range(1, retries + 1):
        try:
            def _oco(sym, q, price, stopPrice, stopLimitPrice):
                return client.create_oco_order(
                    symbol=sym,
                    side='SELL',
                    quantity=q,
                    price=price,
                    stopPrice=stopPrice,
                    stopLimitPrice=stopLimitPrice,
                    stopLimitTimeInForce='GTC'
                )
            oco = safe_api_call('oco_standard_'+symbol, _oco, args=[symbol, qty_str, tp_str, sp_str, sl_str], weight=W_WEIGHT_ORDER, allow_fail=True)
            if oco:
                notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
                return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
            else:
                notify(f"⚠️ OCO standard attempt returned no response (attempt {attempt}).")
        except BinanceAPIException as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (standard) failed: {err}")
            if '-1003' in err:
                RATE_LIMIT_RESET_TS = time.time() + RATE_LIMIT_BACKOFF
                return None
            time.sleep(delay)

    # Attempt alternative param names
    for attempt in range(1, retries + 1):
        try:
            def _oco2(sym, q, ap, bp, p):
                return client.create_oco_order(
                    symbol=sym,
                    side='SELL',
                    quantity=q,
                    aboveType="LIMIT_MAKER",
                    abovePrice=ap,
                    belowType="STOP_LOSS_LIMIT",
                    belowStopPrice=bp,
                    belowPrice=p,
                    belowTimeInForce="GTC"
                )
            oco2 = safe_api_call('oco_alt_'+symbol, _oco2, args=[symbol, qty_str, tp_str, sp_str, sl_str], weight=W_WEIGHT_ORDER, allow_fail=True)
            if oco2:
                notify(f"📌 OCO SELL placed (alt params) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
                return {'tp': tp, 'sl': sp, 'method': 'oco_abovebelow', 'raw': oco2}
            else:
                notify(f"⚠️ OCO alt attempt returned no response (attempt {attempt}).")
        except BinanceAPIException as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (alt) failed: {err}")
            if '-1003' in err:
                RATE_LIMIT_RESET_TS = time.time() + RATE_LIMIT_BACKOFF
                return None
            time.sleep(delay)

    # Fallback to separate TP and SL
    notify("⚠️ All OCO attempts failed — falling back to separate TP (limit) + SL (stop-limit) or MARKET.")
    tp_order = None
    sl_order = None
    try:
        def _tp(sym, q, p):
            return client.order_limit_sell(symbol=sym, quantity=q, price=p)
        tp_order = safe_api_call('tp_limit_' + symbol, _tp, args=[symbol, qty_str, tp_str], weight=W_WEIGHT_ORDER, allow_fail=True)
        if tp_order:
            notify(f"📈 TP LIMIT placed (fallback): {tp_str}, qty={qty_str}")
    except Exception as e:
        notify(f"❌ Fallback TP limit failed: {e}")

    try:
        def _sl(sym, q, spv, p):
            return client.create_order(
                symbol=sym,
                side="SELL",
                type="STOP_LOSS_LIMIT",
                stopPrice=spv,
                price=p,
                timeInForce='GTC',
                quantity=q
            )
        sl_order = safe_api_call('sl_stoplimit_' + symbol, _sl, args=[symbol, qty_str, sp_str, sl_str], weight=W_WEIGHT_ORDER, allow_fail=True)
        if sl_order:
            notify(f"📉 SL STOP_LOSS_LIMIT placed (fallback): trigger={sp_str}, limit={sl_str}, qty={qty_str}")
    except Exception as e:
        notify(f"❌ Fallback SL stop-limit failed: {e}")

    if (tp_order or sl_order):
        return {'tp': tp, 'sl': sp, 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order}}

    fallback_market = place_market_sell_fallback(symbol, qty, f)
    if fallback_market:
        return {'tp': tp, 'sl': sp, 'method': 'market_fallback', 'raw': fallback_market}

    notify("❌ All attempts to protect position failed (no TP/SL placed). TEMP skipping symbol.")
    TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
    return None

def cancel_all_open_orders(symbol, max_cancel=6, inter_delay=0.25):
    try:
        open_orders = get_open_orders_cached(symbol)
        cancelled = 0
        for o in open_orders:
            if cancelled >= max_cancel:
                notify(f"⚠️ Reached max_cancel ({max_cancel}) for {symbol}; leaving remaining orders.")
                break
            try:
                def _cancel(sym, oid):
                    return client.cancel_order(symbol=sym, orderId=oid)
                safe_api_call('cancel_'+str(o.get('orderId')), _cancel, args=[symbol, o.get('orderId')], weight=W_WEIGHT_ORDER, allow_fail=True)
                cancelled += 1
                time.sleep(inter_delay)
            except Exception as e:
                notify(f"⚠️ Cancel failed for {symbol} order {o.get('orderId')}: {e}")
        if cancelled:
            notify(f"❌ Cancelled {cancelled} open orders for {symbol}")
    except Exception as e:
        notify(f"⚠️ Failed to cancel orders: {e}")

# -------------------------
# ORDERBOOK / PICKER (your provided pick_coin adapted)
# -------------------------
def orderbook_bullish(symbol, depth=3, min_imbalance=1.02, max_spread_pct=1.0):
    try:
        ob = get_order_book_safe(symbol=symbol, limit=depth)
        if not ob:
            return False
        bids = ob.get('bids') or []
        asks = ob.get('asks') or []
        if not bids or not asks:
            return False
        top_bid_p = float(bids[0][0]); top_ask_p = float(asks[0][0])
        spread_pct = (top_ask_p - top_bid_p) / (top_bid_p + 1e-12) * 100.0
        bid_sum = sum(float(b[1]) for b in bids[:depth]) + 1e-12
        ask_sum = sum(float(a[1]) for a in asks[:depth]) + 1e-12
        imbalance = bid_sum / ask_sum
        return (imbalance >= min_imbalance) and (spread_pct <= max_spread_pct)
    except Exception:
        return False

def pick_coin():
    global RATE_LIMIT_BACKOFF, TOP_POOL, FINAL_CHOICES, MIN_VOL_RATIO, MIN_VOL_RATIO_1M, MIN_EMA_LIFT
    # local helpers from your version
    def pct_change(open_p, close_p):
        if open_p == 0:
            return 0.0
        return (close_p - open_p) / (open_p + 1e-12) * 100.0

    def ema_local(values, period):
        if not values or period <= 0:
            return None
        alpha = 2.0 / (period + 1.0)
        e = float(values[0])
        for v in values[1:]:
            e = alpha * float(v) + (1 - alpha) * e
        return e

    def compute_rsi_local(closes, period=RSI_PERIOD):
        if not closes or len(closes) < period + 1:
            return None
        gains, losses = [], []
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

    # cheap prefilter
    tickers = get_tickers_cached() or []
    now = time.time()
    pre = []

    for t in tickers:
        sym = t.get('symbol')
        if not sym or not sym.endswith(QUOTE):
            continue
        try:
            last = float(t.get('lastPrice') or 0.0)
            qvol = float(t.get('quoteVolume') or 0.0)
            ch = float(t.get('priceChangePercent') or 0.0)
        except Exception:
            continue
        if not (PRICE_MIN <= last <= PRICE_MAX):
            continue
        if qvol < MIN_VOLUME:
            continue
        if ch is not None and (ch < -10.0 or ch > MAX_24H_CHANGE_ABS):
            continue

        skip_until = TEMP_SKIP.get(sym)
        if skip_until and now < skip_until:
            continue

        last_buy = RECENT_BUYS.get(sym)
        if last_buy and now < last_buy['ts'] + REBUY_COOLDOWN:
            continue

        pre.append((sym, last, qvol, ch))

    if not pre:
        return None

    pre.sort(key=lambda x: x[2], reverse=True)
    candidates = pre[:TOP_POOL]

    # projected weight guard before heavy loop
    soft_limit = int(REQUEST_WEIGHT_LIMIT * WEIGHT_SAFETY_MARGIN)
    est_weight_per_symbol = W_WEIGHT_ORDERBOOK + W_WEIGHT_KLINES + W_WEIGHT_KLINES + W_WEIGHT_ORDERBOOK
    projected = weight_counter['used'] + (W_WEIGHT_TICKER*2) + len(candidates) * est_weight_per_symbol
    if projected >= soft_limit:
        notify(f"⚠️ Projected request-weight {projected} >= soft_limit {soft_limit}. Skipping heavy pick to avoid rate-limit.")
        return None

    scored = []
    for sym, last_price, qvol, change_24h in candidates:
        try:
            time.sleep(REQUEST_SLEEP)

            # fast orderbook
            ob_fast = safe_api_call('ob_fast_'+sym, client.get_order_book, args=[sym, FAST_OB_DEPTH], weight=W_WEIGHT_ORDERBOOK, allow_fail=True)
            if ob_fast is None:
                continue
            bids_f = ob_fast.get('bids') or []
            asks_f = ob_fast.get('asks') or []
            if not bids_f or not asks_f:
                continue
            top_bid = float(bids_f[0][0]); top_ask = float(asks_f[0][0])
            spread_fast = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
            if spread_fast > max(1.5, MAX_OB_SPREAD_PCT * 1.5):
                continue

            kl5 = safe_api_call('kl5_'+sym, client.get_klines, args=[sym, '5m', KLINES_5M_LIMIT], weight=W_WEIGHT_KLINES, allow_fail=True)
            if kl5 is None or len(kl5) < 3:
                continue

            time.sleep(REQUEST_SLEEP)
            kl1 = safe_api_call('kl1_'+sym, client.get_klines, args=[sym, '1m', KLINES_1M_LIMIT], weight=W_WEIGHT_KLINES, allow_fail=True)
            if kl1 is None or len(kl1) < 2:
                continue

            closes_5m = [float(k[4]) for k in kl5]
            vols_5m = []
            for k in kl5:
                if len(k) > 7 and k[7] is not None:
                    vols_5m.append(float(k[7]))
                else:
                    vols_5m.append(float(k[5]) * float(k[4]))

            closes_1m = [float(k[4]) for k in kl1]
            vols_1m = []
            for k in kl1:
                if len(k) > 7 and k[7] is not None:
                    vols_1m.append(float(k[7]))
                else:
                    vols_1m.append(float(k[5]) * float(k[4]))

            open_5m = float(kl5[0][1])
            pct_5m = pct_change(open_5m, closes_5m[-1])
            open_1m = float(kl1[0][1])
            pct_1m = pct_change(open_1m, closes_1m[-1])

            avg_prev_5m = (sum(vols_5m[:-1]) / max(len(vols_5m[:-1]), 1)) if len(vols_5m) > 1 else vols_5m[-1]
            vol_ratio_5m = vols_5m[-1] / (avg_prev_5m + 1e-12)

            avg_prev_1m = (sum(vols_1m[:-1]) / max(len(vols_1m[:-1]), 1)) if len(vols_1m) > 1 else vols_1m[-1]
            vol_ratio_1m = vols_1m[-1] / (avg_prev_1m + 1e-12)

            last3 = closes_5m[-3:] if len(closes_5m) >= 3 else closes_5m
            ups = 0
            if len(last3) >= 2 and last3[1] > last3[0]:
                ups += 1
            if len(last3) >= 3 and last3[2] > last3[1]:
                ups += 1
            if ups < 2:
                continue

            short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
            long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
            ema_uplift = 0.0
            ema_ok = False
            if short_ema is not None and long_ema is not None:
                ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
                ema_ok = (short_ema > long_ema) and (ema_uplift >= MIN_EMA_LIFT)
            if not ema_ok:
                continue

            rsi_val = compute_rsi_local(closes_5m[-(RSI_PERIOD + 1):], RSI_PERIOD) if len(closes_5m) >= RSI_PERIOD + 1 else None
            if rsi_val is not None and rsi_val > 68:
                continue

            time.sleep(REQUEST_SLEEP)
            ob = safe_api_call('ob_full_'+sym, client.get_order_book, args=[sym, OB_DEPTH], weight=W_WEIGHT_ORDERBOOK, allow_fail=True)
            if ob is None:
                continue

            ob_ok = False
            ob_imbalance = 1.0
            ob_spread_pct = 100.0
            bid_quote_liq = 0.0
            bids = ob.get('bids') or []
            asks = ob.get('asks') or []
            if bids and asks:
                top_bid = float(bids[0][0]); top_ask = float(asks[0][0])
                bid_sum = sum(float(b[1]) for b in bids[:OB_DEPTH]) + 1e-12
                ask_sum = sum(float(a[1]) for a in asks[:OB_DEPTH]) + 1e-12
                ob_imbalance = bid_sum / ask_sum
                ob_spread_pct = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
                bid_quote_liq = sum(float(b[0]) * float(b[1]) for b in bids[:OB_DEPTH])
                ob_ok = (ob_imbalance >= MIN_OB_IMBALANCE) and (ob_spread_pct <= MAX_OB_SPREAD_PCT) and (bid_quote_liq >= MIN_OB_LIQUIDITY)

            vol_ok = (vol_ratio_5m >= MIN_VOL_RATIO) and (vol_ratio_1m >= MIN_VOL_RATIO_1M)
            if not vol_ok:
                continue
            if change_24h is not None and change_24h > MAX_24H_RISE_PCT:
                continue
            if not ob_ok:
                continue

            score = 0.0
            score += max(0.0, pct_5m) * 6.0
            score += max(0.0, pct_1m) * 2.0
            score += max(0.0, (vol_ratio_5m - 1.0)) * 4.0 * 100.0
            score += ema_uplift * 8.0 * 100.0
            if rsi_val is not None:
                score += max(0.0, (60.0 - min(rsi_val, 60.0))) * 1.2
            score += max(0.0, change_24h) * 0.6
            if ob_ok:
                score += 25.0
            if last_price <= PRICE_MAX:
                score += 4.0

            scored.append({
                "symbol": sym,
                "last_price": last_price,
                "24h_change": change_24h,
                "24h_vol": qvol,
                "pct_5m": pct_5m,
                "pct_1m": pct_1m,
                "vol_ratio_5m": vol_ratio_5m,
                "vol_ratio_1m": vol_ratio_1m,
                "ema_ok": ema_ok,
                "ema_uplift": ema_uplift,
                "rsi": rsi_val,
                "ob_ok": ob_ok,
                "ob_imbalance": ob_imbalance,
                "ob_spread_pct": ob_spread_pct,
                "ob_bid_liq": bid_quote_liq,
                "score": score
            })
        except Exception as e:
            notify(f"⚠️ pick_coin evaluate error {sym}: {e}")
            continue

    if not scored:
        return None

    scored.sort(key=lambda x: x['score'], reverse=True)
    pool = scored[:TOP_POOL] if len(scored) >= TOP_POOL else scored
    top_n = min(FINAL_CHOICES, len(pool))
    if top_n <= 0:
        best = scored[0]
    else:
        best = random.choice(pool[:top_n])

    return (best['symbol'], best['last_price'], best['24h_vol'], best['24h_change'])

# -------------------------
# MONITOR & ROLL
# -------------------------
def monitor_and_roll(symbol, qty, entry_price, f):
    orig_qty = qty
    curr_tp = entry_price * (1 + BASE_TP_PCT / 100.0)
    curr_sl = entry_price * (1 - BASE_SL_PCT / 100.0)

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

    start_ts = time.time()

    while True:
        try:
            elapsed = time.time() - start_ts
            if elapsed > MAX_HOLD_SECONDS:
                notify(f"⏳ MAX_HOLD_SECONDS exceeded ({elapsed/3600:.2f}h) for {symbol}. Attempting emergency close.")
                try:
                    cancel_all_open_orders(symbol)
                except Exception:
                    pass
                free_qty = get_free_asset(symbol[:-len(QUOTE)])
                if free_qty and free_qty > 0:
                    try:
                        f_local = f if f else get_filters(get_symbol_info_cached(symbol) or {})
                        place_market_sell_fallback(symbol, free_qty, f_local)
                        notify(f"❗ Emergency market sell attempted for {symbol} after hold timeout.")
                    except Exception as e:
                        notify(f"❌ Emergency market sell failed for {symbol}: {e}")
                TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
                return False, entry_price, 0.0

            time.sleep(SLEEP_BETWEEN_CHECKS)
            asset = symbol[:-len(QUOTE)]
            price_now = get_price_cached(symbol)
            if price_now is None:
                try:
                    price_now = float(safe_api_call('symbol_ticker_fallback_'+symbol, lambda sym: client.get_symbol_ticker(symbol=sym), args=[symbol], weight=W_WEIGHT_TICKER, allow_fail=True).get('price'))
                except Exception as e:
                    notify(f"⚠️ Failed to fetch price in monitor (fallback): {e}")
                    continue

            free_qty = get_free_asset(asset)
            available_for_sell = min(round_step(free_qty, f.get('stepSize', 0.0)), orig_qty)
            open_orders = get_open_orders_cached(symbol)

            if available_for_sell < round_step(orig_qty * 0.05, f.get('stepSize', 0.0)) and len(open_orders) == 0:
                exit_price = price_now
                profit_usd = (exit_price - entry_price) * orig_qty
                notify(f"✅ Position closed for {symbol}: exit={exit_price:.8f}, profit≈${profit_usd:.6f}")
                return True, exit_price, profit_usd

            price_delta = price_now - entry_price
            rise_trigger_pct = price_now >= entry_price * (1 + ROLL_ON_RISE_PCT / 100.0)
            rise_trigger_abs = price_delta >= max(ROLL_TRIGGER_DELTA_ABS, entry_price * (ROLL_TRIGGER_PCT/100.0))
            near_trigger = (price_now >= curr_tp * (1 - TRIGGER_PROXIMITY)) and (price_now < curr_tp * 1.05)
            tick = f.get('tickSize', 0.0) or 0.0
            minimal_move = max(entry_price * 0.004, ROLL_TRIGGER_DELTA_ABS * 0.4, tick or 0.0)
            moved_enough = price_delta >= minimal_move
            now_ts = time.time()
            can_roll = (now_ts - last_roll_ts) >= ROLL_COOLDOWN_SECONDS

            if ((near_trigger and moved_enough) or rise_trigger_pct or rise_trigger_abs) and available_for_sell >= f.get('minQty', 0.0) and can_roll:
                if roll_count >= MAX_ROLLS_PER_POSITION:
                    notify(f"⚠️ Reached max rolls ({MAX_ROLLS_PER_POSITION}) for {symbol}, will not roll further.")
                    last_roll_ts = now_ts
                    continue

                notify(f"🔎 Roll triggered for {symbol}: price={price_now:.8f}, entry={entry_price:.8f}, curr_tp={curr_tp:.8f}, delta={price_delta:.6f} (near={near_trigger},pct={rise_trigger_pct},abs={rise_trigger_abs})")
                candidate_tp = curr_tp + ROLL_TP_STEP_ABS
                candidate_sl = curr_sl + ROLL_SL_STEP_ABS
                if candidate_sl > entry_price:
                    candidate_sl = entry_price

                new_tp = clip_tp(candidate_tp, tick)
                new_sl = clip_sl(candidate_sl, tick)
                tick_step = tick or 0.0
                if new_tp <= new_sl + tick_step:
                    if tick_step > 0:
                        new_tp = new_sl + tick_step * 2
                    else:
                        new_tp = new_sl + max(1e-8, ROLL_TP_STEP_ABS)
                if new_tp <= curr_tp:
                    if tick_step > 0:
                        new_tp = math.ceil((curr_tp + tick_step) / tick_step) * tick_step
                    else:
                        new_tp = curr_tp + max(1e-8, ROLL_TP_STEP_ABS)

                sell_qty = round_step(available_for_sell, f.get('stepSize', 0.0))
                if sell_qty <= 0 or sell_qty < f.get('minQty', 0.0):
                    notify(f"⚠️ Roll skipped: sell_qty {sell_qty} too small or < minQty.")
                    last_roll_ts = now_ts
                    continue

                min_notional = f.get('minNotional')
                if min_notional:
                    adjust_cnt = 0
                    max_adj = 40
                    while adjust_cnt < max_adj and sell_qty * new_tp < min_notional - 1e-12:
                        if tick_step > 0:
                            new_tp = clip_tp(new_tp + tick_step, tick_step)
                        else:
                            new_tp = new_tp + max(1e-8, new_tp * 0.001)
                        adjust_cnt += 1
                    if sell_qty * new_tp < min_notional - 1e-12:
                        needed_qty = ceil_step(min_notional / new_tp, f.get('stepSize'))
                        if needed_qty <= available_for_sell + 1e-12 and needed_qty > sell_qty:
                            notify(f"ℹ️ Increasing sell_qty to {needed_qty} to meet minNotional for roll.")
                            sell_qty = needed_qty
                        else:
                            notify(f"⚠️ Roll aborted: cannot meet minNotional for {symbol} even after TP bumps.")
                            last_roll_ts = now_ts
                            continue

                last_roll_ts = now_ts
                cancel_all_open_orders(symbol)
                time.sleep(random.uniform(*ROLL_POST_CANCEL_JITTER))

                oco2 = place_oco_sell(symbol, sell_qty, entry_price,
                                      explicit_tp=new_tp, explicit_sl=new_sl)
                if oco2:
                    roll_count += 1
                    last_tp = curr_tp
                    curr_tp = new_tp
                    curr_sl = new_sl
                    notify(f"🔁 Rolled OCO (abs-step): new TP={curr_tp:.8f}, new SL={curr_sl:.8f}, qty={sell_qty}")
                else:
                    notify("⚠️ Roll attempt failed; previous orders are cancelled. Will try to re-place protective OCO next loop.")
                    time.sleep(0.4)
                    fallback = place_oco_sell(symbol, sell_qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
                    if fallback:
                        notify("ℹ️ Fallback OCO re-placed after failed roll.")
                    else:
                        notify("❌ Fallback OCO also failed; TEMP skipping symbol.")
                        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        except Exception as e:
            notify(f"⚠️ Error in monitor_and_roll: {e}")
            return False, entry_price, 0.0

# -------------------------
# METRICS & STATS
# -------------------------
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
# CLEANUP
# -------------------------
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

# -------------------------
# TRADE CYCLE
# -------------------------
ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0
BUY_LOCK_SECONDS = 60

def trade_cycle():
    global start_balance_usdt, ACTIVE_SYMBOL, LAST_BUY_TS, RATE_LIMIT_BACKOFF
    if start_balance_usdt is None:
        start_balance_usdt = get_free_usdt()
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

            candidate = pick_coin()
            if not candidate:
                notify("⚠️ No eligible coin found. Sleeping...")
                if RATE_LIMIT_RESET_TS and time.time() < RATE_LIMIT_RESET_TS:
                    left = int(RATE_LIMIT_RESET_TS - time.time())
                    notify(f"⏸ Backing off due to prior rate-limit for {left}s.")
                    time.sleep(min(left, 180))
                else:
                    time.sleep(180)
                continue

            symbol, price, volume, change = candidate
            notify(f"🎯 Selected {symbol} for market buy (24h change={change}%, vol≈{volume})")

            last = RECENT_BUYS.get(symbol)
            if last:
                if now < last['ts'] + last.get('cooldown', REBUY_COOLDOWN):
                    notify(f"⏭️ Skipping {symbol} due to recent buy cooldown.")
                    time.sleep(CYCLE_DELAY)
                    continue
                if last.get('price') and price > last['price'] * (1 + REBUY_MAX_RISE_PCT / 100.0):
                    notify(f"⏭️ Skipping {symbol} because price rose >{REBUY_MAX_RISE_PCT}% since last buy.")
                    time.sleep(CYCLE_DELAY)
                    continue

            free_usdt = get_free_usdt()
            usd_to_buy = min(TRADE_USD, free_usdt)
            if usd_to_buy < 1.0:
                notify(f"⚠️ Not enough USDT to buy (free={free_usdt:.4f}). Sleeping...")
                time.sleep(CYCLE_DELAY)
                continue

            buy_res = place_safe_market_buy(symbol, usd_to_buy, require_orderbook=True)
            if not buy_res or buy_res == (None, None):
                notify(f"ℹ️ Buy skipped/failed for {symbol}.")
                time.sleep(CYCLE_DELAY)
                continue

            qty, entry_price = buy_res
            if qty is None or entry_price is None:
                notify(f"⚠️ Unexpected buy result for {symbol}, skipping.")
                time.sleep(CYCLE_DELAY)
                continue

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

            try:
                closed, exit_price, profit_usd = monitor_and_roll(symbol, qty_remaining, entry_price, f)
            finally:
                ACTIVE_SYMBOL = None

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
                RATE_LIMIT_RESET_TS = time.time() + RATE_LIMIT_BACKOFF
                time.sleep(RATE_LIMIT_BACKOFF)
                continue

            notify(f"❌ Trade cycle unexpected error: {e}")
            time.sleep(CYCLE_DELAY)

        time.sleep(30)

# -------------------------
# TRANSFERS & MISC
# -------------------------
def send_profit_to_funding(amount, asset='USDT'):
    try:
        def _transfer():
            return client.universal_transfer(
                type='MAIN_FUNDING' if hasattr(client, 'universal_transfer') else 'SPOT_TO_FUNDING',
                asset=asset,
                amount=str(round(amount, 6))
            )
        result = safe_api_call('transfer', _transfer, weight=W_WEIGHT_ORDER, allow_fail=True)
        notify(f"💸 Profit ${amount:.6f} transferred to funding wallet.")
        return result
    except Exception as e:
        notify(f"❌ Failed to transfer profit: {e}")
        return None

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
    notify("✅ Binance client initialized.")
    bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    bot_thread.start()
    start_flask()