# cryptobot_improved.py
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
# CONFIG
# -------------------------
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
QUOTE = "USDT"

# price / liquidity filters
PRICE_MIN = 0.9
PRICE_MAX = 3.0
MIN_VOLUME = 2_000_000          # daily quote volume baseline

# require small recent move (we prefer coins that just started moving)
RECENT_PCT_MIN = 1.0
RECENT_PCT_MAX = 2.0            # require recent move between 1%..2%

# absolute 24h change guardrails (avoid extreme pump/dump)
MAX_24H_RISE_PCT = 5.0
MAX_24H_CHANGE_ABS = 5.0

MOVEMENT_MIN_PCT = 1.0

# runtime / pacing
TRADE_USD = 8.0
SLEEP_BETWEEN_CHECKS = 30
CYCLE_DELAY = 15
COOLDOWN_AFTER_EXIT = 10

# order / protection
TRIGGER_PROXIMITY = 0.06
STEP_INCREMENT_PCT = 0.02
BASE_TP_PCT = 2.5
BASE_SL_PCT = 2.0

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

# emergency hold timeout (6 hours default)
MAX_HOLD_SECONDS = 6 * 3600  # seconds

# -------------------------
# RATE LIMIT / WEIGHT CONFIG
# -------------------------
REQUEST_WEIGHT_LIMIT = 6000         # Binance default per minute (approx)
RATE_LIMIT_WINDOW = 60              # seconds
WEIGHT_SAFETY_MARGIN = 0.90         # use only 90% of the limit to be safe

# approximate weights (tune if needed)
W_WEIGHT_TICKER = 1
W_WEIGHT_KLINES = 2
W_WEIGHT_ORDERBOOK = 2
W_WEIGHT_SYMBOL_INFO = 1
W_WEIGHT_OPEN_ORDERS = 2
W_WEIGHT_ORDER = 5

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

# per-minute weight counter
weight_counter = {'start_ts': time.time(), 'used': 0}

# simple local caches
_LOCAL_CACHE = {}  # key -> (ts, ttl, value)

# METRICS
METRICS = {}

# -------------------------
# HELPERS
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

def safe_sleep(seconds):
    time.sleep(seconds)

def safe_api_call(func_name, func, args=None, kwargs=None, weight=1, cache_ttl=0, allow_fail=False):
    """
    Safe wrapper for Binance client calls.
    func_name: string for cache/logging
    func: callable
    weight: estimated weight
    cache_ttl: seconds to cache
    allow_fail: if True return None on persistent failures
    """
    global weight_counter, RATE_LIMIT_BACKOFF

    args = args or []
    kwargs = kwargs or {}

    now = time.time()
    # reset per-minute counter if window passed
    if now - weight_counter['start_ts'] >= RATE_LIMIT_WINDOW:
        weight_counter['start_ts'] = now
        weight_counter['used'] = 0

    # backoff active? sleep a bit before calling
    if RATE_LIMIT_BACKOFF and (now - weight_counter['start_ts']) < RATE_LIMIT_BACKOFF:
        notify(f"⏸ Backoff active ({RATE_LIMIT_BACKOFF}s) before calling {func_name}.")
        time.sleep(RATE_LIMIT_BACKOFF)

    # check local cache
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
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
                wait = RATE_LIMIT_BACKOFF
                notify(f"⚠️ Rate limit detected in safe_api_call: backing off {wait}s (err={err}).")
                time.sleep(wait)
                # reset local minute counter
                weight_counter['start_ts'] = time.time()
                weight_counter['used'] = 0
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
# WRAPPERS FOR COMMON ENDPOINTS
# -------------------------
def get_tickers_cached():
    def _fetch():
        return client.get_ticker()
    res = safe_api_call('ticker_all', _fetch, weight=W_WEIGHT_TICKER * 2, cache_ttl=CACHE_TTL, allow_fail=True)
    return res or []

def get_price_cached(symbol):
    # try tickers first
    tickers = get_tickers_cached() or []
    for t in tickers:
        if t.get('symbol') == symbol:
            try:
                return float(t.get('lastPrice') or t.get('price') or 0.0)
            except Exception:
                return None
    # fallback
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
# FORMATTING / ROUNDING HELPERS
# -------------------------
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

# Decimal helpers for price/qty quantization
def dec(v):
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal('0')

def quantize_price_up(price: Decimal, tick: Decimal):
    if not tick or tick == 0:
        return price
    precision = max(0, -tick.as_tuple().exponent)
    return price.quantize(Decimal(10) ** -precision, rounding=ROUND_UP)

def quantize_price_down(price: Decimal, tick: Decimal):
    if not tick or tick == 0:
        return price
    precision = max(0, -tick.as_tuple().exponent)
    return price.quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN)

def quantize_qty_down(qty: Decimal, step: Decimal):
    if not step or step == 0:
        return qty
    try:
        mult = (qty // step)
        quant = (mult * step)
        precision = max(0, -step.as_tuple().exponent)
        return quant.quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN)
    except Exception:
        precision = max(0, -step.as_tuple().exponent)
        return qty.quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN)

# -------------------------
# BALANCE HELPERS
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
# ORDER / OCO / MICRO TP HELPERS (improved)
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
            if not orderbook_bullish(symbol, depth=5, min_imbalance=1.12, max_spread_pct=0.6):
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
        try:
            OPEN = get_open_orders_cached(symbol)
            cache_set(f"open_orders::{symbol or 'all'}", OPEN, 1)
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
                status = safe_api_call('get_order_'+str(order_id), lambda sym, oid: client.get_order(symbol=sym, orderId=oid), args=[symbol, order_id], weight=1, allow_fail=True)
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
            try:
                OPEN = get_open_orders_cached(symbol)
                cache_set(f"open_orders::{symbol or 'all'}", OPEN, 1)
            except Exception:
                pass
        return resp
    except Exception as e:
        notify(f"❌ Market sell fallback failed for {symbol}: {e}")
        return None

# ---------- improved OCO flow (uses safe_api_call) ----------
def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=1.0,
                   explicit_tp: float = None, explicit_sl: float = None,
                   retries=3, delay=0.6):
    """
    Improved OCO placement:
    - quantizes price/qty to tick/step with Decimal
    - handles minNotional attempts (increase qty or raise TP)
    - tries standard schema, then alternative, then fallback TP+SL, then market sell fallback
    - uses safe_api_call with weights/backoff
    """
    global RATE_LIMIT_BACKOFF, TEMP_SKIP

    info = get_symbol_info_cached(symbol)
    if not info:
        notify(f"⚠️ place_oco_sell: couldn't fetch symbol info for {symbol}")
        return None
    f = get_filters(info)
    asset = symbol[:-len(QUOTE)]

    step = dec(f.get('stepSize') or 0)
    tick = dec(f.get('tickSize') or 0)
    min_notional = dec(f.get('minNotional') or 0)
    free_qty = dec(get_free_asset(asset))

    qty_d = dec(qty)
    buy_price_d = dec(buy_price)

    tp_d = dec(explicit_tp) if explicit_tp is not None else (buy_price_d * (Decimal('1') + Decimal(str(tp_pct)) / Decimal('100')))
    sp_d = dec(explicit_sl) if explicit_sl is not None else (buy_price_d * (Decimal('1') - Decimal(str(sl_pct)) / Decimal('100')))
    stop_limit_d = sp_d * Decimal('0.999')

    # clip qty to step
    qty_d = quantize_qty_down(qty_d, step)
    if qty_d <= 0:
        notify("❌ place_oco_sell: quantity too small after clipping")
        return None

    # ensure enough free asset
    if free_qty + Decimal('1e-12') < qty_d:
        new_qty = quantize_qty_down(max(Decimal('0'), free_qty - (step if step > 0 else Decimal('0'))), step)
        if new_qty <= 0:
            notify(f"❌ Not enough free {asset} to place sell. free={free_qty}, required={qty_d}")
            return None
        notify(f"ℹ️ Adjusting sell qty down from {qty_d} to available {new_qty} to avoid insufficient balance.")
        qty_d = new_qty

    # try satisfy minNotional by increasing qty or tp
    if min_notional and min_notional > 0:
        tp_q = quantize_price_up(tp_d, tick)
        notional = qty_d * tp_q
        if notional < min_notional - Decimal('1e-12'):
            # try increase qty if possible
            try:
                needed_qty_float = ceil_step(float(min_notional / tp_q), float(step)) if step and step != 0 else math.ceil(float(min_notional / tp_q))
                needed_qty = dec(needed_qty_float)
            except Exception:
                needed_qty = qty_d
            if needed_qty <= free_qty + Decimal('1e-12') and needed_qty > qty_d:
                notify(f"ℹ️ Increasing qty from {qty_d} to {needed_qty} to meet minNotional.")
                qty_d = quantize_qty_down(needed_qty, step)
            else:
                # bump TP iteratively
                attempts = 0
                max_attempts = 60
                tp_candidate = tp_q
                while attempts < max_attempts and qty_d * tp_candidate < min_notional - Decimal('1e-12'):
                    if tick and tick > 0:
                        tp_candidate = tp_candidate + tick
                        tp_candidate = quantize_price_up(tp_candidate, tick)
                    else:
                        tp_candidate = tp_candidate * (Decimal('1') + Decimal('0.001'))
                    attempts += 1
                if qty_d * tp_candidate >= min_notional - Decimal('1e-12'):
                    tp_d = tp_candidate
                else:
                    notify(f"⚠️ Cannot meet minNotional for OCO on {symbol}. Will attempt fallback flow later.")

    # prepare strings (quantized)
    qty_q = quantize_qty_down(qty_d, step)
    tp_q = quantize_price_up(tp_d, tick)
    sp_q = quantize_price_down(sp_d, tick)
    sl_q = quantize_price_down(stop_limit_d, tick)

    qty_str = format_qty(float(qty_q), float(step))
    tp_str = format_price(str(tp_q), float(tick))
    sp_str = format_price(str(sp_q), float(tick))
    sl_str = format_price(str(sl_q), float(tick))

    # Attempt 1: standard OCO
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
                try:
                    OPEN = get_open_orders_cached(symbol)
                    cache_set(f"open_orders::{symbol or 'all'}", OPEN, 1)
                except Exception:
                    pass
                notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
                return {'tp': float(tp_q), 'sl': float(sp_q), 'method': 'oco', 'raw': oco}
            else:
                notify(f"⚠️ OCO standard attempt returned no response (attempt {attempt}).")
        except BinanceAPIException as e:
            err = str(e)
            notify(f"⚠️ BinanceAPIException in oco_standard_{symbol}: {err} (attempt {attempt}/{retries})")
            if 'NOTIONAL' in err or 'minNotional' in err or '-1013' in err:
                if min_notional:
                    needed_qty = ceil_step(min_notional / float(tp_q), float(step)) if step and step != 0 else None
                    if needed_qty and needed_qty <= float(free_qty) + 1e-12 and needed_qty > float(qty_q):
                        qty_q = needed_qty
                        qty_str = format_qty(float(qty_q), float(step))
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

    # Attempt 2: alternative param names
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
                try:
                    OPEN = get_open_orders_cached(symbol)
                    cache_set(f"open_orders::{symbol or 'all'}", OPEN, 1)
                except Exception:
                    pass
                notify(f"📌 OCO SELL placed (alt params) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
                return {'tp': float(tp_q), 'sl': float(sp_q), 'method': 'oco_abovebelow', 'raw': oco2}
            else:
                notify(f"⚠️ OCO alt attempt returned no response (attempt {attempt}).")
        except BinanceAPIException as e:
            err = str(e)
            notify(f"⚠️ BinanceAPIException in oco_alt_{symbol}: {err} (attempt {attempt}/{retries})")
            if '-1003' in err or 'Too much request weight' in err:
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

    # Fallback: place TP limit and STOP_LOSS_LIMIT
    notify("⚠️ All OCO attempts failed — falling back to separate TP (limit) + SL (stop-limit) or MARKET.")

    tp_order = None
    sl_order = None
    try:
        def _tp(sym, q, p):
            return client.order_limit_sell(symbol=sym, quantity=q, price=p)
        tp_order = safe_api_call('tp_limit_' + symbol, _tp, args=[symbol, qty_str, tp_str], weight=W_WEIGHT_ORDER, allow_fail=True)
        if tp_order:
            try:
                OPEN = get_open_orders_cached(symbol)
                cache_set(f"open_orders::{symbol or 'all'}", OPEN, 1)
            except Exception:
                pass
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
            try:
                OPEN = get_open_orders_cached(symbol)
                cache_set(f"open_orders::{symbol or 'all'}", OPEN, 1)
            except Exception:
                pass
            notify(f"📉 SL STOP_LOSS_LIMIT placed (fallback): trigger={sp_str}, limit={sl_str}, qty={qty_str}")
    except Exception as e:
        notify(f"❌ Fallback SL stop-limit failed: {e}")

    if (tp_order or sl_order):
        return {'tp': float(tp_q), 'sl': float(sp_q), 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order}}

    # Final fallback: market sell
    fallback_market = place_market_sell_fallback(symbol, float(qty_q), f)
    if fallback_market:
        return {'tp': float(tp_q), 'sl': float(sp_q), 'method': 'market_fallback', 'raw': fallback_market}

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
            try:
                cache_set(f"open_orders::{symbol or 'all'}", None, 1)
            except Exception:
                pass
    except Exception as e:
        notify(f"⚠️ Failed to cancel orders: {e}")

# -------------------------
# ORDERBOOK / PICKER LOGIC (uses safe wrappers)
# -------------------------
def orderbook_bullish(symbol, depth=3, min_imbalance=1.02, max_spread_pct=1.0):
    try:
        ob = get_order_book_safe(symbol, limit=depth)
        if not ob:
            return False
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

def pick_coin():
    global RATE_LIMIT_BACKOFF
    cleanup_temp_skip()
    cleanup_recent_buys()
    now = time.time()

    TOP_CANDIDATES = 80
    MIN_VOL_RATIO = 1.4
    KLINES_LIMIT = 8

    tickers = get_tickers_cached() or []
    prefiltered = []

    MIN_QVOL = max(MIN_VOLUME, 300_000)
    for t in tickers:
        sym = t.get('symbol')
        if not sym or not sym.endswith(QUOTE):
            continue

        skip_until = TEMP_SKIP.get(sym)
        if skip_until and now < skip_until:
            continue

        last = RECENT_BUYS.get(sym)
        try:
            price = float(t.get('lastPrice') or 0.0)
            qvol = float(t.get('quoteVolume') or 0.0)
            change_pct = float(t.get('priceChangePercent') or 0.0)
        except Exception:
            continue
        # require decent quote volume
        if qvol < 3_000_000:
            continue
        if last:
            if now < last['ts'] + last.get('cooldown', REBUY_COOLDOWN):
                continue
            last_price = last.get('price')
            if last_price and price > last_price * (1 + REBUY_MAX_RISE_PCT / 100.0):
                continue

        # price boundary
        if not (PRICE_MIN <= price <= PRICE_MAX):
            continue

        # absolute 24h guard: avoid extreme pumps/dumps
        if abs(change_pct) > MAX_24H_CHANGE_ABS:
            continue
        if change_pct > MAX_24H_RISE_PCT:
            continue

        # volume baseline and directional movement requirement
        if qvol < MIN_QVOL:
            continue
        if abs(change_pct) < MOVEMENT_MIN_PCT:
            continue

        prefiltered.append((sym, price, qvol, change_pct))

    if not prefiltered:
        return None

    prefiltered.sort(key=lambda x: x[2], reverse=True)
    prefiltered = prefiltered[:TOP_CANDIDATES]

    candidates = []
    for sym, price, qvol, change_pct in prefiltered:
        try:
            klines = get_klines_safe(sym, interval='5m', limit=KLINES_LIMIT)
            if not klines or len(klines) < 6:
                notify(f"⚠️ Skipping {sym} because klines returned None/short (rate-limit/cache).")
                continue

            vols = []
            closes = []
            for k in klines:
                try:
                    if len(k) > 7 and k[7] is not None:
                        vols.append(float(k[7]))
                    else:
                        vols.append(float(k[5]) * float(k[4]))
                except Exception:
                    vols.append(0.0)
                try:
                    closes.append(float(k[4]))
                except Exception:
                    closes.append(0.0)

            recent_vol = vols[-1] if vols else 0.0
            if recent_vol <= 0:
                continue

            prev_slice = vols[-(KLINES_LIMIT-1):-1] if len(vols) >= 2 else vols[:-1]
            avg_vol = (sum(prev_slice) / max(len(prev_slice), 1)) if prev_slice else recent_vol
            vol_ratio = recent_vol / (avg_vol + 1e-9)

            recent_pct = 0.0
            if len(closes) >= 4 and closes[-4] > 0:
                recent_pct = (closes[-1] - closes[-4]) / (closes[-4] + 1e-12) * 100.0

            # enforce recent band
            if recent_pct < RECENT_PCT_MIN or recent_pct > RECENT_PCT_MAX:
                continue

            # volume spike requirement
            if vol_ratio < MIN_VOL_RATIO:
                continue

            # require at least one consecutive up in last 3 closes
            last3 = closes[-3:]
            ups = 0
            if last3[1] > last3[0]:
                ups += 1
            if last3[2] > last3[1]:
                ups += 1
            if ups < 1:
                continue

            # EMA confirmation
            def ema_local(values, period):
                if not values or period <= 0:
                    return None
                alpha = 2.0 / (period + 1.0)
                e = float(values[0])
                for v in values[1:]:
                    e = alpha * float(v) + (1 - alpha) * e
                return e

            short_period = 3
            long_period = 10
            if len(closes) >= long_period:
                short_ema = ema_local(closes[-short_period:], short_period)
                long_ema = ema_local(closes[-long_period:], long_period)
            elif len(closes) >= short_period:
                short_ema = ema_local(closes[-short_period:], short_period)
                long_ema = ema_local(closes, max(len(closes), 1))
            else:
                short_ema = None
                long_ema = None

            if short_ema is None or long_ema is None:
                continue
            if not (short_ema > long_ema * 1.0005):
                continue

            # RSI sanity
            rsi_ok = True
            if len(closes) >= 15:
                gains = []
                losses = []
                for i in range(1, 15):
                    diff = closes[-15 + i] - closes[-16 + i]
                    if diff > 0:
                        gains.append(diff)
                    else:
                        losses.append(abs(diff))
                avg_gain = sum(gains) / 14.0 if gains else 0.0
                avg_loss = sum(losses) / 14.0 if losses else 1e-9
                rs = avg_gain / (avg_loss if avg_loss > 0 else 1e-9)
                rsi = 100 - (100 / (1 + rs))
                if rsi > 66:
                    rsi_ok = False
            if not rsi_ok:
                continue

            # orderbook bullishness quick check (cheap)
            try:
                if not orderbook_bullish(sym, depth=5, min_imbalance=1.12, max_spread_pct=1.0):
                    continue
            except Exception:
                continue

            ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
            score = (recent_pct * 12.0) + (math.log1p(qvol) * 0.4) + (vol_ratio * 6.0) + (ema_uplift * 80.0)

            candidates.append((sym, price, qvol, change_pct, vol_ratio, recent_pct, score))

        except Exception as e:
            notify(f"⚠️ pick_coin processing error for {sym}: {e}")
            continue

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[-1], reverse=True)
    best = candidates[0]
    return (best[0], best[1], best[2], best[3])

# -------------------------
# MONITOR & ROLL (full with emergency timeout)
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
            # safety: force-exit if we've held the position too long
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
                    res = safe_api_call('symbol_ticker_fallback_'+symbol, lambda sym: client.get_symbol_ticker(symbol=sym), args=[symbol], weight=W_WEIGHT_TICKER, allow_fail=True)
                    price_now = float(res.get('price'))
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
# METRICS
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
# CLEANUP HELPERS
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
                if RATE_LIMIT_BACKOFF:
                    notify(f"⏸ Backing off due to prior rate-limit for {RATE_LIMIT_BACKOFF}s.")
                    time.sleep(RATE_LIMIT_BACKOFF)
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
                cache_set(f"open_orders::{symbol or 'all'}", None, 1)
            except Exception:
                pass

            try:
                closed, exit_price, profit_usd = monitor_and_roll(symbol, qty_remaining, entry_price, f)
            finally:
                ACTIVE_SYMBOL = None
                try:
                    cache_set(f"open_orders::{symbol or 'all'}", None, 1)
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

        time.sleep(30)

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