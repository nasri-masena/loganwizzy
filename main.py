import os
import math
import time
import random
import threading
import requests
import re
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP, getcontext
from flask import Flask
from binance.client import Client
from binance.exceptions import BinanceAPIException

# -------------------------
# CONFIG (tuned for lower REST usage)
# -------------------------
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
QUOTE = "USDT"

# price / liquidity filters
PRICE_MIN = 0.9
PRICE_MAX = 3.0
MIN_VOLUME = 3_000_000  # daily quote volume baseline

# require small recent move (we prefer coins that just started moving)
RECENT_PCT_MIN = 1.0
RECENT_PCT_MAX = 2.0  # require recent move between 1%..2%

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

# klines / selection tuning (reduce heavy calls)
TOP_CANDIDATES = int(os.getenv("TOP_CANDIDATES", "40"))
KLINES_LIMIT = int(os.getenv("KLINES_LIMIT", "4"))
REQUEST_SLEEP = float(os.getenv("REQUEST_SLEEP", "0.25"))  # spacing between heavy calls

# -------------------------
# INIT / GLOBALS
# -------------------------

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
RATE_LIMIT_BACKOFF_MAX = 3600
RATE_LIMIT_BASE_SLEEP = 90
CACHE_TTL = 300

# safe client placeholder (constructed by make_client)
client = None

# -------------------------
# HELPERS: formatting & rounding
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

# -------------------------
# SAFE API / client init
# -------------------------

def make_client(api_key, api_secret):
    """Create client but catch IP ban at startup and set RATE_LIMIT_BACKOFF instead of crashing."""
    global RATE_LIMIT_BACKOFF
    try:
        c = Client(api_key, api_secret)
        return c
    except BinanceAPIException as e:
        err = str(e)
        m = re.search(r'IP banned until (\d+)', err)
        if m:
            try:
                until_ms = int(m.group(1))
                until_ts = until_ms / 1000.0
                wait = max(0, int(until_ts - time.time()))
                RATE_LIMIT_BACKOFF = min(wait if wait > 0 else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
            except Exception:
                RATE_LIMIT_BACKOFF = RATE_LIMIT_BACKOFF_MAX
            notify(f"❗ Binance IP ban detected at startup; backing off ~{RATE_LIMIT_BACKOFF}s (until {time.ctime(time.time()+RATE_LIMIT_BACKOFF)})")
            return None
        else:
            # If it's other error, rethrow so it's visible
            raise

def safe_api_call(fn, *args, retries=2, backoff=0.5, **kwargs):
    """Central wrapper to handle rate-limit / IP ban and exponential backoff."""
    global RATE_LIMIT_BACKOFF
    attempt = 0
    while attempt <= retries:
        # if currently in global backoff, avoid hitting the API
        if RATE_LIMIT_BACKOFF and time.time() < (LAST_FETCH + RATE_LIMIT_BACKOFF):
            # caller should handle; return None to use caches
            return None
        try:
            res = fn(*args, **kwargs)
            # small spacing to avoid bursts
            time.sleep(REQUEST_SLEEP)
            return res
        except BinanceAPIException as e:
            err = str(e)
            # IP ban explicit
            m = re.search(r'IP banned until (\d+)', err)
            if m:
                try:
                    until_ms = int(m.group(1))
                    until_ts = until_ms / 1000.0
                    wait = max(0, int(until_ts - time.time()))
                    RATE_LIMIT_BACKOFF = min(wait if wait > 0 else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                except Exception:
                    RATE_LIMIT_BACKOFF = RATE_LIMIT_BACKOFF_MAX
                notify(f"❗ Binance IP ban detected in safe_api_call; backing off ~{RATE_LIMIT_BACKOFF}s (until {time.ctime(time.time()+RATE_LIMIT_BACKOFF)})")
                time.sleep(RATE_LIMIT_BACKOFF)
                return None
            # rate-limit detection
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                notify(f"⚠️ Rate limit detected in safe_api_call: backing off {RATE_LIMIT_BACKOFF}s (err={err})")
                time.sleep(RATE_LIMIT_BACKOFF)
                attempt += 1
                continue
            # other API errors: retry a bit
            attempt += 1
            time.sleep(backoff * attempt)
            continue
        except Exception:
            attempt += 1
            time.sleep(backoff * attempt)
            continue
    return None

# small helpers that wrap client.* using safe_api_call
def safe_get_ticker():
    return safe_api_call(client.get_ticker) if client else None

def safe_get_klines(symbol, interval, limit):
    return safe_api_call(client.get_klines, symbol=symbol, interval=interval, limit=limit) if client else None

def safe_get_order_book(symbol, limit):
    return safe_api_call(client.get_order_book, symbol=symbol, limit=limit) if client else None

def safe_get_symbol_info(symbol):
    return safe_api_call(client.get_symbol_info, symbol) if client else None

def safe_get_open_orders(symbol=None):
    if not client:
        return None
    if symbol:
        return safe_api_call(client.get_open_orders, symbol=symbol)
    return safe_api_call(client.get_open_orders)

def safe_get_symbol_ticker(symbol):
    return safe_api_call(client.get_symbol_ticker, symbol=symbol) if client else None

def safe_order_market_buy(symbol, quantity):
    return safe_api_call(client.order_market_buy, symbol=symbol, quantity=quantity) if client else None

def safe_order_market_sell(symbol, quantity):
    return safe_api_call(client.order_market_sell, symbol=symbol, quantity=quantity) if client else None

def safe_order_limit_sell(symbol, quantity, price):
    return safe_api_call(client.order_limit_sell, symbol=symbol, quantity=quantity, price=price) if client else None

def safe_create_oco_order(**kwargs):
    return safe_api_call(client.create_oco_order, **kwargs) if client else None

def safe_create_order(**kwargs):
    return safe_api_call(client.create_order, **kwargs) if client else None

def safe_cancel_order(symbol, orderId):
    return safe_api_call(client.cancel_order, symbol=symbol, orderId=orderId) if client else None

def safe_get_order(symbol, orderId):
    return safe_api_call(client.get_order, symbol=symbol, orderId=orderId) if client else None

# -------------------------
# CACHES & UTIL (using safe_api_call)
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
    res = safe_get_symbol_info(symbol)
    if res:
        SYMBOL_INFO_CACHE[symbol] = (res, now)
        return res
    notify(f"⚠️ Failed to fetch symbol info for {symbol} (cached fallback).")
    return None

def get_open_orders_cached(symbol=None):
    now = time.time()
    if OPEN_ORDERS_CACHE['data'] is not None and now - OPEN_ORDERS_CACHE['ts'] < OPEN_ORDERS_TTL:
        data = OPEN_ORDERS_CACHE['data']
        if symbol:
            return [o for o in data if o.get('symbol') == symbol]
        return data

    res = safe_get_open_orders(symbol)  # may return None on backoff
    if res is None:
        # return whatever we have cached (or empty)
        return OPEN_ORDERS_CACHE['data'] or []
    OPEN_ORDERS_CACHE['data'] = res
    OPEN_ORDERS_CACHE['ts'] = now
    return res

def get_tickers_cached():
    global TICKER_CACHE, LAST_FETCH, RATE_LIMIT_BACKOFF
    now = time.time()
    if RATE_LIMIT_BACKOFF and now - LAST_FETCH < RATE_LIMIT_BACKOFF:
        return TICKER_CACHE or []
    if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
        res = safe_get_ticker()
        if res is None:
            notify("⚠️ get_tickers_cached: safe_get_ticker returned None (backoff or API issue).")
            return TICKER_CACHE or []
        TICKER_CACHE = res
        LAST_FETCH = now
    return TICKER_CACHE

def get_price_cached(symbol):
    global TICKER_CACHE, LAST_FETCH, RATE_LIMIT_BACKOFF
    now = time.time()
    if RATE_LIMIT_BACKOFF and TICKER_CACHE is not None and now - LAST_FETCH < RATE_LIMIT_BACKOFF:
        for t in TICKER_CACHE:
            if t.get('symbol') == symbol:
                try:
                    return float(t.get('lastPrice') or t.get('price') or 0.0)
                except Exception:
                    return None
        return None

    if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
        res = safe_get_ticker()
        if res:
            TICKER_CACHE = res
            LAST_FETCH = now
        else:
            notify("⚠️ get_price_cached: failed to refresh tickers (using fallback).")

    if TICKER_CACHE:
        for t in TICKER_CACHE:
            if t.get('symbol') == symbol:
                try:
                    return float(t.get('lastPrice') or t.get('price') or 0.0)
                except Exception:
                    return None

    # last resort (single-symbol endpoint)
    res = safe_get_symbol_ticker(symbol)
    if res:
        try:
            return float(res.get('price') or res.get('lastPrice') or 0.0)
        except Exception:
            return None
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
    ob = safe_get_order_book(symbol, depth)
    if not ob:
        return False
    bids = ob.get('bids') or []
    asks = ob.get('asks') or []
    if not bids or not asks:
        return False
    try:
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
# PICKER (tuned & uses safe calls)
# -------------------------

def pick_coin():
    global RATE_LIMIT_BACKOFF
    cleanup_temp_skip()
    cleanup_recent_buys()
    now = time.time()

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

        if qvol < MIN_QVOL:
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

        # directional movement requirement
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
            klines = safe_get_klines(sym, '5m', KLINES_LIMIT)
            if not klines or len(klines) < 3:
                continue

            vols = []
            closes = []
            for k in klines:
                if len(k) > 7 and k[7] is not None:
                    vols.append(float(k[7]))
                else:
                    vols.append(float(k[5]) * float(k[4]) if float(k[4]) != 0 else 0.0)
                closes.append(float(k[4]) if k[4] is not None else 0.0)

            recent_vol = vols[-1] if vols else 0.0
            if recent_vol <= 0:
                continue

            prev_slice = vols[-(KLINES_LIMIT-1):-1] if len(vols) >= 2 else vols[:-1]
            avg_vol = (sum(prev_slice) / max(len(prev_slice), 1)) if prev_slice else recent_vol
            vol_ratio = recent_vol / (avg_vol + 1e-9)

            recent_pct = 0.0
            if len(closes) >= 4 and closes[-4] > 0:
                recent_pct = (closes[-1] - closes[-4]) / (closes[-4] + 1e-12) * 100.0

            # enforce target recent band
            if recent_pct < RECENT_PCT_MIN or recent_pct > RECENT_PCT_MAX:
                continue

            # volume spike requirement
            if vol_ratio < 1.4:
                continue

            # simple momentum: at least one up
            last3 = closes[-3:]
            ups = 0
            if len(last3) >= 2 and last3[1] > last3[0]:
                ups += 1
            if len(last3) >= 3 and last3[2] > last3[1]:
                ups += 1
            if ups < 1:
                continue

            # EMA confirmation (light)
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
            short_ema = None
            long_ema = None
            if len(closes) >= long_period:
                short_ema = ema_local(closes[-short_period:], short_period)
                long_ema = ema_local(closes[-long_period:], long_period)
            elif len(closes) >= short_period:
                short_ema = ema_local(closes[-short_period:], short_period)
                long_ema = ema_local(closes, max(len(closes), 1))

            if short_ema is None or long_ema is None:
                continue
            if not (short_ema > long_ema * 1.0005):
                continue

            # lightweight orderbook check
            ob = safe_get_order_book(sym, limit=3)
            if not ob:
                continue
            bids = ob.get('bids') or []
            asks = ob.get('asks') or []
            if not bids or not asks:
                continue
            top_bid = float(bids[0][0]); top_ask = float(asks[0][0])
            spread_fast = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
            if spread_fast > 2.5:
                continue

            ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
            score = (recent_pct * 12.0) + (math.log1p(qvol) * 0.35) + (vol_ratio * 6.0) + (ema_uplift * 60.0)

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
# MARKET BUY helpers (use safe calls)
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
            if not orderbook_bullish(symbol, depth=5, min_imbalance=1.1, max_spread_pct=0.6):
                notify(f"⚠️ Orderbook not bullish for {symbol}; aborting market buy.")
                return None, None
        except Exception as e:
            notify(f"⚠️ Orderbook check error for {symbol}: {e}")

    price = get_price_cached(symbol)
    if price is None:
        res = safe_get_symbol_ticker(symbol)
        if res:
            price = float(res.get('price') or res.get('lastPrice') or 0.0)
        else:
            notify(f"⚠️ Failed to fetch ticker for {symbol}.")
            RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                    RATE_LIMIT_BACKOFF_MAX)
            TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
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

    time.sleep(random.uniform(0.05, 0.25))

    order_resp = safe_order_market_buy(symbol, qty_str)
    if order_resp is None:
        notify(f"❌ Market buy failed/blocked for {symbol} (safe_order_network).")
        RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
        TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
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
    return executed_qty, avg_price

# -------------------------
# Micro TP, OCO, sell helpers (use safe calls)
# -------------------------
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

        order = safe_order_limit_sell(symbol, qty_str, price_str)
        if not order:
            notify(f"⚠️ Micro TP placement failed for {symbol} (rate-limit or API).")
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
            status = safe_get_order(symbol, order_id)
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
        resp = safe_order_market_sell(symbol, qty_str)
        if not resp:
            notify(f"❌ Market sell fallback failed (rate-limit or API) for {symbol}")
            return None
        notify(f"✅ Market sell fallback executed for {symbol}")
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        return resp
    except Exception as e:
        notify(f"❌ Market sell fallback failed for {symbol}: {e}")
        return None

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
                notify(f"ℹ️ Increasing qty from {qty} to {needed_qty} to meet minNotional (qty*tp >= {min_notional}).")
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
                    notify(f"⚠️ Cannot meet minNotional for OCO on {symbol} (qty*tp={qty*tp:.8f} < {min_notional}). Will attempt fallback flow.")

    qty_str = format_qty(qty, f['stepSize'])
    tp_str = format_price(tp, f['tickSize'])
    sp_str = format_price(sp, f['tickSize'])
    sl_str = format_price(sl, f['tickSize'])

    # attempt standard OCO via safe wrapper
    for attempt in range(1, retries + 1):
        params = dict(symbol=symbol, side='SELL', quantity=qty_str, price=tp_str, stopPrice=sp_str, stopLimitPrice=sl_str, stopLimitTimeInForce='GTC')
        oco = safe_create_oco_order(**params)
        if oco:
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
        else:
            # detect if backoff set
            if RATE_LIMIT_BACKOFF:
                notify("❗ Rate-limit during OCO creation, backing off.")
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                return None
            time.sleep(delay)

    # attempt alt schema
    for attempt in range(1, retries + 1):
        params = dict(symbol=symbol, side='SELL', quantity=qty_str, aboveType="LIMIT_MAKER", abovePrice=tp_str,
                      belowType="STOP_LOSS_LIMIT", belowStopPrice=sp_str, belowPrice=sl_str, belowTimeInForce="GTC")
        oco2 = safe_create_oco_order(**params)
        if oco2:
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 OCO SELL placed (alt params) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco_abovebelow', 'raw': oco2}
        else:
            if RATE_LIMIT_BACKOFF:
                notify("❗ Rate-limit during OCO alt creation, backing off.")
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                return None
            time.sleep(delay)

    # fallback separate orders
    notify("⚠️ All OCO attempts failed — falling back to separate TP (limit) + SL (stop-limit) or MARKET.")
    tp_order = safe_order_limit_sell(symbol, qty_str, tp_str)
    if tp_order:
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        notify(f"📈 TP LIMIT placed (fallback): {tp_str}, qty={qty_str}")
    else:
        notify(f"❌ Fallback TP limit failed for {symbol} (rate-limit or error).")

    try:
        sl_order = safe_create_order(symbol=symbol, side="SELL", type="STOP_LOSS_LIMIT", stopPrice=sp_str, price=sl_str, timeInForce='GTC', quantity=qty_str)
        if sl_order:
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📉 SL STOP_LOSS_LIMIT placed (fallback): trigger={sp_str}, limit={sl_str}, qty={qty_str}")
    except Exception as e:
        notify(f"❌ Fallback SL stop-limit failed: {e}")

    if (tp_order or (locals().get('sl_order') is not None and locals().get('sl_order'))):
        return {'tp': tp, 'sl': sp, 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order if 'sl_order' in locals() else None}}

    # final fallback to market sell
    fallback_market = place_market_sell_fallback(symbol, qty, f)
    if fallback_market:
        return {'tp': tp, 'sl': sp, 'method': 'market_fallback', 'raw': fallback_market}

    notify("❌ All attempts to protect position failed (no TP/SL placed). TEMP skipping symbol.")
    TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
    return None

# -------------------------
# CANCEL HELPERS
# -------------------------

def cancel_all_open_orders(symbol, max_cancel=6, inter_delay=0.25):
    try:
        open_orders = get_open_orders_cached(symbol)
        cancelled = 0
        for o in open_orders:
            if cancelled >= max_cancel:
                notify(f"⚠️ Reached max_cancel ({max_cancel}) for {symbol}; leaving remaining orders.")
                break
            try:
                safe_cancel_order(symbol, o.get('orderId'))
                cancelled += 1
                time.sleep(inter_delay)
            except Exception as e:
                notify(f"⚠️ Cancel failed for {symbol} order {o.get('orderId')}: {e}")
        if cancelled:
            notify(f"❌ Cancelled {cancelled} open orders for {symbol}")
            OPEN_ORDERS_CACHE['data'] = None
    except Exception as e:
        notify(f"⚠️ Failed to cancel orders: {e}")

# -------------------------
# MONITOR & ROLL (with safety & MAX_HOLD_SECONDS)
# -------------------------

MAX_HOLD_SECONDS = 6 * 3600  # 6 hours max hold as requested

def monitor_and_roll(symbol, qty, entry_price, f):
    start_ts = time.time()
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
                # fallback single-symbol fetch
                res = safe_get_symbol_ticker(symbol)
                if res:
                    price_now = float(res.get('price') or res.get('lastPrice') or 0.0)
                else:
                    notify(f"⚠️ Failed to fetch price in monitor for {symbol}.")
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

                oco2 = place_oco_sell(symbol, sell_qty, entry_price, explicit_tp=new_tp, explicit_sl=new_sl)
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
# MAIN TRADE CYCLE
# -------------------------

ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0
BUY_LOCK_SECONDS = 60

def trade_cycle():
    global start_balance_usdt, ACTIVE_SYMBOL, LAST_BUY_TS, RATE_LIMIT_BACKOFF, client, LAST_FETCH

    client = make_client(API_KEY, API_SECRET)
    if client is None:
        # initial IP-ban handled by make_client; wait and loop
        notify("❗ Client not available at startup (IP ban or API error). Entering backoff loop.")
    else:
        notify("✅ Binance client initialized.")

    # initial snapshot
    try:
        start_balance_usdt = get_free_usdt()
        notify(f"🔰 Start balance snapshot: ${start_balance_usdt:.6f}")
    except Exception:
        start_balance_usdt = None

    while True:
        try:
            # If global backoff active, sleep long and attempt to re-init client after backoff
            if RATE_LIMIT_BACKOFF and time.time() < (LAST_FETCH + RATE_LIMIT_BACKOFF):
                notify(f"⏸ Emergency backoff active for {RATE_LIMIT_BACKOFF}s - pausing trade loop.")
                time.sleep(max(60, RATE_LIMIT_BACKOFF))
                continue

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
                    time.sleep(max(60, RATE_LIMIT_BACKOFF))
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
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            try:
                closed, exit_price, profit_usd = monitor_and_roll(symbol, qty_remaining, entry_price, f)
            finally:
                ACTIVE_SYMBOL = None
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass

            total_profit_usdt = profit_usd or 0.0
            if micro_order and micro_sold_qty and micro_tp_price:
                try:
                    micro_profit = (micro_tp_price - entry_price) * micro_sold_qty
                    total_profit_usdt += micro_profit
                except Exception:
                    pass

            now2 = time.time()
            ent = RECENT_BUYS.get(symbol, {})
            ent['ts'] = now2
            ent['price'] = entry_price
            ent['profit'] = total_profit_usdt
            if ent['profit'] is None:
                ent['cooldown'] = REBUY_COOLDOWN
            elif ent['profit'] < 0:
                ent['cooldown'] = LOSS_COOLDOWN
            else:
                ent['cooldown'] = REBUY_COOLDOWN
            RECENT_BUYS[symbol] = ent

            date_key, m = _update_metrics_for_profit(total_profit_usdt)
            _notify_daily_stats(date_key)

            if closed and total_profit_usdt and total_profit_usdt > 0:
                send_profit_to_funding(total_profit_usdt)

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
    bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    bot_thread.start()
    start_flask()