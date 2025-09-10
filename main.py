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
PRICE_MIN = 1.0
PRICE_MAX = 3.0
MIN_VOLUME = 5_000_000          # daily quote volume baseline

# require small recent move (we prefer coins that just started moving)
RECENT_PCT_MIN = 1.0
RECENT_PCT_MAX = 2.0            # require recent move between 1%..2%

# absolute 24h change guardrails (avoid extreme pump/dump)
MAX_24H_RISE_PCT = 5.0          # disallow > +5% 24h rise
MAX_24H_CHANGE_ABS = 5.0        # require abs(24h change) <= 5.0

MOVEMENT_MIN_PCT = 1.0

# runtime / pacing
TRADE_USD = 10.0
SLEEP_BETWEEN_CHECKS = 45       # increased to reduce polling weight
CYCLE_DELAY = 10
COOLDOWN_AFTER_EXIT = 10

# order / protection
TRIGGER_PROXIMITY = 0.035
STEP_INCREMENT_PCT = 0.02
BASE_TP_PCT = 2.0
BASE_SL_PCT = 1.5

# micro-take profit
MICRO_TP_PCT = 1.0
MICRO_TP_FRACTION = 0.30
MICRO_MAX_WAIT = 18.0

# rolling config
ROLL_ON_RISE_PCT = 1.0            # require stronger % rise
ROLL_TRIGGER_PCT = 0.75
ROLL_TRIGGER_DELTA_ABS = 0.012    # require larger absolute move for roll
ROLL_TP_STEP_ABS = 0.015
ROLL_SL_STEP_ABS = 0.004
ROLL_COOLDOWN_SECONDS = 60        # longer cooldown between rolls
MAX_ROLLS_PER_POSITION = 3
ROLL_POST_CANCEL_JITTER = (0.3, 0.8)

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
CACHE_TTL = 120

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
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                          data={"chat_id": CHAT_ID, "text": text}, timeout=8)
        except Exception:
            pass

def format_price(value, tick_size):
    try:
        tick = Decimal(str(tick_size))
        precision = max(0, -tick.as_tuple().exponent)
        return format(Decimal(str(value)).quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN), f'.{precision}f')
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
# SAFE API WRAPPER (centralized rate-limit handling)
# -------------------------
def safe_api_call(fn, *args, retries=3, backoff=0.25, **kwargs):
    """
    Call an exchange API function with retries and rate-limit handling.
    On detecting rate-limit (Too much request weight / -1003) sets RATE_LIMIT_BACKOFF.
    Returns the call result or None on persistent failure.
    """
    global RATE_LIMIT_BACKOFF
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except BinanceAPIException as e:
            err = str(e)
            last_err = err
            # rate-limit detection
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
                notify(f"❗ Exchange rate-limit detected: backing off for {RATE_LIMIT_BACKOFF}s. Err: {err}")
                time.sleep(RATE_LIMIT_BACKOFF)
                return None
            # common order errors (let caller handle)
            notify(f"⚠️ BinanceAPIException (attempt {attempt}/{retries}): {err}")
            time.sleep(backoff * attempt)
            continue
        except Exception as e:
            last_err = str(e)
            notify(f"⚠️ API call error (attempt {attempt}/{retries}): {e}")
            time.sleep(backoff * attempt)
            continue
    notify(f"❌ API call failed after {retries} attempts: {last_err}")
    return None

# -------------------------
# BALANCES / FILTERS (use safe_api_call)
# -------------------------
def get_free_usdt():
    try:
        res = safe_api_call(client.get_asset_balance, asset=QUOTE)
        if not res:
            return 0.0
        return float(res.get('free') or 0.0)
    except Exception:
        return 0.0

def get_free_asset(asset):
    try:
        res = safe_api_call(client.get_asset_balance, asset=asset)
        if not res:
            return 0.0
        return float(res.get('free') or 0.0)
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

# -------------------------
# TRANSFERS (safe)
# -------------------------
def send_profit_to_funding(amount, asset='USDT'):
    try:
        # try universal_transfer if available
        res = safe_api_call(client.universal_transfer,
                            type='MAIN_FUNDING' if hasattr(client, 'universal_transfer') else 'SPOT_TO_FUNDING',
                            asset=asset, amount=str(round(amount, 6)))
        if res:
            notify(f"💸 Profit ${amount:.6f} transferred to funding wallet.")
            return res
    except Exception:
        pass
    # fallback explicit
    try:
        res = safe_api_call(client.universal_transfer,
                            type='SPOT_TO_FUNDING', asset=asset, amount=str(round(amount, 6)))
        if res:
            notify(f"💸 Profit ${amount:.6f} transferred to funding wallet (fallback).")
            return res
    except Exception as e:
        notify(f"❌ Failed to transfer profit: {e}")
    return None

# -------------------------
# CACHES & UTIL
# -------------------------
TICKER_CACHE = None
LAST_FETCH = 0
SYMBOL_INFO_CACHE = {}
SYMBOL_INFO_TTL = 120
OPEN_ORDERS_CACHE = {'ts': 0, 'data': None}
OPEN_ORDERS_TTL = 45   # increased TTL to reduce cancel/get_open_orders frequency

def get_symbol_info_cached(symbol, ttl=SYMBOL_INFO_TTL):
    now = time.time()
    ent = SYMBOL_INFO_CACHE.get(symbol)
    if ent and now - ent[1] < ttl:
        return ent[0]
    info = safe_api_call(client.get_symbol_info, symbol)
    if info:
        SYMBOL_INFO_CACHE[symbol] = (info, now)
    return info

def get_open_orders_cached(symbol=None):
    now = time.time()
    if OPEN_ORDERS_CACHE['data'] is not None and now - OPEN_ORDERS_CACHE['ts'] < OPEN_ORDERS_TTL:
        data = OPEN_ORDERS_CACHE['data']
        if symbol:
            return [o for o in data if o.get('symbol') == symbol]
        return data
    # use lambda to avoid positional/keyword binding issues
    if symbol:
        data = safe_api_call(lambda: client.get_open_orders(symbol=symbol))
    else:
        data = safe_api_call(lambda: client.get_open_orders())
    if data is None:
        # return previous cached data if available (fail-open)
        return OPEN_ORDERS_CACHE['data'] or []
    OPEN_ORDERS_CACHE['data'] = data
    OPEN_ORDERS_CACHE['ts'] = now
    return data

def get_tickers_cached():
    global TICKER_CACHE, LAST_FETCH, RATE_LIMIT_BACKOFF
    now = time.time()
    if RATE_LIMIT_BACKOFF and now - LAST_FETCH < RATE_LIMIT_BACKOFF:
        return TICKER_CACHE or []
    if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
        res = safe_api_call(client.get_ticker)
        if res:
            TICKER_CACHE = res
            LAST_FETCH = now
            RATE_LIMIT_BACKOFF = 0
        else:
            notify("⚠️ get_tickers_cached: failed to refresh tickers (using cached).")
            return TICKER_CACHE or []
    return TICKER_CACHE

def get_price_cached(symbol):
    global TICKER_CACHE, LAST_FETCH, RATE_LIMIT_BACKOFF
    now = time.time()
    try:
        # refresh if stale
        if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
            res = safe_api_call(client.get_ticker)
            if res:
                TICKER_CACHE = res
                LAST_FETCH = now
                RATE_LIMIT_BACKOFF = 0
        if TICKER_CACHE:
            for t in TICKER_CACHE:
                if t.get('symbol') == symbol:
                    try:
                        return float(t.get('lastPrice') or t.get('price') or 0.0)
                    except Exception:
                        return None
    except Exception as e:
        notify(f"⚠️ get_price_cached general error: {e}")
        return None
    # fallback single-symbol
    res = safe_api_call(lambda: client.get_symbol_ticker(symbol=symbol))
    if not res:
        return None
    return float(res.get('price') or res.get('lastPrice') or 0.0)

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

def orderbook_bullish(symbol, depth=5, min_imbalance=1.05, max_spread_pct=1.5):
    # use lambda wrapper to avoid bound/unbound method positional issues
    ob = safe_api_call(lambda: client.get_order_book(symbol=symbol, limit=depth))
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
# PICKER (tweaked)
# -------------------------
def pick_coin():
    global RATE_LIMIT_BACKOFF

    # --- config (read global if present, else fallback) ---
    QUOTE_LOCAL = globals().get('QUOTE', 'USDT')
    PRICE_MIN = globals().get('PRICE_MIN', 0.5)
    PRICE_MAX = globals().get('PRICE_MAX', 3.0)

    MIN_24H_VOL = globals().get('MIN_24H_VOL', 5_000_000)
    MIN_24H_VOL_UPTO_3 = globals().get('MIN_24H_VOL_UPTO_3', 2_000_000)
    MIN_24H_VOL_LOW_PRICE = globals().get('MIN_24H_VOL_LOW_PRICE', 1_000_000)

    MAX_24H_CHANGE = globals().get('MAX_24H_CHANGE', 15.0)
    MIN_24H_CHANGE = globals().get('MIN_24H_CHANGE', 0.5)

    MIN_5M_PCT = globals().get('MIN_5M_PCT', 0.6)
    MIN_1M_PCT = globals().get('MIN_1M_PCT', 0.3)

    KLINES_5M_LIMIT = globals().get('KLINES_5M_LIMIT', 6)
    KLINES_1M_LIMIT = globals().get('KLINES_1M_LIMIT', 6)
    EMA_SHORT = globals().get('EMA_SHORT', 3)
    EMA_LONG = globals().get('EMA_LONG', 10)
    RSI_PERIOD = globals().get('RSI_PERIOD', 14)

    OB_DEPTH = globals().get('OB_DEPTH', 5)
    MIN_OB_IMBALANCE = globals().get('MIN_OB_IMBALANCE', 1.1)
    MAX_OB_SPREAD_PCT = globals().get('MAX_OB_SPREAD_PCT', 1.2)
    MIN_OB_LIQUIDITY = globals().get('MIN_OB_LIQUIDITY', 3000.0)

    TOP_BY_24H_VOLUME = globals().get('TOP_BY_24H_VOLUME', 200)
    REQUEST_SLEEP = globals().get('REQUEST_SLEEP', 0.12)

    WEIGHT_RECENT_PCT_5M = globals().get('WEIGHT_RECENT_PCT_5M', 4.0)
    WEIGHT_RECENT_PCT_1M = globals().get('WEIGHT_RECENT_PCT_1M', 2.0)
    WEIGHT_VOL_RATIO_5M = globals().get('WEIGHT_VOL_RATIO_5M', 3.0)
    WEIGHT_EMA = globals().get('WEIGHT_EMA', 5.0)
    WEIGHT_RSI = globals().get('WEIGHT_RSI', 1.5)
    WEIGHT_24H_CHANGE = globals().get('WEIGHT_24H_CHANGE', 1.0)
    WEIGHT_OB = globals().get('WEIGHT_OB', 2.0)
    PRICE_TARGET_BOOST = globals().get('PRICE_TARGET_BOOST', 6.0)

    # small helpers local to this function (safe, avoids depending on other defs)
    def meets_volume_for_price(last_price, quote_vol_24h):
        if last_price <= 1.0:
            return quote_vol_24h >= MIN_24H_VOL_LOW_PRICE
        elif last_price <= 3.0:
            return quote_vol_24h >= MIN_24H_VOL_UPTO_3
        else:
            return quote_vol_24h >= MIN_24H_VOL

    def pct_change(open_p, close_p):
        if open_p == 0:
            return 0.0
        return (close_p - open_p) / open_p * 100.0

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

    # fetch tickers (cached wrapper used by your script)
    tickers = get_tickers_cached() or []
    now = time.time()
    pre = []

    # Phase 1: cheap 24h filters & dynamic volume
    for t in tickers:
        sym = t.get('symbol')
        if not sym or not sym.endswith(QUOTE_LOCAL):
            continue
        try:
            last = float(t.get('lastPrice') or 0.0)
            qvol = float(t.get('quoteVolume') or 0.0)
            ch = float(t.get('priceChangePercent') or 0.0)
        except Exception:
            continue
        # price bounds
        if not (PRICE_MIN <= last <= PRICE_MAX):
            continue
        # dynamic volume baseline
        if not meets_volume_for_price(last, qvol):
            continue
        # 24h change guard
        if ch < MIN_24H_CHANGE or ch > MAX_24H_CHANGE:
            continue
        # skip temp-skip or recent buys
        skip_until = TEMP_SKIP.get(sym)
        if skip_until and now < skip_until:
            continue
        last_buy = RECENT_BUYS.get(sym)
        if last_buy:
            if now < last_buy['ts'] + last_buy.get('cooldown', REBUY_COOLDOWN):
                continue
        pre.append((sym, last, qvol, ch))

    if not pre:
        return None

    # sort & limit how many we'll deeply evaluate to limit API calls
    pre.sort(key=lambda x: x[2], reverse=True)
    candidates = pre[:TOP_BY_24H_VOLUME]

    scored = []
    for sym, last_price, qvol, change_24h in candidates:
        # small sleep to avoid heavy bursts
        time.sleep(REQUEST_SLEEP)

        # defensive rate-limit handling when fetching klines
        kl5 = safe_api_call(lambda: client.get_klines(symbol=sym, interval='5m', limit=KLINES_5M_LIMIT))
        if not kl5 or len(kl5) < 3:
            continue
        time.sleep(REQUEST_SLEEP)
        kl1 = safe_api_call(lambda: client.get_klines(symbol=sym, interval='1m', limit=KLINES_1M_LIMIT))
        if not kl1 or len(kl1) < 2:
            continue

        try:
            closes_5m = [float(k[4]) for k in kl5]
            vols_5m = []
            for k in kl5:
                if len(k) > 7 and k[7] is not None:
                    vols_5m.append(float(k[7]))
                else:
                    # fallback approximate quote volume
                    vols_5m.append(float(k[5]) * float(k[4]))
            closes_1m = [float(k[4]) for k in kl1]
            vols_1m = []
            for k in kl1:
                if len(k) > 7 and k[7] is not None:
                    vols_1m.append(float(k[7]))
                else:
                    vols_1m.append(float(k[5]) * float(k[4]))

            # pct moves
            open_5m = float(kl5[0][1])
            pct_5m = pct_change(open_5m, closes_5m[-1])
            open_1m = float(kl1[0][1])
            pct_1m = pct_change(open_1m, closes_1m[-1])

            # vol ratios (last vs avg previous)
            avg_prev_5m = (sum(vols_5m[:-1]) / max(len(vols_5m[:-1]), 1)) if len(vols_5m) > 1 else vols_5m[-1]
            vol_ratio_5m = vols_5m[-1] / (avg_prev_5m + 1e-12)

            avg_prev_1m = (sum(vols_1m[:-1]) / max(len(vols_1m[:-1]), 1)) if len(vols_1m) > 1 else vols_1m[-1]
            vol_ratio_1m = vols_1m[-1] / (avg_prev_1m + 1e-12)

            # EMA
            short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
            long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
            ema_ok = False
            ema_uplift = 0.0
            if short_ema and long_ema:
                ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
                ema_ok = short_ema > long_ema * 1.0005

            # RSI
            rsi_val = compute_rsi_local(closes_5m[-(RSI_PERIOD+1):], RSI_PERIOD) if len(closes_5m) >= RSI_PERIOD+1 else None
            rsi_ok = True
            if rsi_val is not None and rsi_val > 68:
                rsi_ok = False

            # orderbook (use lambda safe wrapper)
            time.sleep(REQUEST_SLEEP)
            ob = safe_api_call(lambda: client.get_order_book(symbol=sym, limit=OB_DEPTH))
            ob_ok = False
            ob_imbalance = 1.0
            ob_spread_pct = 100.0
            bid_quote_liq = 0.0
            if ob:
                bids = ob.get('bids') or []
                asks = ob.get('asks') or []
                if bids and asks:
                    top_bid = float(bids[0][0]); top_ask = float(asks[0][0])
                    bid_sum = sum(float(b[1]) for b in bids[:OB_DEPTH]) + 1e-12
                    ask_sum = sum(float(a[1]) for a in asks[:OB_DEPTH]) + 1e-12
                    ob_imbalance = bid_sum / ask_sum
                    ob_spread_pct = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
                    bid_quote_liq = sum(float(b[0]) * float(b[1]) for b in bids[:OB_DEPTH])
                    if bid_quote_liq >= MIN_OB_LIQUIDITY:
                        ob_ok = (ob_imbalance >= MIN_OB_IMBALANCE) and (ob_spread_pct <= MAX_OB_SPREAD_PCT)
                    else:
                        ob_ok = False

            # scoring
            score = 0.0
            score += max(0.0, pct_5m) * WEIGHT_RECENT_PCT_5M
            score += max(0.0, pct_1m) * WEIGHT_RECENT_PCT_1M
            score += max(0.0, (vol_ratio_5m - 1.0)) * WEIGHT_VOL_RATIO_5M * 100.0
            score += ema_uplift * WEIGHT_EMA * 100.0
            if rsi_val is not None:
                score += max(0.0, (60.0 - min(rsi_val, 60.0))) * WEIGHT_RSI * 0.2
            score += max(0.0, change_24h) * WEIGHT_24H_CHANGE * 0.5
            if ob_ok:
                score += WEIGHT_OB * 10.0
            # price cap boost
            if last_price <= PRICE_MAX:
                score += PRICE_TARGET_BOOST

            strong_candidate = (pct_5m >= MIN_5M_PCT and pct_1m >= MIN_1M_PCT and vol_ratio_5m >= 1.4
                                and ema_ok and rsi_ok and ob_ok)

            scored.append({
                "symbol": sym,
                "last_price": last_price,
                "24h_change": change_24h,
                "24h_vol": qvol,
                "pct_5m": pct_5m,
                "pct_1m": pct_1m,
                "vol_ratio_5m": vol_ratio_5m,
                "ema_ok": ema_ok,
                "ema_uplift": ema_uplift,
                "rsi": rsi_val,
                "ob_ok": ob_ok,
                "ob_imbalance": ob_imbalance,
                "ob_spread_pct": ob_spread_pct,
                "ob_bid_liq": bid_quote_liq,
                "score": score,
                "strong_candidate": strong_candidate
            })
        except Exception as e:
            notify(f"⚠️ pick_coin evaluate error {sym}: {e}")
            continue

    if not scored:
        return None

    scored.sort(key=lambda x: x['score'], reverse=True)

    # Prefer strong candidates if present
    strongs = [s for s in scored if s['strong_candidate']]
    if strongs:
        best = sorted(strongs, key=lambda x: x['score'], reverse=True)[0]
    else:
        best = scored[0]

    # return in expected tuple shape for trade_cycle
    return (best['symbol'], best['last_price'], best['24h_vol'], best['24h_change'])
    
# -------------------------
# MARKET BUY helpers
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
        res = safe_api_call(lambda: client.get_symbol_ticker(symbol=symbol))
        if not res:
            notify(f"⚠️ Failed to fetch ticker for {symbol}.")
            return None, None
        price = float(res.get('price') or res.get('lastPrice') or 0.0)

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

    order_resp = safe_api_call(lambda: client.order_market_buy(symbol=symbol, quantity=qty_str))
    if not order_resp:
        notify(f"❌ Market buy failed for {symbol} (no response).")
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
# Micro TP helper
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

        order = safe_api_call(lambda: client.order_limit_sell(symbol=symbol, quantity=qty_str, price=price_str))
        if not order:
            notify(f"⚠️ Failed to place micro TP order for {symbol}.")
            return None, 0.0, None
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
            status = safe_api_call(lambda: client.get_order(symbol=symbol, orderId=order_id))
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

# -------------------------
# SAFE SELL FALLBACK (market)
# -------------------------
def place_market_sell_fallback(symbol, qty, f):
    try:
        qty_str = format_qty(qty, f.get('stepSize', 0.0))
        notify(f"⚠️ Attempting MARKET sell fallback for {symbol}: qty={qty_str}")
        resp = safe_api_call(lambda: client.order_market_sell(symbol=symbol, quantity=qty_str))
        if resp:
            notify(f"✅ Market sell fallback executed for {symbol}")
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            return resp
        notify(f"❌ Market sell fallback returned no response for {symbol}")
        return None
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
        if qty * tp < min_notional - 1e-12:
            needed_qty = ceil_step(min_notional / tp, f['stepSize'])
            if needed_qty <= free_qty + 1e-12:
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

    # Attempt 1: standard OCO
    for attempt in range(1, retries + 1):
        oco = safe_api_call(lambda: client.create_oco_order(
            symbol=symbol, side='SELL', quantity=qty_str,
            price=tp_str, stopPrice=sp_str, stopLimitPrice=sl_str,
            stopLimitTimeInForce='GTC'
        ))
        if oco:
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
        # if oco None, safe_api_call already logged/backed-off; try alt below or next attempt

    # Attempt 2: alternative param names
    for attempt in range(1, retries + 1):
        oco2 = safe_api_call(lambda: client.create_oco_order(
            symbol=symbol, side='SELL', quantity=qty_str,
            aboveType="LIMIT_MAKER", abovePrice=tp_str,
            belowType="STOP_LOSS_LIMIT", belowStopPrice=sp_str,
            belowPrice=sl_str, belowTimeInForce="GTC"
        ))
        if oco2:
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 OCO SELL placed (alt params) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco_abovebelow', 'raw': oco2}

    # Fallback: try placing TP limit and STOP_LOSS_LIMIT, else market sell
    notify("⚠️ All OCO attempts failed — falling back to separate TP (limit) + SL (stop-limit) or MARKET.")

    tp_order = safe_api_call(lambda: client.order_limit_sell(symbol=symbol, quantity=qty_str, price=tp_str))
    if tp_order:
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        notify(f"📈 TP LIMIT placed (fallback): {tp_str}, qty={qty_str}")
    else:
        notify("⚠️ Fallback TP limit not placed (no response).")

    try:
        sl_order = safe_api_call(lambda: client.create_order(
            symbol=symbol, side="SELL", type="STOP_LOSS_LIMIT",
            stopPrice=sp_str, price=sl_str, timeInForce='GTC', quantity=qty_str
        ))
        if sl_order:
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📉 SL STOP_LOSS_LIMIT placed (fallback): trigger={sp_str}, limit={sl_str}, qty={qty_str}")
        else:
            notify("⚠️ Fallback SL stop-limit not placed (no response).")
    except Exception as e:
        notify(f"❌ Fallback SL stop-limit failed: {e}")

    if tp_order or (locals().get('sl_order') is not None and sl_order):
        return {'tp': tp, 'sl': sp, 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order}}

    # Final fallback: try market sell to avoid being stuck
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
                safe_api_call(lambda: client.cancel_order(symbol=symbol, orderId=o.get('orderId')))
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
# MONITOR & ROLL (improved debounce)
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
    last_roll_price = entry_price
    roll_count = 0
    last_price_seen = entry_price

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
                res = safe_api_call(lambda: client.get_symbol_ticker(symbol=symbol))
                if not res:
                    notify("⚠️ monitor_and_roll: failed to fetch price, continuing.")
                    continue
                price_now = float(res.get('price') or res.get('lastPrice') or 0.0)

            # direction detection
            price_moving_up = price_now > last_price_seen
            last_price_seen = price_now

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
            can_roll_time = (now_ts - last_roll_ts) >= ROLL_COOLDOWN_SECONDS

            # debounce: require price to be above last_roll_price by delta before next roll
            passed_price_debounce = price_now >= (last_roll_price + ROLL_TRIGGER_DELTA_ABS)

            should_roll = ((near_trigger and moved_enough) or rise_trigger_pct or rise_trigger_abs)
            if should_roll and passed_price_debounce and price_moving_up and can_roll_time and available_for_sell >= f.get('minQty', 0.0):
                if roll_count >= MAX_ROLLS_PER_POSITION:
                    notify(f"⚠️ Reached max rolls ({MAX_ROLLS_PER_POSITION}) for {symbol}, will not roll further.")
                    last_roll_ts = now_ts
                    continue

                notify(f"🔎 Roll triggered for {symbol}: price={price_now:.8f}, entry={entry_price:.8f}, curr_tp={curr_tp:.8f}, delta={price_delta:.6f} (near={near_trigger},pct={rise_trigger_pct},abs={rise_trigger_abs},debounce_ok={passed_price_debounce})")
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
                    last_roll_price = price_now
                    notify(f"🔁 Rolled OCO (abs-step): new TP={curr_tp:.8f}, new SL={curr_sl:.8f}, qty={sell_qty} (roll #{roll_count})")
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
# DAILY METRICS
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
                total_profit_usdt = 0.0
                date_key, m = _update_metrics_for_profit(total_profit_usdt)
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
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
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