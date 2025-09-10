import os
import math
import time
import random
import threading
import requests
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from flask import Flask, Response
from binance.client import Client
from binance.exceptions import BinanceAPIException

# -------------------------
# CONFIG (tweak these / env)
# -------------------------
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
QUOTE = os.getenv("QUOTE", "USDT")

# price / liquidity filters
PRICE_MIN = float(os.getenv("PRICE_MIN", "1.0"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "3.0"))
MIN_VOLUME = float(os.getenv("MIN_VOLUME", "5000000"))

# require small recent move
RECENT_PCT_MIN = float(os.getenv("RECENT_PCT_MIN", "1.0"))
RECENT_PCT_MAX = float(os.getenv("RECENT_PCT_MAX", "2.0"))

# 24h guardrails
MAX_24H_RISE_PCT = float(os.getenv("MAX_24H_RISE_PCT", "5.0"))
MAX_24H_CHANGE_ABS = float(os.getenv("MAX_24H_CHANGE_ABS", "5.0"))
MOVEMENT_MIN_PCT = float(os.getenv("MOVEMENT_MIN_PCT", "1.0"))

# runtime / pacing
TRADE_USD = float(os.getenv("TRADE_USD", "7.0"))
SLEEP_BETWEEN_CHECKS = float(os.getenv("SLEEP_BETWEEN_CHECKS", "30"))
CYCLE_DELAY = float(os.getenv("CYCLE_DELAY", "10"))
COOLDOWN_AFTER_EXIT = float(os.getenv("COOLDOWN_AFTER_EXIT", "10"))

# order / protection
TRIGGER_PROXIMITY = float(os.getenv("TRIGGER_PROXIMITY", "0.06"))
STEP_INCREMENT_PCT = float(os.getenv("STEP_INCREMENT_PCT", "0.02"))
BASE_TP_PCT = float(os.getenv("BASE_TPCT", "2.0"))
BASE_SL_PCT = float(os.getenv("BASE_SL_PCT", "2.0"))

# micro-take profit
MICRO_TP_PCT = float(os.getenv("MICRO_TPCT", "1.0"))
MICRO_TP_FRACTION = float(os.getenv("MICRO_TP_FRACTION", "0.40"))
MICRO_MAX_WAIT = float(os.getenv("MICRO_MAX_WAIT", "18.0"))

# rolling config
ROLL_ON_RISE_PCT = float(os.getenv("ROLL_ON_RISE_PCT", "0.5"))
ROLL_TRIGGER_PCT = float(os.getenv("ROLL_TRIGGER_PCT", "0.75"))
ROLL_TRIGGER_DELTA_ABS = float(os.getenv("ROLL_TRIGGER_DELTA_ABS", "0.007"))
ROLL_TP_STEP_ABS = float(os.getenv("ROLL_TP_STEP_ABS", "0.015"))
ROLL_SL_STEP_ABS = float(os.getenv("ROLL_SL_STEP_ABS", "0.004"))
ROLL_COOLDOWN_SECONDS = float(os.getenv("ROLL_COOLDOWN_SECONDS", "90"))
MAX_ROLLS_PER_POSITION = int(os.getenv("MAX_ROLLS_PER_POSITION", "3"))
ROLL_POST_CANCEL_JITTER = (0.3, 0.8)

MIN_ROLL_PROFIT_USD = float(os.getenv("MIN_ROLL_PROFIT_USD", "0.35"))
ESTIMATED_FEE_RATE = float(os.getenv("ESTIMATED_FEE_RATE", "0.001"))

# REBUY / RECENT BUYS CONFIG
REBUY_COOLDOWN = float(os.getenv("REBUY_COOLDOWN", str(60*60)))
LOSS_COOLDOWN = float(os.getenv("LOSS_COOLDOWN", str(60*60*4)))
REBUY_MAX_RISE_PCT = float(os.getenv("REBUY_MAX_RISE_PCT", "5.0"))

# polling, caches
CACHE_TTL = int(os.getenv("CACHE_TTL", "120"))
OPEN_ORDERS_TTL = int(os.getenv("OPEN_ORDERS_TTL", "15"))

# tuning constants used by picker
KLINES_5M_LIMIT = int(os.getenv("KLINES_5M_LIMIT", "6"))
KLINES_1M_LIMIT = int(os.getenv("KLINES_1M_LIMIT", "6"))
EMA_SHORT = int(os.getenv("EMA_SHORT", "3"))
EMA_LONG = int(os.getenv("EMA_LONG", "10"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
OB_DEPTH = int(os.getenv("OB_DEPTH", "5"))
MIN_OB_IMBALANCE = float(os.getenv("MIN_OB_IMBALANCE", "1.1"))
MAX_OB_SPREAD_PCT = float(os.getenv("MAX_OB_SPREAD_PCT", "1.5"))
MIN_OB_LIQUIDITY = float(os.getenv("MIN_OB_LIQUIDITY", "3000.0"))
TOP_BY_24H_VOLUME = int(os.getenv("TOP_BY_24H_VOLUME", "200"))
REQUEST_SLEEP = float(os.getenv("REQUEST_SLEEP", "0.12"))
MIN_5M_PCT = float(os.getenv("MIN_5M_PCT", "0.6"))
MIN_1M_PCT = float(os.getenv("MIN_1M_PCT", "0.3"))

DEDUPE_STORE = os.getenv("DEDUPE_STORE", "order_dedupe.json")
LOG_FILE = os.getenv("LOG_FILE", "cryptobot.log")

getcontext().prec = 28

# -------------------------
# notify with collapse/dedupe (reduce Telegram spam)
# -------------------------
NOTIFY_CACHE = {}
NOTIFY_COLLAPSE_WINDOW = float(os.getenv("NOTIFY_COLLAPSE_WINDOW", "60"))
NOTIFY_MIN_INTERVAL = float(os.getenv("NOTIFY_MIN_INTERVAL", "1.2"))
_last_notify_ts = 0.0

def _send_notify_now(text: str):
    global _last_notify_ts
    now_ts = time.time()
    _last_notify_ts = now_ts
    payload = f"[{datetime.utcnow().isoformat()}] {text}"
    print(payload)
    if BOT_TOKEN and CHAT_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                          data={"chat_id": CHAT_ID, "text": payload}, timeout=6)
        except Exception as e:
            print(f"telegram notify failed: {e}")

def _flush_notify_cache(force=False):
    now = time.time()
    for msg, meta in list(NOTIFY_CACHE.items()):
        count = meta.get('count', 0)
        first = meta.get('first_ts', now)
        if force or (now - first >= NOTIFY_COLLAPSE_WINDOW):
            if count > 1:
                _send_notify_now(f"(x{count}) [suppressed {int(now-first)}s] {msg}")
            else:
                _send_notify_now(msg)
            del NOTIFY_CACHE[msg]

def notify(msg: str):
    try:
        now = time.time()
        meta = NOTIFY_CACHE.get(msg)
        if meta:
            meta['count'] = meta.get('count', 1) + 1
            meta['last_ts'] = now
            NOTIFY_CACHE[msg] = meta
            return
        _flush_notify_cache()
        NOTIFY_CACHE[msg] = {'count': 1, 'first_ts': now, 'last_ts': now}
        if now - _last_notify_ts >= NOTIFY_MIN_INTERVAL:
            _send_notify_now(msg)
    except Exception as e:
        print(f"notify failed: {e}")

# -------------------------
# init client & globals
# -------------------------
if not API_KEY or not API_SECRET:
    notify("❌ API_KEY / API_SECRET must be set in environment. Aborting.")
    raise SystemExit(1)

client = Client(API_KEY, API_SECRET)

TEMP_SKIP = {}  # symbol -> timestamp until skip
RECENT_BUYS = {}
RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_UNTIL = 0

TICKER_CACHE = None
LAST_FETCH = 0
SYMBOL_INFO_CACHE = {}

OPEN_ORDERS_CACHE = {'ts': 0, 'data': None}
METRICS = {}

# -------------------------
# helpers: rounding/formatting & filters
# -------------------------
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
# balances / price / caches
# -------------------------
def get_free_usdt():
    try:
        bal = client.get_asset_balance(asset=QUOTE)
        return float(bal.get('free') or 0.0)
    except Exception:
        return 0.0

def get_free_asset(asset):
    try:
        bal = client.get_asset_balance(asset=asset)
        return float(bal.get('free') or 0.0)
    except Exception:
        return 0.0

def set_rate_limit_backoff(seconds):
    global RATE_LIMIT_BACKOFF, RATE_LIMIT_UNTIL
    RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else seconds, max(seconds, RATE_LIMIT_BACKOFF))
    if RATE_LIMIT_BACKOFF == 0:
        RATE_LIMIT_BACKOFF = seconds
    RATE_LIMIT_UNTIL = time.time() + RATE_LIMIT_BACKOFF
    notify(f"⚠️ Rate limit triggered — backing off for {RATE_LIMIT_BACKOFF}s (until {time.ctime(RATE_LIMIT_UNTIL)})")

def get_symbol_info_cached(symbol, ttl=120):
    now = time.time()
    ent = SYMBOL_INFO_CACHE.get(symbol)
    if ent and now - ent[1] < ttl:
        return ent[0]
    try:
        info = client.get_symbol_info(symbol=symbol)
        SYMBOL_INFO_CACHE[symbol] = (info, now)
        return info
    except Exception as e:
        notify(f"⚠️ Failed to fetch symbol info for {symbol}: {e}")
        return None

def get_open_orders_cached(symbol=None):
    now = time.time()
    if OPEN_ORDERS_CACHE['data'] is not None and now - OPEN_ORDERS_CACHE['ts'] < OPEN_ORDERS_TTL:
        data = OPEN_ORDERS_CACHE['data']
        if symbol:
            return [o for o in data if o.get('symbol') == symbol]
        return data
    try:
        if symbol:
            data = client.get_open_orders(symbol=symbol)
        else:
            data = client.get_open_orders()
        OPEN_ORDERS_CACHE['data'] = data
        OPEN_ORDERS_CACHE['ts'] = now
        return data
    except Exception as e:
        notify(f"⚠️ Failed to fetch open orders: {e}")
        return OPEN_ORDERS_CACHE['data'] or []

def get_tickers_cached():
    global TICKER_CACHE, LAST_FETCH, RATE_LIMIT_UNTIL
    now = time.time()
    if RATE_LIMIT_UNTIL and now < RATE_LIMIT_UNTIL:
        return TICKER_CACHE or []
    if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
        try:
            TICKER_CACHE = client.get_ticker()
            LAST_FETCH = now
        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                set_rate_limit_backoff(90)
                return TICKER_CACHE or []
            notify(f"⚠️ Failed to refresh tickers: {e}")
            return TICKER_CACHE or []
    return TICKER_CACHE

def get_price_cached(symbol):
    global TICKER_CACHE, LAST_FETCH
    if RATE_LIMIT_UNTIL and time.time() < RATE_LIMIT_UNTIL:
        if TICKER_CACHE:
            for t in TICKER_CACHE:
                if t.get('symbol') == symbol:
                    try:
                        return float(t.get('lastPrice') or t.get('price') or 0.0)
                    except Exception:
                        return None
        return None
    now = time.time()
    if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
        try:
            TICKER_CACHE = client.get_ticker()
            LAST_FETCH = now
        except Exception as e:
            notify(f"⚠️ Failed to refresh tickers in get_price_cached: {e}")
    if TICKER_CACHE:
        for t in TICKER_CACHE:
            if t.get('symbol') == symbol:
                try:
                    return float(t.get('lastPrice') or t.get('price') or 0.0)
                except Exception:
                    return None
    try:
        res = client.get_symbol_ticker(symbol=symbol)
        return float(res.get('price') or res.get('lastPrice') or 0.0)
    except Exception as e:
        err = str(e)
        if '-1003' in err or 'Too much request weight' in err:
            set_rate_limit_backoff(90)
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

def orderbook_bullish(symbol, depth=5, min_imbalance=1.05, max_spread_pct=1.5):
    try:
        ob = client.get_order_book(symbol=symbol, limit=depth)
        bids = ob.get('bids') or []
        asks = ob.get('asks') or []
        if not bids or not asks:
            return False
        top_bid = float(bids[0][0]); top_ask = float(asks[0][0])
        spread_pct = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
        bid_sum = sum(float(b[1]) for b in bids[:depth]) + 1e-12
        ask_sum = sum(float(a[1]) for a in asks[:depth]) + 1e-12
        imbalance = bid_sum / ask_sum
        return (imbalance >= min_imbalance) and (spread_pct <= max_spread_pct)
    except Exception:
        return False

# -------------------------
# safe_api_call wrapper (handles BinanceAPIException & rate-limits)
# -------------------------
def safe_api_call(fn, *args, retries=3, backoff=0.25, **kwargs):
    global RATE_LIMIT_UNTIL
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs) if not args else fn(*args, **kwargs)
        except BinanceAPIException as e:
            err = str(e)
            last_err = err
            # malformed params -> no retry
            if 'Mandatory parameter' in err or 'was not sent' in err or 'was empty/null' in err or 'aboveType' in err:
                notify(f"⚠️ Binance malformed params (no-retry): {err}")
                return None
            # rate limit
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                set_rate_limit_backoff(90)
                notify(f"❗ Rate-limit detected: backing off until {time.ctime(RATE_LIMIT_UNTIL)} ({err})")
                return None
            # insufficient balance / no retry
            if 'insufficient balance' in err or 'Account has insufficient balance' in err or 'INSUFFICIENT' in err:
                notify(f"⚠️ BinanceAPIException (no-retry balance): {err}")
                return None
            notify(f"⚠️ BinanceAPIException (attempt {attempt}/{retries}): {err}")
            time.sleep(backoff * attempt)
            continue
        except Exception as e:
            last_err = str(e)
            if '-1003' in last_err or 'Too much request weight' in last_err:
                set_rate_limit_backoff(90)
                notify(f"❗ Rate-limit detected in generic exception: backing off until {time.ctime(RATE_LIMIT_UNTIL)}")
                return None
            notify(f"⚠️ API error (attempt {attempt}/{retries}): {e}")
            time.sleep(backoff * attempt)
            continue
    notify(f"❌ API failed after {retries} attempts: {last_err}")
    return None

# -------------------------
# PICKER
# -------------------------
def pick_coin():
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
        if ch < 0.5 or ch > 15.0:
            continue
        skip_until = TEMP_SKIP.get(sym)
        if skip_until and now < skip_until:
            continue
        last_buy = RECENT_BUYS.get(sym)
        if last_buy:
            if now < last_buy['ts'] + BUY_LOCK_SECONDS:
                continue
        pre.append((sym, last, qvol, ch))

    if not pre:
        return None

    pre.sort(key=lambda x: x[2], reverse=True)
    candidates = pre[:TOP_BY_24H_VOLUME]

    scored = []
    for sym, last_price, qvol, change_24h in candidates:
        time.sleep(REQUEST_SLEEP)
        kl5 = safe_api_call(client.get_klines, symbol=sym, interval='5m', limit=KLINES_5M_LIMIT)
        if not kl5 or len(kl5) < 3:
            continue
        time.sleep(REQUEST_SLEEP)
        kl1 = safe_api_call(client.get_klines, symbol=sym, interval='1m', limit=KLINES_1M_LIMIT)
        if not kl1 or len(kl1) < 2:
            continue
        try:
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

            short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
            long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
            ema_ok = False
            ema_uplift = 0.0
            if short_ema and long_ema:
                ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
                ema_ok = short_ema > long_ema * 1.0005

            rsi_val = compute_rsi_local(closes_5m[-(RSI_PERIOD+1):], RSI_PERIOD) if len(closes_5m) >= RSI_PERIOD+1 else None
            rsi_ok = True
            if rsi_val is not None and rsi_val > 68:
                rsi_ok = False

            time.sleep(REQUEST_SLEEP)
            ob = safe_api_call(client.get_order_book, symbol=sym, limit=OB_DEPTH)
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

            score = 0.0
            score += max(0.0, pct_5m) * 4.0
            score += max(0.0, pct_1m) * 2.0
            score += max(0.0, (vol_ratio_5m - 1.0)) * 3.0 * 100.0
            score += ema_uplift * 5.0 * 100.0
            if rsi_val is not None:
                score += max(0.0, (60.0 - min(rsi_val, 60.0))) * 1.5 * 0.2
            score += max(0.0, change_24h) * 1.0 * 0.5
            if ob_ok:
                score += 2.0 * 10.0
            if last_price <= PRICE_MAX:
                score += 6.0

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
    strongs = [s for s in scored if s['strong_candidate']]
    if strongs:
        best = sorted(strongs, key=lambda x: x['score'], reverse=True)[0]
    else:
        best = scored[0]
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
        ex = order_resp.get('executedQty')
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
    global TEMP_SKIP, RATE_LIMIT_UNTIL
    now = time.time()
    skip_until = TEMP_SKIP.get(symbol)
    if skip_until and now < skip_until:
        notify(f"⏭️ Skipping {symbol} until {time.ctime(skip_until)} (recent failure).")
        return None, None

    if not is_symbol_tradable(symbol):
        notify(f"⛔ Symbol {symbol} not tradable / market closed. Skipping and blacklisting.")
        TEMP_SKIP[symbol] = time.time() + 60 * 60
        return None, None

    info = get_symbol_info_cached(symbol)
    if not info:
        notify(f"❌ place_safe_market_buy: couldn't fetch symbol info for {symbol}")
        TEMP_SKIP[symbol] = time.time() + 60 * 60
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
                set_rate_limit_backoff(90)
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or 90)
                notify(f"❗ Rate-limit fetching price. Backing off.")
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

    try:
        order_resp = client.order_market_buy(symbol=symbol, quantity=qty_str)
    except Exception as e:
        err = str(e)
        notify(f"❌ Market buy failed for {symbol}: {err}")
        if '-1013' in err or 'Market is closed' in err or 'MIN_NOTIONAL' in err or 'minNotional' in err:
            TEMP_SKIP[symbol] = time.time() + 60 * 60
            notify(f"⏸ TEMP skipping {symbol} due to order error.")
        if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
            set_rate_limit_backoff(90)
            TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or 90)
            notify(f"❗ Rate-limit while placing market buy: backing off.")
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
        TEMP_SKIP[symbol] = time.time() + 60 * 60
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

        try:
            order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=price_str)
            notify(f"📍 Micro TP placed for {symbol}: sell {qty_str} @ {price_str}")
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            order_id = None
            if isinstance(order, dict):
                order_id = order.get('orderId')
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
                            # attempt transfer
                            client.universal_transfer(type='SPOT_TO_FUNDING', asset=QUOTE, amount=str(round(profit_to_send,6)))
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

# -------------------------
# SAFE SELL FALLBACK (market)
# -------------------------
def place_market_sell_fallback(symbol, qty, f):
    try:
        qty_str = format_qty(qty, f.get('stepSize', 0.0))
        notify(f"⚠️ Attempting MARKET sell fallback for {symbol}: qty={qty_str}")
        resp = client.order_market_sell(symbol=symbol, quantity=qty_str)
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
# OCO SELL + fallbacks
# -------------------------
def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=1.0, explicit_tp: float = None, explicit_sl: float = None, retries=3, delay=1):
    global RATE_LIMIT_BACKOFF, TEMP_SKIP
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
    tick = f.get('tickSize') or 0.0
    tp = clip_ceil(tp, tick)
    sp = clip_floor(sp, tick)
    sl = clip_floor(stop_limit, tick)

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
                qty = needed_qty
                notify(f"ℹ️ Increasing qty to meet minNotional: {qty}")
            else:
                attempts = 0
                while attempts < 40 and qty * tp < min_notional - 1e-12:
                    if tick and tick > 0:
                        tp = clip_ceil(tp + tick, tick)
                    else:
                        tp = tp + max(1e-8, tp * 0.001)
                    attempts += 1
                if qty * tp < min_notional - 1e-12:
                    notify(f"⚠️ Cannot meet minNotional for OCO on {symbol} (qty*tp={qty*tp:.8f} < {min_notional}). Will attempt fallback flow.")

    qty_str = format_qty(qty, f['stepSize'])
    tp_str = format_price(tp, f.get('tickSize', 0.0))
    sp_str = format_price(sp, f.get('tickSize', 0.0))
    sl_str = format_price(sl, f.get('tickSize', 0.0))

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
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                set_rate_limit_backoff(90)
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or 90)
                return None
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)
        except Exception as e:
            notify(f"⚠️ Unexpected error on OCO attempt: {e}")
            time.sleep(0.2)

    # Attempt 2: alternative param names (some client versions expect above/below)
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
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                set_rate_limit_backoff(90)
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or 90)
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
    TEMP_SKIP[symbol] = time.time() + 60 * 60
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
                client.cancel_order(symbol=symbol, orderId=o.get('orderId'))
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

            # if almost sold or no open orders and small remaining balance -> treat as closed
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

            # additional guard: do not roll if roll_count reached limit
            if roll_count >= MAX_ROLLS_PER_POSITION:
                pass

            sell_qty = round_step(available_for_sell, f.get('stepSize', 0.0))
            if sell_qty <= 0:
                continue

            candidate_tp = curr_tp + ROLL_TP_STEP_ABS
            estimated_gross_profit = (candidate_tp - entry_price) * sell_qty
            estimated_fees = (candidate_tp + entry_price) * sell_qty * ESTIMATED_FEE_RATE
            estimated_net = estimated_gross_profit - estimated_fees

            should_roll_profit_ok = estimated_net >= MIN_ROLL_PROFIT_USD

            if ( (near_trigger and moved_enough) or rise_trigger_pct or rise_trigger_abs ) and available_for_sell >= f.get('minQty', 0.0) and can_roll and roll_count < MAX_ROLLS_PER_POSITION and should_roll_profit_ok:
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

                est_gross = (new_tp - entry_price) * sell_qty
                est_fees = (new_tp + entry_price) * sell_qty * ESTIMATED_FEE_RATE
                est_net = est_gross - est_fees
                if est_net < MIN_ROLL_PROFIT_USD:
                    notify(f"⚠️ Roll skipped: estimated net profit ${est_net:.4f} < MIN_ROLL_PROFIT_USD (${MIN_ROLL_PROFIT_USD})")
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
                        TEMP_SKIP[symbol] = time.time() + 60 * 60
            else:
                pass

        except Exception as e:
            notify(f"⚠️ Error in monitor_and_roll: {e}")
            return False, entry_price, 0.0

# -------------------------
# DAILY METRICS helpers
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
# MAIN TRADE CYCLE
# -------------------------
ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0
BUY_LOCK_SECONDS = float(os.getenv("BUY_LOCK_SECONDS", "60"))

def trade_cycle():
    global ACTIVE_SYMBOL, LAST_BUY_TS, RATE_LIMIT_UNTIL
    start_balance = get_free_usdt()
    notify(f"🔰 Start balance snapshot: ${start_balance:.6f}")

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
                if RATE_LIMIT_UNTIL and time.time() < RATE_LIMIT_UNTIL:
                    notify(f"⏸ Backing off due to prior rate-limit until {time.ctime(RATE_LIMIT_UNTIL)}.")
                    time.sleep(max(1, RATE_LIMIT_UNTIL - time.time()))
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

            try:
                buy_res = place_safe_market_buy(symbol, usd_to_buy, require_orderbook=True)
            except Exception as e:
                err = str(e)
                notify(f"❌ Exception during market buy attempt for {symbol}: {err}")
                if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                    set_rate_limit_backoff(90)
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
                ACTIVE_SYMBOL = None
                LAST_BUY_TS = time.time()
                date_key, m = _update_metrics_for_profit(0.0)
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
                try:
                    client.universal_transfer(type='SPOT_TO_FUNDING', asset=QUOTE, amount=str(round(total_profit_usd,6)))
                    notify(f"💸 Profit ${total_profit_usd:.6f} transferred to funding wallet.")
                except Exception as e:
                    notify(f"⚠️ Profit transfer failed: {e}")

            RATE_LIMIT_BACKOFF = 0
        except Exception as e:
            err = str(e)
            if '-1003' in err or 'Too much request weight' in err:
                set_rate_limit_backoff(90)
                notify(f"❌ Rate limit reached in trade_cycle: backing off until {time.ctime(RATE_LIMIT_UNTIL)}.")
                time.sleep(max(1, RATE_LIMIT_UNTIL - time.time()))
                continue
            notify(f"❌ Trade cycle unexpected error: {e}")
            time.sleep(CYCLE_DELAY)
        time.sleep(30)

# -------------------------
# FLASK KEEPALIVE
# -------------------------
app = Flask(__name__)

@app.route("/health")
def health():
    _flush_notify_cache(force=False)
    return Response("OK", status=200)

@app.route("/metrics")
def metrics():
    body = f"active_symbol {ACTIVE_SYMBOL or 'idle'}\nrecent_buys {len(RECENT_BUYS)}\n"
    return Response(body, status=200, mimetype="text/plain")

def start_flask():
    port = int(os.environ.get("PORT", "5000"))
    notify(f"Starting health server on :{port}")
    app.run(host="0.0.0.0", port=port, threaded=True)

# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    notify("Booting cryptobot (merged OCO + rolling) — validating API")
    try:
        acct = client.get_account()
        notify("✅ API validated, starting trade loop")
    except Exception as e:
        notify(f"❌ API validation failed: {e}")
        raise SystemExit(1)
    t = threading.Thread(target=trade_cycle, daemon=True)
    t.start()
    start_flask()