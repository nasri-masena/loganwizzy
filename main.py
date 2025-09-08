import os
import math
import time
import random
import threading
import requests
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from flask import Flask
from binance.client import Client

# -------------------------
# CONFIG
# -------------------------
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
QUOTE = "USDT"

PRICE_MIN = 1.0
PRICE_MAX = 3.0
MIN_VOLUME = 3_000_000
MOVEMENT_MIN_PCT = 1.0

TRADE_USD = 7.0
SLEEP_BETWEEN_CHECKS = 60
CYCLE_DELAY = 13
COOLDOWN_AFTER_EXIT = 10

TRIGGER_PROXIMITY = 0.06
STEP_INCREMENT_PCT = 0.02
BASE_TP_PCT = 2.0
BASE_SL_PCT = 2.0

MICRO_TP_PCT = 0.8
MICRO_TP_FRACTION = 0.35

ROLL_ON_RISE_PCT = 0.5            # percent rise from entry that can trigger a roll (e.g. 0.8%)
ROLL_TRIGGER_DELTA_ABS = 0.007   # absolute rise from entry in quote units (e.g. 0.012 = 1.2 cents)
ROLL_TP_STEP_ABS = 0.015          # on roll, increase TP by this absolute amount (e.g. 0.010 = 10 cents)
ROLL_SL_STEP_ABS = 0.004         # on roll, raise SL by this absolute amount (e.g. 0.017)
ROLL_COOLDOWN_SECONDS = 30 # minimum seconds between consecutive rolls
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

# rate-limit small decimal precision
getcontext().prec = 28

# -------------------------
# REBUY / RECENT BUYS CONFIG
# -------------------------
RECENT_BUYS = {}            # symbol -> {'ts': float, 'price': float, 'profit': float|None, 'cooldown': int}
REBUY_COOLDOWN = 60 * 60    # 1 hour default after a win
LOSS_COOLDOWN = 60 * 60 * 4 # 4 hours after a loss
REBUY_MAX_RISE_PCT = 5.0    # don't re-buy if price rose > 5% since last buy

# --- rate-limit backoff & cache adjustments ---
RATE_LIMIT_BACKOFF = 0
RATE_LIMIT_BACKOFF_MAX = 300
RATE_LIMIT_BASE_SLEEP = 90

CACHE_TTL = 120   # unified cache TTL for tickers (seconds)

# -------------------------
# HELPERS: formatting & rounding
# -------------------------
def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 1.0:
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
        if s == 0:
            return format(q, 'f')
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
    min_notional = fs.get('MIN_NOTIONAL', {}).get('minNotional', None) if fs.get('MIN_NOTIONAL') else None
    return {
        'stepSize': float(lot['stepSize']) if lot else 0.0,
        'minQty': float(lot['minQty']) if lot else 0.0,
        'tickSize': float(pricef['tickSize']) if pricef else 0.0,
        'minNotional': float(min_notional) if min_notional else None
    }

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
# TICKER CACHE & PICKER
# -------------------------
TICKER_CACHE = None
LAST_FETCH = 0

# small caches for symbol_info and open orders
SYMBOL_INFO_CACHE = {}   # symbol -> (info_dict, ts)
SYMBOL_INFO_TTL = 120    # seconds

OPEN_ORDERS_CACHE = {'ts': 0, 'data': None}
OPEN_ORDERS_TTL = 15      # seconds (short cache)

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
    Return lastPrice for symbol using the global TICKER_CACHE to avoid many single-symbol API calls.
    Falls back to a direct single-symbol request only if cache miss.
    """
    global TICKER_CACHE, LAST_FETCH, RATE_LIMIT_BACKOFF
    now = time.time()
    try:
        # refresh tickers if stale
        if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
            try:
                TICKER_CACHE = client.get_ticker()
                LAST_FETCH = now
                RATE_LIMIT_BACKOFF = 0
            except Exception as e:
                notify(f"⚠️ Failed to refresh tickers in get_price_cached: {e}")
                # don't raise; we'll try fallback below
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
    # fallback to single-symbol call (rare)
    try:
        res = client.get_symbol_ticker(symbol=symbol)
        return float(res.get('price') or res.get('lastPrice') or 0.0)
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

def orderbook_bullish(symbol, depth=5, min_imbalance=1.05, max_spread_pct=1.5):
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
        

def pick_coin():
    global RATE_LIMIT_BACKOFF
    cleanup_temp_skip()
    cleanup_recent_buys()
    now = time.time()

    # relaxed tuning compared to previous aggressive settings
    MAX_24H_RISE_PCT = 12.0   # allow coins that rose up to ~12% in 24h (was 5%)
    RECENT_PCT_MIN = 0.6      # accept smaller immediate moves (was 1.0)
    RECENT_PCT_MAX = 4.0      # allow slightly bigger short moves (was 2.0)
    TOP_CANDIDATES = 200      # evaluate more symbols (was 120)
    MIN_VOL_RATIO = 1.4       # looser volume spike requirement (was 1.8)
    KLINES_LIMIT = 14         # fewer candles to reduce API weight a bit (was 20)

    tickers = get_tickers_cached() or []
    prefiltered = []

    MIN_QVOL = max(MIN_VOLUME, 300_000)  # reduce the hard floor so more pairs pass
    MIN_CHANGE_PCT = max(0.8, MOVEMENT_MIN_PCT if 'MOVEMENT_MIN_PCT' in globals() else 0.8)

    def ema(values, period):
        if not values or period <= 0:
            return None
        alpha = 2.0 / (period + 1.0)
        e = float(values[0])
        for v in values[1:]:
            e = alpha * float(v) + (1 - alpha) * e
        return e

    # Phase 1: coarse prefilter using tickers (cheap)
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

        # avoid immediate rebuys and big rises since last buy
        if last:
            if now < last['ts'] + last.get('cooldown', REBUY_COOLDOWN):
                continue
            last_price = last.get('price')
            if last_price and price > last_price * (1 + REBUY_MAX_RISE_PCT / 100.0):
                continue

        # price boundary
        if not (PRICE_MIN <= price <= PRICE_MAX):
            continue

        # skip EXTREME pump names (24h) but be more permissive
        if change_pct >= MAX_24H_RISE_PCT:
            continue

        # volume and 24h movement baseline
        if qvol < MIN_QVOL:
            continue
        if change_pct < MIN_CHANGE_PCT:
            continue

        prefiltered.append((sym, price, qvol, change_pct))

    if not prefiltered:
        return None

    # focus on top by quote volume to limit heavy klines calls
    prefiltered.sort(key=lambda x: x[2], reverse=True)
    prefiltered = prefiltered[:TOP_CANDIDATES]

    candidates = []
    for sym, price, qvol, change_pct in prefiltered:
        try:
            # fetch klines with defensive rate-limit handling
            try:
                klines = client.get_klines(symbol=sym, interval='5m', limit=KLINES_LIMIT)
            except Exception as e:
                err = str(e)
                if '-1003' in err or 'Too much request weight' in err:
                    notify(f"⚠️ Rate limit while fetching klines for {sym}: {err}. Backing off.")
                    RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                            RATE_LIMIT_BACKOFF_MAX)
                    time.sleep(RATE_LIMIT_BACKOFF)
                    break
                continue

            if not klines or len(klines) < 6:
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

            # average of previous candles (exclude latest)
            prev_slice = vols[-(KLINES_LIMIT-1):-1] if len(vols) >= 2 else vols[:-1]
            avg_vol = (sum(prev_slice) / max(len(prev_slice), 1)) if prev_slice else recent_vol
            vol_ratio = recent_vol / (avg_vol + 1e-9)

            # recent percent change: last vs ~15m ago (4 candles)
            recent_pct = 0.0
            if len(closes) >= 4 and closes[-4] > 0:
                recent_pct = (closes[-1] - closes[-4]) / (closes[-4] + 1e-12) * 100.0

            # require recent breakout in desired small window (looser)
            if recent_pct < RECENT_PCT_MIN or recent_pct > RECENT_PCT_MAX:
                continue

            # require volume spike (looser)
            if vol_ratio < MIN_VOL_RATIO:
                continue

            # quick momentum: at least one consecutive up in last 3 closes
            last3 = closes[-3:]
            ups = 0
            if last3[1] > last3[0]:
                ups += 1
            if last3[2] > last3[1]:
                ups += 1
            if ups < 1:
                continue

            # EMA confirmation
            short_period = 3
            long_period = 10
            if len(closes) >= long_period:
                short_ema = ema(closes[-short_period:], short_period)
                long_ema = ema(closes[-long_period:], long_period)
            elif len(closes) >= short_period:
                short_ema = ema(closes[-short_period:], short_period)
                long_ema = ema(closes, max(len(closes), 1))
            else:
                short_ema = None
                long_ema = None

            if short_ema is None or long_ema is None:
                continue
            if not (short_ema > long_ema * 0.995):  # slightly more tolerant
                continue

            # RSI sanity check (avoid very overbought)
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
                if rsi > 68:  # slightly higher threshold tolerated
                    rsi_ok = False
            if not rsi_ok:
                continue

            # NEW: require immediate orderbook bullishness before accepting candidate
            # but allow looser imbalance/spread (orderbook_bullish uses softer defaults now)
            try:
                if not orderbook_bullish(sym, depth=5, min_imbalance=1.05, max_spread_pct=1.5):
                    continue
            except Exception:
                # if OB fails, be permissive and continue (to avoid losing every candidate)
                # but here we choose to skip noisy OB errors conservatively:
                continue

            # score and append
            ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
            score = change_pct * qvol * vol_ratio * max(1.0, recent_pct / 2.0) * (1.0 + ema_uplift)

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
# MARKET BUY helpers
# -------------------------
def _parse_market_buy_exec(order_resp):
    executed_qty = 0.0
    avg_price = 0.0
    try:
        if not order_resp:
            return 0.0, 0.0
        for key in ('executedQty',):
            val = order_resp.get(key)
            if val is not None:
                try:
                    if float(val) > 0:
                        executed_qty = float(val)
                        break
                except Exception:
                    pass
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
                if str(ot).upper() in ('MARKET', 'LIMIT', 'LIMIT_MAKER', 'STOP_MARKET', 'TAKE_PROFIT_MARKET'):
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

    # optional orderbook confirmation (cheap)
    if require_orderbook:
        try:
            if not orderbook_bullish(symbol, depth=5, min_imbalance=1.1, max_spread_pct=0.6):
                notify(f"⚠️ Orderbook not bullish for {symbol}; aborting market buy.")
                return None, None
        except Exception as e:
            notify(f"⚠️ Orderbook check error for {symbol}: {e}")
            # fail-open: continue (avoid blocking buys on transient ob errors)
    
    # get price (use cache first)
    price = get_price_cached(symbol)
    if price is None:
        try:
            res = client.get_symbol_ticker(symbol=symbol)
            price = float(res.get('price') or res.get('lastPrice') or 0.0)
        except Exception as e:
            err = str(e)
            notify(f"⚠️ Failed to fetch ticker for {symbol}: {err}")
            # on severe rate-limit, back off and TEMP skip
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                        RATE_LIMIT_BACKOFF_MAX)
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                notify(f"❗ Rate-limit fetching price. Backing off {RATE_LIMIT_BACKOFF}s; TEMP skipping {symbol}.")
            return None, None

    # defensive
    try:
        price = float(price)
        if price <= 0:
            notify(f"❌ Invalid price for {symbol}: {price}")
            return None, None
    except Exception:
        notify(f"❌ Invalid price type for {symbol}: {price}")
        return None, None

    # compute target qty, apply step/minQty
    qty_target = usd_amount / price
    qty_target = max(qty_target, f.get('minQty', 0.0))
    qty_target = round_step(qty_target, f.get('stepSize', 0.0))

    # ensure minNotional satisfied (try to bump qty if needed and funds permit)
    min_notional = f.get('minNotional')
    if min_notional:
        notional = qty_target * price
        if notional < min_notional - 1e-12:
            needed_qty = round_step(min_notional / price, f.get('stepSize', 0.0))
            free_usdt = get_free_usdt()
            if needed_qty * price > free_usdt + 1e-8:
                notify(f"❌ Not enough funds for MIN_NOTIONAL on {symbol} (need ${needed_qty*price:.6f}, free=${free_usdt:.6f}).")
                return None, None
            qty_target = needed_qty

    qty_str = format_qty(qty_target, f.get('stepSize', 0.0))
    if not qty_str or float(qty_target) <= 0:
        notify(f"❌ Computed qty invalid for {symbol}: qty_target={qty_target}, qty_str={qty_str}")
        return None, None

    # small random jitter to avoid hammering exchange at exact same times
    time.sleep(random.uniform(0.05, 0.25))

    # place market buy (with error handling)
    try:
        order_resp = client.order_market_buy(symbol=symbol, quantity=qty_str)
    except Exception as e:
        err = str(e)
        notify(f"❌ Market buy failed for {symbol}: {err}")
        # common errors: insufficient funds / min_notional / market closed / rate-limit
        if '-1013' in err or 'Market is closed' in err or 'MIN_NOTIONAL' in err or 'minNotional' in err:
            TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
            notify(f"⏸ TEMP skipping {symbol} due to order error.")
        if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
            # backoff logic
            RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP,
                                    RATE_LIMIT_BACKOFF_MAX)
            TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
            notify(f"❗ Rate-limit while placing market buy: backing off {RATE_LIMIT_BACKOFF}s and TEMP skipping {symbol}.")
        return None, None

    # parse executed qty & avg price
    executed_qty, avg_price = _parse_market_buy_exec(order_resp)

    # small settle wait then reconcile with actual free asset balance
    time.sleep(0.8)
    asset = symbol[:-len(QUOTE)]
    free_after = get_free_asset(asset)
    free_after_clip = round_step(free_after, f.get('stepSize', 0.0))

    # reconciliation heuristic: if parsed executed qty is zero or differs from free balance significantly, use free balance
    if free_after_clip >= f.get('minQty', 0.0) and (executed_qty <= 0 or abs(free_after_clip - executed_qty) / (executed_qty + 1e-9) > 0.02):
        notify(f"ℹ️ Adjusting executed qty from parsed {executed_qty} to actual free balance {free_after_clip}")
        executed_qty = free_after_clip
        if not avg_price or avg_price == 0.0:
            avg_price = price

    # final rounding and sanity checks
    executed_qty = round_step(executed_qty, f.get('stepSize', 0.0))
    if executed_qty < f.get('minQty', 0.0) or executed_qty <= 0:
        notify(f"❌ Executed quantity too small after reconciliation for {symbol}: {executed_qty}")
        return None, None

    # if free balance after is far smaller than expected, skip symbol briefly
    if free_after_clip < max(1e-8, executed_qty * 0.5):
        notify(f"⚠️ After buy free balance {free_after_clip} is much smaller than expected executed {executed_qty}. Skipping symbol for a while.")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None

    notify(f"✅ BUY {symbol}: qty={executed_qty} ~price={avg_price:.8f} notional≈${executed_qty*avg_price:.6f}")
    return executed_qty, avg_price
    
# --- Modified place_micro_tp: returns (order_resp, sell_qty) or (None, 0.0)
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

            # shorter, less-frequent polling to lower API weight
            order_id = None
            if isinstance(order, dict):
                order_id = order.get('orderId') or order.get('orderId')
            if not order_id:
                return order, sell_qty, tp_price

            poll_interval = 0.6  # slightly larger interval
            max_wait = 4.0       # reduced wait window
            waited = 0.0

            while waited < max_wait:
                try:
                    status = client.get_order(symbol=symbol, orderId=order_id)
                except Exception:
                    # break on inability to fetch order (avoid repeated failures)
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
        
# -------------------------
# OCO SELL with fallbacks
# -------------------------
def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=1.0,
                   explicit_tp: float = None, explicit_sl: float = None,
                   retries=3, delay=1):
    """
    Robust OCO placement:
      - respects minNotional (will try to raise TP until qty*TP >= minNotional or give up)
      - tries standard params, alt params, then fallback separate orders
      - returns same style dict as before or None
    """
    global RATE_LIMIT_BACKOFF  # we modify this global on rate-limit detection

    info = get_symbol_info_cached(symbol)
    if not info:
        notify(f"⚠️ place_oco_sell: couldn't fetch symbol info for {symbol}")
        return None
    f = get_filters(info)
    asset = symbol[:-len(QUOTE)]

    # determine base TP/SP
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
    # prefer to ceil TP to next tick so TP isn't accidentally lowered below minNotional
    tp = clip_ceil(tp, f['tickSize'])
    sp = clip_floor(sp, f['tickSize'])
    sl = clip_floor(stop_limit, f['tickSize'])

    if qty <= 0:
        notify("❌ place_oco_sell: quantity too small after clipping")
        return None

    # ensure free asset available
    free_qty = get_free_asset(asset)
    safe_margin = f['stepSize'] if f['stepSize'] and f['stepSize'] > 0 else 0.0
    if free_qty + 1e-12 < qty:
        new_qty = clip_floor(max(0.0, free_qty - safe_margin), f['stepSize'])
        if new_qty <= 0:
            notify(f"❌ Not enough free {asset} to place sell. free={free_qty}, required={qty}")
            return None
        notify(f"ℹ️ Adjusting sell qty down from {qty} to available {new_qty} to avoid insufficient balance.")
        qty = new_qty

    # check and adjust for minNotional: ensure qty * tp >= minNotional
    min_notional = f.get('minNotional')
    tick = f.get('tickSize') or 0.0
    max_adjust_attempts = 40
    adjust_attempt = 0
    if min_notional:
        while adjust_attempt < max_adjust_attempts and qty * tp < min_notional - 1e-12:
            # raise tp by one tick (or small delta) and re-clip (ceil for TP)
            if tick and tick > 0:
                tp = clip_ceil(tp + tick, tick)
            else:
                tp = tp + max(1e-8, tp * 0.001)
            adjust_attempt += 1
        if qty * tp < min_notional - 1e-12:
            notify(f"⚠️ Unable to satisfy MIN_NOTIONAL for OCO on {symbol} (qty*tp={qty*tp:.8f} < {min_notional}). Skipping OCO.")
            return None

    qty_str = format_qty(qty, f['stepSize'])
    tp_str = format_price(tp, f['tickSize'])
    sp_str = format_price(sp, f['tickSize'])
    sl_str = format_price(sl, f['tickSize'])

    # Attempt 1: standard param names
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
            # invalidate cache of open orders
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass
            notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
        except Exception as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (standard) failed: {err}")
            # if API demands alternative param style, break to alt attempts
            if 'aboveType' in err or '-1102' in err or 'Mandatory parameter' in err:
                notify("ℹ️ Detected 'aboveType' style requirement; will attempt alternative param names.")
                break
            # detect rate-limit / weight errors and bail for this symbol
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                notify("❗ Rate-limit detected while placing OCO — backing off and TEMP skipping symbol.")
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                return None
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)

    # Attempt 2: alternative param names (with same minNotional checks)
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
        except Exception as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (alt) failed: {err}")
            # If NOTIONAL failure, try to raise TP and retry a few times (but avoid loop forever)
            if 'NOTIONAL' in err or 'minNotional' in err or 'Filter failure' in err:
                # attempt to bump TP and retry within this loop
                if tick and tick > 0:
                    tp = clip_ceil(tp + tick, tick)
                else:
                    tp = tp + max(1e-8, tp * 0.001)
                tp_str = format_price(tp, f['tickSize'])
                # re-evaluate minNotional satisfied
                if min_notional and qty * tp < min_notional - 1e-12:
                    notify(f"⚠️ Still below minNotional after bump (qty*tp={qty*tp:.8f}), continuing attempts.")
                    time.sleep(delay)
                    continue
            # detect rate-limit here as well
            if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
                notify("❗ Rate-limit detected while placing OCO (alt) — backing off and TEMP skipping symbol.")
                TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
                RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)
                return None
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)

    # Fallback: separate TP limit and SL stop-market
    notify("⚠️ All OCO attempts failed — falling back to separate TP (limit) + SL (stop-market).")
    tp_order = None
    sl_order = None
    try:
        tp_order = client.order_limit_sell(symbol=symbol, quantity=qty_str, price=tp_str)
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        notify(f"📈 TP LIMIT placed (fallback): {tp_str}, qty={qty_str}")
    except Exception as e:
        err = str(e)
        notify(f"❌ Fallback TP limit failed: {err}")
        if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
            notify("❗ Rate-limit detected while placing fallback TP — TEMP skipping symbol.")
            TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
            RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)

    try:
        sl_order = client.create_order(
            symbol=symbol,
            side="SELL",
            type="STOP_MARKET",
            stopPrice=sp_str,
            quantity=qty_str
        )
        try:
            OPEN_ORDERS_CACHE['data'] = None
        except Exception:
            pass
        notify(f"📉 SL STOP_MARKET placed (fallback): trigger={sp_str}, qty={qty_str}")
    except Exception as e:
        err = str(e)
        notify(f"❌ Fallback SL stop-market failed: {err}")
        if '-1003' in err or 'Too much request weight' in err or 'Request has been rejected' in err:
            notify("❗ Rate-limit detected while placing fallback SL — TEMP skipping symbol.")
            TEMP_SKIP[symbol] = time.time() + max(60, RATE_LIMIT_BACKOFF or RATE_LIMIT_BASE_SLEEP)
            RATE_LIMIT_BACKOFF = min(RATE_LIMIT_BACKOFF * 2 if RATE_LIMIT_BACKOFF else RATE_LIMIT_BASE_SLEEP, RATE_LIMIT_BACKOFF_MAX)

    if tp_order or sl_order:
        return {'tp': tp, 'sl': sp, 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order}}
    else:
        notify("❌ All attempts to protect position failed (no TP/SL placed).")
        # small skip for symbol on repeated failure
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None
        
# -------------------------
# CANCEL HELPERS
# -------------------------
def cancel_all_open_orders(symbol, max_cancel=6, inter_delay=0.25):
    """Cancel up to max_cancel open orders for a symbol with small spacing to avoid hitting cancel rate limits."""
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
                # continue canceling other orders where possible
        if cancelled:
            notify(f"❌ Cancelled {cancelled} open orders for {symbol}")
        # invalidate open orders cache
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

            if available_for_sell < round_step(orig_qty * 0.05, f.get('stepSize', 0.0)) and len(open_orders) == 0:
                exit_price = price_now
                profit_usd = (exit_price - entry_price) * orig_qty
                notify(f"✅ Position closed for {symbol}: exit={exit_price:.8f}, profit≈${profit_usd:.6f}")
                return True, exit_price, profit_usd

            price_delta = price_now - entry_price
            rise_trigger_pct = price_now >= entry_price * (1 + ROLL_ON_RISE_PCT / 100.0)
            rise_trigger_abs = price_delta >= ROLL_TRIGGER_DELTA_ABS
            near_trigger = (price_now >= curr_tp * (1 - TRIGGER_PROXIMITY)) and (price_now < curr_tp * 1.05)
            tick = f.get('tickSize', 0.0) or 0.0
            minimal_move = max(ROLL_TRIGGER_DELTA_ABS * 0.4, tick or 0.0)
            moved_enough = price_delta >= minimal_move
            now_ts = time.time()
            can_roll = (now_ts - last_roll_ts) >= ROLL_COOLDOWN_SECONDS

            if ( (near_trigger and moved_enough) or rise_trigger_pct or rise_trigger_abs ) and available_for_sell >= f.get('minQty', 0.0) and can_roll:
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
            
# =========================
# DAILY METRICS (per-date)
# =========================
METRICS = {}  # date_iso -> {'picks': int, 'wins': int, 'losses': int, 'profit': float}

def _update_metrics_for_profit(profit: float):
    """Update today's metrics after a completed buy/trade. profit can be negative or positive."""
    date_key = datetime.now().date().isoformat()
    m = METRICS.get(date_key)
    if m is None:
        m = {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0}
        METRICS[date_key] = m
    # a successful buy counts as a 'pick'
    m['picks'] += 1
    # classify result
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
    """Notify the metrics for date_key in Swahili format requested."""
    m = METRICS.get(date_key, {'picks': 0, 'wins': 0, 'losses': 0, 'profit': 0.0})
    profit_val = m['profit']
    profit_str = f"+{profit_val:.2f} USDT" if profit_val >= 0 else f"{profit_val:.2f} USDT"
    # Send Swahili formatted summary
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
    """
    Main loop:
    - picks a candidate, attempts market buy (with orderbook check)
    - immediately places optional micro-TP for a fraction
    - monitors remaining qty via monitor_and_roll (with OCO + rolling)
    - respects rate-limit backoff and cooldown rules
    - updates daily metrics and notifies summary after each completed trade
    """
    global start_balance_usdt, ACTIVE_SYMBOL, LAST_BUY_TS, RATE_LIMIT_BACKOFF

    # initial snapshot (once)
    if start_balance_usdt is None:
        start_balance_usdt = get_free_usdt()
        notify(f"🔰 Start balance snapshot: ${start_balance_usdt:.6f}")

    while True:
        try:
            # housekeeping
            cleanup_recent_buys()

            # check if there are open orders globally (avoid overlaps)
            open_orders_global = get_open_orders_cached()
            if open_orders_global:
                notify("⏳ Still waiting for previous trade(s) to finish (open orders present)...")
                # reduced wait so bot can re-evaluate sooner
                time.sleep(300)
                continue

            # skip if another active trade is in progress
            if ACTIVE_SYMBOL is not None:
                notify(f"⏳ Active trade in progress for {ACTIVE_SYMBOL}, skipping new buys.")
                time.sleep(CYCLE_DELAY)
                continue

            now = time.time()
            if now - LAST_BUY_TS < BUY_LOCK_SECONDS:
                time.sleep(CYCLE_DELAY)
                continue

            # pick candidate coin
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

            # check recent buys
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

            # check USDT balance
            free_usdt = get_free_usdt()
            usd_to_buy = min(TRADE_USD, free_usdt)
            if usd_to_buy < 1.0:
                notify(f"⚠️ Not enough USDT to buy (free={free_usdt:.4f}). Sleeping...")
                time.sleep(CYCLE_DELAY)
                continue

            # attempt market buy (with orderbook check inside)
            try:
                buy_res = place_safe_market_buy(symbol, usd_to_buy, require_orderbook=True)
            except Exception as e:
                # catch unexpected exceptions from place_safe_market_buy
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
                notify(f"ℹ️ Buy skipped/failed for {symbol}.")
                time.sleep(CYCLE_DELAY)
                continue

            qty, entry_price = buy_res
            if qty is None or entry_price is None:
                notify(f"⚠️ Unexpected buy result for {symbol}, skipping.")
                time.sleep(CYCLE_DELAY)
                continue

            # we count this as a successful 'pick' (a buy executed)
            # (metrics will be updated after the trade completes and profit is known)

            # record recent buy (avoid immediate rebuy)
            RECENT_BUYS[symbol] = {'ts': time.time(), 'price': entry_price, 'profit': None, 'cooldown': REBUY_COOLDOWN}

            # get filters
            info = get_symbol_info_cached(symbol)
            f = get_filters(info) if info else {}
            if not f:
                notify(f"⚠️ Could not fetch filters for {symbol} after buy; aborting monitoring for safety.")
                ACTIVE_SYMBOL = None
                time.sleep(CYCLE_DELAY)
                continue

            # ============================
            # place optional micro-TP
            # ============================
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
                # update metrics: micro TP only, profit is computed below or zero
                total_profit_usd = 0.0
                date_key, m = _update_metrics_for_profit(total_profit_usd)
                _notify_daily_stats(date_key)
                continue

            # mark active symbol
            ACTIVE_SYMBOL = symbol
            LAST_BUY_TS = time.time()

            # invalidate open orders cache (since we just placed new ones)
            try:
                OPEN_ORDERS_CACHE['data'] = None
            except Exception:
                pass

            # ============================
            # monitor & rolling for qty_remaining
            # ============================
            try:
                closed, exit_price, profit_usd = monitor_and_roll(symbol, qty_remaining, entry_price, f)
            finally:
                ACTIVE_SYMBOL = None
                try:
                    OPEN_ORDERS_CACHE['data'] = None
                except Exception:
                    pass

            # ============================
            # combine micro profit + monitor profit
            # ============================
            total_profit_usd = profit_usd or 0.0
            if micro_order and micro_sold_qty and micro_tp_price:
                try:
                    micro_profit = (micro_tp_price - entry_price) * micro_sold_qty
                    total_profit_usd += micro_profit
                except Exception:
                    pass

            # update RECENT_BUYS with results
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

            # update metrics and notify Swahili summary
            date_key, m = _update_metrics_for_profit(total_profit_usd)
            _notify_daily_stats(date_key)

            # send profit to funding if > 0
            if closed and total_profit_usd and total_profit_usd > 0:
                send_profit_to_funding(total_profit_usd)

            # reset rate-limit backoff
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

        # pause between loops
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