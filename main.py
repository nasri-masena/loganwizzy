import os
import math
import time
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
MIN_VOLUME = 5_000_000
MOVEMENT_MIN_PCT = 3.0

TRADE_USD = 7.0
SLEEP_BETWEEN_CHECKS = 60
CYCLE_DELAY = 15
COOLDOWN_AFTER_EXIT = 10

TRIGGER_PROXIMITY = 0.01
STEP_INCREMENT_PCT = 0.02
BASE_TP_PCT = 3.0
BASE_SL_PCT = 1.0

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
# HELPERS: formatting & rounding
# -------------------------
def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 1.0:
        # throttle small burst notifications
        pass
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
    """Format price according to tick_size precision"""
    try:
        tick = Decimal(str(tick_size))
        precision = max(0, -tick.as_tuple().exponent)
        return format(Decimal(str(value)).quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN), f'.{precision}f')
    except Exception:
        return f"{value:.8f}"

def format_qty(qty: float, step: float) -> str:
    """
    Truncate qty DOWN to stepSize and return string with correct decimals.
    Uses Decimal floor division so we avoid too-many-precision errors.
    """
    try:
        if not step or float(step) == 0.0:
            return str(qty)
        q = Decimal(str(qty))
        s = Decimal(str(step))
        multiples = (q // s)
        quant = multiples * s
        precision = max(0, -s.as_tuple().exponent)
        # ensure non-negative
        if quant < Decimal('0'):
            quant = Decimal('0')
        return format(quant, f'.{precision}f')
    except Exception:
        # fallback minimal formatting
        return f"{math.floor(qty):.0f}"

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
    fs = {f['filterType']: f for f in symbol_info.get('filters', [])}
    lot = fs.get('LOT_SIZE')
    pricef = fs.get('PRICE_FILTER')
    min_notional = fs.get('MIN_NOTIONAL', {}).get('minNotional', None)
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
CACHE_TTL = 30

def get_tickers_cached():
    global TICKER_CACHE, LAST_FETCH
    now = time.time()
    if TICKER_CACHE is None or now - LAST_FETCH > CACHE_TTL:
        TICKER_CACHE = client.get_ticker()
        LAST_FETCH = now
    return TICKER_CACHE

def cleanup_temp_skip():
    now = time.time()
    for s, until in list(TEMP_SKIP.items()):
        if now >= until:
            del TEMP_SKIP[s]

def pick_coin():
    """
    Efficient picker:
      - cheap prefilter by 24h quoteVolume, price, 24h change
      - for top candidates fetch 5m klines and compute vol_ratio & recent_pct
      - returns (symbol, price, quoteVolume, change_pct) or None
    """
    cleanup_temp_skip()
    now = time.time()
    tickers = get_tickers_cached() or []
    prefiltered = []

    MIN_QVOL = max(MIN_VOLUME, 500_000)
    MIN_CHANGE_PCT = max(1.0, MOVEMENT_MIN_PCT if 'MOVEMENT_MIN_PCT' in globals() else 1.0)
    TOP_CANDIDATES = 300
    MIN_VOL_RATIO = 1.8
    MIN_RECENT_PCT = 0.5

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

        if not (PRICE_MIN <= price <= PRICE_MAX):
            continue
        if qvol < MIN_QVOL:
            continue
        if change_pct < MIN_CHANGE_PCT:
            continue

        prefiltered.append((sym, price, qvol, change_pct))

    if not prefiltered:
        return None

    prefiltered.sort(key=lambda x: x[2], reverse=True)
    prefiltered = prefiltered[:TOP_CANDIDATES]

    candidates = []
    for sym, price, qvol, change_pct in prefiltered:
        try:
            klines = client.get_klines(symbol=sym, interval='5m', limit=20)
            if not klines or len(klines) < 6:
                continue

            vols = []
            closes = []
            for k in klines:
                # k format: [openTime, open, high, low, close, volume, closeTime, quoteAssetVolume, ...]
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

            if len(vols) >= 11:
                avg_vol = sum(vols[-11:-1]) / 10.0
            else:
                avg_vol = (sum(vols[:-1]) / max(len(vols)-1, 1)) if len(vols) > 1 else recent_vol

            vol_ratio = recent_vol / (avg_vol + 1e-9)

            recent_pct = 0.0
            if len(closes) >= 4 and closes[-4] > 0:
                recent_pct = (closes[-1] - closes[-4]) / (closes[-4] + 1e-12) * 100.0

            if vol_ratio < MIN_VOL_RATIO:
                continue
            if recent_pct < MIN_RECENT_PCT:
                continue

            # quick RSI sanity
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
                if rsi > 70:
                    rsi_ok = False
            if not rsi_ok:
                continue

            score = change_pct * qvol * vol_ratio * max(1.0, recent_pct / 2.0)
            candidates.append((sym, price, qvol, change_pct, vol_ratio, recent_pct, score))

        except Exception:
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
    """
    Return (executed_qty, avg_price) from order response if available.
    """
    executed_qty = 0.0
    avg_price = 0.0
    try:
        if not order_resp:
            return 0.0, 0.0
        exec_qty = None
        # try common spellings
        if 'executedQty' in order_resp:
            exec_qty = order_resp.get('executedQty')
        elif 'executedQty' in order_resp:
            exec_qty = order_resp.get('executedQty')
        if exec_qty is not None and float(exec_qty) > 0:
            executed_qty = float(exec_qty)
            cumm = float(order_resp.get('cummulativeQuoteQty') or order_resp.get('cumulativeQuoteQty') or 0.0)
            if executed_qty > 0 and cumm > 0:
                avg_price = cumm / executed_qty

        if executed_qty == 0.0:
            fills = order_resp.get('fills') or []
            total_qty = 0.0
            total_quote = 0.0
            for f in fills:
                q = float(f.get('qty', 0.0) or 0.0)
                p = float(f.get('price', 0.0) or 0.0)
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
        info = client.get_symbol_info(symbol)
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

def place_safe_market_buy(symbol, usd_amount):
    """
    Market buy: returns (executed_qty, avg_price) or (None, None).
    NOTE: this function NO LONGER places OCO — monitor_and_roll will place the initial protective orders.
    """
    # temp-skip check
    now = time.time()
    skip_until = TEMP_SKIP.get(symbol)
    if skip_until and now < skip_until:
        notify(f"⏭️ Skipping {symbol} until {time.ctime(skip_until)} (recent failure).")
        return None, None

    if not is_symbol_tradable(symbol):
        notify(f"⛔ Symbol {symbol} not tradable / market closed. Skipping and blacklisting.")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None

    info = client.get_symbol_info(symbol)
    if not info:
        notify(f"❌ place_safe_market_buy: couldn't fetch symbol info for {symbol}")
        TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None
    f = get_filters(info)

    try:
        price = float(client.get_symbol_ticker(symbol=symbol)['price'])
    except Exception as e:
        notify(f"⚠️ Failed to fetch ticker for {symbol}: {e}")
        return None, None

    qty_target = usd_amount / price
    qty_target = max(qty_target, f['minQty'])
    qty_target = round_step(qty_target, f['stepSize'])

    if f['minNotional']:
        notional = qty_target * price
        if notional < f['minNotional']:
            needed_qty = round_step(f['minNotional'] / price, f['stepSize'])
            free_usdt = get_free_usdt()
            if needed_qty * price > free_usdt + 1e-8:
                notify(f"❌ Not enough funds for MIN_NOTIONAL on {symbol}")
                return None, None
            qty_target = needed_qty

    qty_str = format_qty(qty_target, f['stepSize'])

    try:
        order_resp = client.order_market_buy(symbol=symbol, quantity=qty_str)
    except Exception as e:
        err = str(e)
        notify(f"❌ Market buy failed for {symbol}: {err}")
        if '-1013' in err or 'Market is closed' in err:
            TEMP_SKIP[symbol] = time.time() + SKIP_SECONDS_ON_MARKET_CLOSED
        return None, None

    executed_qty, avg_price = _parse_market_buy_exec(order_resp)

    # small wait then fetch actual free balance to reconcile (single source of truth)
    time.sleep(0.8)
    asset = symbol[:-len(QUOTE)]
    free_after = get_free_asset(asset)
    free_after_clip = round_step(free_after, f['stepSize'])

    # If parsed executed_qty is zero or disagrees with free balance ( >2% ), prefer balance
    if free_after_clip >= f['minQty'] and (executed_qty <= 0 or abs(free_after_clip - executed_qty) / (executed_qty + 1e-9) > 0.02):
        notify(f"ℹ️ Adjusting executed qty from parsed {executed_qty} to actual free balance {free_after_clip}")
        executed_qty = free_after_clip
        if not avg_price or avg_price == 0.0:
            avg_price = price

    executed_qty = round_step(executed_qty, f['stepSize'])
    if executed_qty < f['minQty'] or executed_qty <= 0:
        notify(f"❌ Executed quantity too small after reconciliation for {symbol}: {executed_qty}")
        return None, None

    notify(f"✅ BUY {symbol}: qty={executed_qty} ~price={avg_price:.8f} notional≈${executed_qty*avg_price:.6f}")

    # IMPORTANT: DO NOT place OCO here. monitor_and_roll will handle it once it confirms balances / open orders.
    return executed_qty, avg_price
    
# -------------------------
# OCO SELL with fallbacks
# -------------------------
def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=1.0,
                   explicit_tp: float = None, explicit_sl: float = None,
                   retries=3, delay=1):
    info = client.get_symbol_info(symbol)
    if not info:
        notify(f"⚠️ place_oco_sell: couldn't fetch symbol info for {symbol}")
        return None
    f = get_filters(info)
    asset = symbol[:-len(QUOTE)]

    tp = explicit_tp if explicit_tp is not None else (buy_price * (1 + tp_pct / 100.0))
    sp = explicit_sl if explicit_sl is not None else (buy_price * (1 - sl_pct / 100.0))
    stop_limit = sp * 0.999

    def clip(v, step):
        if not step or step == 0:
            return v
        return math.floor(v / step) * step

    qty = clip(qty, f['stepSize'])
    tp = clip(tp, f['tickSize'])
    sp = clip(sp, f['tickSize'])
    sl = clip(stop_limit, f['tickSize'])

    if qty <= 0:
        notify("❌ place_oco_sell: quantity too small after clipping")
        return None

    # ensure free asset available (small safety margin)
    free_qty = get_free_asset(asset)
    safe_margin = f['stepSize'] if f['stepSize'] and f['stepSize'] > 0 else 0.0
    if free_qty + 1e-12 < qty:
        new_qty = clip(max(0.0, free_qty - safe_margin), f['stepSize'])
        if new_qty <= 0:
            notify(f"❌ Not enough free {asset} to place sell. free={free_qty}, required={qty}")
            return None
        notify(f"ℹ️ Adjusting sell qty down from {qty} to available {new_qty} to avoid insufficient balance.")
        qty = new_qty

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
            notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
        except Exception as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (standard) failed: {err}")
            if 'aboveType' in err or '-1102' in err or 'Mandatory parameter' in err:
                notify("ℹ️ Detected 'aboveType' style requirement; will attempt alternative param names.")
                break
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)

    # Attempt 2: alternative param names
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
            notify(f"📌 OCO SELL placed (alt params) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty_str}")
            return {'tp': tp, 'sl': sp, 'method': 'oco_abovebelow', 'raw': oco2}
        except Exception as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (alt) failed: {err}")
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
        notify(f"📈 TP LIMIT placed (fallback): {tp_str}, qty={qty_str}")
    except Exception as e:
        notify(f"❌ Fallback TP limit failed: {e}")
    try:
        sl_order = client.create_order(
            symbol=symbol,
            side="SELL",
            type="STOP_MARKET",
            stopPrice=sp_str,
            quantity=qty_str
        )
        notify(f"📉 SL STOP_MARKET placed (fallback): trigger={sp_str}, qty={qty_str}")
    except Exception as e:
        notify(f"❌ Fallback SL stop-market failed: {e}")

    if tp_order or sl_order:
        return {'tp': tp, 'sl': sp, 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order}}
    else:
        notify("❌ All attempts to protect position failed (no TP/SL placed).")
        return None

# -------------------------
# CANCEL HELPERS
# -------------------------
def cancel_all_open_orders(symbol):
    try:
        open_orders = client.get_open_orders(symbol=symbol)
        cancelled = 0
        for o in open_orders:
            client.cancel_order(symbol=symbol, orderId=o.get('orderId') or o.get('orderId'))
            cancelled += 1
        if cancelled:
            notify(f"❌ Cancelled {cancelled} open orders for {symbol}")
    except Exception as e:
        notify(f"⚠️ Failed to cancel orders: {e}")

# -------------------------
# MONITOR & ROLL
# -------------------------
def monitor_and_roll(symbol, qty, entry_price, f):
    """
    Monitor OCO placed by this function (it will place the initial OCO if none exists).
    Returns (closed_bool, exit_price, profit_usd)
    """
    curr_tp = entry_price * (1 + BASE_TP_PCT / 100.0)
    curr_sl = entry_price * (1 - BASE_SL_PCT / 100.0)

    asset = symbol[:-len(QUOTE)]

    # Wait a short while for exchange to update balances/orders and check if protective orders already exist.
    found_open_orders = False
    free_qty = 0.0
    for attempt in range(8):  # ~ up to 4 seconds
        time.sleep(0.5)
        try:
            open_orders = client.get_open_orders(symbol=symbol)
            free_qty = get_free_asset(asset)
            if open_orders:
                found_open_orders = True
                notify(f"ℹ️ Found existing open orders for {symbol} after buy — assuming protective orders already placed.")
                break
            # if free_qty looks like the bought qty (allow tiny slippage) break and place OCO here
            if free_qty >= max(qty * 0.95, f['minQty']):
                break
        except Exception:
            # ignore transient errors and retry
            pass

    # If no open orders found, place initial OCO using available free_qty (prefer actual free balance)
    # set used_qty to free_qty if available, else fallback to qty
    try:
        free_now = get_free_asset(asset)
    except Exception:
        free_now = 0.0
    used_qty = round_step(free_now, f['stepSize']) if free_now >= f['minQty'] else round_step(qty, f['stepSize'])

    # If we don't see open orders and used_qty looks reasonable -> place OCO
    initial_oco = None
    if not found_open_orders:
        initial_oco = place_oco_sell(symbol, used_qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
        if initial_oco is None:
            notify(f"❌ Initial OCO failed for {symbol}, aborting monitor.")
            return False, entry_price, 0.0
        else:
            notify(f"✅ Initial protective orders placed for {symbol} (qty={used_qty}).")
    else:
        # If open orders were found earlier, we assume they're the protective orders (do not abort)
        initial_oco = True

    last_tp = None

    while True:
        try:
            time.sleep(SLEEP_BETWEEN_CHECKS)
            free_qty = get_free_asset(asset)
            open_orders = client.get_open_orders(symbol=symbol)
            price_now = float(client.get_symbol_ticker(symbol=symbol)['price'])

            # Position closed? no free asset or no open orders
            if free_qty < round_step(qty * 0.05, f['stepSize']) and len(open_orders) == 0:
                exit_price = price_now
                profit_usd = (exit_price - entry_price) * qty
                notify(f"✅ Position closed for {symbol}: exit={exit_price:.8f}, profit≈${profit_usd:.6f}")
                return True, exit_price, profit_usd

            # near TP -> roll
            near_trigger = price_now >= curr_tp * (1 - TRIGGER_PROXIMITY)
            if near_trigger:
                notify(f"🔎 Price near TP (price={price_now:.8f}, TP={curr_tp:.8f}). Rolling OCO...")
                cancel_all_open_orders(symbol)

                new_sl = entry_price if last_tp is None else last_tp
                new_tp = curr_tp * (1 + STEP_INCREMENT_PCT)

                available_qty = get_free_asset(asset)
                oco2 = place_oco_sell(symbol, available_qty, entry_price,
                                      tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT,
                                      explicit_tp=new_tp, explicit_sl=new_sl)
                if oco2:
                    last_tp = curr_tp
                    curr_tp = new_tp
                    curr_sl = new_sl
                    notify(f"🔁 Rolled OCO: new TP={curr_tp:.8f}, new SL={curr_sl:.8f}")
                else:
                    notify("⚠️ Roll failed, will retry on next check.")

        except Exception as e:
            notify(f"⚠️ Error in monitor_and_roll: {e}")
            return False, entry_price, 0.0
            
# -------------------------
# MAIN TRADE CYCLE
# -------------------------
ACTIVE_SYMBOL = None
LAST_BUY_TS = 0.0
BUY_LOCK_SECONDS = 60

def trade_cycle():
    global start_balance_usdt, ACTIVE_SYMBOL, LAST_BUY_TS

    if start_balance_usdt is None:
        start_balance_usdt = get_free_usdt()
        notify(f"🔰 Start balance snapshot: ${start_balance_usdt:.6f}")

    while True:
        try:
            open_orders_global = client.get_open_orders()
            if open_orders_global:
                notify("⏳ Still waiting for previous trade(s) to finish...")
                time.sleep(1800)
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
                time.sleep(180)
                continue

            symbol, price, volume, change = candidate
            notify(f"🎯 Selected {symbol} for market buy (24h change={change}%, vol≈{volume})")

            usd_to_buy = min(TRADE_USD, get_free_usdt())
            if usd_to_buy < 1.0:
                notify(f"⚠️ Not enough USDT to buy (free={get_free_usdt():.4f}). Sleeping...")
                time.sleep(CYCLE_DELAY)
                continue

            buy_res = place_safe_market_buy(symbol, usd_to_buy)
            if not buy_res or buy_res == (None, None):
                notify(f"ℹ️ Buy skipped/failed for {symbol}.")
                time.sleep(CYCLE_DELAY)
                continue

            qty, entry_price = buy_res
            if qty is None or entry_price is None:
                notify(f"⚠️ Unexpected buy result for {symbol}, skipping.")
                time.sleep(CYCLE_DELAY)
                continue

            ACTIVE_SYMBOL = symbol
            LAST_BUY_TS = time.time()

            info = client.get_symbol_info(symbol)
            f = get_filters(info)

            closed, exit_price, profit_usd = monitor_and_roll(symbol, qty, entry_price, f)

            ACTIVE_SYMBOL = None

            if closed and profit_usd > 0:
                send_profit_to_funding(profit_usd)

        except Exception as e:
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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    bot_thread.start()
    start_flask()