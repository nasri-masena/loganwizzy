import os
import math
import time
import threading
import requests
from datetime import datetime
from flask import Flask
from binance.client import Client

# =========================
# CONFIG
# =========================
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
QUOTE = "USDT"

PRICE_MIN = 1.0
PRICE_MAX = 3.0
MIN_VOLUME = 5_000_000
MOVEMENT_MIN_PCT = 3.0

TRADE_USD = 7.0  # ⚡ test amount
SLEEP_BETWEEN_CHECKS = 60   # monitor frequency inside monitor_and_roll
CYCLE_DELAY = 15            # default delay between trade cycles (used at end)
COOLDOWN_AFTER_EXIT = 10    # small cooldown after trade finishes

# Rolling params
TRIGGER_PROXIMITY = 0.01      # 1% near TP => roll
STEP_INCREMENT_PCT = 0.02     # each roll increases TP by 2%
BASE_TP_PCT = 3.0             # base TP used by place_oco_sell (in percent)
BASE_SL_PCT = 1.0             # base SL used by place_oco_sell (in percent)

# =========================
# INIT / GLOBALS
# =========================
client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
start_balance_usdt = None

# =========================
# UTIL
# =========================
def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 2:  # short throttle
        return
    LAST_NOTIFY = now_ts
    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(text)
    if BOT_TOKEN and CHAT_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                          data={"chat_id": CHAT_ID, "text": text})
        except Exception:
            pass

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
    fs = {f['filterType']: f for f in symbol_info['filters']}
    lot = fs['LOT_SIZE']
    pricef = fs['PRICE_FILTER']
    min_notional = fs.get('MIN_NOTIONAL', {}).get('minNotional', None)
    return {
        'stepSize': float(lot['stepSize']),
        'minQty': float(lot['minQty']),
        'tickSize': float(pricef['tickSize']),
        'minNotional': float(min_notional) if min_notional else None
    }

def round_step(n, step):
    return math.floor(n / step) * step if step else n

def round_price(p, tick):
    return math.floor(p / tick) * tick if tick else p

def format_price(value, tick_size):
    decimals = str(tick_size).rstrip('0')
    if '.' in decimals:
        precision = len(decimals.split('.')[1])
    else:
        precision = 0
    return f"{value:.{precision}f}"

# =========================
# FUNDING TRANSFER
# =========================
def send_profit_to_funding(amount, asset='USDT'):
    """Use universal transfer: SPOT -> FUNDING"""
    try:
        result = client.universal_transfer(
            type='MAIN_FUNDING' if hasattr(client, 'universal_transfer') else 'SPOT_TO_FUNDING',
            asset=asset,
            amount=str(amount)
        )
        notify(f"💸 Profit ${amount:.6f} transferred to funding wallet.")
        return result
    except Exception as e:
        try:
            result = client.universal_transfer(
                type='SPOT_TO_FUNDING',
                asset=asset,
                amount=str(amount)
            )
            notify(f"💸 Profit ${amount:.6f} transferred to funding wallet (fallback).")
            return result
        except Exception as e2:
            notify(f"❌ Failed to transfer profit: {e} | {e2}")
            return None

# =========================
# SYMBOL PICKER (UPDATED: returns 4-tuple)
# =========================
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

def pick_coin():
    """
    Returns either None or (symbol, price, quoteVolume, change_pct)
    Internally scores candidates using vol spike + 24h change + 24h quoteVolume,
    but the return is kept compatible with trade_cycle (4 items).
    """
    tickers = get_tickers_cached()
    candidates = []
    for t in tickers:
        sym = t['symbol']
        if not sym.endswith(QUOTE):
            continue
        try:
            price = float(t['lastPrice'])
            qvol = float(t.get('quoteVolume') or t.get('quoteVolume', 0))  # 24h quote volume
            change_pct = float(t.get('priceChangePercent') or 0.0)

            # basic range filters
            if not (PRICE_MIN <= price <= PRICE_MAX):
                continue
            if qvol < 500_000:             # adjust to your risk/market
                continue
            # require at least 1% short-term move (24h-based filter here)
            if change_pct < 1.0:
                continue

            # --- additional confirmations (use klines for short-term volume & EMA) ---
            vol_ratio = 1.0
            closes = None
            try:
                klines = client.get_klines(symbol=sym, interval='5m', limit=20)
                if klines and len(klines) >= 2:
                    # Prefer quote asset volume if available at index 7, else approximate by base volume * close
                    vols = []
                    for k in klines:
                        try:
                            # Binance kline: [openTime, open, high, low, close, volume, closeTime, quoteAssetVolume, ...]
                            if len(k) > 7 and k[7] is not None:
                                vols.append(float(k[7]))  # quoteAssetVolume
                            else:
                                vols.append(float(k[5]) * float(k[4]))  # approximate
                        except Exception:
                            vols.append(0.0)
                    recent_vol = vols[-1]
                    if len(vols) >= 11:
                        avg_vol = sum(vols[-11:-1]) / 10.0
                    else:
                        avg_vol = (sum(vols[:-1]) / max(len(vols)-1, 1)) if len(vols) > 1 else vols[-1] or 1.0
                    vol_ratio = recent_vol / (avg_vol + 1e-9)
                    closes = [float(k[4]) for k in klines]
            except Exception:
                vol_ratio = 1.0
                closes = None

            if vol_ratio < 1.8:  # require ~80%+ spike
                continue

            # optional: quick RSI check using closes
            rsi_ok = True
            if closes and len(closes) >= 15:
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

            # score: weight by change and volume spike and quoteVolume
            score = change_pct * qvol * vol_ratio
            candidates.append((sym, price, qvol, change_pct, vol_ratio, score))

        except Exception:
            continue

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[-1], reverse=True)
    # Return only first 4 fields so trade_cycle unpack works:
    best = candidates[0]
    sym, price, qvol, change_pct = best[0], best[1], best[2], best[3]
    return (sym, price, qvol, change_pct)

# =========================
# MARKET BUY (unchanged semantic)
# =========================
def place_safe_market_buy(symbol, usd_amount):
    info = client.get_symbol_info(symbol)
    f = get_filters(info)
    price = float(client.get_symbol_ticker(symbol=symbol)['price'])

    qty = usd_amount / price
    qty = max(qty, f['minQty'])
    qty = round_step(qty, f['stepSize'])

    if f['minNotional']:
        notional = qty * price
        if notional < f['minNotional']:
            needed_qty = round_step(f['minNotional'] / price, f['stepSize'])
            free_usdt = get_free_usdt()
            if needed_qty * price > free_usdt + 1e-8:
                raise RuntimeError(f"Not enough funds: need≈${needed_qty*price:.2f}, free=${free_usdt:.2f}")
            qty = needed_qty

    order = client.order_market_buy(symbol=symbol, quantity=qty)
    notify(f"✅ BUY {symbol}: qty={qty} ~price={price:.8f} notional≈${qty*price:.6f}")

    # place the first OCO (uses default BASE_TP_PCT / BASE_SL_PCT)
    place_oco_sell(symbol, qty, price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)

    return qty, price

# =========================
# OCO SELL (kept compatible + optional explicit TP/SL)
# =========================
def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=1.0,
                   explicit_tp: float = None, explicit_sl: float = None,
                   retries=2, delay=1):
    """
    Robust OCO placement:
      - try standard create_oco_order (price/stopPrice/stopLimitPrice)
      - if aboveType error occurs, try alternative param names (aboveType/belowType...)
      - if both fail, fallback to separate: LIMIT TP + STOP_MARKET SL
    Returns dict {'tp': tp, 'sl': sp, 'method': 'oco'|'oco_abovebelow'|'fallback_separate'} or None.
    """
    info = client.get_symbol_info(symbol)
    if not info:
        notify(f"⚠️ place_oco_sell: couldn't fetch symbol info for {symbol}")
        return None
    f = get_filters(info)

    # compute TP / Stop trigger / stop-limit buffer
    if explicit_tp is not None:
        tp = explicit_tp
    else:
        tp = buy_price * (1 + tp_pct / 100.0)

    if explicit_sl is not None:
        sp = explicit_sl
    else:
        sp = buy_price * (1 - sl_pct / 100.0)

    stop_limit = sp * 0.999  # small buffer under stop trigger

    # clip helpers (tick/step)
    def clip(v, step):
        return math.floor(v / step) * step

    qty = clip(qty, f['stepSize'])
    tp = clip(tp, f['tickSize'])
    sp = clip(sp, f['tickSize'])
    sl = clip(stop_limit, f['tickSize'])

    if qty <= 0:
        notify("❌ place_oco_sell: quantity too small after clipping")
        return None

    tp_str = format_price(tp, f['tickSize'])
    sp_str = format_price(sp, f['tickSize'])
    sl_str = format_price(sl, f['tickSize'])

    # --- Attempt 1: standard OCO (price/stopPrice/stopLimitPrice) ---
    for attempt in range(1, retries + 1):
        try:
            oco = client.create_oco_order(
                symbol=symbol,
                side='SELL',
                quantity=str(qty),
                price=tp_str,
                stopPrice=sp_str,
                stopLimitPrice=sl_str,
                stopLimitTimeInForce='GTC'
            )
            notify(f"📌 OCO SELL placed (standard) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty}")
            return {'tp': tp, 'sl': sp, 'method': 'oco', 'raw': oco}
        except Exception as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (standard) failed: {err}")
            # If error suggests different param-set is required, break to try alt
            if 'aboveType' in err or '-1102' in err or 'Mandatory parameter' in err:
                notify("ℹ️ Detected 'aboveType' style API required, will attempt alternative param names.")
                break
            # otherwise retry small delay
            if attempt < retries:
                time.sleep(delay)
            else:
                # we'll try alternative below
                time.sleep(0.2)

    # --- Attempt 2: alternative param names (aboveType / belowType) ---
    # some versions expect aboveType/abovePrice and belowType/belowStopPrice naming
    for attempt in range(1, retries + 1):
        try:
            oco2 = client.create_oco_order(
                symbol=symbol,
                side='SELL',
                quantity=str(qty),

                # ABOVE (take profit) using alternative names
                aboveType="LIMIT_MAKER",
                abovePrice=tp_str,

                # BELOW (stop loss) using alternative names
                belowType="STOP_LOSS_LIMIT",
                belowStopPrice=sp_str,
                belowPrice=sl_str,
                belowTimeInForce="GTC"
            )
            notify(f"📌 OCO SELL placed (alt params) ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty}")
            return {'tp': tp, 'sl': sp, 'method': 'oco_abovebelow', 'raw': oco2}
        except Exception as e:
            err = str(e)
            notify(f"⚠️ OCO SELL attempt {attempt} (alt) failed: {err}")
            if attempt < retries:
                time.sleep(delay)
            else:
                time.sleep(0.2)

    # --- Fallback: place TP limit + SL stop-market separately ---
    notify("⚠️ All OCO attempts failed — falling back to separate TP (limit) + SL (stop-market).")
    try:
        # place TP limit sell
        tp_order = client.order_limit_sell(symbol=symbol, quantity=str(qty), price=tp_str)
        notify(f"📈 TP LIMIT placed (fallback): {tp_str}, qty={qty}")
    except Exception as e:
        notify(f"❌ Fallback TP limit failed: {e}")
        tp_order = None

    try:
        # place SL as stop-market (market exit when trigger hits)
        sl_order = client.create_order(
            symbol=symbol,
            side="SELL",
            type="STOP_MARKET",
            stopPrice=sp_str,
            quantity=str(qty)
        )
        notify(f"📉 SL STOP_MARKET placed (fallback): trigger={sp_str}, qty={qty}")
    except Exception as e:
        notify(f"❌ Fallback SL stop-market failed: {e}")
        sl_order = None

    if tp_order or sl_order:
        return {'tp': tp, 'sl': sp, 'method': 'fallback_separate', 'raw': {'tp': tp_order, 'sl': sl_order}}
    else:
        notify("❌ All attempts to protect position failed (no TP/SL placed).")
        return None

# =========================
# CANCEL HELPERS
# =========================
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

# =========================
# MONITOR + ROLLING LOGIC
# =========================
def monitor_and_roll(symbol, qty, entry_price, f):
    curr_tp = entry_price * (1 + BASE_TP_PCT / 100.0)
    curr_sl = entry_price * (1 - BASE_SL_PCT / 100.0)

    oco = place_oco_sell(symbol, qty, entry_price, tp_pct=BASE_TP_PCT, sl_pct=BASE_SL_PCT)
    if oco is None:
        notify(f"❌ Initial OCO failed for {symbol}, aborting monitor.")
        return False, entry_price, 0.0

    last_tp = None

    while True:
        try:
            time.sleep(SLEEP_BETWEEN_CHECKS)
            asset = symbol[:-len(QUOTE)]
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

                oco2 = place_oco_sell(symbol, get_free_asset(asset), entry_price,
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

# =========================
# MAIN TRADE CYCLE (single coin at a time)
# =========================
def trade_cycle():
    global start_balance_usdt
    if start_balance_usdt is None:
        start_balance_usdt = get_free_usdt()
        notify(f"🔰 Start balance snapshot: ${start_balance_usdt:.6f}")

    while True:
        try:
            # if any global open orders exist => wait (avoid double buys)
            open_orders_global = client.get_open_orders()
            if open_orders_global:
                notify("⏳ Still waiting for previous trade(s) to finish...")
                time.sleep(1800)   # 30 minutes as you requested for still-waiting
                continue

            candidate = pick_coin()
            if not candidate:
                notify("⚠️ No eligible coin found. Sleeping...")
                time.sleep(600)    # 10 minutes when no coins
                continue

            # unpack the 4-tuple returned by pick_coin (compatible now)
            symbol, price, volume, change = candidate
            notify(f"🎯 Selected {symbol} for market buy (24h change={change}%, vol≈{volume})")

            usd_to_buy = min(TRADE_USD, get_free_usdt())
            if usd_to_buy < 1.0:
                notify(f"⚠️ Not enough USDT to buy (free={get_free_usdt():.4f}). Sleeping...")
                time.sleep(CYCLE_DELAY)
                continue

            # === Market buy (immediately places initial OCO) ===
            qty, entry_price = place_safe_market_buy(symbol, usd_to_buy)
            info = client.get_symbol_info(symbol)
            f = get_filters(info)

            # === Monitor & roll until closed ===
            closed, exit_price, profit_usd = monitor_and_roll(symbol, qty, entry_price, f)

            # === Transfer profits to funding wallet if any ===
            if closed and profit_usd > 0:
                send_profit_to_funding(profit_usd)

        except Exception as e:
            notify(f"❌ Trade cycle unexpected error: {e}")

        # small delay before new cycle
        time.sleep(30)

# =========================
# FLASK KEEPALIVE
# =========================
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot running! ✅"

def start_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

# =========================
# RUN
# =========================
if __name__ == "__main__":
    bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    bot_thread.start()
    start_flask()