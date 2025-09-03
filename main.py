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

TRADE_USD = 8.0  # ⚡ test amount
SLEEP_BETWEEN_CHECKS = 60   # monitor frequency inside monitor_and_roll
CYCLE_DELAY = 15            # delay between trade cycles (you asked 15s)
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
        # python-binance wrapper uses client.universal_transfer with type
        result = client.universal_transfer(
            type='MAIN_FUNDING' if hasattr(client, 'universal_transfer') else 'SPOT_TO_FUNDING',
            asset=asset,
            amount=str(amount)
        )
        notify(f"💸 Profit ${amount:.6f} transferred to funding wallet.")
        return result
    except Exception as e:
        # Try alternate param name / fallback
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
# SYMBOL PICKER
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
    tickers = get_tickers_cached()
    candidates = []
    for t in tickers:
        sym = t['symbol']
        if not sym.endswith(QUOTE):
            continue
        try:
            price = float(t['lastPrice'])
            volume = float(t['quoteVolume'])
            change_pct = float(t['priceChangePercent'])
            if PRICE_MIN <= price <= PRICE_MAX and volume >= MIN_VOLUME and change_pct >= MOVEMENT_MIN_PCT:
                candidates.append((sym, price, volume, change_pct))
        except Exception:
            continue
    if not candidates:
        return None
    # sort by liquidity * move
    candidates.sort(key=lambda x: x[2] * x[3], reverse=True)
    return candidates[0]

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
                   retries=3, delay=2):
    """
    Places an OCO order on Spot via create_oco_order.
    By default it computes TP = buy_price*(1+tp_pct/100), SL = buy_price*(1-sl_pct/100).
    If explicit_tp/explicit_sl provided, those precise levels are used (they'll be clipped to tick).
    Returns the API response or None.
    """
    info = client.get_symbol_info(symbol)
    f = get_filters(info)

    # compute candidate TP/SL
    if explicit_tp is not None:
        tp = explicit_tp
    else:
        tp = buy_price * (1 + tp_pct / 100.0)

    if explicit_sl is not None:
        sp = explicit_sl
    else:
        sp = buy_price * (1 - sl_pct / 100.0)

    stop_limit = sp * 0.999  # small buffer below stop trigger

    # clip to tick/step
    def clip(v, step):
        return math.floor(v / step) * step

    qty = clip(qty, f['stepSize'])
    tp = clip(tp, f['tickSize'])
    sp = clip(sp, f['tickSize'])
    sl = clip(stop_limit, f['tickSize'])

    if qty <= 0:
        raise RuntimeError("❌ Quantity too small for OCO order")

    tp_str = format_price(tp, f['tickSize'])
    sp_str = format_price(sp, f['tickSize'])
    sl_str = format_price(sl, f['tickSize'])

    for attempt in range(1, retries + 1):
        try:
            # create_oco_order expects price, stopPrice, stopLimitPrice, stopLimitTimeInForce
            oco = client.create_oco_order(
                symbol=symbol,
                side='SELL',
                quantity=str(qty),
                price=tp_str,
                stopPrice=sp_str,
                stopLimitPrice=sl_str,
                stopLimitTimeInForce='GTC'
            )
            notify(f"📌 OCO SELL placed ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty}")
            return oco
        except Exception as e:
            notify(f"⚠️ OCO SELL attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                notify("❌ OCO SELL failed after retries.")
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
    """
    Monitor an OCO placed by place_safe_market_buy. When price nears current TP,
    cancel & re-place OCO with higher TP and SL moved up.
    Returns (closed_bool, exit_price, profit_usd)
    """
    # initial state (use same TP/SL logic as place_oco_sell)
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

                # new SL becomes previous TP (or entry if first)
                new_sl = entry_price if last_tp is None else last_tp
                # new TP increases by STEP_INCREMENT_PCT on top of current TP
                new_tp = curr_tp * (1 + STEP_INCREMENT_PCT)

                # we want to call place_oco_sell with explicit TP/SL levels.
                # place_oco_sell expects "buy_price" base but we added explicit_tp/explicit_sl to pass levels directly.
                oco2 = place_oco_sell(symbol, free_qty, entry_price,
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
            # break/return so main cycle can decide next steps
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
                time.sleep(CYCLE_DELAY)
                continue

            candidate = pick_coin()
            if not candidate:
                notify("⚠️ No eligible coin found. Sleeping...")
                time.sleep(CYCLE_DELAY)
                continue

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
        time.sleep(CYCLE_DELAY)

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
