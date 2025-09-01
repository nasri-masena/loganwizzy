import os
import time
import math
import threading
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask
from binance.client import Client

# =========================
# CONFIG
# =========================
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

PRICE_MIN = float(os.getenv("PRICE_MIN", "1.0"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "3.0"))
QUOTE = os.getenv("QUOTE", "USDT")

RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.02"))
DAILY_TARGET_PCT = float(os.getenv("DAILY_TARGET_PCT", "0.20"))
MOVEMENT_MIN_PCT = float(os.getenv("MOVEMENT_MIN_PCT", "3.0"))

INITIAL_TP_PCT = float(os.getenv("INITIAL_TP_PCT", "0.20"))
INITIAL_SL_PCT = float(os.getenv("INITIAL_SL_PCT", "0.05"))
STEP_INCREMENT_PCT = float(os.getenv("STEP_INCREMENT_PCT", "0.03"))
TRIGGER_PROXIMITY = float(os.getenv("TRIGGER_PROXIMITY", "0.04"))

MIN_QUOTE_VOLUME_USD = float(os.getenv("MIN_QUOTE_VOLUME_USD", "5000000"))

SLEEP_BETWEEN_CHECKS = int(os.getenv("SLEEP_BETWEEN_CHECKS", "15"))
COOLDOWN_AFTER_EXIT = int(os.getenv("COOLDOWN_AFTER_EXIT", "30"))

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Africa/Dar_es_Salaam"))

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
start_balance_usdt = None
paused_until = None

# =========================
# UTILITIES
# =========================
LAST_NOTIFY = 0

def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 15:
        return
    LAST_NOTIFY = now_ts
    t = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    text = f"[{t}] {msg}"
    print(text)
    if BOT_TOKEN and CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            requests.post(url, data={"chat_id": CHAT_ID, "text": text})
        except Exception as e:
            print("Telegram notify error:", e)

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

def next_midnight_local():
    now = datetime.now(LOCAL_TZ)
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return tomorrow

def round_step(n, step):
    return math.floor(n / step) * step if step else n

def round_price(p, tick):
    return math.floor(p / tick) * tick if tick else p

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

# ===== REST caching =====
TICKER_CACHE = None
LAST_TICKER_FETCH = 0.0
TICKER_TTL = 60.0

def get_tickers_cached():
    global TICKER_CACHE, LAST_TICKER_FETCH
    now = time.time()
    if TICKER_CACHE is None or (now - LAST_TICKER_FETCH) > TICKER_TTL:
        TICKER_CACHE = client.get_ticker()
        LAST_TICKER_FETCH = now
    return TICKER_CACHE

def cancel_all_open_orders(symbol):
    try:
        client.cancel_open_orders(symbol=symbol)
    except Exception as e:
        notify(f"Cancel open orders error on {symbol}: {e}")

# =========================
# SYMBOL PICKER
# =========================
def pick_symbols_multi(top_n=1, price_min=PRICE_MIN, price_max=PRICE_MAX,
                       min_volume=MIN_QUOTE_VOLUME_USD, movement_min_pct=MOVEMENT_MIN_PCT):
    tickers = get_tickers_cached()
    candidates = []

    for t in tickers:
        sym = t['symbol']
        if not sym.endswith(QUOTE):
            continue
        try:
            price = float(t['lastPrice'])
            volume = float(t['quoteVolume'])
            change = abs(float(t['priceChangePercent']))
            if price_min <= price <= price_max and volume >= min_volume and change >= movement_min_pct:
                score = change * volume
                candidates.append((sym, price, volume, change, score))
        except:
            continue

    candidates.sort(key=lambda x: x[-1], reverse=True)
    return candidates[:top_n]

# =========================
# TRADING FUNCTIONS
# =========================
def place_safe_market_buy(symbol, usd_amount):
    info = client.get_symbol_info(symbol)
    f = get_filters(info)
    price = float(client.get_symbol_ticker(symbol=symbol)['price'])
    qty = usd_amount / price
    qty = max(qty, f['minQty'])
    qty = round_step(qty, f['stepSize'])
    qty = float(f"{qty:.8f}")
    if f['minNotional']:
        notional = qty * price
        if notional < f['minNotional']:
            needed_qty = round_step(f['minNotional'] / price, f['stepSize'])
            free_usdt = get_free_usdt()
            if needed_qty * price > free_usdt + 1e-8:
                raise RuntimeError(f"Not enough funds to meet MIN_NOTIONAL (${f['minNotional']:.2f})")
            qty = needed_qty
    order = client.order_market_buy(symbol=symbol, quantity=qty)
    notify(f"✅ BUY {symbol}: qty={qty} ~price={price:.8f} notional≈${qty*price:.6f}")
    return qty, price, f

def place_safe_oco(symbol, intended_qty, entry_price, f):
    free_qty = get_free_asset(symbol[:-len(QUOTE)])
    qty = min(intended_qty, free_qty)
    qty = max(round_step(qty, f['stepSize']), f['minQty'])
    tp_price = round_price(entry_price * (1 + INITIAL_TP_PCT), f['tickSize'])
    sl_price = round_price(entry_price * (1 - INITIAL_SL_PCT), f['tickSize'])
    stop_limit_price = round_price(sl_price * 0.999, f['tickSize'])
    if f['minNotional']:
        notional = qty * tp_price
        if notional < f['minNotional']:
            needed_qty = round_step(f['minNotional'] / tp_price, f['stepSize'])
            qty = min(needed_qty, free_qty)
    try:
        oco = client.create_oco_order(
            symbol=symbol,
            side='SELL',
            quantity=qty,
            price=str(tp_price),
            stopPrice=str(sl_price),
            stopLimitPrice=str(stop_limit_price),
            stopLimitTimeInForce='GTC'
        )
        notify(f"📌 OCO set for {symbol}: SL={sl_price}, TP={tp_price}, qty={qty}")
        return {'tp': tp_price, 'sl': sl_price, 'orderListId': oco.get('orderListId')}
    except Exception as e:
        notify(f"❌ Failed to place OCO for {symbol}: {e}")
        return None

# =========================
# DAILY TARGET
# =========================
def get_current_total_balance():
    return get_free_usdt()

def check_daily_target_and_pause():
    global paused_until, start_balance_usdt
    if start_balance_usdt is None:
        return False
    current = get_current_total_balance()
    profit_pct = (current - start_balance_usdt) / start_balance_usdt
    if profit_pct >= DAILY_TARGET_PCT:
        paused_until = next_midnight_local()
        notify(f"🚀 Daily Target Achieved: {profit_pct*100:.2f}%. Pausing until {paused_until.isoformat()}")
        return True
    return False

# =========================
# ACTIVE TRADES TRACKER
# =========================
MAX_ACTIVE_TRADES = 1
active_trades = []
LOCKED_SYMBOL = None

# =========================
# TRADE CYCLE
# =========================
def trade_cycle():
    global start_balance_usdt, paused_until, active_trades, LOCKED_SYMBOL

    if start_balance_usdt is None:
        start_balance_usdt = get_current_total_balance()
        notify(f"🔰 Start balance recorded: ${start_balance_usdt:.6f}")

    now = datetime.now(LOCAL_TZ)
    if paused_until and now < paused_until:
        notify(f"⏸️ Bot paused until {paused_until.isoformat()}, sleeping 60s.")
        time.sleep(60)
        return
    if paused_until and now >= paused_until:
        paused_until = None
        start_balance_usdt = get_current_total_balance()
        notify(f"🔁 New day: baseline reset to ${start_balance_usdt:.6f}")

    if len(active_trades) > 0:
        notify("⏳ Trade in progress, skipping new buys.")
        return

    if LOCKED_SYMBOL is None:
        candidates = pick_symbols_multi(top_n=1)
        if not candidates:
            notify("⚠️ No coins meet filters. Sleeping 10s.")
            time.sleep(10)
            return
        symbol, price, qvol, chg, score = candidates[0]
        LOCKED_SYMBOL = symbol
    else:
        symbol = LOCKED_SYMBOL

    notify(f"🎯 Selected {symbol}")
    trade_usd = max(min(19.0, get_current_total_balance()), 0.01)

    try:
        qty, entry_price, f = place_safe_market_buy(symbol, trade_usd)
        active_trades.append({'symbol': symbol, 'qty': qty, 'entry_price': entry_price, 'filters': f})
        notify(f"✅ Buy success: {symbol}, qty={qty}, entry={entry_price:.8f}")
        time.sleep(2)
    except Exception as e:
        notify(f"❌ Buy failed for {symbol}: {e}")
        LOCKED_SYMBOL = None
        return

    try:
        state = place_safe_oco(symbol, qty, entry_price, f)
        if not state:
            active_trades = [t for t in active_trades if t['symbol'] != symbol]
            LOCKED_SYMBOL = None
            return
    except Exception as e:
        notify(f"❌ OCO failed for {symbol}: {e}")
        active_trades = [t for t in active_trades if t['symbol'] != symbol]
        LOCKED_SYMBOL = None
        return

    last_tp = None
    active = True
    while active:
        time.sleep(SLEEP_BETWEEN_CHECKS)
        asset = symbol[:-len(QUOTE)]
        free_qty = get_free_asset(asset)
        open_orders = client.get_open_orders(symbol=symbol)
        price_now = float(client.get_symbol_ticker(symbol=symbol)['price'])
        if free_qty < round_step(qty * 0.05, f['stepSize']) and len(open_orders) == 0:
            exit_price = price_now
            profit_usd = (exit_price - entry_price) * qty
            notify(f"✅ Position closed for {symbol}: profit ≈ ${profit_usd:.6f}")
            active = False
            break
        near_trigger = price_now >= state['tp'] * (1 - TRIGGER_PROXIMITY)
        if near_trigger:
            cancel_all_open_orders(symbol)
            new_sl = entry_price if last_tp is None else last_tp
            new_tp = state['tp'] * (1 + STEP_INCREMENT_PCT)
            last_tp = state['tp']
            state = place_safe_oco(symbol, free_qty, new_tp, f)
            notify(f"🔁 Rolled OCO for {symbol}: new SL={state['sl']:.8f}, new TP={state['tp']:.8f} (price≈{price_now:.8f})")

    active_trades = [t for t in active_trades if t['symbol'] != symbol]
    LOCKED_SYMBOL = None
    check_daily_target_and_pause()
    time.sleep(COOLDOWN_AFTER_EXIT)

# =========================
# RUN FOREVER
# =========================
def run_forever():
    notify("🤖 Scalper OCO bot started.")
    soft_backoff = 90
    hard_backoff = 600
    while True:
        try:
            trade_cycle()
        except Exception as e:
            msg = str(e)
            if "-1003" in msg or "Too many requests" in msg:
                if "IP banned" in msg or "too many requests" in msg.lower():
                    notify("⛔ IP banned (weight). Pausing hard to cool down.")
                    time.sleep(hard_backoff)
                else:
                    notify("⚠️ Weight limit hit. Cooling down briefly.")
                    time.sleep(soft_backoff)
                continue
            elif "MIN_NOTIONAL" in msg or "quantity" in msg or "lot size" in msg:
                notify(f"⚠️ Trade calculation/API error: {msg}. Skipping cycle.")
                time.sleep(10)
                continue
            else:
                notify(f"❌ Cycle error: {msg}. Retrying in 5s.")
                time.sleep(5)

# =========================
# FLASK KEEPALIVE
# =========================
app = Flask(__name__)
@app.route("/")
def home():
    return "Bot is running! ✅"

def start_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

# =========================
# START BOTH THREADS
# =========================
if __name__ == "__main__":
    bot_thread = threading.Thread(target=run_forever, daemon=True)
    bot_thread.start