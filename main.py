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
WHITELIST_COINS = os.getenv("WHITELIST_COINS", "XRP,TIA,JTO,NEXO,VIRTUAL,LDO,AERO,MNT,EPIC,FIL").split(",")

RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.02"))
DAILY_TARGET_PCT = float(os.getenv("DAILY_TARGET_PCT", "0.20"))
MOVEMENT_MIN_PCT = float(os.getenv("MOVEMENT_MIN_PCT", "3.0"))

INITIAL_TP_PCT = float(os.getenv("INITIAL_TP_PCT", "0.20"))
INITIAL_SL_PCT = float(os.getenv("INITIAL_SL_PCT", "0.05"))
STEP_INCREMENT_PCT = float(os.getenv("STEP_INCREMENT_PCT", "0.20"))
TRIGGER_PROXIMITY = float(os.getenv("TRIGGER_PROXIMITY", "0.04"))

MIN_QUOTE_VOLUME_USD = float(os.getenv("MIN_QUOTE_VOLUME_USD", "5000000"))

SLEEP_BETWEEN_CHECKS = int(os.getenv("SLEEP_BETWEEN_CHECKS", "3"))
COOLDOWN_AFTER_EXIT = int(os.getenv("COOLDOWN_AFTER_EXIT", "5"))

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
def notify(msg: str):
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

def compute_trade_amount():
    free = get_free_usdt()
    usd_amount = max(free * RISK_PER_TRADE, 1.0)
    usd_amount = min(usd_amount, free * 0.98)
    return round(usd_amount, 6)

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

def cancel_all_open_orders(symbol):
    try:
        client.cancel_open_orders(symbol=symbol)
    except Exception as e:
        notify(f"Cancel open orders error on {symbol}: {e}")

# =========================
# SYMBOL PICKER
# =========================
def get_whitelist_pairs():
    return [f"{c.strip().upper()}{QUOTE}" for c in WHITELIST_COINS if c.strip()]

def pick_symbols_multi(top_n=3):
    pairs = get_whitelist_pairs()
    candidates = []
    tickers = {t['symbol']: t for t in client.get_ticker()}  # 24h tickers
    for sym in pairs:
        try:
            if sym not in tickers:
                continue
            t = tickers[sym]
            price = float(t.get('lastPrice', t.get('price', 0)))
            qvol = float(t['quoteVolume'])
            chg = abs(float(t['priceChangePercent']))
            if PRICE_MIN <= price <= PRICE_MAX and qvol >= MIN_QUOTE_VOLUME_USD and chg >= MOVEMENT_MIN_PCT:
                score = chg * qvol
                candidates.append((sym, price, qvol, chg, score))
        except Exception:
            continue
    candidates.sort(key=lambda x: x[-1], reverse=True)
    return candidates[:top_n]

# =========================
# TRADING FUNCTIONS
# =========================
def place_market_buy(symbol, usd_amount):
    info = client.get_symbol_info(symbol)
    f = get_filters(info)
    price = float(client.get_symbol_ticker(symbol=symbol)['price'])
    qty = usd_amount / price
    qty = max(qty, f['minQty'])
    qty = round_step(qty, f['stepSize'])
    qty = float(f"{qty:.8f}")
    if qty <= 0:
        raise RuntimeError("Computed quantity <= 0")
    if f['minNotional']:
        notional = qty * price
        if notional < f['minNotional']:
            needed_qty = round_step(f['minNotional'] / price, f['stepSize'])
            if needed_qty > qty:
                qty = needed_qty
    order = client.order_market_buy(symbol=symbol, quantity=qty)
    notify(f"✅ BUY {symbol}: qty={qty} ~price={price:.8f} notional≈${qty*price:.6f}")
    return qty, price, f

def place_oco_sell(symbol, qty, sl_price, tp_price, f):
    sl_price = round_price(sl_price, f['tickSize'])
    tp_price = round_price(tp_price, f['tickSize'])
    stop_limit_price = sl_price
    oco = client.create_oco_order(
        symbol=symbol,
        side='SELL',
        quantity=round_step(qty, f['stepSize']),
        price=str(tp_price),
        stopPrice=str(sl_price),
        stopLimitPrice=str(stop_limit_price),
        stopLimitTimeInForce='GTC'
    )
    notify(f"📌 OCO set for {symbol} SL={sl_price:.8f}, TP={tp_price:.8f}")
    return {'tp': tp_price, 'sl': sl_price, 'orderListId': oco.get('orderListId') if isinstance(oco, dict) else None}

def monitor_and_roll(symbol, qty, entry_price, f):
    tp = entry_price * (1 + INITIAL_TP_PCT)
    sl = entry_price * (1 - INITIAL_SL_PCT)
    state = place_oco_sell(symbol, qty, sl, tp, f)
    last_tp = None
    active = True

    while active:
        time.sleep(SLEEP_BETWEEN_CHECKS)
        asset = symbol[:-len(QUOTE)]
        free_qty = get_free_asset(asset)
        open_orders = client.get_open_orders(symbol=symbol)

        if free_qty < round_step(qty * 0.05, f['stepSize']) and len(open_orders) == 0:
            exit_price = float(client.get_symbol_ticker(symbol=symbol)['price'])
            profit_usd = (exit_price - entry_price) * qty
            notify(f"✅ Position closed for {symbol}: profit ≈ ${profit_usd:.6f}")
            return True, exit_price, profit_usd

        price_now = float(client.get_symbol_ticker(symbol=symbol)['price'])
        near_trigger = price_now >= state['tp'] * (1 - TRIGGER_PROXIMITY)

        if near_trigger:
            cancel_all_open_orders(symbol)
            if last_tp is None:
                new_sl = entry_price
            else:
                new_sl = last_tp
            new_tp = state['tp'] * (1 + STEP_INCREMENT_PCT)
            last_tp = state['tp']
            state = place_oco_sell(symbol, qty=free_qty, sl_price=new_sl, tp_price=new_tp, f=f)
            notify(f"🔁 Rolled OCO for {symbol}: new SL={state['sl']:.8f}, new TP={state['tp']:.8f} (px≈{price_now:.8f})")

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
# MAIN CYCLE
# =========================
def trade_cycle():
    global start_balance_usdt, paused_until
    # Set start baseline at launch
    if start_balance_usdt is None:
        start_balance_usdt = get_current_total_balance()
        notify(f"🔰 Start balance recorded: ${start_balance_usdt:.6f}")

    # Check if paused
    now = datetime.now(LOCAL_TZ)
    if paused_until and now < paused_until:
        notify(f"⏸️ Bot paused until {paused_until.isoformat()}, sleeping 60s.")
        time.sleep(60)
        return

    # Reset after pause
    if paused_until and now >= paused_until:
        paused_until = None
        start_balance_usdt = get_current_total_balance()
        notify(f"🔁 New day: baseline reset to ${start_balance_usdt:.6f}")

    # Pick candidates
    candidates = pick_symbols_multi(top_n=3)
    if not candidates:
        notify("⚠️ No good pairs found in whitelist that meet filters. Sleeping 10s.")
        time.sleep(10)
        return

    for sym_entry in candidates:
        symbol, price, qvol, chg, score = sym_entry
        notify(f"🎯 Selected {symbol} (price {price:.6f}, 24h change {chg:.2f}%, qVol≈{qvol:.0f})")

        trade_usd = compute_trade_amount()
        if trade_usd < 1.0:
            notify("⚠️ Trade USD computed < $1. Skipping.")
            continue

        try:
            qty, entry_price, f = place_market_buy(symbol, trade_usd)
        except Exception as e:
            notify(f"❌ Buy failed for {symbol}: {e}")
            continue

        # Initial OCO + rolling
        try:
            closed, exit_price, profit_usd = monitor_and_roll(symbol, qty, entry_price, f)
        except Exception as e:
            notify(f"❌ Monitoring/rolling error for {symbol}: {e}")
            closed, exit_price, profit_usd = False, None, 0.0

        if closed:
            notify(f"✅ Trade closed: {symbol}, profit ≈ ${profit_usd:.6f}")
        else:
            notify(f"⚠️ Trade ended unexpectedly for {symbol}.")

        if check_daily_target_and_pause():
            break

        time.sleep(COOLDOWN_AFTER_EXIT)

# =========================
# RUN FOREVER WITH THREADING
# =========================
def run_forever():
    notify("🤖 Scalper OCO bot started.")
    while True:
        try:
            trade_cycle()
        except Exception as e:
            notify(f"❌ Cycle error: {e}")
            time.sleep(5)

# =========================
# FLASK KEEPALIVE FOR RENDER
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
    # Run bot in a separate thread
    bot_thread = threading.Thread(target=run_forever, daemon=True)
    bot_thread.start()
    
    # Run Flask server (main thread)
    start_flask()