import os
import time
import math
import threading
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask
from binance.client import Client
from binance.websocket.spot import SpotWebsocketClient

# =========================
# CONFIG
# =========================
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

PRICE_MIN = float(os.getenv("PRICE_MIN", "0.5"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "5.0"))
QUOTE = os.getenv("QUOTE", "USDT")

MOVEMENT_MIN_PCT = float(os.getenv("MOVEMENT_MIN_PCT", "1.0"))
MIN_QUOTE_VOLUME_USD = float(os.getenv("MIN_QUOTE_VOLUME_USD", "1000000"))

INITIAL_TP_PCT = float(os.getenv("INITIAL_TP_PCT", "0.20"))
INITIAL_SL_PCT = float(os.getenv("INITIAL_SL_PCT", "0.05"))

SLEEP_BETWEEN_CHECKS = int(os.getenv("SLEEP_BETWEEN_CHECKS", "15"))
COOLDOWN_AFTER_EXIT = int(os.getenv("COOLDOWN_AFTER_EXIT", "30"))

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Africa/Dar_es_Salaam"))

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
ws_client = SpotWebsocketClient()
start_balance_usdt = None
paused_until = None

# =========================
# UTILITIES
# =========================
LAST_NOTIFY = 0

def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 15:  # throttle 15s
        return
    LAST_NOTIFY = now_ts
    t = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    text = f"[{t}] {msg}"
    print(text)
    if BOT_TOKEN and CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data={"chat_id": CHAT_ID, "text": text}
            )
        except:
            pass

def get_free_usdt():
    try:
        bal = client.get_asset_balance(asset=QUOTE)
        return float(bal['free'])
    except:
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

# =========================
# CANDIDATE SELECTION
# =========================
CANDIDATES = []

def process_tick(msg):
    try:
        symbol = msg['s']
        price = float(msg['c'])
        change_pct = abs(float(msg['P']))
        volume = float(msg['q'])

        if not symbol.endswith(QUOTE):
            return

        if PRICE_MIN <= price <= PRICE_MAX and volume >= MIN_QUOTE_VOLUME_USD and change_pct >= MOVEMENT_MIN_PCT:
            if symbol not in CANDIDATES:
                CANDIDATES.append((symbol, price, change_pct, volume))
    except:
        pass

def start_ws():
    ws_client.start()
    ws_client.ticker(symbol="!ticker@arr", callback=process_tick)

# =========================
# TRADING
# =========================
LOCKED_SYMBOL = None
active_trades = []

def place_market_buy(symbol, usd_amount):
    info = client.get_symbol_info(symbol)
    f = get_filters(info)
    price = float(client.get_symbol_ticker(symbol=symbol)['price'])
    qty = usd_amount / price
    qty = max(qty, f['minQty'])
    qty = round_step(qty, f['stepSize'])
    if f['minNotional']:
        notional = qty * price
        if notional < f['minNotional']:
            qty = round_step(f['minNotional'] / price, f['stepSize'])
    order = client.order_market_buy(symbol=symbol, quantity=qty)
    notify(f"✅ Bought {symbol}: qty={qty}, price={price:.8f}")
    return qty, price, f

def trade_cycle():
    global LOCKED_SYMBOL, active_trades, start_balance_usdt, paused_until

    if start_balance_usdt is None:
        start_balance_usdt = get_free_usdt()
        notify(f"🔰 Start balance recorded: ${start_balance_usdt:.2f}")

    if paused_until and datetime.now(LOCAL_TZ) < paused_until:
        notify(f"⏸️ Bot paused until {paused_until.isoformat()}")
        time.sleep(15)
        return

    if LOCKED_SYMBOL or len(active_trades) > 0:
        return  # Skip if already trading

    # Pick first candidate
    if not CANDIDATES:
        notify("⚠️ No eligible coins at this moment")
        time.sleep(15)
        return

    symbol, price, change_pct, vol = CANDIDATES.pop(0)
    LOCKED_SYMBOL = symbol
    trade_usd = min(10.0, get_free_usdt())

    try:
        qty, entry_price, f = place_market_buy(symbol, trade_usd)
        active_trades.append({'symbol': symbol, 'qty': qty, 'entry_price': entry_price, 'filters': f})
    except Exception as e:
        notify(f"❌ Buy failed for {symbol}: {e}")
        LOCKED_SYMBOL = None

# =========================
# FLASK
# =========================
app = Flask(__name__)
@app.route("/")
def home():
    return "Bot is running! ✅"

def start_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

# =========================
# RUN FOREVER
# =========================
def run_forever():
    notify("🤖 Bot started")
    ws_thread = threading.Thread(target=start_ws, daemon=True)
    ws_thread.start()

    while True:
        try:
            trade_cycle()
            time.sleep(SLEEP_BETWEEN_CHECKS)
        except Exception as e:
            notify(f"❌ Cycle error: {e}")
            time.sleep(15)  # Throttle error notifications
