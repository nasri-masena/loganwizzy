import os
import time
import math
import threading
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask
from binance.client import Client
from binance.streams import ThreadedWebsocketManager

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

MIN_VOLUME_USD = float(os.getenv("MIN_VOLUME_USD", "1000000"))
MIN_PRICE_CHANGE = float(os.getenv("MIN_PRICE_CHANGE", "1.0"))  # percent

TRADE_AMOUNT_USD = float(os.getenv("TRADE_AMOUNT_USD", "10"))

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Africa/Dar_es_Salaam"))

SLEEP_BETWEEN_CHECKS = int(os.getenv("SLEEP_BETWEEN_CHECKS", "5"))

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
selected_coin = None
tickers_data = {}

# =========================
# UTILITIES
# =========================
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
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data={"chat_id": CHAT_ID, "text": text}
            )
        except Exception as e:
            print("Telegram notify error:", e)

def round_step(n, step):
    return math.floor(n / step) * step if step else n

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

def get_free_usdt():
    try:
        bal = client.get_asset_balance(asset=QUOTE)
        return float(bal['free'])
    except:
        return 0.0

# =========================
# WEBSOCKET CALLBACK
# =========================
def handle_ticker(msg):
    if msg['e'] != '24hrTicker':
        return
    symbol = msg['s']
    tickers_data[symbol] = {
        'price': float(msg['c']),
        'volume': float(msg['q']),
        'price_change_percent': abs(float(msg['P']))
    }

# =========================
# CANDIDATE SELECTION
# =========================
def pick_candidate():
    candidates = []
    for sym, data in tickers_data.items():
        if not sym.endswith(QUOTE):
            continue
        price = data['price']
        volume = data['volume']
        change = data['price_change_percent']
        if PRICE_MIN <= price <= PRICE_MAX and volume >= MIN_VOLUME_USD and change >= MIN_PRICE_CHANGE:
            candidates.append((sym, price, volume, change))
    if not candidates:
        return None
    # Sort by highest change*volume
    candidates.sort(key=lambda x: x[3]*x[2], reverse=True)
    return candidates[0][0]  # Return symbol only (unique coin)

# =========================
# BUY FUNCTION
# =========================
def place_market_buy(symbol):
    info = client.get_symbol_info(symbol)
    f = get_filters(info)
    price = float(client.get_symbol_ticker(symbol=symbol)['price'])
    qty = TRADE_AMOUNT_USD / price
    qty = max(qty, f['minQty'])
    qty = round_step(qty, f['stepSize'])
    if f['minNotional'] and qty*price < f['minNotional']:
        notify(f"⚠️ Not enough to meet MIN_NOTIONAL for {symbol}")
        return False
    order = client.order_market_buy(symbol=symbol, quantity=qty)
    notify(f"✅ Bought {symbol}: qty={qty}, price≈{price}")
    return True

# =========================
# BOT CYCLE
# =========================
def bot_cycle():
    global selected_coin
    if selected_coin is not None:
        return  # Already bought a coin this cycle
    symbol = pick_candidate()
    if symbol:
        selected_coin = symbol
        place_market_buy(symbol)
    else:
        notify("⚠️ No eligible coins at this moment.")

# =========================
# WEBSOCKET START
# =========================
def start_ws():
    twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm.start()
    twm.start_symbol_ticker_socket(callback=handle_ticker)
    return twm

# =========================
# RUN FOREVER
# =========================
def run_forever():
    notify("🤖 Scalper bot started.")
    while True:
        try:
            bot_cycle()
        except Exception as e:
            notify(f"❌ Bot cycle error: {e}")
        time.sleep(SLEEP_BETWEEN_CHECKS)

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
# START BOT
# =========================
if __name__ == "__main__":
    ws = start_ws()
    bot_thread = threading.Thread(target=run_forever, daemon=True)
    bot_thread.start()
    start_flask()
