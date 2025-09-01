import os
import time
import math
import threading
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask
from binance.client import Client
from binance.websocket.spot.websocket_client import SpotWebsocketClient as WsClient

# =========================
# CONFIG
# =========================
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

QUOTE = "USDT"
PRICE_MIN = 0.3
PRICE_MAX = 6.0
MIN_VOLUME_USD = 500_000
MOVEMENT_MIN_PCT = 0.5
CANDIDATES_PER_CYCLE = 1
TRADE_USD = 10.0

SLEEP_BETWEEN_CHECKS = 10
LOCAL_TZ = ZoneInfo("Africa/Dar_es_Salaam")

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
LOCKED_COINS = set()

def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 10:
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
        return float(client.get_asset_balance(asset=QUOTE)['free'])
    except:
        return 0.0

def round_step(n, step):
    return math.floor(n / step) * step if step else n

# =========================
# CANDIDATE FILTER
# =========================
candidates = {}

def handle_ticker(msg):
    """
    Real-time ticker from WebSocket
    """
    if 's' not in msg or 'c' not in msg or 'v' not in msg or 'P' not in msg:
        return
    sym = msg['s']
    price = float(msg['c'])
    volume = float(msg['v'])
    change_pct = abs(float(msg['P']))

    if not sym.endswith(QUOTE):
        return
    if PRICE_MIN <= price <= PRICE_MAX and volume >= MIN_VOLUME_USD and change_pct >= MOVEMENT_MIN_PCT:
        candidates[sym] = {'price': price, 'volume': volume, 'change': change_pct}

def pick_candidates():
    # Pick top N coins by change * volume
    sorted_coins = sorted(
        candidates.items(),
        key=lambda x: x[1]['change'] * x[1]['volume'],
        reverse=True
    )
    picked = []
    for sym, data in sorted_coins:
        if sym in LOCKED_COINS:
            continue
        picked.append((sym, data['price']))
        if len(picked) >= CANDIDATES_PER_CYCLE:
            break
    return picked

# =========================
# TRADING FUNCTION
# =========================
def buy_coin(symbol, price):
    free_usdt = get_free_usdt()
    qty = round_step(min(TRADE_USD, free_usdt) / price, 0.0001)
    if qty <= 0:
        notify(f"❌ Not enough USDT to buy {symbol}")
        return False
    try:
        client.order_market_buy(symbol=symbol, quantity=qty)
        notify(f"✅ Bought {symbol} qty={qty} at price≈{price}")
        LOCKED_COINS.add(symbol)
        return True
    except Exception as e:
        notify(f"❌ Failed to buy {symbol}: {e}")
        return False

# =========================
# MAIN BOT LOOP
# =========================
def trade_cycle():
    picked = pick_candidates()
    if not picked:
        notify("⚠️ No eligible coins at this moment.")
        return
    for sym, price in picked:
        buy_coin(sym, price)

# =========================
# RUN WEBSOCKET
# =========================
def start_ws():
    ws_client = WsClient()
    ws_client.start()
    ws_client.ticker_socket(handle_ticker)
    return ws_client

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
# RUN BOTH THREADS
# =========================
if __name__ == "__main__":
    ws_thread = threading.Thread(target=start_ws, daemon=True)
    ws_thread.start()

    bot_thread = threading.Thread(target=lambda: [trade_cycle() or time.sleep(SLEEP_BETWEEN_CHECKS) for _ in iter(int, 1)], daemon=True)
    bot_thread.start()

    start_flask()
