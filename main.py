import os
import time
import math
import threading
import requests
from datetime import datetime
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

MOVEMENT_MIN_PCT = float(os.getenv("MOVEMENT_MIN_PCT", "1.0"))  # minimum spike %
MIN_QUOTE_VOLUME_USD = float(os.getenv("MIN_QUOTE_VOLUME_USD", "1000000"))  # 1M

MAX_CANDIDATES = int(os.getenv("MAX_CANDIDATES", "3"))
TRADE_USD = float(os.getenv("TRADE_USD", "10"))

SLEEP_BETWEEN_CHECKS = int(os.getenv("SLEEP_BETWEEN_CHECKS", "5"))
LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Africa/Dar_es_Salaam"))

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
swm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
PRICE_DATA = {}
LOCK = threading.Lock()

# =========================
# UTILITIES
# =========================
LAST_NOTIFY = 0

def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 5:  # Throttle
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

def get_free_usdt():
    try:
        bal = client.get_asset_balance(asset=QUOTE)
        return float(bal['free'])
    except:
        return 0.0

def round_step(n, step):
    return math.floor(n / step) * step if step else n

# =========================
# SOCKET STREAM CALLBACK
# =========================
def ticker_callback(msg):
    """Real-time ticker update from WebSocket"""
    if msg['e'] != '24hrTicker':
        return
    symbol = msg['s']
    if not symbol.endswith(QUOTE):
        return
    price = float(msg['c'])
    change_pct = abs(float(msg['P']))  # 24h price change %
    volume = float(msg['q']) * price   # quote volume in USD approx
    with LOCK:
        PRICE_DATA[symbol] = {
            'price': price,
            'change_pct': change_pct,
            'volume': volume
        }

# =========================
# CANDIDATE SELECTION
# =========================
def select_candidates():
    candidates = []
    with LOCK:
        for sym, data in PRICE_DATA.items():
            price = data['price']
            vol = data['volume']
            change = data['change_pct']
            if PRICE_MIN <= price <= PRICE_MAX and vol >= MIN_QUOTE_VOLUME_USD and change >= MOVEMENT_MIN_PCT:
                score = change * vol
                candidates.append((sym, price, vol, change, score))
    candidates.sort(key=lambda x: x[-1], reverse=True)
    return candidates[:MAX_CANDIDATES]

# =========================
# MARKET BUY FUNCTION
# =========================
def place_market_buy(symbol, usd_amount):
    try:
        info = client.get_symbol_info(symbol)
        f = {f['filterType']: f for f in info['filters']}
        stepSize = float(f['LOT_SIZE']['stepSize'])
        minQty = float(f['LOT_SIZE']['minQty'])
        price = float(client.get_symbol_ticker(symbol=symbol)['price'])
        qty = round_step(max(usd_amount / price, minQty), stepSize)
        order = client.order_market_buy(symbol=symbol, quantity=qty)
        notify(f"✅ BUY {symbol}: qty={qty} ~price={price:.8f}")
        return qty, price
    except Exception as e:
        notify(f"❌ Buy failed for {symbol}: {e}")
        return None, None

# =========================
# TRADE CYCLE
# =========================
def trade_cycle():
    free_usdt = get_free_usdt()
    if free_usdt < 1.0:
        notify("⚠️ Not enough USDT to trade.")
        return
    candidates = select_candidates()
    if not candidates:
        notify("⚠️ No eligible coins at this moment.")
        return
    for sym, price, vol, change, score in candidates:
        if free_usdt < 1.0:
            break
        qty, bought_price = place_market_buy(sym, min(TRADE_USD, free_usdt))
        if qty:
            free_usdt -= qty * bought_price

# =========================
# RUN FOREVER
# =========================
def run_forever():
    notify("🤖 Real-time scalper bot started.")
    while True:
        try:
            trade_cycle()
            time.sleep(SLEEP_BETWEEN_CHECKS)
        except Exception as e:
            notify(f"❌ Cycle error: {e}")
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
    # Start WebSocket
    swm.start()
    swm.start_symbol_ticker_socket(callback=ticker_callback, symbol="!ticker@arr")
    
    # Bot thread
    bot_thread = threading.Thread(target=run_forever, daemon=True)
    bot_thread.start()
    
    # Flask server
    start_flask()
