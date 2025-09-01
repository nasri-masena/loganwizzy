import os
import time
import math
import threading
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask
from binance.client import Client
from binance import ThreadedWebsocketManager

# =========================
# CONFIG
# =========================
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
QUOTE = os.getenv("QUOTE", "USDT")

PRICE_MIN = float(os.getenv("PRICE_MIN", "1.0"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "3.0"))
MIN_QUOTE_VOLUME_USD = float(os.getenv("MIN_QUOTE_VOLUME_USD", "5000000"))
MOVEMENT_MIN_PCT = float(os.getenv("MOVEMENT_MIN_PCT", "3.0"))

TRADE_USD = float(os.getenv("TRADE_USD", "20.0"))
LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Africa/Dar_es_Salaam"))

SLEEP_AFTER_BUY = int(os.getenv("SLEEP_AFTER_BUY", "2"))

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
active_symbols = set()  # unique coins already bought

# =========================
# UTILITIES
# =========================
LAST_NOTIFY = 0
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
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            requests.post(url, data={"chat_id": CHAT_ID, "text": text})
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
# REAL-TIME TICKERS USING WS
# =========================
real_time_data = {}

def process_message(msg):
    if 's' in msg and 'c' in msg and 'Q' in msg:
        symbol = msg['s']
        price = float(msg['c'])
        volume = float(msg['Q'])
        change_pct = float(msg.get('P', 0))
        real_time_data[symbol] = {
            'price': price,
            'volume': volume,
            'change_pct': change_pct
        }

def start_ws():
    twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm.start()
    twm.start_symbol_ticker_socket(callback=process_message, symbol=None)  # all symbols
    return twm

# =========================
# PICK COINS TO BUY
# =========================
def pick_symbols_real_time(top_n=1):
    candidates = []
    for sym, info in real_time_data.items():
        if not sym.endswith(QUOTE):
            continue
        price = info['price']
        volume = info['volume']
        change = info['change_pct']
        if PRICE_MIN <= price <= PRICE_MAX and volume >= MIN_QUOTE_VOLUME_USD and change >= MOVEMENT_MIN_PCT:
            score = change * volume
            candidates.append((sym, price, volume, change, score))
    candidates.sort(key=lambda x: x[-1], reverse=True)
    return [c for c in candidates if c[0] not in active_symbols][:top_n]

# =========================
# MARKET BUY FUNCTION
# =========================
def place_market_buy(symbol, usd_amount):
    info = client.get_symbol_info(symbol)
    f = get_filters(info)
    price = float(client.get_symbol_ticker(symbol=symbol)['price'])

    qty = usd_amount / price
    qty = max(qty, f['minQty'])
    qty = round_step(qty, f['stepSize'])

    if f['minNotional'] and qty * price < f['minNotional']:
        qty = round_step(f['minNotional'] / price, f['stepSize'])

    order = client.order_market_buy(symbol=symbol, quantity=qty)
    notify(f"✅ BUY {symbol}: qty={qty}, price≈{price}, notional≈${qty*price:.6f}")
    active_symbols.add(symbol)
    return qty, price

# =========================
# TRADE CYCLE
# =========================
def trade_cycle():
    candidates = pick_symbols_real_time(top_n=1)
    if not candidates:
        notify("⚠️ No coins meet the criteria. Waiting 5s.")
        time.sleep(5)
        return

    symbol, price, volume, change, score = candidates[0]
    notify(f"🎯 Selected {symbol} (price≈{price}, change={change}%, volume={volume})")

    free_usdt = get_free_usdt()
    usd_amount = min(TRADE_USD, free_usdt)
    if usd_amount < 1.0:
        notify(f"⚠️ Not enough USDT to buy {symbol}. Free={free_usdt}")
        return

    try:
        qty, buy_price = place_market_buy(symbol, usd_amount)
        time.sleep(SLEEP_AFTER_BUY)
    except Exception as e:
        notify(f"❌ Failed to buy {symbol}: {e}")

# =========================
# RUN FOREVER
# =========================
def run_forever():
    notify("🤖 Real-time Scalper Bot started.")
    while True:
        try:
            trade_cycle()
        except Exception as e:
            notify(f"❌ Trade cycle error: {e}")
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
# START BOT
# =========================
if __name__ == "__main__":
    # Start WS in separate thread
    ws_thread = threading.Thread(target=start_ws, daemon=True)
    ws_thread.start()

    # Start bot in separate thread
    bot_thread = threading.Thread(target=run_forever, daemon=True)
    bot_thread.start()

    # Run Flask (main thread)
    start_flask()
