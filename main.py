import os
import time
import math
import threading
import requests
from datetime import datetime, timedelta
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

PRICE_MIN = float(os.getenv("PRICE_MIN", "0.5"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "5.0"))
QUOTE = os.getenv("QUOTE", "USDT")

MIN_VOLUME_USD = float(os.getenv("MIN_VOLUME_USD", "1000000"))  # 1M USD
MIN_CHANGE_PCT = float(os.getenv("MIN_CHANGE_PCT", "1.0"))     # 1%

SLEEP_BETWEEN_CHECKS = int(os.getenv("SLEEP_BETWEEN_CHECKS", "10"))
LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Africa/Dar_es_Salaam"))

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
LOCKED_SYMBOL = None
CANDIDATES = {}
TICKERS = {}

# =========================
# UTILITIES
# =========================
def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 5:
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
    except:
        return 0.0

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

# =========================
# WEBSOCKET CALLBACK
# =========================
def handle_ticker(msg):
    global TICKERS
    if 'data' in msg and isinstance(msg['data'], list):
        for t in msg['data']:
            sym = t['s']
            price = float(t['c'])
            change_pct = abs(float(t['P']))
            volume = float(t['q']) * price  # approximate quote volume
            TICKERS[sym] = {
                'price': price,
                'change_pct': change_pct,
                'volume': volume
            }

# =========================
# SYMBOL PICKER
# =========================
def pick_candidate():
    global TICKERS, LOCKED_SYMBOL
    candidates = []
    for sym, data in TICKERS.items():
        if not sym.endswith(QUOTE):
            continue
        if PRICE_MIN <= data['price'] <= PRICE_MAX \
            and data['volume'] >= MIN_VOLUME_USD \
            and data['change_pct'] >= MIN_CHANGE_PCT:
            candidates.append((sym, data['price'], data['change_pct'], data['volume']))

    # Sort by change*volume descending
    candidates.sort(key=lambda x: x[2]*x[3], reverse=True)
    if candidates:
        LOCKED_SYMBOL = candidates[0][0]
        return candidates[0][0], candidates[0][1]
    return None, None

# =========================
# TRADE FUNCTION
# =========================
def place_market_buy(symbol, usd_amount):
    info = client.get_symbol_info(symbol)
    f = get_filters(info)
    price = float(client.get_symbol_ticker(symbol=symbol)['price'])
    qty = usd_amount / price
    qty = max(qty, f['minQty'])
    qty = round_step(qty, f['stepSize'])
    qty = float(f"{qty:.8f}")
    # Ensure minNotional
    if f['minNotional'] and qty*price < f['minNotional']:
        needed_qty = round_step(f['minNotional']/price, f['stepSize'])
        free_usdt = get_free_usdt()
        if needed_qty*price > free_usdt:
            notify(f"❌ Not enough USDT for {symbol}, need ${needed_qty*price:.2f}")
            return False
        qty = needed_qty
    try:
        client.order_market_buy(symbol=symbol, quantity=qty)
        notify(f"✅ Bought {symbol}: qty={qty} at price≈{price:.8f}")
        return True
    except Exception as e:
        notify(f"❌ Market buy failed {symbol}: {e}")
        return False

# =========================
# MAIN BOT LOOP
# =========================
def trade_cycle():
    free_usdt = get_free_usdt()
    if free_usdt < 1.0:
        notify("⚠️ Low USDT balance")
        return
    sym, price = pick_candidate()
    if sym:
        place_market_buy(sym, min(19, free_usdt))
    else:
        notify("⚠️ No eligible coins at this moment.")

# =========================
# RUN WEBSOCKET
# =========================
def start_ws():
    ws = WsClient()
    ws.start()
    ws.ticker(symbol="!ticker@arr", callback=handle_ticker)
    return ws

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
# RUN FOREVER
# =========================
def run_forever():
    ws = start_ws()
    notify("🤖 Bot started with real-time tickers")
    while True:
        try:
            trade_cycle()
            time.sleep(SLEEP_BETWEEN_CHECKS)
        except Exception as e:
            notify(f"❌ Cycle error: {e}")
            time.sleep(5)

# =========================
# START BOTH THREADS
# =========================
if __name__ == "__main__":
    bot_thread = threading.Thread(target=run_forever, daemon=True)
    bot_thread.start()
    start_flask()
