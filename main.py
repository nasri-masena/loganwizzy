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
SLEEP_BETWEEN_CHECKS = 60
COOLDOWN_AFTER_EXIT = 30

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0

def notify(msg: str):
    global LAST_NOTIFY
    now_ts = time.time()
    if now_ts - LAST_NOTIFY < 2:  # short throttle for testing
        return
    LAST_NOTIFY = now_ts
    text = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(text)
    if BOT_TOKEN and CHAT_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                          data={"chat_id": CHAT_ID, "text": text})
        except:
            pass

def get_free_usdt():
    try:
        bal = client.get_asset_balance(asset=QUOTE)
        return float(bal['free'])
    except:
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

# =========================
# SYMBOL SELECTION
# =========================
TICKER_CACHE = None
LAST_FETCH = 0
CACHE_TTL = 30  # seconds

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
        except:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[2]*x[3], reverse=True)
    return candidates[0]

# =========================
# MARKET BUY
# =========================
def round_step(n, step):
    return math.floor(n / step) * step if step else n

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
            needed_qty = round_step(f['minNotional']/price, f['stepSize'])
            free_usdt = get_free_usdt()
            if needed_qty*price > free_usdt + 1e-8:
                raise RuntimeError(f"Not enough funds: need≈${needed_qty*price:.2f}, free=${free_usdt:.2f}")
            qty = needed_qty

    order = client.order_market_buy(symbol=symbol, quantity=qty)
    notify(f"✅ BUY {symbol}: qty={qty} ~price={price:.8f} notional≈${qty*price:.6f}")

    # 🔥 Immediately place OCO SELL (TP +3%, SL -1%)
    place_oco_sell(symbol, qty, price, tp_pct=3.0, sl_pct=1.0)

    return qty, price

# =========================
# OCO SELL
# =========================
def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=1.0):
    info = client.get_symbol_info(symbol)
    f = get_filters(info)

    # Calculate TP and SL prices
    take_profit = buy_price * (1 + tp_pct/100.0)
    stop_price   = buy_price * (1 - sl_pct/100.0)
    stop_limit   = stop_price * (1 - 0.002)  # 0.2% below stop for safety

    # Round helper
    def clip(v, step):
        return math.floor(v / step) * step

    qty = clip(qty, f['stepSize'])
    tp  = clip(take_profit, f['tickSize'])
    sp  = clip(stop_price, f['tickSize'])
    sl  = clip(stop_limit, f['tickSize'])

    if qty <= 0:
        raise RuntimeError("❌ Quantity too small for OCO order")

    try:
        order = client.create_oco_order(
            symbol=symbol,
            side="SELL",
            quantity=str(qty),
            price=str(tp),              # Take-Profit
            stopPrice=str(sp),          # Stop trigger
            stopLimitPrice=str(sl),     # Stop-Limit
            stopLimitTimeInForce="GTC"
        )
        notify(f"📌 OCO SELL placed: TP={tp}, SL={sp}/{sl}, qty={qty}")
        return order
    except Exception as e:
        notify(f"❌ OCO SELL failed: {e}")
        return None
        
# =========================
# MAIN LOOP
# =========================
def trade_cycle():
    while True:
        coin = pick_coin()
        if not coin:
            notify("⚠️ No coins meet criteria. Waiting 60s...")
            time.sleep(SLEEP_BETWEEN_CHECKS)
            continue
        symbol, price, volume, change = coin
        notify(f"🎯 Selected {symbol} for market buy (24h change={change}%, volume≈{volume})")
        usd_to_buy = min(TRADE_USD, get_free_usdt())
        if usd_to_buy < 1.0:
            notify("⚠️ Not enough USDT to buy. Waiting 60s...")
            time.sleep(SLEEP_BETWEEN_CHECKS)
            continue
        try:
            place_safe_market_buy(symbol, usd_to_buy)
        except Exception as e:
            notify(f"❌ Market buy failed: {e}")
        time.sleep(COOLDOWN_AFTER_EXIT)

# =========================
# FLASK KEEPALIVE
# =========================
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot running! ✅"

def start_flask():
    app.run(host="0.0.0.0", port=5000)

# =========================
# RUN THREADS
# =========================
if __name__ == "__main__":
    bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    bot_thread.start()
    start_flask()