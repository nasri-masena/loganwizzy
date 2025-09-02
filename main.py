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

PRICE_MIN = 0.0001   # Updated for low-value coins
PRICE_MAX = 0.001    # Updated for low-value coins
MIN_VOLUME = 5_000_000
MOVEMENT_MIN_PCT = 1.0   # Lowered to capture small coin movements

TRADE_USD = 5  # Test amount
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
    if now_ts - LAST_NOTIFY < 2:
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

def round_step(n, step):
    return math.floor(n / step) * step if step else n

def adjust_quantity(symbol, qty):
    info = client.get_symbol_info(symbol)
    step_size = None
    for f in info['filters']:
        if f['filterType'] == 'LOT_SIZE':
            step_size = float(f['stepSize'])
            break
    if step_size:
        precision = int(round(-math.log(step_size, 10), 0))
        return float(f"{qty:.{precision}f}")
    return qty

# =========================
# SYMBOL SELECTION
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
            info = client.get_symbol_info(sym)
            if info['status'] != 'TRADING':
                continue
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

    qty = adjust_quantity(symbol, qty)
    order = client.order_market_buy(symbol=symbol, quantity=qty)
    notify(f"✅ BUY {symbol}: qty={qty} ~price={price:.8f} notional≈${qty*price:.6f}")
    return qty, price

# =========================
# STOP MARKET SELL
# =========================
def place_fixed_stop_market_sell(symbol, qty, buy_price, sl_cents=0.00001):
    f = get_filters(client.get_symbol_info(symbol))
    stop_price = buy_price - sl_cents
    stop_price = round_step(stop_price, f['tickSize'])
    qty = adjust_quantity(symbol, qty)

    try:
        order = client.create_order(
            symbol=symbol,
            side='SELL',
            type='STOP_MARKET',
            quantity=qty,
            stopPrice=str(stop_price)
        )
        notify(f"📌 STOP MARKET SELL set for {symbol}: stopPrice={stop_price}, qty={qty}")
        return order
    except Exception as e:
        notify(f"❌ Failed to place stop market sell for {symbol}: {e}")
        return None

# =========================
# MAIN TRADE CYCLE
# =========================
def trade_cycle():
    while True:
        coin = pick_coin()
        if not coin:
            notify("⚠️ No coins meet criteria or market closed. Waiting 60s...")
            time.sleep(SLEEP_BETWEEN_CHECKS)
            continue

        symbol, price, volume, change = coin
        notify(f"🎯 Selected {symbol} for market buy (24h change={change:.2f}%, volume≈{volume})")

        usd_to_buy = min(TRADE_USD, get_free_usdt())
        if usd_to_buy < 1.0:
            notify("⚠️ Not enough USDT to buy. Waiting 60s...")
            time.sleep(SLEEP_BETWEEN_CHECKS)
            continue

        try:
            qty, buy_price = place_safe_market_buy(symbol, usd_to_buy)
            notify(f"💰 Market buy executed for {symbol}: qty={qty}, price={buy_price:.8f}")

            # Small price coins: set sl_cents lower
            sl_cents = 0.00005 if buy_price < 0.01 else 0.005
            stop_order = place_fixed_stop_market_sell(symbol, qty, buy_price, sl_cents=sl_cents)

            if stop_order:
                notify(f"📌 Stop market SELL order placed for {symbol} at ~{buy_price-sl_cents:.8f}")

        except Exception as e:
            notify(f"❌ Trade cycle failed for {symbol}: {e}")

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
