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

TRADE_USD = 8  # ⚡ test amount
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
    return qty, price
    
def place_stop_loss_limit_sell(symbol, qty, stop_price, limit_price):
    """
    Places a stop-limit sell order (Spot) - executes a limit sell after stop trigger.
    """
    try:
        order = client.create_order(
            symbol=symbol,
            side='SELL',
            type='STOP_LOSS_LIMIT',
            timeInForce='GTC',
            quantity=qty,
            price=f"{limit_price:.8f}",
            stopPrice=f"{stop_price:.8f}"
        )
        notify(f"📌 STOP-LOSS-LIMIT set for {symbol}: stop={stop_price}, limit={limit_price}, qty={qty}")
        return order
    except Exception as e:
        notify(f"❌ Failed to place stop-loss-limit sell for {symbol}: {e}")
        return None
        
# =========================
# MAIN LOOP (updated with STOP SELL notify)
# =========================
def trade_cycle():
    while True:
        coin = pick_coin()
        if not coin:
            notify("⚠️ No coins meet criteria. Waiting 60s...")
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
            # Step 1: Market buy
            qty, buy_price = place_safe_market_buy(symbol, usd_to_buy)
            notify(f"💰 Market buy executed for {symbol}: qty={qty}, price={buy_price:.8f}")

            # Step 2: Stop-loss-limit order (SL 0.5% below buy price, limit 1% below)
            stop_price = buy_price * 0.995   # -0.5%
            limit_price = buy_price * 0.990  # -1.0%
            stop_order = place_stop_loss_limit_sell(symbol, qty, stop_price, limit_price)

            if stop_order:
                notify(f"📌 Stop-loss-limit SELL order placed for {symbol} "
                       f"(stop={stop_price:.8f}, limit={limit_price:.8f})")

        except Exception as e:
            notify(f"❌ Trade cycle failed for {symbol}: {e}")

        # Step 3: Cooldown before next iteration
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