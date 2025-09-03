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

TRADE_USD = 8.0
SLEEP_BETWEEN_CHECKS = 15
COOLDOWN_AFTER_EXIT = 10
MAX_ACTIVE_TRADES = 1

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
active_trades = []
start_balance_usdt = None

# =========================
# UTILS
# =========================
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

# =========================
# FUNDING WALLET TRANSFER
# =========================
def send_profit_to_funding(amount, asset='USDT'):
    try:
        result = client.universal_transfer(
            type='SPOT_TO_FUNDING',
            asset=asset,
            amount=amount
        )
        notify(f"💸 Profit ${amount:.6f} transferred to funding wallet.")
        return result
    except Exception as e:
        notify(f"❌ Failed to transfer profit: {e}")
        return None
        
def handle_profit_transfer():
    global start_balance_usdt
    current_balance = get_free_usdt()
    if start_balance_usdt is None:
        start_balance_usdt = current_balance
        return
    profit = current_balance - start_balance_usdt
    if profit > 0:
        send_profit_to_funding(profit)
        start_balance_usdt = get_free_usdt()

# =========================
# SYMBOL PICKING
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

def pick_symbols_multi(top_n=1):
    tickers = get_tickers_cached()
    candidates = []
    for t in tickers:
        sym = t['symbol']
        if not sym.endswith(QUOTE):
            continue
        try:
            price = float(t['lastPrice'])
            volume = float(t['quoteVolume'])
            change_pct = abs(float(t['priceChangePercent']))
            if PRICE_MIN <= price <= PRICE_MAX and volume >= MIN_VOLUME and change_pct >= MOVEMENT_MIN_PCT:
                score = volume * change_pct
                candidates.append((sym, price, volume, change_pct, score))
        except:
            continue
    candidates.sort(key=lambda x: x[-1], reverse=True)
    return candidates[:top_n]

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

    # 🔥 Immediately place OCO SELL
    place_oco_sell(symbol, qty, price, tp_pct=3.0, sl_pct=1.0)

    return qty, price

# =========================
# OCO SELL
# =========================
def format_price(value, tick_size):
    decimals = str(tick_size).rstrip('0')
    if '.' in decimals:
        precision = len(decimals.split('.')[1])
    else:
        precision = 0
    return f"{value:.{precision}f}"

def place_oco_sell(symbol, qty, buy_price, tp_pct=3.0, sl_pct=0.5, retries=3, delay=2):
    info = client.get_symbol_info(symbol)
    f = get_filters(info)

    take_profit = buy_price * (1 + tp_pct/100.0)
    stop_price  = buy_price * (1 - sl_pct/100.0)
    stop_limit  = stop_price * (1 - 0.002)

    def clip(v, step):
        return math.floor(v / step) * step

    qty = clip(qty, f['stepSize'])
    tp  = clip(take_profit, f['tickSize'])
    sp  = clip(stop_price,  f['tickSize'])
    sl  = clip(stop_limit,  f['tickSize'])

    if qty <= 0:
        raise RuntimeError("❌ Quantity too small for OCO order")

    tp_str = format_price(tp, f['tickSize'])
    sp_str = format_price(sp, f['tickSize'])
    sl_str = format_price(sl, f['tickSize'])

    for attempt in range(1, retries+1):
        try:
            order = client.create_oco_order(
                symbol=symbol,
                side="SELL",
                quantity=str(qty),
                price=tp_str,
                stopPrice=sp_str,
                stopLimitPrice=sl_str,
                stopLimitTimeInForce="GTC"
            )
            notify(f"📌 OCO SELL placed ✅ TP={tp_str}, SL={sp_str}/{sl_str}, qty={qty}")
            return order
        except Exception as e:
            notify(f"⚠️ OCO SELL attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                notify("❌ OCO SELL failed after retries.")
                return None

# =========================
# TRADE CYCLE
# =========================
def trade_cycle():
    global active_trades
    while True:
        try:
            if len(active_trades) >= MAX_ACTIVE_TRADES:
                time.sleep(5)
                continue

            syms = pick_symbols_multi(top_n=1)
            if not syms:
                notify("⚠️ No symbol candidates found.")
                time.sleep(10)
                continue

            sym, price, _, _, _ = syms[0]
            free_usdt = get_free_usdt()
            if free_usdt < TRADE_USD:
                notify(f"⚠️ Not enough USDT to trade. Free={free_usdt:.2f}")
                time.sleep(30)
                continue

            qty, entry_price = place_safe_market_buy(sym, TRADE_USD)
            active_trades.append({"symbol": sym, "qty": qty, "entry": entry_price})

            time.sleep(COOLDOWN_AFTER_EXIT)
            handle_profit_transfer()

            notify("⏳ Subiri sekunde 15 kabla ya cycle mpya...")
            time.sleep(15)

        except Exception as e:
            notify(f"❌ Error in trade cycle: {e}")
            time.sleep(10)

# =========================
# FLASK SERVER
# =========================
app = Flask(__name__)
@app.route("/")
def home():
    return "Bot running! ✅"

def start_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

# =========================
# START
# =========================
if __name__ == "__main__":
    bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    bot_thread.start()
    start_flask()
