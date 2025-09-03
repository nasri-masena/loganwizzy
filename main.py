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
STEP_INCREMENT_PCT = 0.03
TRIGGER_PROXIMITY = 0.04

MAX_ACTIVE_TRADES = 1  # trades in parallel

# =========================
# INIT
# =========================
client = Client(API_KEY, API_SECRET)
LAST_NOTIFY = 0
active_trades = []
LOCKED_SYMBOL = None
start_balance_usdt = None

# =========================
# UTILITIES
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

def get_free_asset(asset):
    try:
        bal = client.get_asset_balance(asset=asset)
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

def round_price(p, tick):
    return math.floor(p / tick) * tick if tick else p

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
        
def handle_daily_profit():
    global start_balance_usdt
    current_balance = get_free_usdt()
    profit = current_balance - start_balance_usdt
    if profit > 0:
        send_profit_to_funding(profit)
        start_balance_usdt = get_free_usdt()  # reset baseline

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
            needed_qty = round_step(f['minNotional']/price, f['stepSize'])
            free_usdt = get_free_usdt()
            if needed_qty * price > free_usdt + 1e-8:
                raise RuntimeError("Not enough funds for MIN_NOTIONAL")
            qty = needed_qty

    order = client.order_market_buy(symbol=symbol, quantity=qty)
    notify(f"✅ BUY {symbol}: qty={qty} ~price={price:.8f}")
    return qty, price, f

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

def place_oco_order(symbol, qty, entry_price, f, tp_pct=0.03, sl_pct=0.01):
    tp_price = round_price(entry_price * (1 + tp_pct), f['tickSize'])
    sl_price = round_price(entry_price * (1 - sl_pct), f['tickSize'])
    stop_limit_price = round_price(sl_price * 0.999, f['tickSize'])

    try:
        oco = client.create_oco_order(
            symbol=symbol,
            side='SELL',
            quantity=str(qty),
            price=str(tp_price),
            stopPrice=str(sl_price),
            stopLimitPrice=str(stop_limit_price),
            stopLimitTimeInForce='GTC'
        )
        notify(f"📌 OCO set for {symbol}: SL={sl_price}, TP={tp_price}, qty={qty}")
        return {'tp': tp_price, 'sl': sl_price}
    except Exception as e:
        notify(f"❌ Failed to place OCO: {e}")
        return None

def cancel_all_open_orders(symbol):
    try:
        client.cancel_open_orders(symbol=symbol)
    except Exception as e:
        notify(f"Cancel open orders error on {symbol}: {e}")

# =========================
# MONITOR & ROLL
# =========================
def monitor_and_roll(symbol, qty, entry_price, f):
    state = place_oco_order(symbol, qty, entry_price, f)
    if state is None:
        return False, entry_price, 0.0

    last_tp = None
    active = True
    while active:
        time.sleep(SLEEP_BETWEEN_CHECKS)
        free_qty = get_free_asset(symbol[:-len(QUOTE)])
        open_orders = client.get_open_orders(symbol=symbol)
        price_now = float(client.get_symbol_ticker(symbol=symbol)['price'])

        # Position closed
        if free_qty < round_step(qty * 0.05, f['stepSize']) and len(open_orders) == 0:
            exit_price = price_now
            profit_usd = (exit_price - entry_price) * qty
            notify(f"✅ Position closed for {symbol}: profit ≈ ${profit_usd:.6f}")
            active_trades[:] = [t for t in active_trades if t['symbol'] != symbol]
            return True, exit_price, profit_usd

        # Roll TP/SL
        near_trigger = price_now >= state['tp'] * (1 - TRIGGER_PROXIMITY)
        if near_trigger:
            cancel_all_open_orders(symbol)
            new_sl = entry_price if last_tp is None else last_tp
            new_tp = state['tp'] * (1 + STEP_INCREMENT_PCT)
            last_tp = state['tp']
            state = place_oco_order(symbol, free_qty, new_tp, f)
            if state:
                notify(f"🔁 Rolled OCO for {symbol}: new SL={state['sl']:.8f}, new TP={state['tp']:.8f} (price≈{price_now:.8f})")

# =========================
# MAIN LOOP
# =========================
def trade_cycle():
    global LOCKED_SYMBOL, start_balance_usdt, active_trades

    if start_balance_usdt is None:
        start_balance_usdt = get_free_usdt()
        notify(f"🔰 Start balance recorded: ${start_balance_usdt:.6f}")

    while True:
        try:
            # Haitaongeza trade mpya ikiwa kuna active trade
            if len(active_trades) >= MAX_ACTIVE_TRADES:
                time.sleep(5)
                continue

            # Chagua coin 1 tu
            candidates = pick_symbols_multi(top_n=1)
            if not candidates:
                notify("⚠️ No eligible coins to buy. Sleeping 10s.")
                time.sleep(10)
                continue

            symbol, price, volume, change, score = candidates[0]
            if symbol in [t['symbol'] for t in active_trades]:
                time.sleep(5)
                continue

            LOCKED_SYMBOL = symbol
            notify(f"🎯 Selected {symbol} for trading.")

            trade_usd = max(min(TRADE_USD, get_free_usdt()), 1.0)

            # === Step 1: Market Buy ===
            try:
                qty, entry_price, f = place_market_buy(symbol, trade_usd)
                active_trades.append({'symbol': symbol, 'qty': qty, 'entry_price': entry_price, 'filters': f})
            except Exception as e:
                notify(f"❌ Buy failed: {e}")
                LOCKED_SYMBOL = None
                time.sleep(5)
                continue

            # === Step 2 & 3: Monitor OCO & Roll ===
            success, exit_price, profit_usd = monitor_and_roll(symbol, qty, entry_price, f)

            # === Step 4: Transfer profit to funding wallet ===
            if success and profit_usd > 0:
                send_profit_to_funding(profit_usd)

            # Remove from active trades & unlock
            active_trades[:] = [t for t in active_trades if t['symbol'] != symbol]
            LOCKED_SYMBOL = None

            time.sleep(COOLDOWN_AFTER_EXIT)

        except Exception as e:
            notify(f"❌ Trade cycle error: {e}")
            time.sleep(5)
            
# =========================
# FLASK KEEPALIVE
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