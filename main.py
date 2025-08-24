import time
import threading
import os
from flask import Flask
import requests
import hmac, hashlib, urllib.parse, json, os
from binance.client import Client
from binance.enums import *

# ======== CONFIG ========
API_KEY = os.getenv("API_KEY")  # Weka API Key yako
API_SECRET = os.getenv("API_SECRET")    # Weka Secret Key yako
BOT_TOKEN = os.getenv("BOT_TOKEN") # Telegram bot token
CHAT_ID = os.getenv("CHAT_ID")       # Telegram chat ID
TRADE_DELAY = 20   # Delay between trades in seconds
ERROR_DELAY = 300  # Delay on error
MIN_TRADE_USD = 0.001
MAX_DAILY_TRADES = 10
TRADE_LOG_FILE = "trade_log.json"
# ======================

client = Client(API_KEY, API_SECRET)
CHEAP_COINS = []
daily_trades = 0
app = Flask(__name__)

# ======== UTILITY FUNCTIONS ========
def notify(msg):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg})
    except Exception as e:
        print("Telegram Error:", e)

def get_trade_log():
    if os.path.exists(TRADE_LOG_FILE):
        with open(TRADE_LOG_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_trade_log(log):
    with open(TRADE_LOG_FILE, 'w') as f:
        json.dump(log, f, indent=2)

def log_trade(symbol, qty, price, action):
    log = get_trade_log()
    if symbol not in log:
        log[symbol] = {"buy": [], "sell": []}
    log[symbol][action].append({"qty": qty, "price": price, "timestamp": time.time()})
    save_trade_log(log)

def get_usdt_balance():
    try:
        return float(client.get_asset_balance(asset='USDT')['free'])
    except:
        return 0

def is_market_stable():
    try:
        btc_change = float(client.get_ticker_24hr(symbol='BTCUSDT')['priceChangePercent'])
        return btc_change > -3
    except:
        return True

def fetch_trending_memecoins():
    try:
        res = requests.get("https://api.coingecko.com/api/v3/search/trending")
        data = res.json()
        return [coin['item']['symbol'].upper() + 'USDT' for coin in data['coins']]
    except:
        return []

def fetch_high_volume_coins(limit=10, min_buy_ratio=0.6):
    try:
        tickers = client.get_ticker()
        high_volume = []
        for t in tickers[:limit]:
            symbol = t['symbol']
            if not symbol.endswith("USDT"):
                continue
            try:
                trades = client.get_recent_trades(symbol=symbol, limit=500)
                buy_count = sum(1 for tr in trades if not tr['isBuyerMaker'])
                total = len(trades)
                if total == 0:
                    continue
                buy_ratio = buy_count / total
                if buy_ratio >= min_buy_ratio:
                    high_volume.append({
                        "symbol": symbol,
                        "buy_ratio": round(buy_ratio*100,2),
                        "volume": float(t['quoteVolume'])
                    })
            except Exception:
                continue
        high_volume.sort(key=lambda x: x["volume"], reverse=True)
        return high_volume
    except Exception as e:
        notify(f"❌ High Volume Fetch Error: {e}")
        return []

# ======== TRADING FUNCTIONS ========
def sell_other_assets():
    global daily_trades
    try:
        account = client.get_account()
        for balance in account['balances']:
            asset = balance['asset']
            free = float(balance['free'])
            if free > 0 and asset != 'USDT':
                symbol = asset + 'USDT'
                try:
                    price = float(client.get_symbol_ticker(symbol=symbol)['price'])
                    value = price * free
                    if value >= MIN_TRADE_USD:
                        info = client.get_symbol_info(symbol)
                        step_size = float([f for f in info['filters'] if f['filterType']=='LOT_SIZE'][0]['stepSize'])
                        qty = free - (free % step_size)
                        qty = round(qty, 6)
                        if qty > 0:
                            client.order_market_sell(symbol=symbol, quantity=qty)
                            notify(f"✅ Sold {qty} {asset} (~${value:.5f})")
                            log_trade(symbol, qty, price, 'sell')
                            daily_trades += 1
                            time.sleep(TRADE_DELAY)
                except Exception as e:
                    notify(f"❌ Sell Error on {symbol}: {e}")
                    time.sleep(ERROR_DELAY)
    except Exception as e:
        notify(f"❌ Sell Process Error: {e}")
        time.sleep(ERROR_DELAY)

def buy_cheap_coins():
    """
    Buy cheap coins suitable for micro trading.
    Optimized for low balance ($6).
    """
    global daily_trades, CHEAP_COINS
    try:
        if not is_market_stable():
            notify("📉 Market unstable. Skipping buys.")
            return

        usdt = get_usdt_balance()
        if usdt < 1:  # Skip if balance too low
            notify("⚠️ Balance too low for trading.")
            return

        CHEAP_COINS = fetch_cheap_coins(max_price=0.1)
        if not CHEAP_COINS:
            notify("⚠️ No cheap coins found for trading.")
            return

        # Score coins based on 24hr % change (volatility)
        coin_scores = []
        for symbol in CHEAP_COINS:
            try:
                change = abs(float(client.get_ticker_24hr(symbol=symbol)['priceChangePercent']))
                coin_scores.append((symbol, change))
            except:
                continue

        total_score = sum(score for _, score in coin_scores)
        if total_score == 0:
            notify("⚠️ Total score is 0, skipping buys.")
            return

        for symbol, score in coin_scores:
            if daily_trades >= MAX_DAILY_TRADES:
                notify("⚠️ Daily trade limit reached.")
                break

            try:
                portion = (score / total_score) * usdt * 0.8  # 20% buffer for fees
                price = float(client.get_symbol_ticker(symbol=symbol)['price'])
                info = client.get_symbol_info(symbol)
                step_size = float([f for f in info['filters'] if f['filterType']=='LOT_SIZE'][0]['stepSize'])

                qty = portion / price
                qty = qty - (qty % step_size)
                qty = round(qty, 6)

                change_percent = float(client.get_ticker_24hr(symbol=symbol)['priceChangePercent'])
                if change_percent < -10 and qty > 0:
                    client.order_market_buy(symbol=symbol, quantity=qty)
                    notify(f"✅ Bought {qty} of {symbol} | Micro Allocation")
                    log_trade(symbol, qty, price, 'buy')
                    daily_trades += 1
                    time.sleep(TRADE_DELAY)
            except Exception as e:
                notify(f"❌ Buy Error on {symbol}: {e}")
                time.sleep(ERROR_DELAY)
    except Exception as e:
        notify(f"❌ Buy Process Error: {e}")
        time.sleep(ERROR_DELAY)

def monitor_coins_pro(trailing_percent=3, partial_profit_percent=5, stop_loss=-10):
    log = get_trade_log()
    for symbol, trades in log.items():
        if not trades['buy']:
            continue
        last_trade = trades['buy'][-1]
        bought_price = last_trade['price']
        qty = last_trade['qty']
        if 'trailing_stop' not in last_trade:
            last_trade['trailing_stop'] = bought_price * (1 - trailing_percent/100)
        try:
            current_price = float(client.get_symbol_ticker(symbol=symbol)['price'])
            change_percent = (current_price - bought_price)/bought_price*100
            if change_percent >= partial_profit_percent and last_trade.get('partial_sold') is None:
                sell_qty = round(qty * 0.5, 6)
                if sell_qty > 0:
                    client.order_market_sell(symbol=symbol, quantity=sell_qty)
                    notify(f"✅ Partial sold {sell_qty} {symbol} at +{change_percent:.2f}%")
                    log_trade(symbol, sell_qty, current_price, 'sell')
                    last_trade['partial_sold'] = True
            if current_price > last_trade['trailing_stop']*(1+trailing_percent/100):
                last_trade['trailing_stop'] = current_price*(1-trailing_percent/100)
            if current_price <= last_trade['trailing_stop']:
                client.order_market_sell(symbol=symbol, quantity=qty)
                notify(f"⚠️ Trailing stop sold {symbol} at {current_price}")
                log_trade(symbol, qty, current_price, 'sell')
            elif change_percent <= stop_loss:
                client.order_market_sell(symbol=symbol, quantity=qty)
                notify(f"❌ Stop-loss sold {symbol} at {current_price} ({change_percent:.2f}%)")
                log_trade(symbol, qty, current_price, 'sell')
            save_trade_log(log)
        except Exception as e:
            notify(f"❌ Pro Monitor Error on {symbol}: {e}")

def auto_detect_usdt_topup(prev_usdt):
    current = get_usdt_balance()
    if current > prev_usdt + 0.5:
        notify(f"💰 New Capital Detected: +${current - prev_usdt:.2f}")
        return True
    return False

def transfer_profit_to_funding():
    try:
        usdt = get_usdt_balance()
        if usdt > 5:
            url = "https://api.binance.com/sapi/v1/asset/transfer"
            headers = {'X-MBX-APIKEY': API_KEY}
            params = {
                'type': 2,
                'asset': 'USDT',
                'amount': usdt,
                'timestamp': int(time.time()*1000)
            }
            query_string = urllib.parse.urlencode(params)
            signature = hmac.new(API_SECRET.encode(), query_string.encode(), hashlib.sha256).hexdigest()
            full_url = f"{url}?{query_string}&signature={signature}"
            res = requests.post(full_url, headers=headers)
            if res.status_code == 200:
                notify(f"✅ Transferred ${usdt} USDT profit to Funding wallet")
                return True
        return False
    except:
        return False

# ======== SINGLE-CYCLE FUNCTION ========
def run_bot_cycle():
    prev_usdt = get_usdt_balance()
    try:
        notify("🤖 Bot Cycle Started")
        sell_other_assets()
        buy_cheap_coins()
        monitor_coins_pro(trailing_percent=3, partial_profit_percent=5, stop_loss=-10)
        if auto_detect_usdt_topup(prev_usdt):
            sell_other_assets()
            buy_cheap_coins()
            monitor_coins_pro(trailing_percent=3, partial_profit_percent=5, stop_loss=-10)
        transfer_profit_to_funding()
        notify("✅ Cycle Completed")
    except Exception as e:
        notify(f"❌ Unexpected Error: {e}")

# ======== FLASK ROUTE ========
@app.route("/run_cycle")
def run_cycle():
    threading.Thread(target=run_bot_cycle).start()  # run cycle in background
    return "Cycle triggered!"

@app.route("/")
def home():
    return "Bot is running! Ping received."

app = Flask(__name__)
...
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)