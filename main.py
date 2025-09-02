import os
import time
import requests
from datetime import datetime
from flask import Flask
from binance.client import Client
from binance.exceptions import BinanceAPIException

# =========================
# LOAD CONFIG
# =========================

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

client = Client(API_KEY, API_SECRET)
app = Flask(__name__)

# =========================
# HELPERS
# =========================
def send_telegram(msg: str):
    """Send message to Telegram bot"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg})
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")

def get_market_pairs():
    """Fetch all USDT pairs"""
    try:
        tickers = client.get_ticker()
        return [t for t in tickers if t["symbol"].endswith("USDT")]
    except Exception as e:
        print(f"⚠️ Failed to fetch market pairs: {e}")
        time.sleep(15)
        return []

def pick_strong_coin(pairs):
    """
    Select one coin that is pumping:
    - Price change > 2% in 5 mins
    - High volume
    """
    strong = sorted(
        pairs,
        key=lambda x: float(x["priceChangePercent"]),
        reverse=True
    )
    if strong and float(strong[0]["priceChangePercent"]) > 2:
        return strong[0]
    return None

def buy_coin(symbol, amount_usdt=20):
    """Place market buy order"""
    try:
        price = float(client.get_symbol_ticker(symbol=symbol)["price"])
        qty = round(amount_usdt / price, 5)
        order = client.order_market_buy(symbol=symbol, quantity=qty)
        send_telegram(f"✅ Bought {symbol} at {price} with {qty} units")
        print(order)
        return order
    except BinanceAPIException as e:
        send_telegram(f"⚠️ Buy failed for {symbol}: {e}")
        time.sleep(15)
    except Exception as e:
        send_telegram(f"⚠️ Error while buying {symbol}: {e}")
        time.sleep(15)
    return None

# =========================
# MAIN LOOP
# =========================
def run_bot():
    while True:
        pairs = get_market_pairs()
        if not pairs:
            continue

        coin = pick_strong_coin(pairs)
        if coin:
            symbol = coin["symbol"]
            change = coin["priceChangePercent"]
            send_telegram(f"🎯 Selected {symbol} (change {change}%) at {datetime.now()}")
            buy_coin(symbol, 20)
            time.sleep(60)  # Delay after buy
        else:
            print("⚠️ No eligible coins at this moment.")
            time.sleep(15)

# =========================
# FLASK ENDPOINT
# =========================
@app.route("/")
def index():
    return "🚀 Trading bot running..."

if __name__ == "__main__":
    send_telegram("🤖 Bot started successfully")
    run_bot()