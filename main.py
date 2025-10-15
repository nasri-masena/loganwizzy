import os
import time
import threading
import statistics
import requests
from flask import Flask

# -------------------------
# Telegram Config
# -------------------------
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CHAT_ID")

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        r = requests.post(url, json=payload)
        return r.status_code == 200
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

# -------------------------
# Helper Functions
# -------------------------
def compute_recent_volatility(closes, lookback=5):
    try:
        if not closes or len(closes) < 2:
            return None
        rets = []
        for i in range(1, len(closes)):
            prev = float(closes[i-1])
            cur = float(closes[i])
            if prev <= 0:
                continue
            rets.append((cur - prev) / prev)
        if not rets:
            return None
        recent = rets[-lookback:] if lookback and len(rets) >= 1 else rets
        if len(recent) == 0:
            return None
        if len(recent) == 1:
            return abs(recent[0])
        try:
            vol = statistics.pstdev(recent)
        except Exception:
            vol = statistics.stdev(recent) if len(recent) > 1 else abs(recent[-1])
        if vol is None:
            return None
        vol = abs(vol)
        vol = min(vol, 5.0)
        return vol
    except Exception:
        return None

def orderbook_bullish(symbol, client, depth=3, min_imbalance=1.02, max_spread_pct=1.0):
    try:
        ob = client.get_order_book(symbol=symbol, limit=depth)
        bids = ob.get('bids') or []
        asks = ob.get('asks') or []
        if not bids or not asks:
            return False
        top_bid_p, top_bid_q = float(bids[0][0]), float(bids[0][1])
        top_ask_p, top_ask_q = float(asks[0][0]), float(asks[0][1])
        spread_pct = (top_ask_p - top_bid_p) / (top_bid_p + 1e-12) * 100.0
        bid_sum = sum(float(b[1]) for b in bids[:depth]) + 1e-12
        ask_sum = sum(float(a[1]) for a in asks[:depth]) + 1e-12
        imbalance = bid_sum / ask_sum
        return (imbalance >= min_imbalance) and (spread_pct <= max_spread_pct)
    except Exception:
        return False

def ema_local(values, period):
    if not values or period <= 0:
        return None
    alpha = 2.0 / (period + 1.0)
    e = float(values[0])
    for v in values[1:]:
        e = alpha * float(v) + (1 - alpha) * e
    return e

def compute_rsi_local(closes, period=14):
    if not closes or len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(0.0, diff))
        losses.append(max(0.0, -diff))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period if sum(losses[:period]) != 0 else 1e-9
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    rs = avg_gain / (avg_loss if avg_loss > 0 else 1e-9)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def pct_change(open_p, close_p):
    if open_p == 0:
        return 0.0
    return (close_p - open_p) / open_p * 100.0

# -------------------------
# Dummy Binance Client & Data (replace these with real API calls)
# -------------------------
class DummyBinanceClient:
    def get_order_book(self, symbol, limit):
        # Dummy orderbook
        return {
            'bids': [['100', '0.5'], ['99.5', '0.3'], ['99', '0.2']],
            'asks': [['100.5', '0.5'], ['101', '0.3'], ['101.5', '0.2']]
        }
    def get_klines(self, symbol, interval, limit):
        # Dummy klines: [open_time, open, high, low, close, volume, ...]
        data = []
        for i in range(limit):
            data.append([0, 100 + i, 101 + i, 99 + i, 100.5 + i, 5000 + 100*i])
        return data

def get_tickers_cached():
    # Replace with your Binance tickers API call
    return [
        {'symbol': 'BTCUSDT', 'lastPrice': '27000', 'quoteVolume': '200000000', 'priceChangePercent': '3.1'},
        {'symbol': 'ETHUSDT', 'lastPrice': '1500', 'quoteVolume': '100000000', 'priceChangePercent': '2.2'},
        {'symbol': 'BNBUSDT', 'lastPrice': '300', 'quoteVolume': '50000000', 'priceChangePercent': '1.2'},
        {'symbol': 'BTCBUSD', 'lastPrice': '27000', 'quoteVolume': '15000000', 'priceChangePercent': '2.5'},
    ]

client = DummyBinanceClient()
QUOTE = "USDT"
PRICE_MIN = 1.0
PRICE_MAX = 4.0
MIN_VOLUME = 1000000
KLINES_5M_LIMIT = 6
KLINES_1M_LIMIT = 6
EMA_SHORT = 3
EMA_LONG = 10
RSI_PERIOD = 14
OB_DEPTH = 3
MIN_OB_IMBALANCE = 1.1
MAX_OB_SPREAD_PCT = 1.5
MIN_OB_LIQUIDITY = 3000.0
TOP_BY_24H_VOLUME = 5
REQUEST_SLEEP = 0.1
MIN_5M_PCT = 0.6
MIN_1M_PCT = 0.3
TEMP_SKIP = {}
RECENT_BUYS = {}
BUY_LOCK_SECONDS = 600

# -------------------------
# pick_coin (advanced signal picker)
def pick_coin():
    tickers = get_tickers_cached() or []
    now = time.time()
    pre = []

    for t in tickers:
        sym = t.get('symbol')
        if not sym or not sym.endswith(QUOTE):
            continue
        try:
            last = float(t.get('lastPrice') or 0.0)
            qvol = float(t.get('quoteVolume') or 0.0)
            ch = float(t.get('priceChangePercent') or 0.0)
        except Exception:
            continue
        if not (PRICE_MIN <= last <= PRICE_MAX):
            continue
        if qvol < MIN_VOLUME:
            continue
        if ch < 0.5 or ch > 15.0:
            continue
        skip_until = TEMP_SKIP.get(sym)
        if skip_until and now < skip_until:
            continue
        last_buy = RECENT_BUYS.get(sym)
        if last_buy:
            if now < last_buy['ts'] + BUY_LOCK_SECONDS:
                continue
        pre.append((sym, last, qvol, ch))

    if not pre:
        return None

    pre.sort(key=lambda x: x[2], reverse=True)
    candidates = pre[:TOP_BY_24H_VOLUME]

    scored = []
    for sym, last_price, qvol, change_24h in candidates:
        time.sleep(REQUEST_SLEEP)
        kl5 = client.get_klines(symbol=sym, interval='5m', limit=KLINES_5M_LIMIT)
        if not kl5 or len(kl5) < 3:
            continue
        time.sleep(REQUEST_SLEEP)
        kl1 = client.get_klines(symbol=sym, interval='1m', limit=KLINES_1M_LIMIT)
        if not kl1 or len(kl1) < 2:
            continue
        try:
            closes_5m = [float(k[4]) for k in kl5]
            closes_1m = [float(k[4]) for k in kl1]
            pct_5m = pct_change(float(kl5[0][1]), closes_5m[-1])
            pct_1m = pct_change(float(kl1[0][1]), closes_1m[-1])
            vol_5m = compute_recent_volatility(closes_5m)
            vol_1m = compute_recent_volatility(closes_1m, lookback=3)
            short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
            long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
            ema_ok = False
            ema_uplift = 0.0
            if short_ema and long_ema:
                ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
                ema_ok = short_ema > long_ema * 1.0005
            rsi_val = compute_rsi_local(closes_5m[-(RSI_PERIOD+1):], RSI_PERIOD) if len(closes_5m) >= RSI_PERIOD+1 else None
            rsi_ok = True
            if rsi_val is not None and rsi_val > 68:
                rsi_ok = False
            ob_bull = orderbook_bullish(sym, client, depth=OB_DEPTH, min_imbalance=MIN_OB_IMBALANCE, max_spread_pct=MAX_OB_SPREAD_PCT)
            score = 0.0
            score += max(0.0, pct_5m) * 4.0
            score += max(0.0, pct_1m) * 2.0
            score += ema_uplift * 5.0 * 100.0
            score += max(0.0, change_24h) * 1.0 * 0.5
            if vol_5m is not None:
                score += max(0.0, (0.01 - min(vol_5m, 0.01))) * 100.0
            if rsi_val is not None:
                score += max(0.0, (60.0 - min(rsi_val, 60.0))) * 1.5 * 0.2
            if ob_bull:
                score += 20.0
            strong_candidate = (pct_5m >= MIN_5M_PCT and pct_1m >= MIN_1M_PCT and ema_ok and rsi_ok and ob_bull)
            scored.append({
                "symbol": sym,
                "last_price": last_price,
                "24h_change": change_24h,
                "24h_vol": qvol,
                "pct_5m": pct_5m,
                "pct_1m": pct_1m,
                "vol_5m": vol_5m,
                "vol_1m": vol_1m,
                "ema_ok": ema_ok,
                "ema_uplift": ema_uplift,
                "rsi": rsi_val,
                "ob_bull": ob_bull,
                "score": score,
                "strong_candidate": strong_candidate
            })
        except Exception as e:
            print(f"pick_coin evaluate error {sym}: {e}")
            continue

    if not scored:
        return None

    scored.sort(key=lambda x: x['score'], reverse=True)
    strongs = [s for s in scored if s['strong_candidate']]
    if strongs:
        best = sorted(strongs, key=lambda x: x['score'], reverse=True)[0]
    else:
        best = scored[0]
    msg = f"""
🚀 *COIN SIGNAL*: `{best['symbol']}`
Price: `{best['last_price']}`
24h Change: `{best['24h_change']}`%
5m Change: `{best['pct_5m']:.2f}`%
1m Change: `{best['pct_1m']:.2f}`%
Volatility 5m: `{best['vol_5m']}`
EMA OK: `{best['ema_ok']}` Uplift: `{best['ema_uplift']:.4f}`
RSI: `{best['rsi']}`
Orderbook Bullish: `{best['ob_bull']}`
Score: `{best['score']:.2f}`
"""
    send_telegram(msg)
    return (best['symbol'], best['last_price'], best['24h_vol'], best['24h_change'])

# -------------------------
# Flask & Trade Cycle
CYCLE_SECONDS = int(os.getenv("CYCLE_SECONDS", "8"))
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot running! ✅"

def trade_cycle():
    while True:
        result = pick_coin()
        if result:
            print(f"Sent signal for {result[0]}")
        else:
            print("No coin found in this cycle.")
        time.sleep(CYCLE_SECONDS)

def start_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)

if __name__ == "__main__":
    bot_thread = threading.Thread(target=trade_cycle, daemon=True)
    bot_thread.start()
    start_flask()