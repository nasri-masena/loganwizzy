import os
import time
import math
import threading
import requests
import statistics
import shelve
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask

# -------------------------
# Optional: binance client
# -------------------------
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException, BinanceRequestException
except Exception:
    Client = None
    BinanceAPIException = Exception
    BinanceRequestException = Exception

# -------------------------
# Config (tweak these)
# -------------------------
# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# Binance
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

ENABLE_TRADING = os.getenv("ENABLE_TRADING", "True").lower() in ("1","true","yes")
# If using fixed USDT allocation per trade:
BUY_USDT_AMOUNT = float(os.getenv("BUY_USDT_AMOUNT", "8.0"))  # USDT to spend per buy
# Profit target for limit sell in percent
LIMIT_PROFIT_PCT = float(os.getenv("LIMIT_PROFIT_PCT", "1.0"))  # 1.0 means +1%
# If you prefer market buy by base qty instead of quote, set BUY_BY_QUOTE=False and configure BUY_BASE_QTY
BUY_BY_QUOTE = os.getenv("BUY_BY_QUOTE", "True").lower() in ("1","true","yes")
BUY_BASE_QTY = float(os.getenv("BUY_BASE_QTY", "0.0"))  # only used if BUY_BY_QUOTE False

# Safety / risk
MAX_CONCURRENT_POS = int(os.getenv("MAX_CONCURRENT_POS", "5"))

# Existing config from your script (kept)
BINANCE_REST = "https://api.binance.com"
QUOTE = "USDT"
PRICE_MIN = 1.0
PRICE_MAX = 4.0
MIN_VOLUME = 1_000_000
TOP_BY_24H_VOLUME = 6
CYCLE_SECONDS = int(os.getenv("CYCLE_SECONDS", "4"))
KLINES_5M_LIMIT = 6
KLINES_1M_LIMIT = 6
EMA_SHORT = 3
EMA_LONG = 10
RSI_PERIOD = 14
OB_DEPTH = 3
MIN_OB_IMBALANCE = 1.2
MAX_OB_SPREAD_PCT = 1.0
MIN_5M_PCT = 0.6
MIN_1M_PCT = 0.3
CACHE_TTL = 1.0
MAX_WORKERS = 8
RECENT_BUYS = {}
BUY_LOCK_SECONDS = int(os.getenv("BUY_LOCK_SECONDS", "900"))

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "6"))
PUBLIC_CONCURRENCY = int(os.getenv("PUBLIC_CONCURRENCY", "6"))

RECENT_BUYS_DB = os.path.join(os.getcwd(), "recent_buys.db")

# -------------------------
# Concurrency / cache
# -------------------------
REQUESTS_SEMAPHORE = threading.BoundedSemaphore(value=PUBLIC_CONCURRENCY)
RECENT_BUYS_LOCK = threading.Lock()
_cache = {}
_cache_lock = threading.Lock()

def cache_get(key):
    with _cache_lock:
        v = _cache.get(key)
        if not v:
            return None
        ts, val = v
        if time.time() - ts > CACHE_TTL:
            _cache.pop(key, None)
            return None
        return val

def cache_set(key, val):
    with _cache_lock:
        _cache[key] = (time.time(), val)

# -------------------------
# Persistence
# -------------------------
def load_recent_buys():
    try:
        with shelve.open(RECENT_BUYS_DB) as db:
            data = db.get("data", {})
            if isinstance(data, dict):
                RECENT_BUYS.update(data)
    except Exception:
        pass

def persist_recent_buys():
    try:
        with shelve.open(RECENT_BUYS_DB) as db:
            db["data"] = RECENT_BUYS
    except Exception:
        pass

load_recent_buys()

# -------------------------
# Telegram helper
# -------------------------
def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram not configured. Message:")
        print(message)
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.status_code == 200
    except Exception as e:
        print("Telegram error", e)
        return False

# -------------------------
# Math / indicators (kept)
# -------------------------
def pct_change(open_p, close_p):
    try:
        if open_p == 0:
            return 0.0
        return (close_p - open_p) / open_p * 100.0
    except Exception:
        return 0.0

def ema_local(values, period):
    if not values or period <= 0 or len(values) < 1:
        return None
    alpha = 2.0 / (period + 1.0)
    e = float(values[0])
    for v in values[1:]:
        e = alpha * float(v) + (1 - alpha) * e
    return e

def compute_rsi_local(closes, period=14):
    if not closes or len(closes) < period + 1:
        return None
    gains = []
    losses = []
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

def compute_recent_volatility(closes, lookback=5):
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
    return abs(min(vol, 5.0))

def orderbook_bullish(ob, depth=10, min_imbalance=1.5, max_spread_pct=0.4, min_quote_depth=1000.0):
    try:
        bids = ob.get('bids') or []
        asks = ob.get('asks') or []
        if len(bids) < 1 or len(asks) < 1:
            return False
        top_bid = float(bids[0][0]); top_ask = float(asks[0][0])
        spread_pct = (top_ask - top_bid) / (top_bid + 1e-12) * 100.0
        bid_quote = sum(float(b[0]) * float(b[1]) for b in bids[:depth]) + 1e-12
        ask_quote = sum(float(a[0]) * float(a[1]) for a in asks[:depth]) + 1e-12
        if bid_quote < min_quote_depth:
            return False
        imbalance = bid_quote / ask_quote
        return (imbalance >= min_imbalance) and (spread_pct <= max_spread_pct)
    except Exception:
        return False

# -------------------------
# Binance public calls (cached)
# -------------------------
def fetch_tickers():
    key = "tickers"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        with REQUESTS_SEMAPHORE:
            resp = requests.get(BINANCE_REST + "/api/v3/ticker/24hr", timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        cache_set(key, data)
        return data
    except Exception as e:
        print("fetch_tickers error", e)
        return []

def fetch_klines(symbol, interval, limit):
    key = f"klines:{symbol}:{interval}:{limit}"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        with REQUESTS_SEMAPHORE:
            resp = requests.get(BINANCE_REST + "/api/v3/klines", params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        cache_set(key, data)
        return data
    except Exception:
        return []

def fetch_order_book(symbol, limit=OB_DEPTH):
    key = f"depth:{symbol}:{limit}"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        params = {"symbol": symbol, "limit": max(5, limit)}
        with REQUESTS_SEMAPHORE:
            resp = requests.get(BINANCE_REST + "/api/v3/depth", params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        cache_set(key, data)
        return data
    except Exception:
        return {}

# -------------------------
# Evaluate symbol (kept, minor tweak)
# -------------------------
def evaluate_symbol(sym, last_price, qvol, change_24h):
    try:
        if not (PRICE_MIN <= last_price <= PRICE_MAX):
            return None
        if qvol < MIN_VOLUME:
            return None
        if change_24h < 0.5 or change_24h > 20.0:
            return None
        kl5 = fetch_klines(sym, "5m", KLINES_5M_LIMIT)
        kl1 = fetch_klines(sym, "1m", KLINES_1M_LIMIT)
        ob = fetch_order_book(sym, limit=OB_DEPTH)
        if not kl5 or len(kl5) < 3 or not kl1 or len(kl1) < 2:
            return None
        closes_5m = [float(k[4]) for k in kl5]
        closes_1m = [float(k[4]) for k in kl1]
        pct_5m = pct_change(float(kl5[0][1]), closes_5m[-1])
        pct_1m = pct_change(float(kl1[0][1]), closes_1m[-1])
        if pct_5m < MIN_5M_PCT or pct_1m < MIN_1M_PCT:
            return None
        vol_5m = compute_recent_volatility(closes_5m)
        vol_1m = compute_recent_volatility(closes_1m, lookback=3)
        short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
        long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
        ema_ok = False
        ema_uplift = 0.0
        if short_ema and long_ema and long_ema != 0:
            ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
            ema_ok = short_ema > long_ema * 1.0005
        rsi_val = compute_rsi_local(closes_5m[-(RSI_PERIOD+1):], RSI_PERIOD) if len(closes_5m) >= RSI_PERIOD+1 else None
        if rsi_val is not None and rsi_val > 70:
            return None
        ob_bull = orderbook_bullish(ob, depth=OB_DEPTH, min_imbalance=MIN_OB_IMBALANCE, max_spread_pct=MAX_OB_SPREAD_PCT)
        score = 0.0
        score += max(0.0, pct_5m) * 4.0
        score += max(0.0, pct_1m) * 2.0
        score += ema_uplift * 500.0
        score += max(0.0, change_24h) * 0.5
        if vol_5m is not None:
            score += max(0.0, (0.01 - min(vol_5m, 0.01))) * 100.0
        if rsi_val is not None:
            score += max(0.0, (60.0 - min(rsi_val, 60.0))) * 1.0
        if ob_bull:
            score += 25.0
        strong_candidate = (pct_5m >= MIN_5M_PCT and pct_1m >= MIN_1M_PCT and ema_ok and ob_bull)
        return {
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
        }
    except Exception:
        return None

# -------------------------
# Binance trading helpers
# -------------------------
_binance_client = None
_exchange_info_cache = {}
_symbol_info_cache = {}

def init_binance_client():
    global _binance_client
    if not Client:
        print("python-binance not installed")
        return None
    if _binance_client:
        return _binance_client
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("Binance API keys not set")
        return None
    _binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    return _binance_client

def fetch_exchange_info():
    key = "exchange_info"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        client = init_binance_client()
        if not client:
            # fallback to public REST
            with REQUESTS_SEMAPHORE:
                r = requests.get(BINANCE_REST + "/api/v3/exchangeInfo", timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            cache_set(key, data)
            return data
        data = client.get_exchange_info()
        cache_set(key, data)
        return data
    except Exception as e:
        print("fetch_exchange_info error", e)
        return {}

def get_symbol_info(symbol):
    if symbol in _symbol_info_cache:
        return _symbol_info_cache[symbol]
    ex = fetch_exchange_info()
    for s in ex.get("symbols", []):
        if s.get("symbol") == symbol:
            _symbol_info_cache[symbol] = s
            return s
    return None

def round_step(value, step):
    # round down to step precision
    step = float(step)
    if step == 0:
        return float(value)
    precision = int(round(-math.log10(step))) if step < 1 else 0
    # floor to step
    qty = math.floor(float(value) / step) * step
    # format to precision
    fmt = "{:0." + str(max(0, precision)) + "f}"
    return float(fmt.format(qty))

def adjust_qty_price_for_filters(symbol, qty, price):
    s = get_symbol_info(symbol)
    if not s:
        return qty, price
    filters = {f["filterType"]: f for f in s.get("filters", [])}
    # adjust qty by LOT_SIZE or MARKET_LOT_SIZE
    lot = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE")
    if lot:
        step = float(lot.get("stepSize", "1"))
        minQty = float(lot.get("minQty", "0"))
        qty = round_step(qty, step)
        if qty < minQty:
            qty = 0.0
    # adjust price by PRICE_FILTER tickSize
    pf = filters.get("PRICE_FILTER")
    if pf:
        tick = float(pf.get("tickSize", "0"))
        price = round_step(price, tick)
    # check minNotional
    mn = filters.get("MIN_NOTIONAL")
    if mn:
        min_not = float(mn.get("minNotional", "0"))
        if price * qty < min_not:
            # mark invalid
            return 0.0, price
    return qty, price

def place_market_buy_by_quote(symbol, quote_qty):
    """
    Use quoteOrderQty to place market buy using USDT amount. Falls back to compute qty.
    """
    client = init_binance_client()
    if not client:
        raise RuntimeError("Binance client not available")
    try:
        # prefer using quoteOrderQty (buy with USDT)
        order = client.order_market_buy(symbol=symbol, quoteOrderQty=str(quote_qty))
        return order
    except BinanceAPIException as e:
        # sometimes exchange rejects quoteOrderQty; fallback to compute qty and use quantity param
        print("market buy by quote failed:", e)
        # compute price from ticker
        book = fetch_order_book(symbol, limit=5)
        if not book:
            raise
        top_ask = float(book["asks"][0][0])
        raw_qty = quote_qty / top_ask
        qty, _ = adjust_qty_price_for_filters(symbol, raw_qty, top_ask)
        if qty <= 0:
            raise RuntimeError("Computed qty below symbol min after filters")
        order = client.order_market_buy(symbol=symbol, quantity=str(qty))
        return order

def place_limit_sell(symbol, qty, sell_price):
    client = init_binance_client()
    if not client:
        raise RuntimeError("Binance client not available")
    # adjust qty/price to symbol filters
    qty_adj, price_adj = adjust_qty_price_for_filters(symbol, qty, sell_price)
    if qty_adj <= 0:
        raise RuntimeError("Qty invalid after filters")
    try:
        order = client.order_limit_sell(symbol=symbol, quantity=str(qty_adj), price=str(price_adj), timeInForce='GTC')
        return order
    except BinanceAPIException as e:
        raise

def parse_market_fill(order_resp):
    """
    For market orders, binance returns 'fills' with price/qty. Compute executed qty and avg price.
    """
    fills = order_resp.get("fills") or []
    if not fills:
        # some responses include 'executedQty' and 'cummulativeQuoteQty'
        executedQty = float(order_resp.get("executedQty", 0) or 0)
        avg_price = 0.0
        if executedQty:
            cquote = float(order_resp.get("cummulativeQuoteQty", 0) or 0)
            avg_price = cquote / executedQty if executedQty else 0.0
        return executedQty, avg_price
    total_qty = 0.0
    total_quote = 0.0
    for f in fills:
        q = float(f.get("qty", 0))
        p = float(f.get("price", 0))
        total_qty += q
        total_quote += q * p
    avg_price = (total_quote / total_qty) if total_qty else 0.0
    return total_qty, avg_price

def wait_for_order_fill(symbol, order_id, timeout=10, poll=0.5):
    client = init_binance_client()
    if not client:
        return None
    start = time.time()
    while time.time() - start < timeout:
        try:
            o = client.get_order(symbol=symbol, orderId=order_id)
            status = o.get("status")
            if status in ("FILLED", "CANCELED", "REJECTED"):
                return o
        except Exception:
            pass
        time.sleep(poll)
    return None

# -------------------------
# Execute trade (called after pick)
# -------------------------
def execute_trade(chosen):
    symbol = chosen["symbol"]
    now = time.time()
    with RECENT_BUYS_LOCK:
        if len([k for k,v in RECENT_BUYS.items() if not v.get("closed")]) >= MAX_CONCURRENT_POS:
            send_telegram(f"⚠️ Max concurrent positions reached. Skipping {symbol}")
            return False
        RECENT_BUYS[symbol] = {"ts": now, "reserved": False, "closed": False}
        persist_recent_buys()

    client = init_binance_client()
    if not client:
        send_telegram(f"⚠️ Trading disabled or client missing. Skipping live trade for {symbol}")
        return False

    try:
        # Decide buy method
        if BUY_BY_QUOTE and BUY_USDT_AMOUNT > 0:
            order = place_market_buy_by_quote(symbol, BUY_USDT_AMOUNT)
        elif not BUY_BY_QUOTE and BUY_BASE_QTY > 0:
            order = client.order_market_buy(symbol=symbol, quantity=str(BUY_BASE_QTY))
        else:
            send_telegram("⚠️ Buy amount not configured. Skipping trade.")
            return False

        # parse fill
        executed_qty, avg_price = parse_market_fill(order)
        if executed_qty <= 0:
            send_telegram(f"⚠️ Buy executed but no fill qty for {symbol}. Raw: {order}")
            return False

        buy_msg = (f"✅ BUY EXECUTED: `{symbol}`\n"
                   f"Qty: `{executed_qty}` @ `{avg_price}`\n"
                   f"Spent(quote): `{round(executed_qty*avg_price,6)}`")
        send_telegram(buy_msg)

        # place limit sell at target
        target_price = avg_price * (1.0 + (LIMIT_PROFIT_PCT/100.0))
        # adjust quantities/prices
        try:
            sell_order = place_limit_sell(symbol, executed_qty, target_price)
            sell_price_used = float(sell_order.get("price") or target_price)
            send_telegram(f"💰 LIMIT SELL PLACED: `{symbol}` Qty `{executed_qty}` @ `{sell_price_used}` (+{LIMIT_PROFIT_PCT}%)")
            # store position
            with RECENT_BUYS_LOCK:
                RECENT_BUYS[symbol].update({"qty": executed_qty, "buy_price": avg_price, "sell_price": sell_price_used, "order_id": sell_order.get("orderId")})
                persist_recent_buys()
            return True
        except Exception as e:
            send_telegram(f"⚠️ Failed to place limit sell for {symbol}: {e}")
            return False

    except BinanceAPIException as e:
        send_telegram(f"‼️ Binance API error during buy {symbol}: {e}")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(symbol, None)
            persist_recent_buys()
        return False
    except Exception as e:
        send_telegram(f"‼️ Unexpected error during trade {symbol}: {e}")
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(symbol, None)
            persist_recent_buys()
        return False

# -------------------------
# pick_coin remains mostly same, but triggers execute_trade if enabled
# -------------------------
def pick_coin():
    tickers = fetch_tickers()
    now = time.time()
    pre = []
    for t in tickers:
        sym = t.get("symbol")
        if not sym or not sym.endswith(QUOTE):
            continue
        try:
            last = float(t.get("lastPrice") or 0.0)
            qvol = float(t.get("quoteVolume") or 0.0)
            ch = float(t.get("priceChangePercent") or 0.0)
        except Exception:
            continue
        if not (PRICE_MIN <= last <= PRICE_MAX):
            continue
        if qvol < MIN_VOLUME:
            continue
        if ch < 0.5 or ch > 20.0:
            continue
        with RECENT_BUYS_LOCK:
            last_buy = RECENT_BUYS.get(sym)
            if last_buy and now < last_buy.get("ts", 0) + BUY_LOCK_SECONDS:
                continue
        pre.append((sym, last, qvol, ch))
    if not pre:
        return None
    pre.sort(key=lambda x: x[2], reverse=True)
    candidates = pre[:TOP_BY_24H_VOLUME]
    results = []
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates) or 1)) as ex:
        futures = {ex.submit(evaluate_symbol, sym, last, qvol, ch): sym for (sym, last, qvol, ch) in candidates}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)
    if not results:
        return None
    strongs = [r for r in results if r["strong_candidate"]]
    chosen_pool = strongs if strongs else results
    chosen = sorted(chosen_pool, key=lambda x: x["score"], reverse=True)[0]

    msg = (
        f"🚀 *COIN SIGNAL*: `{chosen['symbol']}`\n"
        f"Price: `{chosen['last_price']}`\n"
        f"24h Change: `{chosen['24h_change']}`%\n"
        f"5m Change: `{chosen['pct_5m']:.2f}`%\n"
        f"1m Change: `{chosen['pct_1m']:.2f}`%\n"
        f"Volatility 5m: `{chosen['vol_5m']}`\n"
        f"EMA OK: `{chosen['ema_ok']}` Uplift: `{chosen['ema_uplift']:.4f}`\n"
        f"RSI: `{chosen['rsi']}`\n"
        f"Orderbook Bullish: `{chosen['ob_bull']}`\n"
        f"Score: `{chosen['score']:.2f}`"
    )

    now_send = time.time()
    with RECENT_BUYS_LOCK:
        last_buy = RECENT_BUYS.get(chosen["symbol"])
        if last_buy and now_send < last_buy.get("ts", 0) + BUY_LOCK_SECONDS:
            print(f"[{time.strftime('%H:%M:%S')}] Skipped duplicate (reserved): {chosen['symbol']}")
            return None
        RECENT_BUYS[chosen["symbol"]] = {"ts": now_send, "reserved": True}
        persist_recent_buys()

    sent = send_telegram(msg)
    if sent:
        with RECENT_BUYS_LOCK:
            RECENT_BUYS[chosen["symbol"]].update({"ts": time.time(), "reserved": False})
            persist_recent_buys()
        print(f"[{time.strftime('%H:%M:%S')}] Signal -> {chosen['symbol']} score={chosen['score']:.2f}")

        # trigger live trade if enabled
        if ENABLE_TRADING:
            t = threading.Thread(target=execute_trade, args=(chosen,), daemon=True)
            t.start()
        return chosen
    else:
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(chosen["symbol"], None)
            persist_recent_buys()
        print(f"[{time.strftime('%H:%M:%S')}] Telegram send failed for {chosen['symbol']}")
        return None

# -------------------------
# Main loop / web healthcheck
# -------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "Signal bot running"

def trade_cycle():
    while True:
        try:
            res = pick_coin()
            if res:
                print(f"[{time.strftime('%H:%M:%S')}] Signal -> {res['symbol']} score={res['score']:.2f}")
            else:
                print(f"[{time.strftime('%H:%M:%S')}] No signal")
        except Exception as e:
            print("cycle error", e)
        time.sleep(CYCLE_SECONDS)

if __name__ == "__main__":
    t = threading.Thread(target=trade_cycle, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), threaded=True)
