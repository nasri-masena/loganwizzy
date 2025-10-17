import os
import time
import math
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics
import shelve
from flask import Flask

# -------------------------
# Config
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

BINANCE_REST = "https://api.binance.com"
QUOTE = "USDT"
PRICE_MIN = 1.0
PRICE_MAX = 4.0
MIN_VOLUME = 1_000_000        # 24h quote volume threshold
TOP_BY_24H_VOLUME = 6
CYCLE_SECONDS = int(os.getenv("CYCLE_SECONDS", "4"))
KLINES_5M_LIMIT = 12
KLINES_1M_LIMIT = 12
EMA_SHORT = 5
EMA_LONG = 20
RSI_PERIOD = 9
MIN_OB_IMBALANCE = 1.5
MAX_OB_SPREAD_PCT = 0.4
MIN_5M_PCT = 0.4
MIN_1M_PCT = 0.2
PREBUY_BREAKOUT_MARGIN = 0.004
MIN_FINAL_EMA_UPLIFT = 0.001
OB_DEPTH = 10
PUBLIC_CONCURRENCY = int(os.getenv("PUBLIC_CONCURRENCY", "4"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "6"))
PUBLIC_CONCURRENCY = int(os.getenv("PUBLIC_CONCURRENCY", "6"))
# persistence file
RECENT_BUYS_DB = os.path.join(os.getcwd(), "recent_buys.db")

# -------------------------
# Locks / semaphores / cache
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
# Persist recent buys (shelve)
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

# load at startup
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
# Math / indicators
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

def fetch_agg_trades(symbol, limit=100):
    key = f"agg:{symbol}:{limit}"
    cached = cache_get(key)
    if cached:
        return cached
    try:
        params = {"symbol": symbol, "limit": limit}
        with REQUESTS_SEMAPHORE:
            resp = requests.get(BINANCE_REST + "/api/v3/aggTrades", params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        cache_set(key, data)
        return data
    except Exception:
        return []

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
        # use quote-value = price * qty
        bid_quote = sum(float(b[0]) * float(b[1]) for b in bids[:depth]) + 1e-12
        ask_quote = sum(float(a[0]) * float(a[1]) for a in asks[:depth]) + 1e-12
        if bid_quote < min_quote_depth:
            return False
        imbalance = bid_quote / ask_quote
        return (imbalance >= min_imbalance) and (spread_pct <= max_spread_pct)
    except Exception:
        return False
        
# -------------------------
# Binance public calls (cached + semaphore)
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
# Candidate evaluation (parallel)
# -------------------------
def evaluate_symbol(ticker):
    try:
        sym = ticker.get("symbol")
        last_price = float(ticker.get("lastPrice") or 0.0)
        qvol = float(ticker.get("quoteVolume") or 0.0)
        change_24h = float(ticker.get("priceChangePercent") or 0.0)

        # base filters (as before)
        if not (PRICE_MIN <= last_price <= PRICE_MAX):
            return None
        if qvol < MIN_VOLUME:
            return None
        if change_24h < 0.5 or change_24h > 20.0:
            return None

        # quick taker-buy ratio filter (prefer real buy pressure)
        taker_buy_q = float(ticker.get("takerBuyQuoteAssetVolume") or 0.0)
        if (taker_buy_q / (qvol + 1e-12)) < 0.25:
            return None

        kl5 = fetch_klines(sym, "5m", KLINES_5M_LIMIT)
        kl1 = fetch_klines(sym, "1m", KLINES_1M_LIMIT)
        ob = fetch_order_book(sym, limit=OB_DEPTH)
        if not kl5 or len(kl5) < 4 or not kl1 or len(kl1) < 2:
            return None

        closes_5m = [float(k[4]) for k in kl5]
        closes_1m = [float(k[4]) for k in kl1]
        pct_5m = pct_change(float(kl5[0][1]), closes_5m[-1])
        pct_1m = pct_change(float(kl1[0][1]), closes_1m[-1])
        if pct_5m < MIN_5M_PCT or pct_1m < MIN_1M_PCT:
            return None

        vol_5m = compute_recent_volatility(closes_5m)
        short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
        long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
        ema_ok = False
        ema_uplift = 0.0
        if short_ema and long_ema and long_ema != 0:
            ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
            ema_ok = short_ema > long_ema * (1.0 + MIN_FINAL_EMA_UPLIFT)

        rsi_val = compute_rsi_local(closes_5m[-(RSI_PERIOD+1):], RSI_PERIOD) if len(closes_5m) >= RSI_PERIOD+1 else None
        if rsi_val is not None and rsi_val > 72:
            return None

        ob_bull = orderbook_bullish(ob, depth=OB_DEPTH, min_imbalance=MIN_OB_IMBALANCE, max_spread_pct=MAX_OB_SPREAD_PCT)

        # quick aggTrades check: recent buyer-initiated volume in last N trades
        agg = fetch_agg_trades(sym, limit=200)
        buy_quote = 0.0
        total_quote = 0.0
        for a in agg[-60:]:  # last ~60 agg trades
            price = float(a.get('p', a.get('price', 0)))
            qty = float(a.get('q', a.get('qty', 0)))
            is_buyer_maker = a.get('m')  # True when buyer is maker => SELL trade
            quote = price * qty
            total_quote += quote
            if not is_buyer_maker:
                buy_quote += quote
        buy_ratio = buy_quote / (total_quote + 1e-12)
        if buy_ratio < 0.45:
            return None

        # scoring (same as before, adjusted)
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
        if buy_ratio > 0.65:
            score += 30.0

        strong_candidate = (pct_5m >= MIN_5M_PCT and pct_1m >= MIN_1M_PCT and ema_ok and ob_bull and buy_ratio > 0.5)
        return {
            "symbol": sym,
            "last_price": last_price,
            "24h_change": change_24h,
            "24h_vol": qvol,
            "pct_5m": pct_5m,
            "pct_1m": pct_1m,
            "vol_5m": vol_5m,
            "ema_ok": ema_ok,
            "ema_uplift": ema_uplift,
            "rsi": rsi_val,
            "ob_bull": ob_bull,
            "buy_ratio": buy_ratio,
            "score": score,
            "strong_candidate": strong_candidate
        }
    except Exception:
        return None
        
# -------------------------
# Main picker (atomic + persistence)
# -------------------------
def pick_coin():
    try:
        tickers = fetch_tickers()
        now = time.time()
        pre = []

        # collect full ticker dicts and skip recent buys
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
            pre.append(t)

        if not pre:
            return None

        # evaluate top-by-volume candidates in parallel
        pre.sort(key=lambda x: float(x.get("quoteVolume", 0.0)), reverse=True)
        candidates = pre[:TOP_BY_24H_VOLUME]
        results = []
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates) or 1)) as ex:
            futures = {ex.submit(evaluate_symbol, cand): cand.get("symbol") for cand in candidates}
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    if res:
                        results.append(res)
                except Exception:
                    pass

        if not results:
            return None

        # prefer strong candidates, else pick highest score
        strongs = [r for r in results if r.get("strong_candidate")]
        chosen_pool = strongs if strongs else results
        chosen = sorted(chosen_pool, key=lambda x: x["score"], reverse=True)[0]
        sym = chosen["symbol"]

        # FINAL stricter verification with fresh market data
        kl5 = fetch_klines(sym, "5m", KLINES_5M_LIMIT)
        kl1 = fetch_klines(sym, "1m", KLINES_1M_LIMIT)
        ob = fetch_order_book(sym, limit=OB_DEPTH)
        reason = None

        if not kl5 or len(kl5) < 4:
            reason = "klines insufficient"
        else:
            closes_5m = [float(k[4]) for k in kl5]
            closes_1m = [float(k[4]) for k in kl1] if kl1 and len(kl1) >= 2 else []
            # breakout
            PREBUY_BREAKOUT_MARGIN = globals().get("PREBUY_BREAKOUT_MARGIN", 0.004)
            recent_high = max(closes_5m[:-1]) if len(closes_5m) > 1 else closes_5m[-1]
            if not (closes_5m[-1] > recent_high * (1.0 + PREBUY_BREAKOUT_MARGIN)):
                reason = "no breakout"
            # pct checks
            pct_5m = pct_change(float(kl5[0][1]), closes_5m[-1])
            pct_1m = pct_change(float(kl1[0][1]), closes_1m[-1]) if closes_1m else 0.0
            if pct_5m < MIN_5M_PCT or pct_1m < MIN_1M_PCT:
                reason = f"pct fail 5m={pct_5m:.2f} 1m={pct_1m:.2f}"
            # EMA uplift
            short_ema = ema_local(closes_5m[-EMA_SHORT:], EMA_SHORT) if len(closes_5m) >= EMA_SHORT else None
            long_ema = ema_local(closes_5m[-EMA_LONG:], EMA_LONG) if len(closes_5m) >= EMA_LONG else None
            ema_uplift = 0.0
            if short_ema and long_ema and long_ema != 0:
                ema_uplift = max(0.0, (short_ema - long_ema) / (long_ema + 1e-12))
            MIN_FINAL_EMA_UPLIFT = globals().get("MIN_FINAL_EMA_UPLIFT", 0.001)
            if not (short_ema and long_ema and ema_uplift >= MIN_FINAL_EMA_UPLIFT and short_ema > long_ema * (1.0 + MIN_FINAL_EMA_UPLIFT)):
                reason = "ema fail"
            # orderbook quick check
            if not orderbook_bullish(ob, depth=OB_DEPTH, min_imbalance=MIN_OB_IMBALANCE, max_spread_pct=MAX_OB_SPREAD_PCT):
                reason = "orderbook weak"
            # volatility guard
            vol5 = compute_recent_volatility(closes_5m) or 0.0
            if vol5 > 0.05:
                reason = "volatility high"
            # aggTrades re-check buy pressure
            agg = fetch_agg_trades(sym, limit=200)
            if agg:
                buy_quote = 0.0
                total_quote = 0.0
                recent_agg = agg[-60:]
                for a in recent_agg:
                    price = float(a.get('p', a.get('price', 0)))
                    qty = float(a.get('q', a.get('qty', 0)))
                    is_buyer_maker = a.get('m')  # True => buyer is maker => SELL trade
                    quote = price * qty
                    total_quote += quote
                    if not is_buyer_maker:
                        buy_quote += quote
                buy_ratio = buy_quote / (total_quote + 1e-12)
                if buy_ratio < 0.45:
                    reason = "agg buy weak"
            else:
                # if no agg data treat as weak
                reason = reason or "agg missing"

        if reason:
            with RECENT_BUYS_LOCK:
                RECENT_BUYS[sym] = {"ts": now, "reserved": False}
                try:
                    persist_recent_buys()
                except Exception:
                    pass
            print(f"[{time.strftime('%H:%M:%S')}] Candidate {sym} rejected: {reason}")
            return None

        # prepare message
        msg = (
            f"🚀 *COIN SIGNAL*: `{sym}`\n"
            f"Price: `{chosen.get('last_price')}`\n"
            f"24h Change: `{chosen.get('24h_change')}`%\n"
            f"5m Change: `{pct_5m:.2f}`%\n"
            f"1m Change: `{pct_1m:.2f}`%\n"
            f"Volatility 5m: `{vol5}`\n"
            f"EMA Uplift: `{ema_uplift:.6f}`\n"
            f"Orderbook Bullish: `{orderbook_bullish(ob, depth=OB_DEPTH, min_imbalance=MIN_OB_IMBALANCE, max_spread_pct=MAX_OB_SPREAD_PCT)}`\n"
            f"Score: `{chosen.get('score'):.2f}`"
        )

        # atomic reserve/persist, then send
        now_send = time.time()
        with RECENT_BUYS_LOCK:
            last_buy = RECENT_BUYS.get(sym)
            if last_buy and now_send < last_buy.get("ts", 0) + BUY_LOCK_SECONDS:
                print(f"[{time.strftime('%H:%M:%S')}] Skipped duplicate (reserved): {sym}")
                return None
            RECENT_BUYS[sym] = {"ts": now_send, "reserved": True}
            try:
                persist_recent_buys()
            except Exception:
                pass

        sent = send_telegram(msg)
        if sent:
            with RECENT_BUYS_LOCK:
                RECENT_BUYS[sym].update({"ts": time.time(), "reserved": False})
                try:
                    persist_recent_buys()
                except Exception:
                    pass
            print(f"[{time.strftime('%H:%M:%S')}] Signal -> {sym} score={chosen.get('score'):.2f}")
            return chosen
        else:
            with RECENT_BUYS_LOCK:
                RECENT_BUYS.pop(sym, None)
                try:
                    persist_recent_buys()
                except Exception:
                    pass
            print(f"[{time.strftime('%H:%M:%S')}] Telegram send failed for {sym}")
            return None

    except Exception as e:
        print("pick_coin error", e)
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
