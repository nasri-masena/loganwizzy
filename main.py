# signal_bot_with_monitor.py
import os, time, math, threading, requests, statistics, shelve
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask
# try import python-binance
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
except Exception:
    Client = None
    BinanceAPIException = Exception

# ---------- config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
ENABLE_TRADING = os.getenv("ENABLE_TRADING", "True").lower() in ("1","true","yes")
BUY_USDT_AMOUNT = float(os.getenv("BUY_USDT_AMOUNT", "8.0"))
LIMIT_PROFIT_PCT = float(os.getenv("LIMIT_PROFIT_PCT", "1.0"))
BUY_BY_QUOTE = os.getenv("BUY_BY_QUOTE", "True").lower() in ("1","true","yes")
BUY_BASE_QTY = float(os.getenv("BUY_BASE_QTY", "0.0"))
MAX_CONCURRENT_POS = int(os.getenv("MAX_CONCURRENT_POS", "5"))

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

# new settings
SHORT_BUY_SELL_DELAY = float(os.getenv("SHORT_BUY_SELL_DELAY", "0.3"))
HOLD_THRESHOLD_HOURS = float(os.getenv("HOLD_THRESHOLD_HOURS", "4.0"))
MONITOR_INTERVAL = float(os.getenv("MONITOR_INTERVAL", "60"))  # seconds between monitor checks

# ---------- caches/locks ----------
REQUESTS_SEMAPHORE = threading.BoundedSemaphore(value=PUBLIC_CONCURRENCY)
RECENT_BUYS_LOCK = threading.Lock()
_cache = {}
_cache_lock = threading.Lock()
OPEN_ORDERS_CACHE = {"data": None, "ts": 0}
OPEN_ORDERS_LOCK = threading.Lock()
TEMP_SKIP = {}
RATE_LIMIT_BACKOFF = None

# ---------- cache/persist ----------
def cache_get(key):
    with _cache_lock:
        v = _cache.get(key)
        if not v: return None
        ts,val = v
        if time.time()-ts > CACHE_TTL:
            _cache.pop(key,None); return None
        return val
def cache_set(key,val):
    with _cache_lock: _cache[key]=(time.time(),val)

def load_recent_buys():
    try:
        with shelve.open(RECENT_BUYS_DB) as db:
            data = db.get("data", {})
            if isinstance(data, dict): RECENT_BUYS.update(data)
    except Exception: pass

def persist_recent_buys():
    try:
        with shelve.open(RECENT_BUYS_DB) as db:
            db["data"] = RECENT_BUYS
    except Exception: pass

load_recent_buys()

# ---------- telegram ----------
def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print(message); return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.status_code == 200
    except Exception as e:
        print("Telegram error", e); return False
notify = send_telegram

# ---------- indicators & helpers (kept) ----------
def pct_change(open_p, close_p):
    try:
        if open_p == 0: return 0.0
        return (close_p-open_p)/open_p*100.0
    except: return 0.0

def ema_local(values, period):
    if not values or period<=0: return None
    alpha = 2.0/(period+1.0); e=float(values[0])
    for v in values[1:]: e = alpha*float(v)+(1-alpha)*e
    return e

def compute_rsi_local(closes, period=14):
    if not closes or len(closes)<period+1: return None
    gains=[]; losses=[]
    for i in range(1,len(closes)):
        d=closes[i]-closes[i-1]; gains.append(max(0,d)); losses.append(max(0,-d))
    avg_gain=sum(gains[:period])/period
    avg_loss=sum(losses[:period])/period if sum(losses[:period])!=0 else 1e-9
    for i in range(period,len(gains)):
        avg_gain=(avg_gain*(period-1)+gains[i])/period
        avg_loss=(avg_loss*(period-1)+losses[i])/period
    rs=avg_gain/(avg_loss if avg_loss>0 else 1e-9); return 100-(100/(1+rs))

def compute_recent_volatility(closes, lookback=5):
    if not closes or len(closes)<2: return None
    rets=[]
    for i in range(1,len(closes)):
        prev=float(closes[i-1]); cur=float(closes[i])
        if prev<=0: continue
        rets.append((cur-prev)/prev)
    if not rets: return None
    recent=rets[-lookback:] if lookback and len(rets)>=1 else rets
    if len(recent)==0: return None
    if len(recent)==1: return abs(recent[0])
    try: vol=statistics.pstdev(recent)
    except Exception: vol=statistics.stdev(recent) if len(recent)>1 else abs(recent[-1])
    return abs(min(vol,5.0))

def orderbook_bullish(ob, depth=10, min_imbalance=1.5, max_spread_pct=0.4, min_quote_depth=1000.0):
    try:
        bids=ob.get('bids') or []; asks=ob.get('asks') or []
        if len(bids)<1 or len(asks)<1: return False
        top_bid=float(bids[0][0]); top_ask=float(asks[0][0])
        spread_pct=(top_ask-top_bid)/(top_bid+1e-12)*100.0
        bid_quote=sum(float(b[0])*float(b[1]) for b in bids[:depth])+1e-12
        ask_quote=sum(float(a[0])*float(a[1]) for a in asks[:depth])+1e-12
        if bid_quote<min_quote_depth: return False
        imbalance=bid_quote/ask_quote
        return (imbalance>=min_imbalance) and (spread_pct<=max_spread_pct)
    except: return False

# ---------- public REST helpers ----------
def fetch_tickers():
    key="tickers"; c=cache_get(key)
    if c: return c
    try:
        with REQUESTS_SEMAPHORE:
            r=requests.get(BINANCE_REST+"/api/v3/ticker/24hr", timeout=REQUEST_TIMEOUT); r.raise_for_status(); data=r.json(); cache_set(key,data); return data
    except Exception as e:
        print("fetch_tickers error", e); return []

def fetch_klines(symbol, interval, limit):
    key=f"klines:{symbol}:{interval}:{limit}"; c=cache_get(key)
    if c: return c
    try:
        params={"symbol":symbol,"interval":interval,"limit":limit}
        with REQUESTS_SEMAPHORE:
            r=requests.get(BINANCE_REST+"/api/v3/klines", params=params, timeout=REQUEST_TIMEOUT); r.raise_for_status(); data=r.json(); cache_set(key,data); return data
    except: return []

def fetch_order_book(symbol, limit=OB_DEPTH):
    key=f"depth:{symbol}:{limit}"; c=cache_get(key)
    if c: return c
    try:
        params={"symbol":symbol,"limit":max(5,limit)}
        with REQUESTS_SEMAPHORE:
            r=requests.get(BINANCE_REST+"/api/v3/depth", params=params, timeout=REQUEST_TIMEOUT); r.raise_for_status(); data=r.json(); cache_set(key,data); return data
    except: return {}

# ---------- client / exchange info ----------
_binance_client=None; _symbol_info_cache={}
def init_binance_client():
    global _binance_client
    if not Client: print("python-binance not installed"); return None
    if _binance_client: return _binance_client
    if not BINANCE_API_KEY or not BINANCE_API_SECRET: print("Binance API keys not set"); return None
    _binance_client = Client(BINANCE_API_KEY, BINANCE_API_SECRET); return _binance_client

def fetch_exchange_info():
    key="exchange_info"; c=cache_get(key)
    if c: return c
    try:
        client=init_binance_client()
        if not client:
            with REQUESTS_SEMAPHORE:
                r=requests.get(BINANCE_REST+"/api/v3/exchangeInfo", timeout=REQUEST_TIMEOUT); r.raise_for_status(); data=r.json(); cache_set(key,data); return data
        data=client.get_exchange_info(); cache_set(key,data); return data
    except Exception as e:
        print("fetch_exchange_info error", e); return {}

def get_symbol_info(symbol):
    if symbol in _symbol_info_cache: return _symbol_info_cache[symbol]
    ex=fetch_exchange_info()
    for s in ex.get("symbols",[]):
        if s.get("symbol")==symbol: _symbol_info_cache[symbol]=s; return s
    return None

# ---------- format helpers ----------
def format_qty(q, step):
    if not step or step==0: return str(q)
    prec=int(round(-math.log10(step))) if step<1 else 0
    return ("{:0."+str(prec)+"f}").format(float(q))

def format_price(p, tick):
    if not tick or tick==0: return str(p)
    prec=int(round(-math.log10(tick))) if tick<1 else 0
    return ("{:0."+str(prec)+"f}").format(float(p))

def ceil_step(v, step):
    if not step or step==0: return v
    return math.ceil(v/step)*step

def get_free_asset(asset):
    c=init_binance_client()
    if not c: return 0.0
    try:
        b=c.get_asset_balance(asset=asset)
        if b: return float(b.get("free") or 0.0)
    except: pass
    try:
        acc=c.get_account()
        for bal in acc.get("balances",[]):
            if bal.get("asset")==asset: return float(bal.get("free") or 0.0)
    except: pass
    return 0.0

# ---------- evaluate_symbol (kept previous strict variant) ----------
def evaluate_symbol(sym, last_price, qvol, change_24h):
    try:
        if not (PRICE_MIN<=last_price<=PRICE_MAX): return None
        if qvol<MIN_VOLUME: return None
        if change_24h<0.5 or change_24h>20.0: return None
        kl5=fetch_klines(sym,"5m",KLINES_5M_LIMIT); kl1=fetch_klines(sym,"1m",KLINES_1M_LIMIT); ob=fetch_order_book(sym,limit=OB_DEPTH)
        if not kl5 or len(kl5)<3 or not kl1 or len(kl1)<2: return None
        closes_5m=[float(k[4]) for k in kl5]; closes_1m=[float(k[4]) for k in kl1]
        pct_5m=pct_change(float(kl5[0][1]), closes_5m[-1]); pct_1m=pct_change(float(kl1[0][1]), closes_1m[-1])
        if pct_5m<MIN_5M_PCT or pct_1m<MIN_1M_PCT: return None
        vol_5m=compute_recent_volatility(closes_5m); vol_1m=compute_recent_volatility(closes_1m,lookback=3)
        short_ema=ema_local(closes_5m[-EMA_SHORT:],EMA_SHORT) if len(closes_5m)>=EMA_SHORT else None
        long_ema=ema_local(closes_5m[-EMA_LONG:],EMA_LONG) if len(closes_5m)>=EMA_LONG else None
        ema_ok=False; ema_uplift=0.0
        if short_ema and long_ema and long_ema!=0:
            ema_uplift=max(0.0,(short_ema-long_ema)/(long_ema+1e-12)); ema_ok=short_ema>long_ema*1.0005
        rsi_val=compute_rsi_local(closes_5m[-(RSI_PERIOD+1):],RSI_PERIOD) if len(closes_5m)>=RSI_PERIOD+1 else None
        if rsi_val is not None and rsi_val>70: return None
        ob_bull=orderbook_bullish(ob,depth=OB_DEPTH,min_imbalance=MIN_OB_IMBALANCE,max_spread_pct=MAX_OB_SPREAD_PCT)
        score=0.0
        score+=max(0.0,pct_5m)*4.0; score+=max(0.0,pct_1m)*2.0; score+=ema_uplift*500.0; score+=max(0.0,change_24h)*0.5
        if vol_5m is not None: score += max(0.0,(0.01-min(vol_5m,0.01))) * 100.0
        if rsi_val is not None: score += max(0.0,(60.0-min(rsi_val,60.0))) * 1.0
        if ob_bull: score += 25.0
        strong_candidate = (pct_5m>=MIN_5M_PCT and pct_1m>=MIN_1M_PCT and ema_ok and ob_bull)
        return {"symbol":sym,"last_price":last_price,"24h_change":change_24h,"24h_vol":qvol,"pct_5m":pct_5m,"pct_1m":pct_1m,"vol_5m":vol_5m,"vol_1m":vol_1m,"ema_ok":ema_ok,"ema_uplift":ema_uplift,"rsi":rsi_val,"ob_bull":ob_bull,"score":score,"strong_candidate":strong_candidate}
    except Exception:
        return None

# ---------- trading helpers (market buy by quote + parse fill) ----------
def place_market_buy_by_quote(symbol, quote_qty):
    client = init_binance_client()
    if not client: raise RuntimeError("Binance client not available")
    try:
        order = client.order_market_buy(symbol=symbol, quoteOrderQty=str(quote_qty))
        return order
    except BinanceAPIException as e:
        # fallback compute qty
        book = fetch_order_book(symbol, limit=5)
        if not book: raise
        top_ask = float(book["asks"][0][0])
        raw_qty = quote_qty / top_ask
        qty, _ = adjust_qty_price_for_filters(symbol, raw_qty, top_ask)
        if qty <= 0: raise RuntimeError("Computed qty below symbol min after filters")
        order = client.order_market_buy(symbol=symbol, quantity=str(qty))
        return order

def parse_market_fill(order_resp):
    fills = order_resp.get("fills") or []
    if not fills:
        executedQty = float(order_resp.get("executedQty",0) or 0)
        avg_price = 0.0
        if executedQty:
            cquote = float(order_resp.get("cummulativeQuoteQty",0) or 0)
            avg_price = cquote / executedQty if executedQty else 0.0
        return executedQty, avg_price
    total_qty=0.0; total_quote=0.0
    for f in fills:
        q=float(f.get("qty",0)); p=float(f.get("price",0))
        total_qty+=q; total_quote+=q*p
    avg_price=(total_quote/total_qty) if total_qty else 0.0
    return total_qty, avg_price

# ---------- place_market_sell_fallback (adapted) ----------
def place_market_sell_fallback(symbol, qty, f=None):
    try:
        if not f:
            info = get_symbol_info(symbol)
            f = None
            if info:
                for fil in info.get("filters",[]):
                    if fil.get("filterType") in ("LOT_SIZE","MARKET_LOT_SIZE"):
                        f = fil; break
            f = f or {}
    except Exception:
        f = f or {}
    try:
        try:
            qty_str = format_qty(qty, float(f.get('stepSize', 0.0)))
        except Exception:
            qty_str = str(qty)
        notify(f"⚠️ Attempting MARKET sell fallback for {symbol}: qty={qty_str}")
        c = init_binance_client()
        if not c:
            notify(f"❌ Market sell fallback failed for {symbol}: Binance client unavailable"); return None
        try:
            resp = c.order_market_sell(symbol=symbol, quantity=qty_str)
            notify(f"✅ Market sell fallback executed for {symbol}")
            try:
                with OPEN_ORDERS_LOCK:
                    OPEN_ORDERS_CACHE['data']=None; OPEN_ORDERS_CACHE['ts']=0
            except Exception: pass
            return resp
        except Exception as e:
            if 'NOTIONAL' in str(e) or '-1013' in str(e):
                notify(f"🚫 Market sell for {symbol} failed: position below minNotional. Suppressing future attempts for 24 hours.")
                TEMP_SKIP[symbol] = time.time() + 24*60*60; return None
            try:
                resp = c.create_order(symbol=symbol, side='SELL', type='MARKET', quantity=qty_str)
                notify(f"✅ Market sell fallback executed for {symbol} (via create_order)")
                try:
                    with OPEN_ORDERS_LOCK:
                        OPEN_ORDERS_CACHE['data']=None; OPEN_ORDERS_CACHE['ts']=0
                except Exception: pass
                return resp
            except Exception as e2:
                if 'NOTIONAL' in str(e2) or '-1013' in str(e2):
                    notify(f"🚫 Market sell for {symbol} failed: position below minNotional. Suppressing future attempts for 24 hours.")
                    TEMP_SKIP[symbol] = time.time() + 24*60*60; return None
                notify(f"❌ Market sell fallback failed for {symbol}: {e2}"); return None
    except Exception as e:
        if 'NOTIONAL' in str(e) or '-1013' in str(e):
            notify(f"🚫 Market sell for {symbol} failed: position below minNotional. Suppressing future attempts for 24 hours.")
            TEMP_SKIP[symbol] = time.time() + 24*60*60
        notify(f"❌ Market sell fallback failed for {symbol}: {e}"); return None

# ---------- place_limit_sell_strict (kept as before) ----------
# (Use the function from previous message - omit here for brevity in message; assume it's present unchanged)
# For brevity in this reply, assume place_limit_sell_strict is defined exactly as provided earlier
# (make sure to paste the exact function content into your script in place of this comment)

# ---------- execute_trade (stores qty & ts for monitor) ----------
def execute_trade(chosen):
    symbol = chosen["symbol"]; now=time.time()
    with RECENT_BUYS_LOCK:
        if len([k for k,v in RECENT_BUYS.items() if not v.get("closed")]) >= MAX_CONCURRENT_POS:
            send_telegram(f"⚠️ Max concurrent positions reached. Skipping {symbol}"); return False
        RECENT_BUYS[symbol] = {"ts": now, "reserved": False, "closed": False}
        persist_recent_buys()
    client = init_binance_client()
    if not client:
        send_telegram(f"⚠️ Trading disabled or client missing. Skipping live trade for {symbol}"); return False
    try:
        if BUY_BY_QUOTE and BUY_USDT_AMOUNT>0:
            order = place_market_buy_by_quote(symbol, BUY_USDT_AMOUNT)
        elif not BUY_BY_QUOTE and BUY_BASE_QTY>0:
            order = client.order_market_buy(symbol=symbol, quantity=str(BUY_BASE_QTY))
        else:
            send_telegram("⚠️ Buy amount not configured. Skipping trade."); return False
        executed_qty, avg_price = parse_market_fill(order)
        if executed_qty <= 0:
            send_telegram(f"⚠️ Buy executed but no fill qty for {symbol}. Raw: {order}")
            with RECENT_BUYS_LOCK: RECENT_BUYS.pop(symbol,None); persist_recent_buys()
            return False
        send_telegram(f"✅ BUY EXECUTED: `{symbol}` Qty:`{executed_qty}` @ `{avg_price}` Spent:`{round(executed_qty*avg_price,6)}`")
        # wait short delay to let balances settle BEFORE placing limit sell
        time.sleep(SHORT_BUY_SELL_DELAY)
        # attempt limit sell
        sell_price = avg_price * (1.0 + (LIMIT_PROFIT_PCT/100.0))
        sell_resp = place_limit_sell_strict(symbol, executed_qty, sell_price)
        if sell_resp:
            with RECENT_BUYS_LOCK:
                RECENT_BUYS[symbol].update({"qty": executed_qty, "buy_price": avg_price, "sell_price": sell_price, "sell_resp": sell_resp, "ts": now, "closed": False})
                persist_recent_buys()
            send_telegram(f"💰 LIMIT SELL initiated: `{symbol}` Qty `{executed_qty}` @ `{sell_price}` (+{LIMIT_PROFIT_PCT}%)")
            return True
        else:
            notify(f"⚠️ limit sell placement failed for {symbol}, attempting market sell fallback.")
            place_market_sell_fallback(symbol, executed_qty, None)
            with RECENT_BUYS_LOCK: RECENT_BUYS.pop(symbol,None); persist_recent_buys()
            return False
    except BinanceAPIException as e:
        send_telegram(f"‼️ Binance API error during buy {symbol}: {e}")
        with RECENT_BUYS_LOCK: RECENT_BUYS.pop(symbol,None); persist_recent_buys()
        return False
    except Exception as e:
        send_telegram(f"‼️ Unexpected error during trade {symbol}: {e}")
        with RECENT_BUYS_LOCK: RECENT_BUYS.pop(symbol,None); persist_recent_buys()
        return False

# ---------- pick_coin (NO blocking for stale positions; uses MAX_CONCURRENT_POS only) ----------
def pick_coin():
    tickers = fetch_tickers(); now=time.time()
    pre=[]
    for t in tickers:
        sym=t.get("symbol")
        if not sym or not sym.endswith(QUOTE): continue
        try:
            last=float(t.get("lastPrice") or 0.0); qvol=float(t.get("quoteVolume") or 0.0); ch=float(t.get("priceChangePercent") or 0.0)
        except: continue
        if not (PRICE_MIN<=last<=PRICE_MAX): continue
        if qvol<MIN_VOLUME: continue
        if ch<0.5 or ch>20.0: continue
        with RECENT_BUYS_LOCK:
            last_buy = RECENT_BUYS.get(sym)
            if last_buy and now < last_buy.get("ts",0)+BUY_LOCK_SECONDS: continue
        pre.append((sym,last,qvol,ch))
    if not pre: return None
    pre.sort(key=lambda x:x[2], reverse=True)
    candidates=pre[:TOP_BY_24H_VOLUME]; results=[]
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS,len(candidates) or 1)) as ex:
        futures={ex.submit(evaluate_symbol,sym,last,qvol,ch):sym for (sym,last,qvol,ch) in candidates}
        for fut in as_completed(futures):
            res=fut.result()
            if res: results.append(res)
    if not results: return None
    strongs=[r for r in results if r["strong_candidate"]]
    chosen_pool = strongs if strongs else results
    chosen = sorted(chosen_pool, key=lambda x:x["score"], reverse=True)[0]
    msg = (f"🚀 *COIN SIGNAL*: `{chosen['symbol']}`\nPrice:`{chosen['last_price']}`\n24h:`{chosen['24h_change']}`%\n5m:`{chosen['pct_5m']:.2f}`%\n1m:`{chosen['pct_1m']:.2f}`%\nVol5m:`{chosen['vol_5m']}`\nEMA_OK:`{chosen['ema_ok']}` Uplift:`{chosen['ema_uplift']:.4f}`\nRSI:`{chosen['rsi']}`\nOrderbook Bullish:`{chosen['ob_bull']}`\nScore:`{chosen['score']:.2f}`")
    with RECENT_BUYS_LOCK:
        last_buy=RECENT_BUYS.get(chosen["symbol"])
        if last_buy and now < last_buy.get("ts",0)+BUY_LOCK_SECONDS:
            return None
        # enforce MAX_CONCURRENT_POS
        if len([k for k,v in RECENT_BUYS.items() if not v.get("closed")]) >= MAX_CONCURRENT_POS:
            notify(f"⚠️ Max concurrent positions active ({MAX_CONCURRENT_POS}). Skipping new buy.")
            return None
        RECENT_BUYS[chosen["symbol"]] = {"ts": time.time(), "reserved": True, "closed": False}
        persist_recent_buys()
    sent = send_telegram(msg)
    if sent:
        with RECENT_BUYS_LOCK:
            RECENT_BUYS[chosen["symbol"]].update({"ts":time.time(), "reserved":False})
            persist_recent_buys()
        if ENABLE_TRADING:
            t=threading.Thread(target=execute_trade, args=(chosen,), daemon=True); t.start()
        return chosen
    else:
        with RECENT_BUYS_LOCK:
            RECENT_BUYS.pop(chosen["symbol"],None); persist_recent_buys()
        return None

# ---------- monitor_positions: auto market-sell positions older than threshold ----------
def monitor_positions():
    while True:
        try:
            now=time.time()
            to_process=[]
            with RECENT_BUYS_LOCK:
                for sym, pos in list(RECENT_BUYS.items()):
                    if pos.get("closed"): continue
                    ts = pos.get("ts", 0)
                    qty = pos.get("qty") or 0.0
                    # if we have no qty stored, try skip (we can't sell)
                    if qty <= 0:
                        # attempt to infer free asset? skip for safety
                        continue
                    if now - ts >= (HOLD_THRESHOLD_HOURS * 3600.0):
                        to_process.append((sym, pos))
            for sym,pos in to_process:
                notify(f"⚠️ Position {sym} open > {HOLD_THRESHOLD_HOURS}h — executing market sell fallback to close position.")
                try:
                    qty = pos.get("qty")
                    resp = place_market_sell_fallback(sym, qty, None)
                    with RECENT_BUYS_LOCK:
                        RECENT_BUYS[sym].update({"closed": True, "closed_ts": time.time(), "close_method": "market_fallback", "close_resp": resp})
                        persist_recent_buys()
                    notify(f"ℹ️ Position {sym} force-sold by monitor. Bot can resume buying new coins.")
                except Exception as e:
                    notify(f"⚠️ Monitor failed to market-sell {sym}: {e}")
            time.sleep(MONITOR_INTERVAL)
        except Exception:
            time.sleep(MONITOR_INTERVAL)

# ---------- main loop / web ----------
app = Flask(__name__)
@app.route("/")
def home(): return "Signal bot running"

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
    # start monitor thread
    tmon = threading.Thread(target=monitor_positions, daemon=True); tmon.start()
    t = threading.Thread(target=trade_cycle, daemon=True); t.start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")), threaded=True)
