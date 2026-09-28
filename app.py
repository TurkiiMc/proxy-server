"""
Server — Yahoo Finance API Proxy (v1.2.1)
يوفر نقاط وصول للشموع والاقتباسات مع مناعة كاملة:
- سقف تزامن 16 اتصال Yahoo في اللحظة (يمنع اختناق CPU و503)
- كاش ذاكري قصير (60s اقتباسات / 300s شموع) لامتصاص تكرار بوت+محرك+تطبيق
- /ping و /health لا يلمسان Yahoo أبداً (يردان فوراً حتى تحت العاصفة)
- v1.2.1: مضيفان بديلان (query1/query2) + محاولتان + تهدئة عند 429
           + تسجيل كل فشل في السجلات (لا مزيد من الأخطاء الصامتة)
"""
import os, time, threading
from concurrent.futures import ThreadPoolExecutor
import requests
from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse

app = FastAPI(title="Faisal Proxy Server", version="1.2.1")

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
CHART_HOSTS = ["https://query1.finance.yahoo.com",
               "https://query2.finance.yahoo.com"]
YAHOO_WORKERS = 16          # سقف التزامن الخارجي
QUOTE_TTL = 60              # ثانية لكاش الاقتباس
CANDLE_TTL = 300            # ثانية لكاش الشموع
BULK_CAP = 150              # أقصى رموز في طلب واحد

exec_pool = ThreadPoolExecutor(max_workers=YAHOO_WORKERS)

# ===== كاش ذاكري قصير =====
_cache = {}
_cache_lock = threading.Lock()

def cache_get(key, ttl):
    with _cache_lock:
        item = _cache.get(key)
        if item and (time.time() - item[0]) < ttl:
            return item[1]
    return None

def cache_set(key, val):
    with _cache_lock:
        if len(_cache) > 5000:
            _cache.clear()
        _cache[key] = (time.time(), val)

# ===== نواة Yahoo (مضيفان + محاولتان + تهدئة) =====
def _chart(sym, range_, interval):
    last_err = "no attempt"
    for host in CHART_HOSTS:
        for attempt in range(2):
            try:
                r = requests.get(f"{host}/v8/finance/chart/{sym}",
                                 params={"range": range_, "interval": interval,
                                         "includePrePost": "false"},
                                 headers=UA, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    res = (data.get("chart") or {}).get("result")
                    if res:
                        return res[0], None
                    last_err = "No data"
                elif r.status_code == 429:
                    last_err = "HTTP 429"
                    time.sleep(1.5 + attempt)
                    continue
                else:
                    last_err = f"HTTP {r.status_code}"
            except Exception as e:
                last_err = str(e)
            time.sleep(0.8)
    return None, last_err

def _meta_quote(result):
    meta = result.get("meta", {})
    return {
        "price": meta.get("regularMarketPrice", 0),
        "close": meta.get("regularMarketPrice", 0),
        "previousClose": meta.get("chartPreviousClose", 0),
        "volume": meta.get("regularMarketVolume", 0),
        "currency": meta.get("currency", "USD"),
        "exchange": meta.get("exchangeName", ""),
        "marketCap": meta.get("marketCap", 0),
    }

# ===== نقاط الفحص والإيقاظ (لا تلمس Yahoo أبداً) =====
@app.get("/", response_class=PlainTextResponse)
def root():
    """نقطة جذرية خفيفة لإيقاظ الخدمة"""
    return "Faisal Proxy Server — Online"

@app.get("/ping", response_class=PlainTextResponse)
def ping():
    """نقطة خفيفة لـ cron-job.org"""
    return "pong"

@app.get("/health")
def health():
    """نقطة فحص لـ UptimeRobot و Render و cron-job — ترد فوراً دائماً"""
    return {"ok": True, "service": "faisal-proxy", "timestamp": int(time.time())}

# ===== Yahoo Finance Chart API =====
@app.get("/yahoo/candles")
def yahoo_candles(
    symbol: str = Query(..., description="رمز السهم، مثال: AAPL"),
    period: str = Query("6mo", description="الفترة: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, max"),
    interval: str = Query("1d", description="الفاصل: 1d أو 1h أو 5m, 15m, 30m"),
):
    sym = symbol.strip().upper()
    key = f"c:{sym}:{period}:{interval}"
    hit = cache_get(key, CANDLE_TTL)
    if hit:
        return hit
    try:
        result, err = _chart(sym, period, interval)
        if err:
            print(f"[proxy] candles fail {sym}: {err}")
            return {"success": False, "error": err}
        ts = result.get("timestamp") or []
        q = ((result.get("indicators") or {}).get("quote") or [{}])[0] or {}
        opens = q.get("open") or []; highs = q.get("high") or []
        lows = q.get("low") or []; closes = q.get("close") or []
        vols = q.get("volume") or []
        candles = []
        for i, t in enumerate(ts):
            if i < len(closes) and closes[i] is not None:
                candles.append({
                    "time": t,
                    "open": opens[i] if i < len(opens) else None,
                    "high": highs[i] if i < len(highs) else None,
                    "low": lows[i] if i < len(lows) else None,
                    "close": closes[i],
                    "volume": vols[i] if i < len(vols) else 0,
                })
        if not candles:
            return {"success": False, "error": "No data"}
        out = {"success": True, "symbol": sym, **_meta_quote(result),
               "candles": candles}
        cache_set(key, out)
        return out
    except Exception as e:
        print(f"[proxy] candles exc {sym}: {e}")
        return {"success": False, "error": str(e)}

def _one_quote(sym):
    try:
        result, err = _chart(sym, "1d", "1d")
        if err:
            print(f"[proxy] quote fail {sym}: {err}")
            return sym, None
        return sym, _meta_quote(result)
    except Exception as e:
        print(f"[proxy] quote exc {sym}: {e}")
        return sym, None

@app.get("/yahoo/quote")
def yahoo_quote(symbol: str = Query(..., description="رمز السهم، مثال: AAPL")):
    sym = symbol.strip().upper()
    hit = cache_get(f"q:{sym}", QUOTE_TTL)
    if hit:
        return {"success": True, "symbol": sym, **hit}
    sym_out, q = _one_quote(sym)
    if not q:
        return {"success": False, "error": "No data"}
    cache_set(f"q:{sym}", q)
    return {"success": True, "symbol": sym, **q}

@app.get("/yahoo/last")
def yahoo_last(
    symbols: str = Query(..., description="رموز مفصولة بفواصل: AAPL,MSFT,OFAL"),
):
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()][:BULK_CAP]
    quotes = {}
    todo = []
    for s in syms:
        hit = cache_get(f"q:{s}", QUOTE_TTL)
        if hit:
            quotes[s] = hit
        else:
            todo.append(s)
    if todo:
        try:
            for sym, q in exec_pool.map(_one_quote, todo, timeout=100):
                if q:
                    quotes[sym] = q
                    cache_set(f"q:{sym}", q)
        except Exception as e:
            print(f"[proxy] bulk partial: {e}")  # نُرجع ما تجمع فقط
    if todo and not quotes:
        print(f"[proxy] ⚠️ دفعة فارغة: {len(todo)} رمزاً فشلت كلها")
    return {"success": True, "count": len(quotes), "quotes": quotes}

# ===== تشغيل الخادم =====
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
