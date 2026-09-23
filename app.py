import httpx
import asyncio
import time
import random
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone

app = FastAPI(title="Yahoo Crumb Proxy", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ═══════════ الطبقة 1: تدوير بصمة المتصفح ═══════════
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ═══════════ الطبقة 2: جلسة دائمة + crumb ذاتي الشفاء ═══════════
_client = None
_crumb = None
_crumb_lock = asyncio.Lock()

# ═══════════ الطبقة 3: كاش 60 ثانية (يحمي الـ IP من الضغط) ═══════════
_cache = {}          # (symbol, period) -> (timestamp, payload)
CACHE_TTL = 60


def _new_client():
    return httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=10.0),
        follow_redirects=True,
        headers={
            "User-Agent": random.choice(UAS),
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )


async def _get_client():
    global _client
    if _client is None or _client.is_closed:
        _client = _new_client()
    return _client


async def _refresh_crumb(force=False):
    """مصافحة: fc.yahoo.com يغرس الـ Cookie ثم getcrumb يولّد الـ crumb"""
    global _crumb
    async with _crumb_lock:
        if _crumb is not None and not force:
            return _crumb
        c = await _get_client()
        try:
            await c.get("https://fc.yahoo.com")   # 404 متوقع — الهدف غرس الـ cookie
            r = await c.get("https://query2.finance.yahoo.com/v1/test/getcrumb")
            _crumb = r.text.strip() if (r.status_code == 200 and r.text.strip()) else None
        except Exception:
            _crumb = None
        return _crumb


async def _burn_session():
    """حرق الجلسة عند الحجب حتى تُبنى من جديد مع crumb جديد"""
    global _client, _crumb
    _crumb = None
    if _client is not None and not _client.is_closed:
        try:
            await _client.aclose()
        except Exception:
            pass
    _client = None


async def _fetch_chart(symbol, period):
    """جلب مع retries + تبديل مضيف + إعادة بناء الجلسة عند 401/429/999"""
    last_err = None
    for attempt in range(4):
        c = await _get_client()
        crumb = await _refresh_crumb()
        host = "query2" if attempt % 2 == 0 else "query1"
        params = {"range": period, "interval": "1d",
                  "includePrePost": "false", "events": "splits,div"}
        if crumb:
            params["crumb"] = crumb
        try:
            r = await c.get(
                f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}",
                params=params,
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code in (401, 403, 429, 999):
                await _burn_session()
                last_err = r.status_code
                await asyncio.sleep(1.5 + attempt * 2)
                continue
            last_err = r.status_code
        except Exception as e:
            last_err = repr(e)
            await asyncio.sleep(1 + attempt)
    raise HTTPException(502, detail=f"Yahoo blocked/unreachable: {last_err}")


def _parse(data, symbol):
    """نفس منطقك الأصلي حرفياً — للحفاظ على توافق app.py"""
    result = data.get("chart", {}).get("result", [])
    if not result:
        raise HTTPException(404, "No data")
    chart = result[0]
    ts = chart.get("timestamp", [])
    q = chart["indicators"]["quote"][0]
    candles = []
    for i, t in enumerate(ts):
        if i < len(q["close"]) and q["close"][i] is not None:
            candles.append({
                "date": datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d"),
                "open": q["open"][i],
                "high": q["high"][i],
                "low": q["low"][i],
                "close": q["close"][i],
                "volume": q["volume"][i] or 0,
            })
    splits = []
    for ts_key, sp in (chart.get("events", {}).get("splits", {}) or {}).items():
        splits.append({
            "date": datetime.fromtimestamp(int(ts_key), tz=timezone.utc).strftime("%Y-%m-%d"),
            "numerator": sp.get("numerator", 1),
            "denominator": sp.get("denominator", 1),
            "ratio": sp.get("splitRatio", ""),
        })
    return {"success": True, "symbol": symbol, "count": len(candles),
            "candles": candles, "splits": splits}


# ═══════════ المسارات ═══════════
@app.get("/")
async def root():
    return {"message": "Proxy working", "version": "2.0", "crumb_ready": _crumb is not None}


@app.get("/yahoo/candles")
async def yahoo_candles(symbol: str = Query(...), period: str = Query("6mo")):
    sym = symbol.upper()
    key = (sym, period)
    now = time.time()
    hit = _cache.get(key)
    if hit and (now - hit[0]) < CACHE_TTL:
        return hit[1]                       # ← من الكاش، لا نضرب Yahoo
    data = await _fetch_chart(sym, period)
    payload = _parse(data, sym)
    _cache[key] = (now, payload)
    return payload


@app.get("/health")
async def health():
    """فحص عميق: يجلب AAPL فعلاً — يكشف الحجب وليس فقط نوم الحاوية"""
    try:
        data = await _fetch_chart("AAPL", "1mo")
        ok = bool(data.get("chart", {}).get("result"))
        if ok:
            return {"status": "ok", "yahoo": True, "cache_entries": len(_cache)}
        return JSONResponse({"status": "degraded", "yahoo": False,
                             "cache_entries": len(_cache)}, status_code=200)
    except Exception as e:
        return JSONResponse({"status": "down", "yahoo": False, "error": str(e)},
                            status_code=503)
