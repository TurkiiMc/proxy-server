from fastapi import FastAPI
import requests

app = FastAPI()

@app.get("/")
def root():
    return {"service": "faisal-proxy", "status": "alive"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/yahoo/candles")
def yahoo_candles(symbol: str, period: str = "6mo", interval: str = "1d"):
    try:
        r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                         params={"range": period, "interval": interval},
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        if r.status_code != 200:
            return {"success": False, "error": f"HTTP {r.status_code}"}
        data = r.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return {"success": False, "error": "no result"}
        res = result[0]
        ts = res.get("timestamp", [])
        q = res.get("indicators", {}).get("quote", [{}])[0]
        candles = []
        for i, t in enumerate(ts):
            o = (q.get("open") or [None]*len(ts))[i]
            h = (q.get("high") or [None]*len(ts))[i]
            l = (q.get("low") or [None]*len(ts))[i]
            c = (q.get("close") or [None]*len(ts))[i]
            v = (q.get("volume") or [None]*len(ts))[i]
            if None in (o, h, l, c): continue
            candles.append({"date": t, "open": o, "high": h, "low": l, "close": c, "volume": v or 0})
        splits = []
        ev = res.get("events", {}).get("splits")
        if ev:
            for s in ev:
                splits.append({"date": s.get("date"), "numerator": s.get("numerator", 1),
                               "denominator": s.get("denominator", 1)})
        return {"success": True, "candles": candles, "splits": splits}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/yahoo/last")
def yahoo_last(symbols: str = ""):
    """اقتباسات مجمّعة لآخر سعر وحجم (لـ discovery pool)"""
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()][:200]
    out = {}
    def one(s):
        try:
            r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{s}",
                             params={"range": "5d", "interval": "1d"},
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if r.status_code != 200: return
            meta = r.json()["chart"]["result"][0]["meta"]
            p = meta.get("regularMarketPrice") or 0
            v = meta.get("regularMarketVolume") or 0
            if p: out[s] = {"price": p, "volume": v}
        except Exception: pass
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=20) as ex:
        list(ex.map(one, syms))
    return {"ok": True, "quotes": out}
