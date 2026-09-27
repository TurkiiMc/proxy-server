"""
Faisal Proxy Server - Yahoo Finance API Proxy
يوفر نقاط وصول للشموع والاقتباسات من Yahoo Finance
"""
import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse

app = FastAPI(title="Faisal Proxy Server", version="1.1.0")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ===== نقاط الفحص والإيقاظ =====

@app.get("/", response_class=PlainTextResponse)
def root():
    """نقطة جذرية خفيفة لإيقاظ الخدمة"""
    return "Faisal Proxy Server - Online"

@app.get("/ping", response_class=PlainTextResponse)
def ping():
    """نقطة خفيفة لـ cron-job.org"""
    return "pong"

@app.get("/health")
def health():
    """نقطة فحص لـ UptimeRobot و Render و cron-job"""
    return {"ok": True, "service": "faisal-proxy", "timestamp": int(time.time())}

# ===== Yahoo Finance Chart API =====

@app.get("/yahoo/candles")
def yahoo_candles(
    symbol: str = Query(..., description="رمز السهم، مثال: AAPL"),
    period: str = Query("6mo", description="الفترة: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, max"),
    interval: str = Query("1d", description="الفاصل: 1m, 5m, 15m, 30m, 60m, 1h, 1d, 5d, 1wk, 1mo")
):
    """جلب الشموع من Yahoo Finance Chart API"""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}"
        params = {
            "range": period,
            "interval": interval,
            "includePrePost": "false",
            "events": "div,split"
        }

        r = requests.get(url, params=params, headers=HEADERS, timeout=30)

        if r.status_code != 200:
            return {"success": False, "error": f"HTTP {r.status_code}", "candles": []}

        data = r.json()

        if "chart" not in data or "result" not in data["chart"] or not data["chart"]["result"]:
            return {"success": False, "error": "No data available", "candles": []}

        result = data["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        indicators = result.get("indicators", {})
        quote = indicators.get("quote", [{}])[0]

        if not timestamps or not quote:
            return {"success": False, "error": "Empty data", "candles": []}

        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])

        candles = []
        for i in range(len(timestamps)):
            if closes[i] is None:
                continue
            candles.append({
                "date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamps[i])),
                "open": round(opens[i], 4) if opens[i] else 0,
                "high": round(highs[i], 4) if highs[i] else 0,
                "low": round(lows[i], 4) if lows[i] else 0,
                "close": round(closes[i], 4),
                "volume": int(volumes[i]) if volumes[i] else 0
            })

        splits = []
        events = result.get("events", {})
        if "splits" in events:
            for date_str, split_data in events["splits"].items():
                splits.append({
                    "date": time.strftime("%Y-%m-%d", time.gmtime(int(date_str))),
                    "numerator": split_data.get("numerator", 1),
                    "denominator": split_data.get("denominator", 1)
                })

        return {
            "success": True,
            "symbol": symbol.upper(),
            "period": period,
            "interval": interval,
            "candles": candles,
            "splits": splits,
            "count": len(candles)
        }

    except requests.Timeout:
        return {"success": False, "error": "Request timeout", "candles": []}
    except requests.RequestException as e:
        return {"success": False, "error": str(e), "candles": []}
    except Exception as e:
        return {"success": False, "error": f"Internal error: {str(e)}", "candles": []}

# ===== Yahoo Finance Last Quote (مجمّع) =====

@app.get("/yahoo/last")
def yahoo_last(
    symbols: str = Query(..., description="رموز مفصولة بفاصلة، مثال: AAPL,MSFT,GOOGL")
):
    """
    جلب آخر اقتباس لعدة رموز دفعة واحدة
    ✅ معدل: timeout=8 و max_workers=30 لسرعة أعلى ومهام أقصر
    """
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]

    if not symbol_list:
        return {"ok": False, "error": "No symbols provided", "quotes": {}}

    if len(symbol_list) > 200:
        return {"ok": False, "error": "Maximum 200 symbols per request", "quotes": {}}

    quotes = {}

    def fetch_one(symbol):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            params = {
                "range": "1d",
                "interval": "1d",
                "includePrePost": "false"
            }

            r = requests.get(url, params=params, headers=HEADERS, timeout=8)

            if r.status_code != 200:
                return symbol, None

            data = r.json()

            if "chart" not in data or "result" not in data["chart"] or not data["chart"]["result"]:
                return symbol, None

            result = data["chart"]["result"][0]
            meta = result.get("meta", {})

            price = meta.get("regularMarketPrice", 0)
            volume = meta.get("regularMarketVolume", 0)

            if price and price > 0:
                return symbol, {
                    "price": round(price, 4),
                    "volume": int(volume) if volume else 0,
                    "currency": meta.get("currency", "USD"),
                    "exchange": meta.get("exchangeName", "")
                }
            return symbol, None

        except Exception:
            return symbol, None

    # جلب متوازي — 30 خيطاً لسرعة أعلى
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_one, sym): sym for sym in symbol_list}
        for future in futures:
            symbol, data = future.result()
            if data:
                quotes[symbol] = data

    return {
        "ok": True,
        "quotes": quotes,
        "count": len(quotes),
        "requested": len(symbol_list)
    }

# ===== نقطة فحص رمز واحد =====

@app.get("/yahoo/quote")
def yahoo_quote(symbol: str = Query(..., description="رمز السهم")):
    """جلب اقتباس مفصل لرمز واحد"""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}"
        params = {
            "range": "1d",
            "interval": "1d",
            "includePrePost": "false"
        }

        r = requests.get(url, params=params, headers=HEADERS, timeout=15)

        if r.status_code != 200:
            return {"success": False, "error": f"HTTP {r.status_code}"}

        data = r.json()

        if "chart" not in data or "result" not in data["chart"] or not data["chart"]["result"]:
            return {"success": False, "error": "No data"}

        result = data["chart"]["result"][0]
        meta = result.get("meta", {})

        return {
            "success": True,
            "symbol": symbol.upper(),
            "price": meta.get("regularMarketPrice", 0),
            "previousClose": meta.get("chartPreviousClose", 0),
            "volume": meta.get("regularMarketVolume", 0),
            "currency": meta.get("currency", "USD"),
            "exchange": meta.get("exchangeName", ""),
            "marketCap": meta.get("marketCap", 0)
        }

    except Exception as e:
        return {"success": False, "error": str(e)}

# ===== تشغيل الخادم =====

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
