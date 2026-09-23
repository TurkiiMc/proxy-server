import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
import threading
import io
import os
from datetime import datetime, timedelta
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# ===== الإعدادات (المفاتيح من محفظة Render) =====
# ============================================================
st.set_page_config(page_title="Stock Screener Pro", page_icon="🎯", layout="wide")

PROXY_URL     = os.environ.get("PROXY_URL", "https://faisal-proxy.onrender.com").rstrip("/")
FINNHUB_KEY   = os.environ.get("FINNHUB_KEY", "")
ALPACA_KEY    = os.environ.get("ALPACA_KEY", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET", "")
ALPACA_ENABLED = bool(ALPACA_KEY and ALPACA_SECRET)
ALPACA_BASE   = "https://data.alpaca.markets/v2"
ON_RENDER     = bool(os.environ.get("RENDER_SERVICE_NAME"))

MARKET_CAP_MAX = 20_000_000
FLOAT_MAX      = 5_000_000
PRICE_MAX      = 5.0
VOLUME_MIN     = 100_000
SPLIT_MAX_DAYS = 50

# ثوابت التسريع وطريقة الارتكاز
MAX_WORKERS = 6
LADDER_MIN_CANDLES = 2
LADDER_STEP_MIN, LADDER_STEP_MAX = 15.0, 35.0
MA_TOUCH_PCT = 3.0
PIVOT_UNIVERSE_LIMIT = 200

# ============================================================
# ===== Rate Limiter =====
# ============================================================
class RateLimiter:
    def __init__(self, max_calls=55, period=60):
        self.max_calls = max_calls
        self.period = period
        self.calls = deque()
        self.lock = threading.Lock()
    def wait(self):
        with self.lock:
            now = time.time()
            while self.calls and now - self.calls[0] > self.period:
                self.calls.popleft()
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0]) + 0.1
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.calls.append(time.time())

finnhub_limiter = RateLimiter(max_calls=55, period=60)
alpaca_limiter  = RateLimiter(max_calls=200, period=60)

# ============================================================
# ===== CSS =====
# ============================================================
st.markdown("""<style>
.main{direction:rtl}
h1,h2,h3{direction:rtl;text-align:right}
.score-card{background:linear-gradient(135deg,#f8f9fa,#e9ecef);padding:25px;border-radius:15px;text-align:center;margin:15px 0;box-shadow:0 4px 6px rgba(0,0,0,0.1)}
.score-big{font-size:56px;font-weight:bold;margin:0}
.verdict{font-size:22px;margin-top:10px;font-weight:600}
.stButton>button{width:100%;background:linear-gradient(90deg,#00b894,#0984e3);color:white;font-weight:bold;border-radius:10px;padding:12px;border:none}
.info-box{background:#e8f4f8;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #0984e3}
.warn-box{background:#fff3cd;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #fdcb6e}
.success-box{background:#d4edda;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #00b894}
.danger-box{background:#f8d7da;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #d63031}
.split-box{background:#ffe5e5;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #d63031}
.plan-box{background:#f0f7ff;padding:15px;border-radius:10px;margin:10px 0;border:2px solid #0984e3}
.news-item{background:#fff;padding:10px;border-radius:6px;margin:5px 0;border-right:3px solid #0984e3;font-size:14px}
.filter-box{background:#fff8e1;padding:15px;border-radius:10px;margin:10px 0;border:2px solid #fdcb6e}
.plan-table{width:100%;border-collapse:collapse;margin-top:10px}
.plan-table td{padding:8px;border-bottom:1px solid #d0e4f5;font-size:16px}
.live-price{font-size:32px;font-weight:bold;padding:15px;text-align:center;border-radius:10px;margin:5px;background:linear-gradient(135deg,#f8f9fa,#e9ecef)}
.live-up{color:#00b894}.live-down{color:#d63031}.live-neutral{color:#636e72}
</style>""", unsafe_allow_html=True)

# ============================================================
# ===== القائمة الاحتياطية =====
# ============================================================
FALLBACK_UNIVERSE = [
    "AEMD","AKAN","LFS","GDHG","BJDX","DXST","VSME","CLIK","DGHG","CPOP",
    "HTCR","MBRX","MWC","NXTS","SVRE","YYAI","BFRG","BIAF","BNKK","CDTG",
    "SHPH","SONN","TNXP","PHIO","SNPX","AVGR","BDRX","BIOR","CLRB","CRKN",
    "CYTX","DTSS","EEIQ","ELAB","EVGN","EYEN","FWBI","GCTK","GNPX","HCDI",
    "HILS","HOTH","IMCC","INBS","INDP","IPDN","IVDA","JWEL","KITT","KRKR",
    "LGMK","LGVN","LUCY","LUXH","MEGL","MLGO","MNPR","MRIN","MTNB","MYNZ",
    "NEXI","NITO","NKGN","NUKK","NVOS","OMQS","ONCO","OPGN","OPTT","PAVM",
    "PHGE","PLRX","PMN","PRFX","PRST","PXMD","QNRX","RDHL","RIME","RKDA",
    "RSLS","SBFM","SCPX","SEEL","SGBX","SLXN","SNDL","SOBR","SPRB","STAF",
    "STI","SXTP","SYRA","TCON","TCRT","THMO","TIVC","TNON","TOMZ","TRNR",
    "TRVN","TSBX","UPC","USEG","VBIV","VERO","VINO","VIRI","VRPX","VTVT",
    "WATT","WISA","WKEY","XELB","XERS","XLO","XRTX","YCBD","ZAPP","ZCMD",
    "ZJYL","SOPA","PRSO","ELYM","ALLR","AGRI","ALZN","AMST","APRE","AUID"
]

# ============================================================
# ===== كاش يدوي آمن للخيوط =====
# ============================================================
_c_store, _c_lock = {}, threading.Lock()
_f_store, _f_lock = {}, threading.Lock()

def _ttl(store, lock, key, ttl, producer):
    now = time.time()
    with lock:
        hit = store.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = producer()
    with lock:
        store[key] = (now, val)
    return val

# ============================================================
# ===== طبقة البيانات =====
# ============================================================
def alpaca_candles(symbol, period="6mo"):
    if not ALPACA_ENABLED:
        return pd.DataFrame(), []
    alpaca_limiter.wait()
    period_map = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730}
    days = period_map.get(period, 180)
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        headers = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}
        r = requests.get(f"{ALPACA_BASE}/stocks/{symbol}/bars", headers=headers,
                         params={"timeframe": "1Day", "start": start, "limit": 1000, "adjustment": "all"},
                         timeout=15)
        if r.status_code == 200:
            bars = r.json().get("bars", [])
            if bars:
                df = pd.DataFrame(bars)
                df["date"] = pd.to_datetime(df["t"])
                df = df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"})
                return df.set_index("date").sort_index(), []
    except Exception:
        pass
    return pd.DataFrame(), []


def alpaca_realtime_trade(symbol):
    if not ALPACA_ENABLED:
        return None
    alpaca_limiter.wait()
    try:
        headers = {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET}
        r = requests.get(f"{ALPACA_BASE}/stocks/{symbol}/trades/latest", headers=headers, timeout=10)
        if r.status_code == 200:
            t = r.json().get("trade", {})
            if t:
                return {"price": float(t.get("p", 0)), "size": int(t.get("s", 0)), "timestamp": t.get("t", "")}
    except Exception:
        pass
    return None


@st.cache_data(ttl=300)
def stooq_candles(symbol, period="1y"):
    try:
        url = f"https://stooq.com/q/d/l/?s={symbol.lower()}.us&i=d"
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200 or len(r.text) < 50 or "No data" in r.text:
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.capitalize() for c in df.columns]
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()
        days = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730}.get(period, 365)
        return df[df.index >= (datetime.now() - timedelta(days=days))]
    except Exception:
        return pd.DataFrame()


def _candles_uncached(symbol, period="6mo"):
    # 1) Alpaca
    if ALPACA_ENABLED:
        try:
            df, splits = alpaca_candles(symbol, period)
            if not df.empty and len(df) >= 20:
                return df, splits, "alpaca"
        except Exception:
            pass
    # 2) Yahoo Proxy (مع إعادة محاولة تغطي Cold Start)
    if PROXY_URL:
        for attempt in range(2):
            try:
                r = requests.get(PROXY_URL + "/yahoo/candles",
                                 params={"symbol": symbol, "period": period},
                                 timeout=60 if attempt == 0 else 30)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("success") and data.get("candles"):
                        df = pd.DataFrame(data["candles"])
                        df["date"] = pd.to_datetime(df["date"])
                        df = df.set_index("date").sort_index()
                        df.columns = [c.capitalize() for c in df.columns]
                        return df, data.get("splits", []), "yahoo_proxy"
                if r.status_code in (429, 503):
                    time.sleep(3)
            except Exception:
                time.sleep(2)
    # 3) yfinance (محلياً فقط)
    if not ON_RENDER:
        try:
            import yfinance as yf
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period)
            if not df.empty:
                df.columns = [c.capitalize() for c in df.columns]
                splits_list = []
                for date, ratio in ticker.splits.items():
                    if ratio and ratio < 1:
                        num, den = 1, int(round(1 / ratio))
                    else:
                        num, den = int(ratio), 1
                    splits_list.append({"date": date.strftime("%Y-%m-%d"),
                                        "numerator": num, "denominator": den})
                return df, splits_list, "yfinance"
        except Exception:
            pass
    # 4) Stooq طوارئ
    df = stooq_candles(symbol, period)
    if not df.empty:
        return df, [], "stooq"
    return pd.DataFrame(), [], "none"


def get_candles_unified(symbol, period="6mo"):
    return _ttl(_c_store, _c_lock, (symbol, period), 300,
                lambda: _candles_uncached(symbol, period))


def scan_parallel(symbols, period, progress_cb=None):
    """جلب متوازٍ — أسرع ~6 أضعاف من الحلقة التسلسلية"""
    out, total, done = [], len(symbols), 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(get_candles_unified, s, period): s for s in symbols}
        for f in as_completed(futs):
            s = futs[f]; done += 1
            if progress_cb:
                try: progress_cb(done, total)
                except Exception: pass
            try:
                hist, splits, src = f.result()
                if not hist.empty and len(hist) >= 30:
                    out.append((s, hist, splits, src))
            except Exception:
                pass
    return out


@st.cache_data(ttl=60)
def get_realtime_price(symbol):
    if ALPACA_ENABLED:
        t = alpaca_realtime_trade(symbol)
        if t and t["price"] > 0:
            return {"price": t["price"], "source": "alpaca", "live": True}
    if FINNHUB_KEY:
        finnhub_limiter.wait()
        try:
            r = requests.get("https://finnhub.io/api/v1/quote",
                             params={"symbol": symbol, "token": FINNHUB_KEY}, timeout=10)
            if r.status_code == 200:
                q = r.json()
                if q.get("c", 0) > 0:
                    return {"price": float(q["c"]), "percent_change": float(q.get("dp", 0)),
                            "source": "finnhub", "live": True}
        except Exception:
            pass
    return None


def get_dynamic_universe(limit=500):
    cache_key = f"universe_{limit}"
    if cache_key not in st.session_state:
        st.session_state[cache_key] = {"data": None, "ts": 0}
    now = datetime.now().timestamp()
    cache = st.session_state[cache_key]
    if cache["data"] and (now - cache["ts"]) < 3600:
        return cache["data"]
    try:
        from tradingview_screener import Query, col
        q = (Query()
             .select('name', 'close', 'volume', 'market_cap_basic', 'float_shares')
             .where(col('exchange') == 'NASDAQ',
                    col('market_cap_basic') <= MARKET_CAP_MAX,
                    col('float_shares') < FLOAT_MAX,
                    col('close') < PRICE_MAX,
                    col('volume') > VOLUME_MIN,
                    col('type') == 'stock')
             .order_by('market_cap_basic', ascending=True)
             .limit(limit))
        df, _ = q.get_scanner_data()
        if df is not None and not df.empty:
            tickers = df['ticker'].tolist() if 'ticker' in df.columns else df['name'].tolist()
            tickers = [t.split(':')[-1] if ':' in t else t for t in tickers]
            tickers = list(dict.fromkeys(tickers))
            st.session_state[cache_key] = {"data": tickers, "ts": now}
            return tickers
    except Exception:
        pass
    st.session_state[cache_key] = {"data": FALLBACK_UNIVERSE, "ts": now}
    return FALLBACK_UNIVERSE


def _metrics_uncached(symbol):
    info = {"floatShares": 0, "shortPercentOfFloat": 0, "sharesShort": 0, "marketCap": 0}
    if not FINNHUB_KEY:
        return info
    finnhub_limiter.wait()
    try:
        r = requests.get("https://finnhub.io/api/v1/stock/metric",
                         params={"symbol": symbol, "metric": "all", "token": FINNHUB_KEY}, timeout=15)
        if r.status_code == 200:
            m = r.json().get("metric", {})
            ff = m.get("freeFloat") or 0
            info["floatShares"] = ff * 1000000 if ff and ff < 1000 else ff
            sp = m.get("shortPercentOfFloat") or 0
            info["shortPercentOfFloat"] = sp / 100 if sp > 1 else sp
            info["sharesShort"] = m.get("sharesShort") or 0
            mc = m.get("marketCapitalization") or 0
            info["marketCap"] = mc * 1000000 if mc and mc < 100000 else mc
    except Exception:
        pass
    return info


def finnhub_metrics(symbol):
    return _ttl(_f_store, _f_lock, symbol, 300, lambda: _metrics_uncached(symbol))


def check_offering(symbol):
    try:
        headers = {"User-Agent": "FaisalBot contact@example.com"}
        r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers, timeout=10)
        if r.status_code != 200:
            return {"has_offering": False}
        cik = None
        for v in r.json().values():
            if v["ticker"].upper() == symbol.upper():
                cik = str(v["cik_str"]).zfill(10)
                break
        if not cik:
            return {"has_offering": False}
        time.sleep(0.15)
        r = requests.get("https://data.sec.gov/submissions/CIK" + cik + ".json", headers=headers, timeout=10)
        if r.status_code != 200:
            return {"has_offering": False}
        recent = r.json().get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        cutoff = datetime.now() - timedelta(days=30)
        of = {"S-1", "S-3", "424B3", "424B5"}
        for form, date_str in zip(forms, dates):
            if form in of:
                try:
                    fdate = datetime.strptime(date_str, "%Y-%m-%d")
                    if fdate >= cutoff:
                        return {"has_offering": True, "form": form, "days": (datetime.now() - fdate).days}
                except Exception:
                    continue
        return {"has_offering": False}
    except Exception:
        return {"has_offering": False}


@st.cache_data(ttl=600)
def check_negative_news(symbol):
    if not FINNHUB_KEY:
        return {"has_negative": False, "items": [], "status": "no_key"}
    finnhub_limiter.wait()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        r = requests.get("https://finnhub.io/api/v1/company-news",
                         params={"symbol": symbol, "from": week_ago, "to": today, "token": FINNHUB_KEY},
                         timeout=15)
        if r.status_code != 200:
            return {"has_negative": False, "items": [], "status": "error"}
        news = r.json()
        if not news:
            return {"has_negative": False, "items": [], "status": "no_news"}
        neg_critical = ["bankruptcy", "chapter 11", "chapter 7", "delisting", "delisted", "fraud",
                        "sec investigation", "sec probe", "halted", "trading halt", "going concern"]
        neg_high = ["lawsuit", "class action", "sued", "net loss", "layoffs", "layoff", "restructuring",
                    "downgrade", "downgraded", "price target cut", "misses", "missed estimates",
                    "revenue decline", "warns", "warning", "guidance cut"]
        neg_offering = ["public offering", "private placement", "registered direct", "dilution",
                        "shelf offering", "atm offering", "stock offering"]
        neg_medium = ["investigation", "probe", "subpoena", "recall", "delay", "rejected", "cancellation"]
        matches = []
        for item in news[:30]:
            text = ((item.get("headline") or "") + " " + (item.get("summary") or "")).lower()
            level = keyword = None
            for kw in neg_critical:
                if kw in text: level, keyword = "CRITICAL", kw; break
            if not level:
                for kw in neg_offering:
                    if kw in text: level, keyword = "OFFERING", kw; break
            if not level:
                for kw in neg_high:
                    if kw in text: level, keyword = "HIGH", kw; break
            if not level:
                for kw in neg_medium:
                    if kw in text: level, keyword = "MEDIUM", kw; break
            if level:
                try:
                    date_str = datetime.fromtimestamp(item.get("datetime", 0)).strftime("%Y-%m-%d")
                except Exception:
                    date_str = "?"
                matches.append({"headline": (item.get("headline") or "")[:120],
                                "source": item.get("source", "?"), "date": date_str,
                                "level": level, "keyword": keyword, "url": item.get("url", "")})
        return {"has_negative": len(matches) > 0, "items": matches[:5], "status": "done",
                "critical_count": sum(1 for m in matches if m["level"] == "CRITICAL"),
                "offering_count": sum(1 for m in matches if m["level"] == "OFFERING"),
                "high_count": sum(1 for m in matches if m["level"] == "HIGH")}
    except Exception:
        return {"has_negative": False, "items": [], "status": "error"}


# ============================================================
# ===== المؤشرات الفنية =====
# ============================================================
def atr(high, low, close, period=14):
    if len(close) < period + 1:
        return None
    prev = close.shift(1)
    tr = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    val = tr.rolling(period).mean().iloc[-1]
    return float(val) if pd.notna(val) else None


def rsi(close, period=14):
    if len(close) < period + 1:
        return 50.0
    d = close.diff()
    g = d.clip(lower=0).rolling(period).mean()
    l = (-d.clip(upper=0)).rolling(period).mean()
    rs = g / l.replace(0, np.nan)
    val = (100 - (100 / (1 + rs))).iloc[-1]
    return float(val) if pd.notna(val) else 50.0


def macd(close):
    if len(close) < 26:
        return False, False, 0.0
    e12 = close.ewm(span=12, adjust=False).mean()
    e26 = close.ewm(span=26, adjust=False).mean()
    m = e12 - e26
    s = m.ewm(span=9, adjust=False).mean()
    h = m - s
    return m.iloc[-1] > s.iloc[-1], h.iloc[-1] > (h.iloc[-3] if len(h) > 3 else h.iloc[-1]), float(h.iloc[-1])


def macd_status(mp, mi, hist):
    if mp and mi: return "إيجابي ويتحسن ✅", "#00b894"
    elif mp: return "إيجابي لكن يضعف 🟡", "#fdcb6e"
    elif mi: return "سلبي لكن يتحسن 🟡", "#fdcb6e"
    else: return "سلبي ويضعف ❌", "#d63031"


def sma(close, p):
    return float(close.rolling(p).mean().iloc[-1]) if len(close) >= p else None


def stoch(high, low, close, kp=14, dp=3):
    if len(close) < kp: return 50.0
    ll = low.rolling(kp).min()
    hh = high.rolling(kp).max()
    d = (hh - ll).replace(0, np.nan)
    k = 100 * ((close - ll) / d)
    v = k.rolling(dp).mean().iloc[-1]
    return float(v) if pd.notna(v) else 50.0


def sr(hist, w=20):
    if hist.empty or len(hist) < w: return None, None
    return float(hist["Low"].rolling(w).min().iloc[-1]), float(hist["High"].rolling(w).max().iloc[-1])


def detect_trend_strength(hist):
    if len(hist) < 50:
        return "neutral"
    close = hist["Close"]
    s20 = close.rolling(20).mean().iloc[-1]
    s50 = close.rolling(50).mean().iloc[-1]
    cur = close.iloc[-1]
    if cur > s20 > s50: return "strong_uptrend"
    elif cur > s20: return "weak_uptrend"
    elif cur < s20 < s50: return "strong_downtrend"
    elif cur < s20: return "weak_downtrend"
    return "neutral"


def fibonacci_levels(low, high):
    diff = high - low
    return {"1.272": round(high + diff * 0.272, 3), "1.618": round(high + diff * 0.618, 3)}


# ============================================================
# ===== كاشفات الأنماط =====
# ============================================================
def detect_former_runner(hist):
    if len(hist) < 20: return False
    return bool((hist["Close"].pct_change() > 0.5).any())


def detect_w_pattern(hist):
    if len(hist) < 40: return None
    h = hist["Close"].tail(60)
    lows = h[(h.shift(1) > h) & (h.shift(-1) > h)]
    if len(lows) < 2: return None
    last_two = lows.tail(2)
    if abs(last_two.iloc[0] - last_two.iloc[1]) / last_two.iloc[0] * 100 < 4:
        neckline = float(h.loc[last_two.index[0]:last_two.index[1]].max())
        return {"bottom1": round(float(last_two.iloc[0]), 3),
                "bottom2": round(float(last_two.iloc[1]), 3),
                "neckline": round(neckline, 3)}
    return None


def detect_bull_trap(hist):
    if len(hist) < 25: return False
    resistance = hist["High"].tail(20).max()
    recent = hist.tail(5)
    if not (recent["High"] > resistance * 0.98).any(): return False
    return hist["Close"].iloc[-1] < resistance * 0.97


def detect_failed_spike(hist):
    if len(hist) < 20: return None
    recent = hist.tail(20)
    max_high = float(recent["High"].max())
    min_low = float(recent["Low"].min())
    current = float(hist["Close"].iloc[-1])
    if min_low <= 0: return None
    spike_pct = (max_high - min_low) / min_low * 100
    drop_from_high = (max_high - current) / max_high * 100
    if spike_pct > 50 and drop_from_high > 40:
        return {"spike_pct": round(spike_pct, 1), "drop_pct": round(drop_from_high, 1), "peak": round(max_high, 3)}
    return None


def detect_gap_fill(hist):
    if len(hist) < 10: return None
    for i in range(len(hist) - 10, len(hist) - 1):
        try:
            prev_close = hist["Close"].iloc[i]
            curr_open = hist["Open"].iloc[i + 1]
            gap_pct = (curr_open - prev_close) / prev_close * 100
            if abs(gap_pct) > 5:
                current = hist["Close"].iloc[-1]
                if gap_pct < 0 and current < curr_open:
                    return {"direction": "down", "gap_price": round(curr_open, 3), "gap_pct": round(gap_pct, 1)}
                elif gap_pct > 0 and current > curr_open:
                    return {"direction": "up", "gap_price": round(curr_open, 3), "gap_pct": round(gap_pct, 1)}
        except Exception:
            continue
    return None


def detect_candle_patterns(hist):
    if len(hist) < 5: return None
    patterns = []
    for i in range(-3, 0):
        try:
            o, c, hi, lo = hist["Open"].iloc[i], hist["Close"].iloc[i], hist["High"].iloc[i], hist["Low"].iloc[i]
            body, rt = abs(c - o), hi - lo
            if rt == 0: continue
            uw, lw = hi - max(o, c), min(o, c) - lo
            if lw > body * 2 and uw < body * 0.5 and body < rt * 0.3:
                patterns.append("Hammer")
            if i > -len(hist):
                po, pc = hist["Open"].iloc[i - 1], hist["Close"].iloc[i - 1]
                if pc < po and c > o and c > po and o < pc:
                    patterns.append("Bullish Engulfing")
        except Exception:
            continue
    return patterns if patterns else None


def detect_reverse_split(splits, max_days=365):
    if not splits:
        return {"has_split": False, "days_since": 9999}
    cutoff = datetime.now() - timedelta(days=max_days)
    for sp in splits:
        try:
            sp_date = datetime.strptime(sp["date"], "%Y-%m-%d")
            if sp_date >= cutoff:
                num = int(sp.get("numerator", 1))
                den = int(sp.get("denominator", 1))
                return {"has_split": num < den, "date": sp["date"],
                        "ratio": f"{num}:{den}", "days_since": (datetime.now() - sp_date).days}
        except Exception:
            continue
    return {"has_split": False, "days_since": 9999}


def detect_stability(hist, support, min_sessions=2):
    if not support or len(hist) < min_sessions + 2: return None
    recent = hist.tail(min_sessions + 3)
    threshold = support * 0.98
    closes, lows = recent["Close"].values, recent["Low"].values
    sessions_held = 0
    for c in reversed(closes):
        if c >= threshold: sessions_held += 1
        else: break
    if sessions_held < min_sessions: return None
    recent_lows = lows[-sessions_held:]
    higher_lows = all(recent_lows[i] >= recent_lows[i-1] * 0.99 for i in range(1, len(recent_lows))) if len(recent_lows) > 1 else False
    if sessions_held >= 3 and higher_lows:
        strength, color, points = "🔥 ثبات قوي", "#00b894", 10
    elif sessions_held >= 2 and higher_lows:
        strength, color, points = "✅ ثبات جيد", "#00b894", 7
    elif sessions_held >= 2:
        strength, color, points = "🟡 ثبات مقبول", "#fdcb6e", 5
    else:
        strength, color, points = "⚠️ ثبات ضعيف", "#fdcb6e", 0
    return {"sessions_held": sessions_held, "higher_lows": higher_lows,
            "strength": strength, "color": color, "points": points}


def rebound_progress(hist, support, resistance):
    if not support or not resistance or resistance == support: return None
    return round((hist["Close"].iloc[-1] - support) / (resistance - support) * 100, 1)


def detect_liquidity_sweep(hist):
    if len(hist) < 15: return False
    support = hist["Low"].tail(20).min()
    recent = hist.tail(5)
    for i in range(1, len(recent) - 1):
        if recent["Low"].iloc[i] < support * 1.02:
            if recent["Close"].iloc[i + 1] > support:
                return True
    return False


def detect_spring(hist):
    if len(hist) < 40: return None
    support = hist["Low"].tail(40).min()
    last10 = hist.tail(10)
    return {"detected": True} if (last10["Low"].min() < support * 0.97 and last10["Close"].iloc[-1] > support) else None


def detect_lps(hist):
    if len(hist) < 40: return None
    resistance = hist["High"].tail(30).max()
    recent = hist.tail(15)
    if not (recent["High"].max() > resistance * 0.98): return None
    pullback = recent["Low"].min()
    return {"detected": True} if (pullback > resistance * 0.95 and hist["Close"].iloc[-1] > pullback) else None


# ============================================================
# ===== آلة حالات التقسيم العكسي =====
# ============================================================
PHASE_META = {
    "READY":    {"label": "🟢 جاهز للدخول",             "color": "#00b894"},
    "WATCH":    {"label": "👁️ مراقبة — خارج المنطقة",   "color": "#0984e3"},
    "RETEST":   {"label": "🔄 اختبار المنطقة جارٍ",      "color": "#6c5ce7"},
    "PROOF":    {"label": "⏳ منطقة بانتظار إثبات حياة",  "color": "#fdcb6e"},
    "ZONE":     {"label": "🔍 تكوين منطقة",              "color": "#e17055"},
    "WASHOUT":  {"label": "🌪️ غسيل جارٍ",                "color": "#636e72"},
    "REJECTED": {"label": "🚫 مستبعد",                   "color": "#d63031"},
}


def count_reverse_splits(splits, months=36):
    if not splits:
        return 0
    cutoff = datetime.now() - timedelta(days=months * 30)
    cnt = 0
    for sp in splits:
        try:
            d = datetime.strptime(sp["date"], "%Y-%m-%d")
            if d >= cutoff and int(sp.get("numerator", 1)) < int(sp.get("denominator", 1)):
                cnt += 1
        except Exception:
            continue
    return cnt


def post_split_phase(symbol, hist, splits, min_days=10, max_days=90):
    sp = detect_reverse_split(splits, max_days=max_days)
    if not sp.get("has_split"):
        return None
    D, ratio = sp["days_since"], sp["ratio"]
    try:
        den = int(ratio.split(":")[1])
    except Exception:
        den = 10
    if den >= 20: min_days = max(min_days, 20)
    if den >= 50: min_days = max(min_days, 30)

    post = hist[hist.index >= pd.Timestamp(sp["date"])]
    if len(post) < 10:
        return {"phase_key": "WASHOUT", "symbol": symbol, "days": D, "ratio": ratio,
                "price": float(hist["Close"].iloc[-1]), "drawdown": 0, "vol_dryup": False,
                "vol_contract": False, "zone": None, "rebound": 0, "dist": None,
                "stability": False, "rejections": []}

    price = float(hist["Close"].iloc[-1])
    post_high = float(post["High"].max())
    drawdown = round((post_high - price) / post_high * 100, 1) if post_high else 0

    early_vol = float(post["Volume"].head(10).mean())
    recent_vol = float(post["Volume"].tail(5).mean())
    vol_dryup = (recent_vol < early_vol * 0.50) if early_vol > 0 else False

    atr_now = atr(hist["High"], hist["Low"], hist["Close"], 14)
    atr_early = atr(post["High"].head(20), post["Low"].head(20), post["Close"].head(20), 10)
    vol_contract = bool(atr_now and atr_early and atr_now < atr_early * 0.5)

    zone = None
    window = post.tail(20)
    if len(window) >= 10:
        zone_low = float(window["Low"].min())
        band = zone_low * 1.04
        touches = int((window["Low"] <= band).sum())
        closes_below = int((window["Close"] < zone_low).sum())
        if touches >= 3 and closes_below == 0:
            zone = {"low": round(zone_low, 3), "high": round(band, 3), "touches": touches}

    stability = detect_stability(hist, zone["low"], 2) is not None if zone else False
    rebound = round((price - zone["low"]) / zone["low"] * 100, 1) if zone else 0
    dist = round((price - zone["low"]) / zone["low"] * 100, 1) if zone else None
    proof = rebound >= 10

    if D < min_days or not vol_dryup:
        key = "WASHOUT"
    elif not zone:
        key = "ZONE"
    elif not proof:
        key = "PROOF"
    elif not stability:
        key = "RETEST"
    else:
        key = "READY" if dist <= 15 else "WATCH"

    return {"phase_key": key, "symbol": symbol, "days": D, "ratio": ratio, "price": price,
            "drawdown": drawdown, "vol_dryup": vol_dryup, "vol_contract": vol_contract,
            "zone": zone, "rebound": rebound, "dist": dist, "stability": stability, "rejections": []}


def split_rejections(symbol, hist, phase_info):
    reasons = []
    price = float(hist["Close"].iloc[-1])
    zone = phase_info.get("zone")
    if price < 1.30:
        reasons.append("السعر تحت $1.30 — خطر استيفاء/تقسيم آخر")
    if zone and zone["low"] < 1.20:
        reasons.append("قاع المنطقة قرب $1 — فخ الاستيفاء")
    return reasons


def split_rejections_serial(splits):
    if count_reverse_splits(splits, months=36) >= 2:
        return "مقسّم تسلسلي (≥2 تقسيم عكسي خلال 36 شهر)"
    return None


@st.cache_data(ttl=600)
def advanced_veto(symbol):
    offering = check_offering(symbol)
    if offering.get("has_offering"):
        return True, f"طرح SEC نشط ({offering.get('form','?')} قبل {offering.get('days','?')} يوم)"
    news = check_negative_news(symbol)
    if news.get("critical_count", 0) > 0:
        return True, f"أخبار حرجة ({news.get('critical_count')} خبر)"
    return False, ""


# ============================================================
# ===== طريقة الارتكاز: شموع 4H =====
# ============================================================
@st.cache_data(ttl=300)
def get_4h(symbol):
    """شموع 4 ساعات: جلب 1h من البروكسي ثم تجميع كل 4 شموع داخل الجلسة"""
    if not PROXY_URL:
        return None
    try:
        r = requests.get(PROXY_URL + "/yahoo/candles",
                         params={"symbol": symbol, "period": "3mo", "interval": "1h"},
                         timeout=60)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get("success") or not data.get("candles"):
            return None
        df = pd.DataFrame(data["candles"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        df.columns = [c.capitalize() for c in df.columns]
        df["_d"] = df.index.normalize()
        df["_c"] = df.groupby("_d").cumcount() // 4
        g = df.groupby(["_d", "_c"])
        h4 = g.agg(t=("Open", lambda s: s.index[0]),
                   Open=("Open", "first"), High=("High", "max"),
                   Low=("Low", "min"), Close=("Close", "last"),
                   Volume=("Volume", "sum")).reset_index(drop=True)
        return h4.set_index("t").sort_index()
    except Exception:
        return None


def build_red_ladder(h4):
    """سلّم الشموع الساقطة على 4H: الذيل(Low) مقاومة→دعم، الرأس(High) هدف"""
    if h4 is None or len(h4) < 10:
        return None
    reds = h4[h4["Close"] < h4["Open"]].tail(40)
    price = float(h4["Close"].iloc[-1])
    above = reds[reds["Low"] > price].sort_values("Low")
    if len(above) < LADDER_MIN_CANDLES:
        return None
    ladder = []
    for _, c in above.iterrows():
        tail, head = float(c["Low"]), float(c["High"])
        ladder.append({"date": str(c.name)[:16], "tail": round(tail, 4),
                       "head": round(head, 4),
                       "step_pct": round((head - tail) / tail * 100, 1)})
    broken = reds[reds["Low"] <= price].sort_values("Low").tail(3)
    supports = [round(float(c["Low"]), 4) for _, c in broken.iterrows()]
    return {"ladder": ladder, "supports": supports,
            "resistance": ladder[0]["tail"], "target": ladder[0]["head"],
            "steps": len(ladder)}


def ma_band_touch(hist, level, tol=MA_TOUCH_PCT):
    m1, m2 = sma(hist["Close"], 20), sma(hist["Close"], 30)
    if not m1 or not m2:
        return False
    lo, hi = min(m1, m2) * (1 - tol / 100), max(m1, m2) * (1 + tol / 100)
    return lo <= level <= hi


def filter_pivot_stock(symbol, hd, h4, splits):
    """فلتر طريقة الارتكاز — بدون شورت إطلاقاً"""
    lad = build_red_ladder(h4)
    if not lad:
        return None
    pts, reasons = 30, [f"سلّم 4H: {lad['steps']} شمعة حمراء فوق السعر"]

    res = lad["resistance"]
    if ma_band_touch(hd, res) or ma_band_touch(h4, res):
        pts += 25
        reasons.append("✅ المقاومة الحالية على نطاق متوسط 20–30")

    steps25 = sum(1 for L in lad["ladder"] if LADDER_STEP_MIN <= L["step_pct"] <= LADDER_STEP_MAX)
    if steps25 >= 1:
        pts += 15
        reasons.append(f"{steps25} شمعة بهدف ~25%")

    if lad["supports"]:
        pts += 10
        reasons.append(f"ذيول محروقة تحولت لدعم: {lad['supports']}")

    n_splits = count_reverse_splits(splits, months=60)
    cap = None
    if n_splits > 3:
        cap = sma(hd["Close"], 20)
        reasons.append(f"مقسم {n_splits} مرات → سقف الهدف تحت MA20 (${round(cap,3) if cap else '-'})")

    target = lad["target"] if (cap is None or lad["target"] <= cap) else round(cap, 3)
    return {"symbol": symbol, "points": pts, "ladder": lad,
            "target": target, "cap_ma20": cap, "reasons": reasons}


# ============================================================
# ===== نظام النقاط (مع الإصلاحات) =====
# ============================================================
def score(symbol, hist, info, splits=None, news=None, max_split_days=365, offering=None):
    close, high, low, vol = hist["Close"], hist["High"], hist["Low"], hist["Volume"]
    price = float(close.iloc[-1])
    r = rsi(close)
    mp, mi, macd_hist = macd(close)
    macd_txt, macd_color = macd_status(mp, mi, macd_hist)
    sk = stoch(high, low, close)
    s20, s30, s50 = sma(close, 20), sma(close, 30), sma(close, 50)
    sup, res = sr(hist, 20)
    fs = info.get("floatShares", 0) or 0
    mc = info.get("marketCap", 0) or 0
    spct = info.get("shortPercentOfFloat", 0) or 0
    cv = int(vol.iloc[-1]) if len(vol) else 0
    avg_vol = float(vol.tail(20).mean()) if len(vol) >= 20 else 0
    rv = round(cv / avg_vol, 2) if avg_vol > 0 else 0

    bd = {}
    if 23 <= r <= 27: bd["RSI"] = 25
    elif 20 <= r < 23: bd["RSI"] = 12
    elif 27 < r <= 30: bd["RSI"] = 15
    elif 30 < r <= 35: bd["RSI"] = 6
    else: bd["RSI"] = 0

    split_info = detect_reverse_split(splits, max_days=max_split_days)
    if split_info["has_split"]:
        d = split_info["days_since"]
        bd["Split"] = 20 if d <= 180 else (10 if d <= 365 else 0)
    else:
        bd["Split"] = 0

    if fs:
        if fs < 1000000: bd["Float"] = 15
        elif fs < 5000000: bd["Float"] = 12
        elif fs < 10000000: bd["Float"] = 8
        elif fs < 20000000: bd["Float"] = 5
        else: bd["Float"] = 0
    else: bd["Float"] = 0

    if spct > 0.40: bd["Short"] = 20
    elif spct > 0.30: bd["Short"] = 15
    elif spct > 0.20: bd["Short"] = 10
    elif spct > 0.10: bd["Short"] = 5
    else: bd["Short"] = 0

    if mp and mi: bd["MACD"] = 15
    elif mi: bd["MACD"] = 8
    else: bd["MACD"] = 0

    if s20 and s30 and s50:
        b = sum(1 for x in [s20, s30, s50] if price < x)
        bd["MA"] = {3: 15, 2: 8, 1: 5}.get(b, 0)
    else: bd["MA"] = 0

    if sk < 20: bd["Stoch"] = 10
    elif sk < 30: bd["Stoch"] = 8
    elif sk < 40: bd["Stoch"] = 6
    elif sk < 50: bd["Stoch"] = 4
    else: bd["Stoch"] = 0

    support_broken = bool(sup and price < sup)
    ds = None
    if sup:
        ds = round((price - sup) / price * 100, 2)
        if support_broken: bd["Support"] = -20
        elif ds < 3: bd["Support"] = 10
        elif ds < 5: bd["Support"] = 7
        elif ds < 8: bd["Support"] = 3
        elif ds < 15: bd["Support"] = 0
        else: bd["Support"] = -15
    else: bd["Support"] = 0

    if rv > 5: bd["RVOL"] = 5
    elif rv >= 2: bd["RVOL"] = 3
    else: bd["RVOL"] = 0

    stability = detect_stability(hist, sup, min_sessions=2)
    bd["Stability"] = stability["points"] if stability else 0

    rebound = rebound_progress(hist, sup, res)
    if rebound is not None:
        if rebound < 30: bd["Rebound"] = 5
        elif rebound < 60: bd["Rebound"] = 0
        else: bd["Rebound"] = -10

    is_runner = detect_former_runner(hist)
    bd["Runner"] = 5 if is_runner else 0

    w_pat = detect_w_pattern(hist)
    bd["W_Pattern"] = 10 if w_pat else 0

    total = max(0, min(sum(bd.values()), 100))

    if news:
        if news.get("offering_count", 0) > 0: total = max(0, total - 20)
        if news.get("high_count", 0) >= 2: total = max(0, total - 10)

    failed_spike = detect_failed_spike(hist)

    hard_veto = support_broken or bool(failed_spike)
    if news and news.get("critical_count", 0) > 0: hard_veto = True
    if offering and offering.get("has_offering"): hard_veto = True

    if hard_veto:
        total, v, c = 0, "مرفوض (خطر)", "#d63031"
    elif total >= 80: v, c = "مثالي", "#00b894"
    elif total >= 65: v, c = "ممتاز", "#00b894"
    elif total >= 50: v, c = "جيد", "#fdcb6e"
    elif total >= 35: v, c = "ضعيف", "#e17055"
    else: v, c = "مرفوض", "#d63031"

    return {
        "symbol": symbol, "price": price, "rsi": round(r, 2), "stoch": round(sk, 2),
        "macd_pos": mp, "macd_imp": mi, "macd_hist": round(macd_hist, 4),
        "macd_txt": macd_txt, "macd_color": macd_color, "sma20": s20, "sma50": s50,
        "support": sup, "resistance": res, "dist_sup": ds, "float": fs, "marketCap": mc,
        "rvol": rv, "short_pct": spct, "breakdown": bd, "total": total, "verdict": v,
        "color": c, "rebound": rebound, "split_info": split_info, "stability": stability,
        "sweep": detect_liquidity_sweep(hist), "spring": detect_spring(hist),
        "lps": detect_lps(hist), "w_pattern": w_pat, "runner": is_runner,
        "bull_trapering": detect_bull_trap(hist), "failed_spike": failed_spike,
        "gap": detect_gap_fill(hist), "candles": detect_candle_patterns(hist),
        "support_broken": support_broken, "hard_veto": hard_veto,
    }


# ============================================================
# ===== عرض التحليل + خطة الدخول الديناميكية =====
# ============================================================
def render_full_analysis(sym, hist, splits, info, news, offering, r):
    st.markdown(f'<div class="score-card"><div class="score-big" style="color:{r["color"]}">{r["total"]}/100</div><div class="verdict">{r["verdict"]}</div></div>', unsafe_allow_html=True)

    if r.get("hard_veto"):
        reasons = []
        if r.get("support_broken"): reasons.append("الدعم مكسور")
        if r.get("failed_spike"): reasons.append("Failed Spike")
        if news and news.get("critical_count", 0) > 0: reasons.append("أخبار حرجة")
        if offering and offering.get("has_offering"): reasons.append("طرح SEC نشط")
        st.markdown(f'<div class="danger-box">🚫 <b>إقصاء فوري:</b> {" | ".join(reasons)} — لا تُتداول هذه الحالة</div>', unsafe_allow_html=True)
        return

    live = get_realtime_price(sym)
    if live:
        st.info(f"🟢 السعر اللحظي: ${live['price']:.3f} (المصدر: {live.get('source')})")

    if news.get("critical_count", 0) > 0:
        st.markdown(f'<div class="danger-box">🚨 <b>أخبار حرجة!</b> {news["critical_count"]} خبر خطير</div>', unsafe_allow_html=True)
    if news.get("offering_count", 0) > 0:
        st.markdown(f'<div class="danger-box">⚠️ <b>طرح جديد محتمل!</b> {news["offering_count"]} خبر طرح/تخفيف</div>', unsafe_allow_html=True)
    if offering.get("has_offering"):
        st.markdown(f'<div class="danger-box">📋 <b>طرح في SEC:</b> {offering.get("form","")} قبل {offering.get("days",0)} يوم</div>', unsafe_allow_html=True)

    if news.get("items"):
        st.markdown("#### 📰 آخر الأخبار السلبية")
        for item in news["items"]:
            emoji, color = {"CRITICAL": ("🚨", "#d63031"), "OFFERING": ("💰", "#d63031"),
                            "HIGH": ("⚠️", "#e17055")}.get(item["level"], ("🟡", "#fdcb6e"))
            st.markdown(f'<div class="news-item" style="border-right-color:{color}">{emoji} <b>{item["headline"]}</b><br><small>{item["source"]} - {item["date"]}</small></div>', unsafe_allow_html=True)

    if r["stability"]:
        s = r["stability"]
        hl = " + قيعان أعلى ✅" if s["higher_lows"] else ""
        st.markdown(f'<div class="success-box" style="border-right-color:{s["color"]}">📊 <b>نموذج الثبات:</b> {s["strength"]} - {s["sessions_held"]} جلسات فوق الدعم{hl}</div>', unsafe_allow_html=True)
    elif r["support"]:
        st.markdown('<div class="warn-box">⚠️ <b>نموذج الثبات:</b> أقل من جلستين فوق الدعم - انتظر</div>', unsafe_allow_html=True)

    if r["bull_trapering"]:
        st.markdown('<div class="danger-box">Bull Trap: كسر مقاومة ثم فشل - خطر!</div>', unsafe_allow_html=True)
    if r["split_info"].get("has_split"):
        d = r["split_info"]["days_since"]
        st.markdown(f'<div class="split-box">Reverse Split: {r["split_info"]["ratio"]} - قبل {d} يوم{" - حديث!" if d <= 180 else ""}</div>', unsafe_allow_html=True)
    if r["short_pct"] > 0:
        warn = " - مرتفع! Squeeze محتمل" if r["short_pct"] > 0.20 else ""
        st.markdown(f'<div class="info-box">Short Float: {round(r["short_pct"]*100, 2)}%{warn}</div>', unsafe_allow_html=True)
    if r["marketCap"]:
        st.markdown(f'<div class="info-box">Market Cap: ${round(r["marketCap"]/1000000, 2)}M</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="info-box" style="border-right-color:{r["macd_color"]}"><b>MACD:</b> {r["macd_txt"]} (قيمة: {r["macd_hist"]})</div>', unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("السعر", f"${round(r['price'], 3)}")
    c2.metric("RSI", r['rsi'])
    c3.metric("Stoch", r['stoch'])
    c4.metric("RVOL", r['rvol'])
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("MA20", f"${round(r['sma20'], 2)}" if r['sma20'] else "-")
    c2.metric("MA50", f"${round(r['sma50'], 2)}" if r['sma50'] else "-")
    c3.metric("الدعم", f"${round(r['support'], 3)}" if r['support'] else "-")
    c4.metric("المقاومة", f"${round(r['resistance'], 3)}" if r['resistance'] else "-")

    if r["runner"]: st.markdown('<div class="success-box">Former Runner: سبق أن انفجر +50% في يوم!</div>', unsafe_allow_html=True)
    if r["w_pattern"]:
        wp = r["w_pattern"]
        st.markdown(f'<div class="success-box">W Pattern: قاعان ${wp["bottom1"]} / ${wp["bottom2"]} - خط العنق: ${wp["neckline"]}</div>', unsafe_allow_html=True)
    if r["sweep"]: st.markdown('<div class="success-box">سحب سيولة: تم كشفه!</div>', unsafe_allow_html=True)
    if r["spring"]: st.markdown('<div class="success-box">Spring (وايكوف): كسر ثم استرداد!</div>', unsafe_allow_html=True)
    if r["lps"]: st.markdown('<div class="success-box">LPS (وايكوف): اختراق ثم اختبار ناجح!</div>', unsafe_allow_html=True)
    if r["candles"]: st.markdown(f'<div class="success-box">شموع انعكاسية: {", ".join(r["candles"])}</div>', unsafe_allow_html=True)
    if r["gap"]:
        g = r["gap"]
        st.markdown(f'<div class="info-box">فجوة {"هبوط" if g["direction"]=="down" else "صعود"}: {g["gap_pct"]}% عند ${g["gap_price"]}</div>', unsafe_allow_html=True)
    if r["rebound"] is not None:
        if r["rebound"] > 60:
            st.markdown(f'<div class="warn-box">الارتداد: {r["rebound"]}% - متأخر</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="info-box">تقدم الارتداد: {r["rebound"]}% - مبكر</div>', unsafe_allow_html=True)
    st.info(f"Free Float: {round(r['float']/1000000, 2)}M سهم" if r["float"] else "Free Float: غير متوفر")

    # ═══════════ خطة الدخول الديناميكية + إصلاح الأهداف ═══════════
    if r["support"] and r["resistance"]:
        sup_val, res_val, current_price = r["support"], r["resistance"], r["price"]
        atr_val = atr(hist["High"], hist["Low"], hist["Close"], 14)
        trend = detect_trend_strength(hist)
        fib = fibonacci_levels(hist["Low"].tail(60).min(), hist["High"].tail(60).max())

        if atr_val:
            entry1 = round(sup_val + atr_val * 0.5, 3)
            entry2 = round(sup_val + atr_val * 0.25, 3)
            entry3 = round(sup_val + atr_val * 0.1, 3)
            stop_hard = round(sup_val - atr_val * 2.0, 3)
            t2_raw = round(current_price + (current_price - sup_val) * 1.618, 3)
        else:
            entry1, entry2, entry3 = round(sup_val*1.02, 3), round(sup_val*1.01, 3), round(sup_val*1.005, 3)
            stop_hard = round(sup_val * 0.94, 3)
            t2_raw = round(res_val * 1.2, 3)
        stop_struct = round(sup_val * 0.97, 3)
        stop = stop_hard
        # أهداف مرتبة تصاعدياً دائماً (إصلاح خلل KITT)
        target1 = round(res_val, 3)
        target2 = round(max(t2_raw, target1 * 1.15), 3)
        target3 = round(max(fib.get("1.618", res_val * 1.3), target2 * 1.5), 3)

        atr_pct = round(atr_val / current_price * 100, 1) if atr_val else 0
        vol_flag = atr_pct > 8
        eff_risk = risk_percent / 2 if vol_flag else risk_percent
        max_risk = portfolio_size * (eff_risk / 100)
        mkt = round(current_price, 3)

        if current_price < sup_val:
            ladder = []; avg_entry = mkt
            note = "🚫 الدعم مكسور — لا دخول"; note_color = "#d63031"
        elif current_price > entry1:
            ladder = [
                {"t": "🥇 دخول أولي (40%)", "p": entry1, "ord": "Limit", "pct": 0.40},
                {"t": "🥈 تعزيز (35%)",     "p": entry2, "ord": "Limit", "pct": 0.35},
                {"t": "🥉 دخول أخير (25%)", "p": entry3, "ord": "Limit", "pct": 0.25},
            ]
            avg_entry = round(entry1*0.40 + entry2*0.35 + entry3*0.25, 3)
            note = "⏳ السعر فوق السلّم — علّق أوامرك تحت السوق وانتظر"; note_color = "#0984e3"
        elif current_price >= entry2:
            ladder = [
                {"t": "🥇 دخول فوري (40%)", "p": mkt,    "ord": "Market", "pct": 0.40},
                {"t": "🥈 تعزيز (35%)",     "p": entry2, "ord": "Limit",  "pct": 0.35},
                {"t": "🥉 دخول أخير (25%)", "p": entry3, "ord": "Limit",  "pct": 0.25},
            ]
            avg_entry = round(mkt*0.40 + entry2*0.35 + entry3*0.25, 3)
            note = "⚡ السعر داخل السلّم — الشريحة الأولى سوقاً الآن والبقية معلقة تحت"; note_color = "#00b894"
        elif current_price >= entry3:
            ladder = [
                {"t": "🥇 دخول فوري (40%)",  "p": mkt, "ord": "Market", "pct": 0.40},
                {"t": "🥈 تعزيز فوري (35%)", "p": mkt, "ord": "Market", "pct": 0.35},
                {"t": "🥉 دخول أخير (25%)",  "p": entry3, "ord": "Limit", "pct": 0.25},
            ]
            avg_entry = round(mkt*0.75 + entry3*0.25, 3)
            note = "⚡ السعر عميق داخل السلّم — شريحتان سوقاً الآن وواحدة معلقة"; note_color = "#00b894"
        else:
            ladder = [{"t": "🎯 دخول كامل (100%)", "p": mkt, "ord": "Market", "pct": 1.0}]
            avg_entry = mkt
            note = "🟢 السعر عند المنطقة العميقة (فوق الدعم مباشرة) — دخول كامل مع التحقق من الثبات"; note_color = "#00b894"

        risk_ps = round(avg_entry - stop, 3)
        shares = int(max_risk / risk_ps) if risk_ps > 0 else 0
        for L in ladder:
            L["sh"] = int(shares * L["pct"])
        risk_pct = round(risk_ps / avg_entry * 100, 2) if avg_entry > 0 else 0
        rr = [round((t - avg_entry) / risk_ps, 2) if risk_ps > 0 else 0 for t in (target1, target2, target3)]
        rew_pct = [round((t - avg_entry) / avg_entry * 100, 2) if avg_entry > 0 else 0 for t in (target1, target2, target3)]
        pos_value = round(shares * avg_entry, 2)
        trail_dist = round(atr_val * 1.5, 3) if atr_val else round(avg_entry * 0.05, 3)

        trend_map = {
            "strong_uptrend": ("✅ اتجاه صاعد قوي", "#00b894"),
            "weak_uptrend": ("🟢 اتجاه صاعد", "#00b894"),
            "neutral": ("🟡 اتجاه عرضي", "#fdcb6e"),
            "weak_downtrend": ("🟠 اتجاه هابط ضعيف", "#e17055"),
            "strong_downtrend": ("🔴 اتجاه هابط قوي", "#d63031"),
        }
        trend_txt, trend_color = trend_map[trend]

        st.markdown("### 📊 خطة التداول الاحترافية")
        st.markdown(f'<div style="background:{note_color}22;padding:15px;border-radius:10px;margin:10px 0;border:2px solid {note_color};text-align:center;font-size:18px;font-weight:bold">{note}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="background:{trend_color}15;padding:10px;border-radius:8px;margin:5px 0;border-right:4px solid {trend_color}"><b>📈 حالة الاتجاه:</b> {trend_txt}</div>', unsafe_allow_html=True)

        if vol_flag:
            st.markdown(f'<div class="warn-box">⚠️ <b>تقلب يومي {atr_pct}%</b> — الوقف الصلب بعيد ({risk_pct}%) بتصميم مضاد للضجيج، ولذا خُفّضت المخاطرة تلقائياً إلى {eff_risk}%.<br>📌 <b>وقف القرار:</b> أي <b>إغلاق يومي</b> تحت ${stop_struct} يُنهي الصفقة فوراً حتى لو لم يلمس الوقف الصلب.</div>', unsafe_allow_html=True)

        st.markdown("#### 🎯 أوامر الدخول (حسب موقع السعر الآن)")
        rows_html = ""
        for L in ladder:
            color = "#00b894" if L["ord"] == "Market" else "#0984e3"
            rows_html += (f'<tr><td><b>{L["t"]}</b></td>'
                          f'<td style="color:{color}"><b>${L["p"]}</b> — {L["ord"]}</td>'
                          f'<td>{L["sh"]} سهم</td></tr>')
        st.markdown(f"""
        <div class="plan-box"><table class="plan-table">
        <tr><td colspan="3"><b>السعر الحالي:</b> ${mkt} | <b>المتوسط المتوقع:</b> ${avg_entry}</td></tr>
        {rows_html}
        <tr><td><b>🛡️ الوقف الصلب (فجوات)</b></td><td style="color:#d63031"><b>${stop}</b> (-{risk_pct}%)</td><td>—</td></tr>
        <tr><td><b>📌 وقف القرار (إغلاق يومي)</b></td><td style="color:#e17055"><b>${stop_struct}</b></td><td>تحت الدعم</td></tr>
        <tr><td><b>🎯 هدف 1 (بيع 33%)</b></td><td style="color:#00b894"><b>${target1}</b> (+{rew_pct[0]}%)</td><td>R:R 1:{rr[0]}</td></tr>
        <tr><td><b>🚀 هدف 2 (بيع 33% + Trailing)</b></td><td style="color:#0984e3"><b>${target2}</b> (+{rew_pct[1]}%)</td><td>R:R 1:{rr[1]}</td></tr>
        <tr><td><b>🌟 هدف 3 (بيع الباقي)</b></td><td style="color:#6c5ce7"><b>${target3}</b> (+{rew_pct[2]}%)</td><td>R:R 1:{rr[2]}</td></tr>
        <tr><td><b>📈 Trailing Stop</b></td><td>ينشط عند ${target1} بمسافة ${trail_dist}</td><td>—</td></tr>
        <tr><td><b>💵 قيمة الصفقة</b></td><td>${pos_value:,}</td><td>{round(pos_value/portfolio_size*100, 1)}% من المحفظة</td></tr>
        <tr><td><b>⚖️ المخاطرة القصوى</b></td><td style="color:#d63031">${round(max_risk, 2)}</td><td>{eff_risk}%</td></tr>
        </table></div>""", unsafe_allow_html=True)

        with st.expander("⚙️ قواعد التنفيذ"):
            st.markdown("""
            - أوامر **Market** تُنفذ الآن، وأوامر **Limit** تُعلّق تحت السعر الحالي فقط
            - لا تحرّك الوقف للخسارة أبداً
            - إغلاق يومي تحت وقف القرار = خروج فوري مهما كان السعر
            - عند الهدف 1: بيع 33% + تفعيل Trailing Stop
            - عند الهدف 2: بيع 33% + نقل الوقف لنقطة الدخول
            - وقف زمني: 5-7 جلسات بلا حركة = إعادة تقييم
            """)

    st.markdown("### تفصيل النقاط")
    st.dataframe(pd.DataFrame(list(r["breakdown"].items()), columns=["المعيار", "النقاط"]),
                 use_container_width=True, hide_index=True)
    st.markdown(f'<a href="https://fintel.io/ss/us/{sym.lower()}" target="_blank">عرض تفاصيل Short Interest على Fintel</a>', unsafe_allow_html=True)


# ============================================================
# ===== لوحة رادار التقسيم =====
# ============================================================
def render_split_board(rows):
    if not rows:
        st.warning("لا توجد أسهم بتقسيم عكسي ضمن الفلاتر الحالية")
        return
    counts = {}
    for r in rows:
        counts[r["phase_key"]] = counts.get(r["phase_key"], 0) + 1

    st.markdown("#### 📊 توزيع المراحل")
    cols = st.columns(6)
    for idx, key in enumerate(["READY", "WATCH", "RETEST", "PROOF", "ZONE", "WASHOUT"]):
        meta = PHASE_META[key]
        with cols[idx]:
            st.markdown(
                f'<div style="background:{meta["color"]}18;border:1.5px solid {meta["color"]};'
                f'border-radius:12px;padding:10px;text-align:center;margin:3px">'
                f'<div style="font-size:24px;font-weight:bold;color:{meta["color"]}">{counts.get(key, 0)}</div>'
                f'<div style="font-size:11px">{meta["label"]}</div></div>',
                unsafe_allow_html=True)

    for key in ["READY", "WATCH", "RETEST", "PROOF"]:
        group = sorted([r for r in rows if r["phase_key"] == key], key=lambda x: x["days"])
        if not group:
            continue
        st.markdown(f"#### {PHASE_META[key]['label']} — ({len(group)})")
        for r in group:
            zone_txt = f"${r['zone']['low']}–{r['zone']['high']}" if r["zone"] else "—"
            with st.expander(f"**{r['symbol']}** | تقسيم {r['ratio']} قبل {r['days']} يوم | المنطقة {zone_txt} | ارتداد {r['rebound']}%"):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("عمر التقسيم", f"{r['days']} يوم")
                c2.metric("النسبة", r["ratio"])
                c3.metric("الغسيل من القمة", f"-{r['drawdown']}%")
                c4.metric("البعد عن القاع", f"{r['dist']}%" if r['dist'] is not None else "-")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("جفاف الحجم", "✅" if r["vol_dryup"] else "❌")
                c2.metric("تقلص التقلب", "✅" if r["vol_contract"] else "❌")
                c3.metric("لمسات القاع", str(r["zone"]["touches"]) if r["zone"] else "0")
                c4.metric("ثبات جلستين", "✅" if r["stability"] else "❌")
                if r["zone"]:
                    st.markdown(f'<div class="info-box">📐 <b>المنطقة المعتمدة:</b> ${r["zone"]["low"]} — ${r["zone"]["high"]} ({r["zone"]["touches"]} لمسات)</div>', unsafe_allow_html=True)
                if key == "READY":
                    st.success("✅ داخل منطقة الدخول (≤15%) — افتح تبويب «تحليل سهم» للخطة الكاملة")
                elif key == "WATCH":
                    st.info("👁️ الدعم مؤكد لكن السعر خارج منطقة 15% — انتظر تراجعاً هادئاً بحجم منخفض")
                elif key == "RETEST":
                    st.info("🔄 السهم يختبر المنطقة — راقب ثبات جلستين بقيعان صاعدة")
                else:
                    st.info("⏳ منطقة متكونة لكن بلا إثبات حياة (ارتداد ≥10%) بعد")

    early = [r for r in rows if r["phase_key"] in ("ZONE", "WASHOUT")]
    if early:
        with st.expander(f"🌪️ مراحل مبكرة — قائمة انتظار ({len(early)})"):
            st.dataframe(pd.DataFrame([{
                "الرمز": r["symbol"], "اليوم": r["days"], "النسبة": r["ratio"],
                "المرحلة": PHASE_META[r["phase_key"]]["label"], "الغسيل %": r["drawdown"],
                "جفاف": "✅" if r["vol_dryup"] else "❌",
                "قاع المنطقة": r["zone"]["low"] if r["zone"] else "-",
            } for r in early]), use_container_width=True, hide_index=True)

    rejected = [r for r in rows if r["phase_key"] == "REJECTED"]
    if rejected:
        with st.expander(f"🚫 مستبعدات بأسباب ({len(rejected)})"):
            for r in rejected:
                st.markdown(f"- **{r['symbol']}** ({r['ratio']}, يوم {r['days']}): " + " | ".join(r["rejections"]))


# ============================================================
# ===== الواجهة =====
# ============================================================
st.markdown("# 🎯 Stock Screener Pro")

with st.sidebar:
    st.markdown("### ⚙️ إعدادات الخطة")
    portfolio_size = st.number_input("حجم المحفظة ($)", min_value=1000, value=10000, step=1000)
    risk_percent = st.slider("نسبة المخاطرة (%)", 0.5, 5.0, 2.0, 0.5)
    st.markdown("### 🔌 حالة المصادر")
    st.markdown(f"**Alpaca:** {'🟢' if ALPACA_ENABLED else '⚪ غير مفعل'}")
    st.markdown(f"**Finnhub:** {'🟢' if FINNHUB_KEY else '🔴'}")
    st.markdown(f"**Yahoo Proxy:** {'🟢' if PROXY_URL else '🔴'}")
    st.markdown(f"**بيئة Render:** {'🟢 نعم' if ON_RENDER else '⚪ محلي'}")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📈 تحليل سهم", "🛰️ رادار التقسيم", "⭐ أفضل 10", "📡 رادار الاكتشاف", "⚡ سعر لحظي", "🕯️ فلتر الارتكاز"])

# ===== TAB 1 =====
with tab1:
    col1, col2 = st.columns([3, 1])
    with col1:
        sym = st.text_input("رمز السهم", "AEMD").upper()
    with col2:
        st.write("")
        st.write("")
        btn = st.button("🔍 تحليل شامل", key="a")
    if btn and sym:
        with st.spinner("جاري تحليل " + sym):
            hist, splits, source = get_candles_unified(sym, "6mo")
            if hist.empty:
                st.error("❌ لا بيانات لـ " + sym)
            else:
                st.success(f"✅ المصدر: **{source}** ({len(hist)} شمعة)")
                if source == "stooq":
                    st.warning("⚠️ مصدر طوارئ (Stooq): بيانات يومية بلا تقسيمات — رادار التقسيم غير متاح لهذا السهم")
                info = finnhub_metrics(sym)
                offering = check_offering(sym)
                news = check_negative_news(sym)
                r = score(sym, hist, info, splits, news, offering=offering)
                render_full_analysis(sym, hist, splits, info, news, offering, r)

# ===== TAB 2 =====
with tab2:
    st.markdown("### 🛰️ رادار التقسيم العكسي — لوحة دورة الحياة")
    st.markdown(
        '<div class="filter-box"><b>منطق الرادار:</b><br>'
        '• <b>الحدث:</b> تقسيم عكسي خلال آخر 90 يوم<br>'
        '• <b>الغسيل:</b> جفاف حجم <50% من حجم أول 10 جلسات بعد التقسيم<br>'
        '• <b>المنطقة:</b> ≥3 قيعان داخل ±4% بلا إغلاق تحتها<br>'
        '• <b>إثبات الحياة:</b> ارتداد ≥10% | <b>الدخول:</b> داخل 15% من القاع<br>'
        '• <b>استبعاد فوري:</b> طرح SEC / خبر حرج / سعر <$1.30 / مقسّم تسلسلي</div>',
        unsafe_allow_html=True)

    if st.button("🛰️ امسح دورة التقسيم", key="sp"):
        with st.spinner("تحميل قائمة الأسهم..."):
            universe = get_dynamic_universe(limit=500)
        st.info(f"📡 {len(universe)} سهم — جلب متوازٍ")
        pg = st.progress(0)
        raw = scan_parallel(universe, "1y", progress_cb=lambda i, n: pg.progress(i / n))
        pg.empty()
        rows = []
        for s, h, sps, src in raw:
            if src == "stooq":
                continue
            try:
                pi = post_split_phase(s, h, sps)
                if not pi:
                    continue
                rej = split_rejections(s, h, pi)
                serial = split_rejections_serial(sps)
                if serial:
                    rej.append(serial)
                if rej:
                    pi["phase_key"] = "REJECTED"
                    pi["rejections"] = rej
                elif pi["phase_key"] in ("READY", "WATCH", "RETEST"):
                    veto, why = advanced_veto(s)
                    if veto:
                        pi["phase_key"] = "REJECTED"
                        pi["rejections"] = [why]
                rows.append(pi)
            except Exception:
                continue
        st.session_state["split_board"] = rows
        st.success(f"✅ اكتمل المسح: {len(rows)} سهم بتقسيم عكسي")

    if "split_board" in st.session_state:
        render_split_board(st.session_state["split_board"])
    else:
        st.info("اضغط «امسح دورة التقسيم» لبدء أول مسح")

# ===== TAB 3 =====
with tab3:
    st.markdown("### ⭐ أفضل 10 أسهم — TradingView Screener")
    if st.button("🔍 ابدأ الفحص الديناميكي", key="t"):
        with st.spinner("جاري تحميل القائمة..."):
            universe = get_dynamic_universe(limit=300)
        st.info(f"📡 {len(universe)} سهم — جلب متوازٍ")
        pg = st.progress(0)
        raw = scan_parallel(universe, "3mo", progress_cb=lambda i, n: pg.progress(i / n))
        pg.empty()
        cands = []
        for s, h, sps, src in raw:
            try:
                cheap = score(s, h, {}, sps)
                if cheap["total"] >= 20 and not cheap["hard_veto"]:
                    cands.append((s, h, sps))
            except Exception:
                continue
        st.info(f"🔎 {len(cands)} نجحت بالفلتر الرخيص — Finnhub للناجين فقط")
        pg2 = st.progress(0)
        res = []
        for i, (s, h, sps) in enumerate(cands):
            pg2.progress((i + 1) / max(len(cands), 1))
            try:
                r = score(s, h, finnhub_metrics(s), sps)
                if r["total"] >= 40:
                    res.append(r)
            except Exception:
                continue
        pg2.empty()
        if res:
            res.sort(key=lambda x: x["total"], reverse=True)
            for i, r in enumerate(res[:10], 1):
                with st.expander(f"{i}. **{r['symbol']}** - {r['total']}/100 {r['verdict']}"):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("السعر", f"${round(r['price'], 3)}")
                    c2.metric("RSI", r['rsi'])
                    c3.metric("Stoch", r['stoch'])
                    if r["support"]:
                        st.write(f"الدعم: ${round(r['support'], 3)} (على بعد {r['dist_sup']}%)")
                    if r["split_info"].get("has_split"):
                        st.write(f"Reverse Split: {r['split_info']['ratio']} قبل {r['split_info']['days_since']} يوم")
                    if r["stability"]:
                        st.write(f"الثبات: {r['stability']['strength']} - {r['stability']['sessions_held']} جلسات")
        else:
            st.warning("لا نتائج.")

# ===== TAB 4 =====
with tab4:
    st.markdown("### 📡 رادار الاكتشاف المبكر")
    st.caption("أسهم Squeeze محتملة")
    if st.button("🔍 ابحث ديناميكياً", key="h"):
        with st.spinner("جاري تحميل القائمة..."):
            universe = get_dynamic_universe(limit=300)
        st.info(f"📡 {len(universe)} سهم — جلب متوازٍ")
        pg = st.progress(0)
        raw = scan_parallel(universe, "3mo", progress_cb=lambda i, n: pg.progress(i / n))
        pg.empty()
        cands = []
        for s, h, sps, src in raw:
            try:
                if score(s, h, {}, sps)["rsi"] <= 35:
                    cands.append((s, h, sps))
            except Exception:
                continue
        st.info(f"🔎 {len(cands)} سهم بنقاط RSI مؤهلة — Finnhub لها فقط")
        pg2 = st.progress(0)
        res = []
        for i, (s, h, sps) in enumerate(cands):
            pg2.progress((i + 1) / max(len(cands), 1))
            try:
                r = score(s, h, finnhub_metrics(s), sps)
                if r["total"] < 40 or r.get("hard_veto"):
                    continue
                res.append(r)
            except Exception:
                continue
        pg2.empty()
        if res:
            res.sort(key=lambda x: x["total"], reverse=True)
            for r in res[:10]:
                with st.expander(f"**{r['symbol']}** - {r['total']}/100"):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("السعر", f"${round(r['price'], 3)}")
                    c2.metric("RSI", r['rsi'])
                    c3.metric("Stoch", r['stoch'])
                    if r["short_pct"] > 0:
                        st.write(f"Short Float: {round(r['short_pct']*100, 2)}%")
        else:
            st.info("لا فرص حالياً.")

# ===== TAB 5 =====
with tab5:
    st.markdown("### ⚡ مراقبة الأسعار اللحظية")
    watchlist = [s.strip().upper() for s in st.text_input(
        "رموز الأسهم (مفصولة بفواصل)", value="AAPL,TSLA,NVDA,AMZN,META").split(",") if s.strip()]
    c1, c2 = st.columns(2)
    with c1:
        auto_refresh = st.checkbox("🔄 تحديث تلقائي", value=False)
    with c2:
        refresh_interval = st.selectbox("⏱️ الفاصل (ثوانٍ)", [5, 10, 15, 30], index=1)

    if st.button("🔄 تحديث الآن", key="live") or auto_refresh:
        if not watchlist:
            st.warning("⚠️ أدخل رمزاً واحداً على الأقل")
        else:
            cols = st.columns(min(len(watchlist), 5))
            for idx, s in enumerate(watchlist[:20]):
                p = get_realtime_price(s)
                with cols[idx % len(cols)]:
                    if p:
                        chg = p.get("percent_change", 0)
                        cls = "live-up" if chg > 0 else "live-down" if chg < 0 else "live-neutral"
                        st.markdown(
                            f'<div class="live-price"><div style="font-size:14px;color:#636e72">🟢 {s}</div>'
                            f'<div style="font-size:28px">${p["price"]:.2f}</div>'
                            f'<div class="{cls}" style="font-size:16px">{chg:+.2f}%</div>'
                            f'<div style="font-size:11px;color:#b2bec3">{p.get("source")}</div></div>',
                            unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div class="live-price"><div style="font-size:14px">{s}</div><div style="color:#d63031;font-size:18px">❌ غير متاح</div></div>', unsafe_allow_html=True)
            if auto_refresh:
                time.sleep(refresh_interval)
                st.rerun()

# ===== TAB 6 =====
with tab6:
    st.markdown("### 🕯️ فلتر سهم الارتكاز — الشموع الساقطة (4H)")
    st.markdown(
        '<div class="filter-box"><b>قواعد الطريقة (بدون شورت):</b><br>'
        '• سلّم شموع حمراء على 4H: الذيل مقاومة، الرأس هدف<br>'
        '• أي ذيل يحترق يتحول لدعم تلقائياً<br>'
        '• لكل شمعة هدف ~25% | المقاومة على نطاق متوسط 20–30 = تعزيز<br>'
        '• المقسم >3 مرات: سقف الهدف تحت MA20 | أخبار حرجة/طرح = إقصاء</div>',
        unsafe_allow_html=True)

    if st.button("🕯️ امسح بطريقة الارتكاز", key="pv"):
        with st.spinner("تحميل القائمة..."):
            universe = get_dynamic_universe(limit=PIVOT_UNIVERSE_LIMIT)

        def fetch_both(s):
            hd, sps, src = get_candles_unified(s, "6mo")
            if hd.empty:
                return None
            h4 = get_4h(s)
            if h4 is None or h4.empty:
                return None
            return s, hd, h4, sps

        pg = st.progress(0)
        rows = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = [ex.submit(fetch_both, s) for s in universe]
            for i, f in enumerate(as_completed(futs)):
                pg.progress((i + 1) / len(universe))
                try:
                    rr_ = f.result()
                    if rr_:
                        rows.append(rr_)
                except Exception:
                    pass
        pg.empty()

        hits = []
        for s, hd, h4, sps in rows:
            res = filter_pivot_stock(s, hd, h4, sps)
            if res:
                hits.append(res)

        for h in hits:
            veto, why = advanced_veto(h["symbol"])
            if veto:
                h["veto"] = True
                h["reasons"].append(f"🚫 إقصاء: {why}")
        st.session_state["pivot_hits"] = hits
        st.success(f"✅ {len(hits)} سهم مطابق لطريقة الارتكاز")

    if "pivot_hits" in st.session_state:
        hits = st.session_state["pivot_hits"]
        if not hits:
            st.warning("لا سهام مطابقة للطريقة حالياً")
        for h in sorted(hits, key=lambda x: x["points"], reverse=True):
            veto = "🚫 " if h.get("veto") else ""
            with st.expander(f"{veto}**{h['symbol']}** — {h['points']} نقطة | الهدف الحالي ${h['target']}"):
                for reason in h["reasons"]:
                    st.markdown(f'<div class="info-box">{reason}</div>', unsafe_allow_html=True)
                st.markdown("#### السلّم (من الأقرب للأبعد)")
                st.dataframe(pd.DataFrame(h["ladder"]["ladder"]),
                             use_container_width=True, hide_index=True)
                if h["ladder"]["supports"]:
                    st.markdown(f'<div class="success-box">🛡️ ذيول تحولت لدعم: {h["ladder"]["supports"]}</div>', unsafe_allow_html=True)

st.markdown("---")
st.caption("⚠️ تعليمي فقط - ليس توصية استثمارية")
