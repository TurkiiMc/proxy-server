"""
Server — Yahoo Finance API Proxy (v1.17.1)
- سقف تزامن 16 اتصال Yahoo في اللحظة (يمنع اختناق CPU و503)
- كاش ذاكري: 60s اقتباسات / 300s شموع / 3600s إحصاءات شورت
- /ping و /health لا يلمسان Yahoo أبداً (يردان فوراً تحت العاصفة)
- مضيفان بديلان (query1/query2) + محاولتان + تهدئة عند 429 + 404 فوري
- v1.3: تحويل النقطة→شرطة (BRK.A → BRK-A)
- v1.5: جلسة Crumb لجلب الشورت والفلوت من quoteSummary (مصدر الوقود)
- v1.6: /ibkr/borrow رسوم الاقتراض والمتاح من ملف IBKR العام (كاش 15 دقيقة)
- v1.7: /yahoo/candles?prepost=true يضيف التداول الممتد (قبل الافتتاح وبعد الإغلاق) — الافتراضي false كما كان
- v1.8: /watch مراقبة قائمة «قبل الخبر» كل 5 دقائق (سحب واسترداد، حركة مبكرة، صعود، كسر)
- v1.9: Massive بديلاً حين تخلو Yahoo من صفقات اليوم · /watch/test للمقارنة · لا تُقرأ صفقة الأمس كأنها اليوم
- v1.10: /watch/replay يعيد تشغيل المراقب على أيام سابقة (متى كان سينبّه، ومتى بلغت الحركة ذروتها)
- v1.11: /h4/compare يقارن حدود 4H (Webull · ساعة ثابتة · الجلسة الرسمية) لمطابقة Webull وتطبيق فيصل
- v1.12: المراقب بقواعد فيصل — 🌀 سحب 4–15% فقط، و🧱 كسر أعمق، و🎯 اختبار منطقة الدعم (10–20% تحت اختبار المقاومة)
- v1.13: رسالة واحدة مختصرة لكل جولة مجمّعة حسب النوع (سطر لكل سهم)، و🎯 و📉 و🧱 عند حدوثها اليوم فقط
         (لا كل يوم والسهم ما زال فيها)، و🎯 لـ«جاهز» و«قريب» فقط، وأرقام مختصرة، وتسجيل رفض تيليجرام
- v1.14: 🌀 السحب من 10% إلى 15% تحت الدعم (كان 4%) كما في «سبرنت داون 10% > 15%»
- v1.15: 🚀 و⚡ على أعلى سعر في الفترة لا آخر سعر فقط — القفزة التي تصعد وتعود بين فحصين لا تفوت
         (SMTK: 1.63 بعد الإغلاق ثم 1.07)، مع حجم شمعة القمة · مرة لكل فترة (قبل الافتتاح، الجلسة، بعد الإغلاق)
         · 🧲 بجانب السهم الذي عليه محفّز معلّق (اندماج أو اتفاقية لم تكتمل — من الرادار)
- v1.16: رسالة أبسط — سطر واحد لكل حدث يبدأ برمزه وينتهي بالفعل (بلا عناوين ولا شرح)، وفئة السهم تظهر لـ✅ و👀 فقط
         · 🩳 بلغ هدف هبوط الشورت (فيصل: «لكل ألف شورت 1–2%») — من الرادار، متابعة الثبات لا دخول
- v1.17: أهدأ — الفحص كل 5 دقائق كما هو، لكن الرسالة الفورية **بصوت** لما يحتاج قراراً فقط: 🛡️ هبوط على سهم عليه أمر،
         و🌀 و🎯 لـ✅ و👀. البقية (🚀 ⚡ 📉 🧱 🩳 ولـ👁️) تُجمع في **ملخص صامت كل 30 دقيقة** بوقت كل حدث (WATCH_DIGEST_MIN)
- v1.17.1: قيمة WATCH_DIGEST_MIN الخاطئة («60 دقيقة» أو فارغة) لا توقف الخادم أبداً — يُقرأ أول رقم (5–240) وإلا 30
- تسجيل كل فشل في السجلات (لا أخطاء صامتة)
"""
import os, time, threading
from concurrent.futures import ThreadPoolExecutor
import requests
from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse

app = FastAPI(title="Faisal Proxy Server", version="1.17.1")

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
CHART_HOSTS = ["https://query1.finance.yahoo.com",
               "https://query2.finance.yahoo.com"]
YAHOO_WORKERS = 16
QUOTE_TTL = 60
CANDLE_TTL = 300
STATS_TTL = 3600
BULK_CAP = 150

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

# ===== نواة Yahoo =====
def _y(sym):
    # Yahoo يريد شرطة لفئات الأسهم: BRK.A (Finnhub) → BRK-A (Yahoo)
    return sym.replace(".", "-")

def _chart(sym, range_, interval, prepost=False):
    last_err = "no attempt"
    for host in CHART_HOSTS:
        for attempt in range(2):
            try:
                r = requests.get(f"{host}/v8/finance/chart/{_y(sym)}",
                                 params={"range": range_, "interval": interval,
                                         "includePrePost": "true" if prepost else "false"},
                                 headers=UA, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    res = (data.get("chart") or {}).get("result")
                    if res:
                        return res[0], None
                    last_err = "No data"
                elif r.status_code == 404:
                    return None, "HTTP 404"   # خطأ حتمي — لا إعادة محاولة
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

# ===== v1.5: جلسة Crumb لـ quoteSummary =====
_crumb = {"session": None, "value": None, "ts": 0.0}
_crumb_lock = threading.Lock()

def _crumb_session():
    with _crumb_lock:
        now = time.time()
        if _crumb["session"] and (now - _crumb["ts"]) < 1800:
            return _crumb["session"], _crumb["value"]
        s = requests.Session()
        s.headers.update(UA)
        try:
            s.get("https://fc.yahoo.com", timeout=15)   # 404 متوقع — يضبط الـ cookie
        except Exception:
            pass
        val = None
        for host in CHART_HOSTS:
            try:
                r = s.get(f"{host}/v1/test/getcrumb", timeout=15)
                if r.status_code == 200 and 0 < len(r.text.strip()) < 50:
                    val = r.text.strip()
                    break
            except Exception:
                continue
        if val:
            _crumb.update({"session": s, "value": val, "ts": now})
            print("[proxy] crumb session OK ✅")
            return s, val
        print("[proxy] crumb session FAILED ❌")
        return None, None

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
    prepost: bool = Query(False, description="true = إضافة التداول الممتد (للفواصل داخل اليوم فقط)"),
):
    sym = symbol.strip().upper()
    key = f"c:{sym}:{period}:{interval}:{int(prepost)}"
    hit = cache_get(key, CANDLE_TTL)
    if hit:
        return hit
    try:
        result, err = _chart(sym, period, interval, prepost)
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
            print(f"[proxy] bulk partial: {e}")
    if todo and not quotes:
        print(f"[proxy] ⚠️ دفعة فارغة: {len(todo)} رمزاً فشلت كلها")
    return {"success": True, "count": len(quotes), "quotes": quotes}

# ===== v1.5: مصدر الوقود (الشورت والفلوت) =====
@app.get("/yahoo/stats")
def yahoo_stats(symbol: str = Query(..., description="رمز السهم، مثال: CISS")):
    """جلب الشورت والفلوت من quoteSummary عبر جلسة crumb"""
    sym = symbol.strip().upper()
    key = f"s:{sym}"
    hit = cache_get(key, STATS_TTL)
    if hit:
        return hit
    for attempt in range(2):
        s, crumb = _crumb_session()
        if not s:
            return {"success": False, "error": "no crumb session"}
        try:
            r = s.get(f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{_y(sym)}",
                      params={"modules": "defaultKeyStatistics", "crumb": crumb},
                      timeout=15)
            if r.status_code == 401 and attempt == 0:
                with _crumb_lock:
                    _crumb.update({"session": None, "value": None, "ts": 0.0})
                continue
            if r.status_code != 200:
                print(f"[proxy] stats fail {sym}: HTTP {r.status_code}")
                return {"success": False, "error": f"HTTP {r.status_code}"}
            data = r.json()
            ks = (data.get("quoteSummary") or {}).get("result", [{}])[0].get("defaultKeyStatistics", {})
            def raw(v): return v.get("raw") if isinstance(v, dict) else v
            out = {
                "success": True, "symbol": sym,
                "sharesShort": raw(ks.get("sharesShort")),
                "shortPercentOfFloat": raw(ks.get("shortPercentOfFloat")),
                "floatShares": raw(ks.get("floatShares")),
                "shortRatio": raw(ks.get("shortRatio")),
            }
            cache_set(key, out)
            return out
        except Exception as e:
            print(f"[proxy] stats exc {sym}: {e}")
            return {"success": False, "error": str(e)}
    return {"success": False, "error": "crumb retry failed"}

# ===== v1.6: رسوم الاقتراض من IBKR (ملف FTP العام — نفس مصدر IBorrowDesk) =====
import io, ftplib

IBKR_TTL = 900   # الملف يتحدث كل ~15 دقيقة عند IBKR
_ibkr = {"ts": 0.0, "data": {}, "err": None, "file_time": None}
_ibkr_lock = threading.Lock()

def _ibkr_load():
    """يحمّل usa.txt مرة كل 15 دقيقة ويحفظه في الذاكرة. عند الفشل يعيد آخر نسخة ناجحة (إن وُجدت)"""
    with _ibkr_lock:
        if _ibkr["data"] and (time.time() - _ibkr["ts"]) < IBKR_TTL:
            return _ibkr["data"], None
        try:
            buf = io.BytesIO()
            with ftplib.FTP("ftp3.interactivebrokers.com", timeout=60) as ftp:
                ftp.login(user="shortstock", passwd="")
                ftp.retrbinary("RETR usa.txt", buf.write)
            text = buf.getvalue().decode("utf-8", errors="ignore")
            data, header, stamp = {}, None, None
            for ln in text.splitlines():
                if ln.startswith("#BOF"):
                    stamp = " ".join(p.strip() for p in ln.split("|")[1:3]); continue
                if ln.startswith("#SYM"):
                    header = [h.strip().lstrip("#") for h in ln.split("|")]; continue
                if ln.startswith("#") or not header:
                    continue
                row = dict(zip(header, ln.split("|")))
                sym = (row.get("SYM") or "").strip().upper()
                if not sym:
                    continue
                try:
                    fee = float(row.get("FEERATE") or 0)
                except ValueError:
                    fee = None
                av_raw = (row.get("AVAILABLE") or "0").strip()
                try:
                    av = float(av_raw.replace(">", "").replace(",", ""))
                except ValueError:
                    av = 0.0
                data[sym] = {"fee": fee, "available": av, "available_raw": av_raw}
            if not data:
                raise RuntimeError("الملف فارغ")
            _ibkr.update(ts=time.time(), data=data, err=None, file_time=stamp)
            print(f"[proxy] IBKR borrow OK ✅ {len(data)} رمزاً ({stamp})")
            return data, None
        except Exception as e:
            _ibkr["err"] = str(e)
            print(f"[proxy] IBKR borrow FAILED ❌ {e}")
            return _ibkr["data"], str(e)

@app.get("/ibkr/borrow")
def ibkr_borrow(symbols: str = Query(..., description="رموز مفصولة بفواصل: NXTT,MYSZ")):
    """رسوم الاقتراض السنوية % والأسهم المتاحة للاقتراض من IBKR"""
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()][:500]
    data, err = _ibkr_load()
    if not data:
        return {"success": False, "error": err or "no data"}
    out = {s: data[s] for s in syms if s in data}
    return {"success": True, "count": len(out), "file_time": _ibkr["file_time"],
            "stale": bool(err), "error": err, "borrow": out}



# =====================================================================
# v1.8: /watch — مراقبة قائمة «قبل الخبر» كل 5 دقائق (يستدعيها cron-job.org)
# تقرأ القائمة من المستودع العام (data/coiled.json يكتبه الرادار)، وتجلب شموع 5 دقائق مع التداول الممتد،
# وترسل إلى تيليجرام: 🌀 سحب واسترداد · ⚡ حركة مبكرة · 🚀 صعود · 📉 كسر مستوى التنبيه · 🛡️ هبوط على سهم بأمر.
# كل نوع مرة واحدة لكل سهم في اليوم، و🚀 و⚡ مرة لكل فترة (v1.15). مراقبة فقط — لا تحليل ولا أوامر.
# متغيرات البيئة: COILED_URL · TELEGRAM_TOKEN · TELEGRAM_CHAT_ID · WATCH_KEY (اختياري) · GH_TOKEN (للمستودع الخاص)
# =====================================================================
from datetime import datetime, timedelta, timezone


def _env_min(name, default, lo=5.0, hi=240.0):
    """v1.17.1: دقائق من متغير بيئة بلا أي خطر على التشغيل — «60» أو «60 دقيقة» أو «٦٠» ← 60؛ فارغ أو خطأ ← الافتراضي"""
    import re
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        m = re.search(r"\d+(?:[.,]\d+)?", raw)
        v = float(m.group(0).replace(",", ".")) if m else None
    except Exception:
        v = None
    if v is None:
        print(f"[watch] {name}={raw!r} ليست رقماً — نستخدم {default:g}")
        return default
    if not lo <= v <= hi:
        print(f"[watch] {name}={v:g} خارج {lo:g}–{hi:g} — نستخدم {min(max(v, lo), hi):g}")
    return min(max(v, lo), hi)


WATCH = {"up_early_pct": 7.0,     # ⚡ حركة مبكرة (قبل الافتتاح/بعد الإغلاق) — احتمال تسريب قبل الخبر
         "up_pct": 15.0,          # 🚀
         "drop_pct": 8.0,         # 🛡️ لسهم عليه أمر
         "sweep_min_pct": 10.0,   # 🌀 سحب فيصل: نزول 10–15% تحت الدعم ثم عودة فوقه («سبرنت داون 10% > 15%» — v1.14؛ كان 4%)
         "sweep_max_pct": 15.0,   #    أعمق من ذلك = كسر لا سحب
         "max_symbols": 120,
         "digest_min": _env_min("WATCH_DIGEST_MIN", 30.0)}   # v1.17: الملخص الصامت كل 30 دقيقة (v1.17.1: قراءة آمنة)
_watch_sent = {}                  # {"YYYY-MM-DD:SYM:kind": 1} — يُصفَّر يومياً
_watch_buf = {}                   # v1.17: {المفتاح: (النوع، الرتبة، الرمز، السطر)} — بانتظار الملخص الصامت
_watch_flush = {"at": 0.0}
_watch_lock = threading.Lock()
_watch_last = {"at": None, "checked": 0, "alerts": 0, "nodata": 0, "error": None, "running": False}


def _ny_now(ts=None):
    """توقيت نيويورك بلا اعتماد على قاعدة المناطق (التوقيت الصيفي: الأحد الثاني من مارس → الأحد الأول من نوفمبر)"""
    u = datetime.fromtimestamp(ts, timezone.utc) if ts is not None else datetime.now(timezone.utc)
    y = u.year
    mar = datetime(y, 3, 8, 7, tzinfo=timezone.utc); mar += timedelta(days=(6 - mar.weekday()) % 7)
    nov = datetime(y, 11, 1, 6, tzinfo=timezone.utc); nov += timedelta(days=(6 - nov.weekday()) % 7)
    return u + timedelta(hours=-4 if mar <= u < nov else -5)


def _tg(text, silent=False):
    """silent=True (v1.17): تصل الرسالة بلا صوت ولا اهتزاز (disable_notification في تيليجرام)"""
    tok, chat = os.environ.get("TELEGRAM_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        print("[watch] لا TELEGRAM_TOKEN/CHAT_ID"); return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          json={"chat_id": chat, "text": text[:4000], "disable_notification": bool(silent)}, timeout=15)
        if r.status_code != 200:                    # v1.13: رفض تيليجرام لم يعد صامتاً (توكن/معرّف/حظر)
            print(f"[watch] telegram رفض {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"[watch] telegram: {e}")
        return False


def _n(x):
    """رقم مختصر للعرض: 4 أرقام معنوية دون أصفار زائدة (1.600000023841858 ← 1.6 · 0.4710 ← 0.471)"""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return str(x)
    return f"{x:,.0f}" if abs(x) >= 1000 else f"{x:.4g}"


RLM = "‏"                     # يبدأ به كل سطر ليُعرض من اليمين لليسار في تيليجرام ولو بدأ برمز لاتيني
# v1.16: ترتيب الأحداث في رسالة الجولة — كل سطر يبدأ برمزه (لا عناوين)، والأهم أولاً
WATCH_ORDER = ["reclaim", "down", "up", "early", "zone", "short", "break", "alert"]
_TIER_RANK = {"✅": 0, "👀": 1, "👁️": 2}


def _urgent(k, rank):
    """v1.17: ما يحتاج قراراً الآن يصل فوراً بصوت — 🛡️ هبوط على سهم عليه أمر، و🌀 و🎯 لـ✅ و👀. الباقي للملخص الصامت"""
    return k == "down" or (k in ("reclaim", "zone") and rank <= 1)


def _watch_digest(found, title=None):
    """found: {نوع: [(رتبة الفئة، الرمز، السطر)]} ← رسائل تيليجرام (أقل من 3,500 حرف لكل رسالة)"""
    head = f"{RLM}👁️ المراقب · {_riyadh(time.time())}" + (f" · {title}" if title else "")
    lines = []
    for k in WATCH_ORDER:
        lines += [f"{RLM}{line}" for _, _, line in sorted(found.get(k) or [])]
    if any("🧲" in ln for ln in lines):
        lines.append(f"{RLM}🧲 = اندماج أو اتفاقية معلّقة")
    msgs, cur = [], [head]
    for ln in lines:
        if sum(len(x) + 1 for x in cur) + len(ln) > 3500:
            msgs.append("\n".join(cur))
            cur = [head + " (تابع)"]
        cur.append(ln)
    if len(cur) > 1:
        msgs.append("\n".join(cur))
    return msgs


def _at(arr, i):
    return arr[i] if arr and i < len(arr) else None


# حدود الفترات بالدقائق من منتصف ليل نيويورك
PERIODS = {"قبل الافتتاح": (240, 570), "الجلسة": (570, 960), "بعد الإغلاق": (960, 1200)}


def _sessions(result):
    """من شموع 5 دقائق (يومان مع الممتد): آخر سعر وفترته، والمرجع، وقاع اليوم وحجمه،
    وأعلى سعر في الفترة الحالية وحجم شمعته (v1.15)"""
    ts = result.get("timestamp") or []
    q = ((result.get("indicators") or {}).get("quote") or [{}])[0] or {}
    bars = []
    for i, t in enumerate(ts):
        c = _at(q.get("close"), i)
        if c is None:
            continue
        c = float(c)
        ny = _ny_now(t)
        lo, hi = _at(q.get("low"), i), _at(q.get("high"), i)
        bars.append({"d": ny.date(), "m": ny.hour * 60 + ny.minute, "c": c,
                     "l": float(lo if lo is not None else c), "h": max(float(hi), c) if hi is not None else c,
                     "v": float(_at(q.get("volume"), i) or 0)})
    reg = [b for b in bars if 570 <= b["m"] < 960]
    if not bars or not reg:
        return None
    rdays = sorted({b["d"] for b in reg})
    last = bars[-1]
    if last["m"] < 240 or last["m"] >= 1200:
        return None
    today = last["d"]
    if last["m"] < 570:
        session, ref_d = "قبل الافتتاح", rdays[-1] if rdays[-1] < today else (rdays[-2] if len(rdays) > 1 else None)
    elif last["m"] < 960:
        session, ref_d = "الجلسة", rdays[-2] if len(rdays) > 1 and rdays[-1] == today else (rdays[-1] if rdays[-1] < today else None)
    else:
        session, ref_d = "بعد الإغلاق", today
    if ref_d is None:
        return None
    ref = [b for b in reg if b["d"] == ref_d][-1]["c"]
    day = [b for b in bars if b["d"] == today and 240 <= b["m"] < 1200]
    if session == "بعد الإغلاق":
        day = [b for b in day if b["m"] >= 960]          # قاع ما بعد الإغلاق فقط (لا قاع الجلسة)
    a, z = PERIODS[session]
    seg = [b for b in day if a <= b["m"] < z] or [last]
    top = max(seg, key=lambda b: b["h"])                 # أعلى سعر في الفترة الحالية، ولو عاد السعر بعده
    return {"session": session, "px": last["c"], "ref": ref, "chg": (last["c"] / ref - 1) * 100,
            "low": min(b["l"] for b in day), "vol": sum(b["v"] for b in day), "date": str(today),
            "high": top["h"], "high_vol": top["v"]}


# ---- v1.9: Massive (Polygon سابقاً) بديلاً حين لا تُظهر Yahoo صفقات اليوم ----
MASSIVE_BASE = "https://api.massive.com"
_mv_state = {"blocked_until": 0.0, "last_status": None}


def _massive_result(sym):
    """شموع 5 دقائق ليومين (تشمل الجلسات الممتدة) بصيغة Yahoo نفسها، أو None"""
    key = os.environ.get("MASSIVE_API_KEY")
    if not key or time.time() < _mv_state["blocked_until"]:
        return None
    today = _ny_now().date()
    frm = str(today - timedelta(days=5))
    try:
        r = requests.get(f"{MASSIVE_BASE}/v2/aggs/ticker/{_y(sym)}/range/5/minute/{frm}/{today}",
                         params={"adjusted": "true", "sort": "asc", "limit": 5000, "apiKey": key}, timeout=20)
    except Exception as e:
        _mv_state["last_status"] = str(e); return None
    _mv_state["last_status"] = r.status_code
    if r.status_code == 429:                       # حد الطلبات: توقف دقيقتين
        _mv_state["blocked_until"] = time.time() + 120; return None
    if r.status_code != 200:
        return None
    rows = (r.json() or {}).get("results") or []
    if not rows:
        return None
    return {"timestamp": [int(x["t"] // 1000) for x in rows],
            "indicators": {"quote": [{"open": [x.get("o") for x in rows], "high": [x.get("h") for x in rows],
                                      "low": [x.get("l") for x in rows], "close": [x.get("c") for x in rows],
                                      "volume": [x.get("v") or 0 for x in rows]}]}}


def _fresh(s):
    """الملخص لليوم الحالي بنيويورك (لا صفقة الأمس متأخرة تُقرأ كأنها اليوم)"""
    return bool(s) and s["date"] == str(_ny_now().date())


def _watch_one(sym, v):
    result, err = _chart(sym, "2d", "5m", True)
    if err and "429" in str(err):
        return "429", []                           # حد طلبات Yahoo — يُحسب في الجولة للتهدئة
    s = _sessions(result) if (result and not err) else None
    src = "Yahoo"
    if not _fresh(s):
        mres = _massive_result(sym)
        ms = _sessions(mres) if mres else None
        if _fresh(ms):
            s, src = ms, "Massive"
    if not _fresh(s):
        return None, []
    px, chg, low, ref = s["px"], s["chg"], s["low"], s["ref"]
    entry, stop = v.get("entry"), v.get("stop")
    tier = (v.get("kinds") or [""])[0]
    icon = tier.split()[0] if tier.strip() else ""
    actionable = ("جاهز" in tier) or ("قريب" in tier)          # «جاهز» و«قريب» — عليهما أوامر في منطقة الاختبار
    rank = _TIER_RANK.get(icon, 3)
    # v1.16: الفئة بجانب الرمز لـ✅ و👀 فقط (المراقبة المبكرة هي الأغلب، فلا داعي لتكرار 👁️) · 🧲 محفّز معلّق
    who = sym + (f" {icon}" if icon in ("✅", "👀") else "") + (" 🧲" if v.get("pending") else "")
    pct = f"{chg:+.1f}%"
    hits = []                                       # (النوع، الرتبة، السطر المختصر) — الشرح في عنوان المجموعة
    dkeys = {}                                      # مفتاح التكرار إن اختلف عن النوع (🚀 و⚡ مرة لكل فترة)
    mn, mx = WATCH["sweep_min_pct"] / 100, WATCH["sweep_max_pct"] / 100
    lvl = next((L for L in (v.get("support"), v.get("alert"))
                if L and L * (1 - mx) <= low <= L * (1 - mn) and px > L), None)
    if lvl:
        dep = (1 - low / lvl) * 100
        hits.append(("reclaim", rank, f"🌀 {who} {_n(px)} · نزل {dep:.0f}% إلى {_n(low)} تحت {_n(lvl)} وعاد · "
                                      f"Limit {_n(lvl)}، الوقف تحت {_n(low)}"))
    sup_ = v.get("support")
    # v1.13: الكسر والمنطقة ومستوى التنبيه «عند حدوثها اليوم» — كان المرجع (إغلاق الجلسة السابقة) فوق المستوى
    if sup_ and low < sup_ * (1 - mx) and px < sup_ and ref >= sup_ * (1 - mx):
        hits.append(("break", rank, f"🧱 {who} {_n(px)} · كسر الدعم {_n(sup_)} (أدنى {_n(low)}) · لا دخول، 5 جلسات"))
    zn = v.get("zone") or []
    if (actionable and len(zn) == 2 and zn[0] and zn[1] and not lvl
            and zn[0] <= low <= zn[1] and px >= zn[0] and ref > zn[1]):
        hits.append(("zone", rank, f"🎯 {who} {_n(px)} · دخل المنطقة {_n(zn[0])}–{_n(zn[1])} · انتظر إغلاقاً فوق قمة الاختبار"))
    if entry and chg <= -WATCH["drop_pct"]:
        hits.append(("down", rank, f"🛡️ {who} {_n(px)} ({pct}) · أمرك {_n(entry)}"
                                   + (f" · ⛔ تحت الوقف {_n(stop)}" if stop and px <= stop else " · قد يُنفَّذ")
                                   + " · راجع الخبر أو ألغِ"))
    # v1.15: 🚀 و⚡ على أعلى سعر في الفترة، لا آخر سعر فقط — القفزة التي تصعد وتعود بين فحصين لا تفوت
    # (SMTK بعد الإغلاق: 1.63 ثم 1.07 خلال دقائق، والفحص التالي رأى 1.07 فقط)
    per = s["session"]
    hi = max(float(s.get("high") or px), px)
    hchg = (hi / ref - 1) * 100 if ref else chg

    def _spike(th):
        if hi < px * 1.03:                          # السعر ما زال قرب القمة: السطر المعتاد
            return ""
        hv = int(s.get("high_vol") or 0)            # السهم الصغير قد تصنع قمته صفقة صغيرة واحدة
        return (f" · لمس {_n(hi)} ({hchg:+.0f}%) ثم {'عاد' if chg < th else 'تراجع'}"
                + (f" بحجم {hv:,}" if hv else ""))

    if hchg >= WATCH["up_pct"]:
        hits.append(("up", rank, f"🚀 {who} {_n(px)} ({pct} {per}){_spike(WATCH['up_pct'])}"
                                 + (f" · أمرك {_n(entry)} لن يُنفَّذ" if entry and px > entry * 1.05 else "") + " · لا تطارد"))
        dkeys["up"] = f"up@{per}"
    elif hchg >= WATCH["up_early_pct"] and per != "الجلسة":
        hits.append(("early", rank, f"⚡ {who} {_n(px)} ({pct} {per}){_spike(WATCH['up_early_pct'])} · حجم {int(s['vol']):,}"
                                    " · راقب ولا تطارد"))
        dkeys["early"] = f"early@{per}"
    al = v.get("alert")
    if not entry and al and px < al and not lvl and ref >= al:
        hits.append(("alert", rank, f"📉 {who} {_n(px)} ({pct}) · تحت مستوى التنبيه {_n(al)} · راقب"))
    # v1.16 (فيصل، BRTX): 🩳 بلغ هدف هبوط الشورت اليوم — «تم الهبوط، متابعة مبكرة للثبات، لا يوجد دخول آمن»
    stt = v.get("short_target") or []
    if len(stt) == 2 and stt[0] and low <= stt[0] < ref:
        hits.append(("short", rank, f"🩳 {who} {_n(px)} · بلغ هدف هبوط الشورت {_n(stt[0])}"
                                    + (f" (التالي {_n(stt[1])})" if stt[1] and stt[1] < stt[0] else "")
                                    + " · راقب الثبات، لا دخول"))
    return s["date"], [(k, r_, line, dkeys.get(k, k)) for k, r_, line in hits]


def _watch_run():
    st = _watch_last
    try:
        url = os.environ.get("COILED_URL")
        if not url:
            st["error"] = "لا COILED_URL"; return
        tok = os.environ.get("GH_TOKEN")          # للمستودع الخاص: مفتاح GitHub للقراءة فقط
        hdr = {"Authorization": f"token {tok}"} if tok else {}
        r = requests.get(url, params={"t": int(time.time() // 300)}, headers=hdr, timeout=20)
        if r.status_code != 200:
            st["error"] = f"تعذّر قراءة القائمة: HTTP {r.status_code}" + (" (مستودع خاص؟ أضف GH_TOKEN)" if r.status_code == 404 and not tok else "")
            print(f"[watch] {st['error']}"); return
        data = r.json() if r.status_code == 200 else {}
        co = {k: v for k, v in (data.get("list") or {}).items() if isinstance(v, dict)}
        co = dict(sorted(co.items(), key=lambda kv: (not kv[1].get("entry"), kv[0]))[:WATCH["max_symbols"]])
        futs = {s: exec_pool.submit(_watch_one, s, v) for s, v in co.items()}
        found, keys, nodata, limited = {}, [], 0, 0
        now_t = time.time()
        for s, f in futs.items():
            try:
                d, hits = f.result(timeout=60)
            except Exception:
                d, hits = None, []
            if d == "429":
                limited += 1; continue
            if d is None:
                nodata += 1; continue
            for k, rank, line, dk in hits:
                key = f"{d}:{s}:{dk}"                  # dk = النوع، أو «up@الفترة» لـ🚀 و⚡ (مرة لكل فترة)
                with _watch_lock:
                    if key in _watch_sent or key in _watch_buf:     # كل نوع مرة واحدة لكل سهم في اليوم
                        continue
                    if not _urgent(k, rank):           # v1.17: للملخص الصامت، بوقت الحدث
                        _watch_buf[key] = (k, rank, s, f"{line} · {_riyadh(now_t)}")
                        continue
                found.setdefault(k, []).append((rank, s, line))
                keys.append(key)
        # v1.13: لا يُعلَّم التنبيه «مُرسلاً» إلا إذا قبله تيليجرام، فيُعاد في الجولة التالية
        sent_ok = all([_tg(m) for m in _watch_digest(found)]) if keys else None
        if sent_ok:
            with _watch_lock:
                if len(_watch_sent) > 5000:
                    _watch_sent.clear()
                for key in keys:
                    _watch_sent[key] = 1
        flushed = _watch_flush_due(now_t)
        msgs = keys
        # حماية عنوان الخادم: إن رفضت Yahoo كثيراً (429) نهدأ 15 دقيقة — الرادار يستخدم البروكسي نفسه
        if limited >= max(5, len(co) // 10):
            st["cooldown_until"] = time.time() + 900
            print(f"[watch] ⚠️ Yahoo رفضت {limited} طلباً (429) — تهدئة 15 دقيقة")
        st.update({"checked": len(co), "alerts": len(msgs), "nodata": nodata, "limited_429": limited, "error": None,
                   "list_date": data.get("date"), "telegram_ok": sent_ok, "digest_sent": flushed,
                   "digest_waiting": len(_watch_buf)})
        print(f"[watch] فُحص {len(co)} · تنبيهات {len(msgs)} · بلا بيانات {nodata}")
    except Exception as e:
        st["error"] = str(e); print(f"[watch] خطأ: {e}")
    finally:
        st["at"] = datetime.now(timezone.utc).isoformat(timespec="seconds"); st["running"] = False


def _watch_flush_due(now_t=None):
    """v1.17: الملخص الصامت — كل digest_min دقيقة إن وُجد ما ينتظر. يعيد عدد الأسطر المرسلة (0 إن لم يحن)"""
    now_t = now_t or time.time()
    with _watch_lock:
        items = dict(_watch_buf)
    if not items or now_t - _watch_flush["at"] < WATCH["digest_min"] * 60:
        return 0
    found = {}
    for k, rank, s, line in items.values():
        found.setdefault(k, []).append((rank, s, line))
    if not all(_tg(m, silent=True) for m in _watch_digest(found, title=f"ملخص {WATCH['digest_min']:g} دقيقة")):
        return 0                                    # يبقى في الانتظار ويُعاد في الجولة التالية
    with _watch_lock:
        for key in items:
            _watch_buf.pop(key, None)
            _watch_sent[key] = 1
    _watch_flush["at"] = now_t
    return len(items)


def _riyadh(t):
    return (datetime.fromtimestamp(t, timezone.utc) + timedelta(hours=3)).strftime("%H:%M")


@app.get("/watch/replay", response_class=PlainTextResponse)
def watch_replay(symbols: str = Query(..., description="حتى 8 رموز: PMAX,INLF"), days: int = Query(30),
                 key: str = Query(""), all: int = Query(0, description="1 = كل الأيام لا أيام الحركة فقط")):
    """يعيد تشغيل منطق المراقب على شموع Yahoo الحقيقية (5 دقائق مع الممتد) لأيام سابقة:
    متى كان سيرسل ⚡ و🚀، وكم بلغت الحركة قبل الافتتاح وفي الجلسة وبعد الإغلاق، وهل وصلت صفقات ما قبل الافتتاح أصلاً."""
    need = os.environ.get("WATCH_KEY")
    if need and key != need:
        return "key"
    syms = [x.strip().upper() for x in symbols.split(",") if x.strip()][:8]
    rng = "5d" if days <= 5 else "1mo" if days <= 30 else "60d"
    E, U = WATCH["up_early_pct"], WATCH["up_pct"]
    out = [f"إعادة تشغيل المراقب على بيانات Yahoo — الفترة {rng}",
           f"⚡ = +{E:.0f}% خارج الجلسة · 🚀 = +{U:.0f}% · الأوقات: نيويورك (الرياض) · التنبيه الفعلي خلال 5 دقائق من الوقت المذكور",
           ""]
    for sym in syms:
        res, err = _chart(sym, rng, "5m", True)
        if err or not res:
            out += [f"■ {sym}: لا بيانات ({err})", ""]; continue
        ts = res.get("timestamp") or []
        q = ((res.get("indicators") or {}).get("quote") or [{}])[0] or {}
        cl, hi = q.get("close") or [], q.get("high") or []
        bars = []
        for i, t in enumerate(ts):
            c = cl[i] if i < len(cl) else None
            if c is None:
                continue
            h = hi[i] if i < len(hi) and hi[i] is not None else c
            ny = _ny_now(t)
            bars.append((ny.date(), ny.hour * 60 + ny.minute, float(c), float(h), t))
        days_ = sorted({b[0] for b in bars})
        reg_close = {}
        for d in days_:
            r = [b for b in bars if b[0] == d and 570 <= b[1] < 960]
            if r:
                reg_close[d] = r[-1][2]
        out.append(f"■ {sym}")
        prev, shown, pre_ok, n_days = None, 0, 0, 0

        def seg_line(seg, ref, name, early):
            if not seg or not ref:
                return f"{name}: لا صفقات", 0.0
            mx = max(seg, key=lambda b: b[3])
            mxp = (mx[3] / ref - 1) * 100
            # v1.15: كما في المراقب الحي — أول شمعة بلغ أعلاها الحد (لا إغلاقها)
            f_e = next((b for b in seg if (b[3] / ref - 1) * 100 >= E), None) if early else None
            f_u = next((b for b in seg if (b[3] / ref - 1) * 100 >= U), None)
            txt = f"{name}: أقصى {mxp:+.1f}% عند {_ny_now(mx[4]).strftime('%H:%M')} ({_riyadh(mx[4])})"
            if f_e and not (f_u and f_u[4] <= f_e[4]):
                txt += f" · ⚡ {_ny_now(f_e[4]).strftime('%H:%M')} ({_riyadh(f_e[4])})"
            if f_u:
                txt += f" · 🚀 {_ny_now(f_u[4]).strftime('%H:%M')} ({_riyadh(f_u[4])})"
            return txt + f" · {len(seg)} شمعة", mxp

        for d in days_:
            if prev is None:
                prev = reg_close.get(d)
                continue
            n_days += 1
            pre = [b for b in bars if b[0] == d and 240 <= b[1] < 570]
            reg = [b for b in bars if b[0] == d and 570 <= b[1] < 960]
            post = [b for b in bars if b[0] == d and 960 <= b[1] < 1200]
            pre_ok += 1 if pre else 0
            l1, m1 = seg_line(pre, prev, "قبل الافتتاح", True)
            l2, m2 = seg_line(reg, prev, "الجلسة", False)
            l3, m3 = seg_line(post, reg_close.get(d), "بعد الإغلاق", True)
            if all or max(m1, m2, m3) >= E:
                out += [f"{d}", f"  {l1}", f"  {l2}", f"  {l3}"]
                shown += 1
            prev = reg_close.get(d) or prev
        if not shown:
            out.append(f"  لا حركة ≥{E:.0f}% في الفترة")
        out += [f"  تغطية ما قبل الافتتاح في Yahoo: {pre_ok} من {n_days} يوماً فيها صفقات", ""]
    return "\n".join(out)


# ---- v1.11: مقارنة حدود شموع 4H بقيم Webull وتطبيق فيصل ----
_H4_BINS = {"webull": [(240, 480), (480, 570), (570, 810), (810, 960), (960, 1200)],
            "clock": [(240, 480), (480, 720), (720, 960), (960, 1200)],
            "regular": [(570, 810), (810, 960)]}


def _ema_list(xs, n):
    out, a = [], 2 / (n + 1)
    for x in xs:
        out.append(x if not out else out[-1] + a * (x - out[-1]))
    return out


def _rsi_last(cl, n=14):
    if len(cl) < n + 2:
        return None
    up = dn = None
    for i in range(1, len(cl)):
        d = cl[i] - cl[i - 1]
        u, w = max(d, 0.0), max(-d, 0.0)
        up = u if up is None else up + (u - up) / n          # Wilder (كما في الرادار)
        dn = w if dn is None else dn + (w - dn) / n
    return round(100.0 if dn == 0 else 100 - 100 / (1 + up / dn), 2)


def _macd_last(cl):
    if len(cl) < 35:
        return None
    m = [a - b for a, b in zip(_ema_list(cl, 12), _ema_list(cl, 26))]
    sg = _ema_list(m, 9)
    return {"macd": round(m[-1], 4), "signal": round(sg[-1], 4), "hist": round(m[-1] - sg[-1], 4)}


@app.get("/h4/compare", response_class=PlainTextResponse)
def h4_compare(symbol: str = Query("NTCL"), key: str = Query("")):
    """RSI14 وMACD لآخر شمعة 4H بثلاث طرق للتقسيم + RSI اليومي — للمطابقة مع Webull وتطبيق فيصل في الوقت نفسه"""
    need = os.environ.get("WATCH_KEY")
    if need and key != need:
        return "key"
    sym = symbol.strip().upper()
    out = [f"{sym} — قارن هذه القيم بما يظهر في Webull (4 ساعات) وفي تطبيق فيصل الآن", ""]
    d, err = _chart(sym, "6mo", "1d", False)
    if d:
        q = ((d.get("indicators") or {}).get("quote") or [{}])[0] or {}
        dc = [x for x in (q.get("close") or []) if x is not None]
        out.append(f"اليومي: RSI14 {_rsi_last(dc)} · MACD {_macd_last(dc)}")
    for mode, bins in _H4_BINS.items():
        res, err = _chart(sym, "3mo", "1h", mode != "regular")
        if err or not res:
            out.append(f"{mode}: لا بيانات ({err})"); continue
        q = ((res.get("indicators") or {}).get("quote") or [{}])[0] or {}
        groups, order = {}, []
        for i, t in enumerate(res.get("timestamp") or []):
            c = (q.get("close") or [None])[i] if i < len(q.get("close") or []) else None
            if c is None:
                continue
            ny = _ny_now(t); m = ny.hour * 60 + ny.minute
            b = next((a for a, z in bins if a <= m < z), None)
            if b is None:
                continue
            k = (ny.date(), b)
            if k not in groups:
                order.append(k)
            groups[k] = float(c)                     # آخر إغلاق داخل الشمعة
        cl = [groups[k] for k in order]
        last = order[-3:]
        lbl = " · ".join(f"{k[0].strftime('%m-%d')} {k[1] // 60:02d}:{k[1] % 60:02d}" for k in last)
        out.append(f"{mode}: RSI14 {_rsi_last(cl)} · MACD {_macd_last(cl)} · آخر الشموع: {lbl}")
    res, err = _chart(sym, "5d", "1h", True)
    if res:
        ts = res.get("timestamp") or []
        if ts:
            day = _ny_now(ts[-1]).date()
            hrs = [_ny_now(t).strftime("%H:%M") for t in ts if _ny_now(t).date() == day]
            out += ["", f"أوقات شموع الساعة من Yahoo ليوم {day}: {' '.join(hrs)}"]
    out += ["", "webull = حدود Webull (الافتراضي الآن) · clock = 4/8/12/16 · regular = الجلسة الرسمية فقط (القديم).",
            "خلال السوق تختلف القيم بسبب الشمعة الجارية؛ قارن والسوق مغلق."]
    return "\n".join(out)


@app.get("/watch/test")
def watch_test(symbol: str = Query("GNPX"), key: str = Query("")):
    """يقارن Yahoo وMassive لسهم: عدد شموع ما قبل الافتتاح/الجلسة/ما بعد الإغلاق لآخر يومين، وحالة Massive"""
    need = os.environ.get("WATCH_KEY")
    if need and key != need:
        return {"ok": False, "error": "key"}
    sym = symbol.strip().upper()

    def count(res):
        if not res:
            return None
        out = {}
        for t in res.get("timestamp") or []:
            ny = _ny_now(t); m = ny.hour * 60 + ny.minute
            seg = "قبل الافتتاح" if 240 <= m < 570 else "الجلسة" if m < 960 else "بعد الإغلاق" if m < 1200 else "خارج"
            k = f"{ny.date()} {seg}"
            out[k] = out.get(k, 0) + 1
        return out
    y, err = _chart(sym, "5d", "5m", True)
    _mv_state["blocked_until"] = 0
    mv = _massive_result(sym)
    return {"symbol": sym, "yahoo": count(y) if not err else err, "massive": count(mv),
            "massive_status": _mv_state["last_status"],
            "massive_key": bool(os.environ.get("MASSIVE_API_KEY")),
            "note": "massive_status: 200 يعمل · 403 غير مشمول في خطتك · 401 مفتاح خاطئ · 429 حد الطلبات"}


@app.get("/watch")
def watch(key: str = Query("", description="WATCH_KEY إن ضُبط")):
    """يُستدعى كل 5 دقائق. يرد فوراً ويعمل في الخلفية (مهلة cron-job.org قصيرة)."""
    need = os.environ.get("WATCH_KEY")
    if need and key != need:
        return {"ok": False, "error": "key"}
    ny = _ny_now()
    m = ny.hour * 60 + ny.minute
    if ny.weekday() >= 5 or not (240 <= m < 1200):
        return {"ok": True, "skipped": "خارج 4:00–20:00 نيويورك", "last": _watch_last}
    if _watch_last["running"]:
        return {"ok": True, "busy": True, "last": _watch_last}
    if time.time() < (_watch_last.get("cooldown_until") or 0):
        return {"ok": True, "cooldown": "تهدئة بعد رفض Yahoo (429)", "last": _watch_last}
    _watch_last["running"] = True
    threading.Thread(target=_watch_run, daemon=True).start()
    return {"ok": True, "started": True, "last": _watch_last}


# ===== تشغيل الخادم =====
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
