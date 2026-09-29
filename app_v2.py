import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
import threading
import io
import csv
import os
from datetime import datetime, timedelta
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="Stock Screener Pro", page_icon="🎯", layout="wide")

PROXY_URL   = os.environ.get("PROXY_URL", "https://faisal-proxy.onrender.com").rstrip("/")
FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
# FIX 9: حُذف ON_RENDER (غير مستخدم)

def _env(key, default=""):
    val = os.environ.get(key)
    if val: return val
    try:
        v = st.secrets.get(key)
        if v: return str(v)
    except Exception: pass
    return default

CORE_LIST_RAW = _env("CORE_LIST", "")

# القيم الافتراضية — تُعدَّل من الشريط الجانبي (تحسين 1)
MARKET_CAP_MAX = 20_000_000
FLOAT_MAX      = 5_000_000
PRICE_MIN      = 0.50
PRICE_MAX      = 5.0
VOLUME_MIN     = 50_000
SHORT_MIN      = 0.10
MIN_SCORE_LIVE = 40
JOURNAL_PATH   = _env("JOURNAL_PATH", "journal.json")
import json
MAX_WORKERS = 12
LADDER_MIN_CANDLES = 2
MA_TOUCH_PCT = 3.0

class RateLimiter:
    def __init__(self, max_calls=55, period=60):
        self.max_calls, self.period = max_calls, period
        self.calls, self.lock = deque(), threading.Lock()
    def wait(self):
        with self.lock:
            now = time.time()
            while self.calls and now - self.calls[0] > self.period:
                self.calls.popleft()
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0]) + 0.1
                if sleep_time > 0: time.sleep(sleep_time)
            self.calls.append(time.time())

finnhub_limiter = RateLimiter(max_calls=55, period=60)

st.markdown("""<style>
.main{direction:rtl} h1,h2,h3{direction:rtl;text-align:right}
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
.live-box{background:#e3f2fd;padding:15px;border-radius:10px;margin:10px 0;border:2px solid #2196f3}
.plan-table{width:100%;border-collapse:collapse;margin-top:10px}
.plan-table td{padding:8px;border-bottom:1px solid #d0e4f5;font-size:16px}
</style>""", unsafe_allow_html=True)

_c_store, _c_lock = {}, threading.Lock()
_f_store, _f_lock = {}, threading.Lock()

def _ttl(store, lock, key, ttl, producer):
    now = time.time()
    with lock:
        hit = store.get(key)
        if hit and now - hit[0] < ttl: return hit[1]
    val = producer()
    with lock: store[key] = (now, val)
    return val

@st.cache_data(ttl=300)
def stooq_candles(symbol, period="1y"):
    try:
        r = requests.get(f"https://stooq.com/q/d/l/?s={symbol.lower()}.us&i=d",
                         timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200 or len(r.text) < 50 or "No data" in r.text: return pd.DataFrame()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.capitalize() for c in df.columns]
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()
        days = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730}.get(period, 365)
        return df[df.index >= (datetime.now() - timedelta(days=days))]
    except Exception: return pd.DataFrame()

def _candles_uncached(symbol, period="6mo"):
    # 1. المحاولة الأولى: البروكسي (المصدر الموثوق)
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
                        if "time" in df.columns:   # صيغة faisal-proxy: ثوانٍ Unix
                            df["date"] = pd.to_datetime(df["time"], unit="s"); df = df.drop(columns=["time"])
                        else:
                            df["date"] = pd.to_datetime(df["date"])
                        df = df.set_index("date").sort_index()
                        df.index = df.index.normalize()
                        df = drop_incomplete_today(df)
                        df.columns = [c.capitalize() for c in df.columns]
                        return df, data.get("splits", []), "yahoo_proxy"
                else:
                    print(f"[APP] Proxy failed for {symbol}: HTTP {r.status_code}")
                if r.status_code in (429, 503): time.sleep(3)
            except Exception as e:
                print(f"[APP] Proxy exception for {symbol}: {e}")
                time.sleep(2)

    # 2. المحاولة الثانية: Stooq (كاحتياطي أخير فقط)
    df = stooq_candles(symbol, period)
    if not df.empty:
        return df, [], "stooq"

    print(f"[APP] All data sources failed for {symbol}")
    return pd.DataFrame(), [], "none"

def get_candles_unified(symbol, period="6mo"):
    return _ttl(_c_store, _c_lock, (symbol, period), 300, lambda: _candles_uncached(symbol, period))

def scan_parallel(symbols, period, progress_cb=None):
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
                if not hist.empty and len(hist) >= 30: out.append((s, hist, splits, src))
            except Exception: pass
    return out

@st.cache_data(ttl=60)
def get_realtime_price(symbol):
    if FINNHUB_KEY:
        finnhub_limiter.wait()
        try:
            r = requests.get("https://finnhub.io/api/v1/quote",
                             params={"symbol": symbol, "token": FINNHUB_KEY}, timeout=10)
            if r.status_code == 200:
                q = r.json()
                if q.get("c", 0) > 0:
                    return {"price": float(q["c"]), "percent_change": float(q.get("dp", 0)), "source": "finnhub"}
        except Exception: pass
    return None

# ===== Finnhub primitives للمسح الحي =====
def finnhub_get(path, params=None):
    if not FINNHUB_KEY: return None
    finnhub_limiter.wait()
    try:
        r = requests.get(f"https://finnhub.io/api/v1{path}",
                         params={**(params or {}), "token": FINNHUB_KEY}, timeout=20)
        if r.status_code == 200: return r.json()
    except Exception: pass
    return None

def finnhub_all_symbols():
    data = finnhub_get("/stock/symbol", {"exchange": "US"})
    if not data: return []
    ok_mic = {"XNAS", "XNYS", "ARCX", "BATS", "XASE"}
    out = []
    for s in data:
        if s.get("type") != "Common Stock": continue
        if s.get("mic") not in ok_mic: continue
        sym = s.get("symbol")
        if sym: out.append(sym)
    return out

@st.cache_data(ttl=86400)
def sec_all_symbols():
    """تحسين 2: كون بديل مجاني من SEC عند غياب Finnhub — رموز البورصات المنظمة فقط"""
    try:
        r = requests.get("https://www.sec.gov/files/company_tickers_exchange.json",
                         headers={"User-Agent": "FaisalBot contact@example.com"}, timeout=25)
        j = r.json()
        fields, data = j.get("fields", []), j.get("data", [])
        ti, ei = fields.index("ticker"), fields.index("exchange")
        out = []
        for row in data:
            t = (row[ti] or "").upper().strip()
            ex = (row[ei] or "").upper().strip()
            if not t or not ex or "OTC" in ex: continue
            if any(ch in t for ch in "-.^$= "): continue   # وحدات/ضمانات/ممتازة
            out.append(t)
        return list(dict.fromkeys(out))
    except Exception: return []

# ===== كون الأسهم المقسّمة من SEC (بحث نصي كامل، مجاني بلا مفتاح) =====
import re as _re
SEC_UA = _env("SEC_UA", "FaisalBot contact@example.com")
EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
SPLIT_QUERIES = [
    # (الاستعلام، النماذج، النوع، القوة) — "likely" = دليل تنفيذ (CUSIP جديد)، "mention" = مجرد ذكر
    ('"reverse stock split" CUSIP', "8-K,6-K", "done", "likely"),
    ('"reverse share split" CUSIP', "8-K,6-K", "done", "likely"),
    ('"share consolidation" CUSIP', "8-K,6-K", "done", "likely"),   # صيغة الشركات الأجنبية
    ('"reverse stock split"', "8-K,6-K", "done", "mention"),
    ('"share consolidation"', "8-K,6-K", "done", "mention"),
    ('"reverse stock split"', "DEF 14A,PRE 14A,DEF 14C,PRE 14C", "upcoming", None),
]
_STRENGTH = {"likely": 2, "mention": 1, None: 0}


SPLIT_MAX_DAYS = 89   # نافذة التقسيم: أقل من 90 يوماً


@st.cache_data(ttl=6 * 3600)
def sec_split_universe(days_done=SPLIT_MAX_DAYS, days_upcoming=90):
    """{ticker: {kind: done|upcoming, strength, form, date, filings}}
    done+likely = تقسيم منفذ مرجّح (CUSIP جديد أو بند 5.03) · done+mention = مجرد ذكر → يُعامل كنية
    upcoming = دعوة تصويت أو نية"""
    out = {}
    end = datetime.now()
    for q, forms, kind, strength in SPLIT_QUERIES:
        start = (end - timedelta(days=days_done if kind == "done" else days_upcoming)).strftime("%Y-%m-%d")
        for off in range(0, 1000, 100):
            try:
                r = requests.get(EFTS_URL, params={"q": q, "forms": forms, "dateRange": "custom", "startdt": start,
                                                   "enddt": end.strftime("%Y-%m-%d"), "from": off},
                                 headers={"User-Agent": SEC_UA, "Accept": "application/json"}, timeout=20)
                hits = r.json().get("hits", {}).get("hits", []) if r.status_code == 200 else []
            except Exception:
                hits = []
            if not hits:
                break
            for h in hits:
                src = h.get("_source", {})
                form = src.get("form") or ""
                date = src.get("file_date") or ""
                items = src.get("items") or []
                hit_strength = "likely" if (kind == "done" and "5.03" in [str(i) for i in items]) else strength
                for name in src.get("display_names", []):
                    m = _re.search(r"\(([A-Z0-9 ,.\-]+)\)\s*\(CIK", name)
                    if not m:
                        continue
                    for t in (x.strip() for x in m.group(1).split(",")):
                        if not _re.fullmatch(r"[A-Z]{1,5}", t):
                            continue
                        rec = out.get(t)
                        if not rec:
                            out[t] = {"kind": kind, "strength": hit_strength, "form": form, "date": date, "filings": 1}
                            continue
                        rec["filings"] += 1
                        better = _STRENGTH[hit_strength] > _STRENGTH[rec.get("strength")]
                        same_newer = hit_strength == rec.get("strength") and date > rec["date"]
                        if kind == "done" and (rec["kind"] != "done" or better or same_newer):
                            rec.update(kind="done", strength=hit_strength if (rec["kind"] != "done" or better) else rec["strength"],
                                       form=form, date=date)
                        elif kind == rec["kind"] == "upcoming" and date > rec["date"]:
                            rec.update(form=form, date=date)
            time.sleep(0.15)   # حد SEC: 10 طلبات/ثانية
    # مجرد الذكر دون دليل تنفيذ = نية/إشعار امتثال → ينتقل لقائمة القادمة
    for t, rec in out.items():
        if rec["kind"] == "done" and rec.get("strength") == "mention":
            rec["kind"], rec["intent"] = "upcoming", True
    return out


def market_universe():
    """Finnhub أولاً، ثم SEC كبديل مجاني"""
    if FINNHUB_KEY:
        syms = finnhub_all_symbols()
        if syms: return syms, "Finnhub"
    syms = sec_all_symbols()
    return syms, "SEC"

def finnhub_metrics_live(sym):
    m = finnhub_get("/stock/metric", {"symbol": sym, "metric": "all"})
    return (m or {}).get("metric", {})

def proxy_bulk_quotes(symbols):
    out = {}
    for i in range(0, len(symbols), 200):
        chunk = symbols[i:i+200]
        try:
            r = requests.get(PROXY_URL + "/yahoo/last",
                             params={"symbols": ",".join(chunk)}, timeout=120)
            if r.status_code == 200:
                for k, v in r.json().get("quotes", {}).items():
                    out[k] = {"close": v.get("price", 0), "volume": v.get("volume", 0)}
        except Exception: pass
        time.sleep(0.5)
    return out

def stooq_bulk(symbols):
    out = {}
    for i in range(0, len(symbols), 150):
        chunk = symbols[i:i+150]
        q = ",".join(s.lower() + ".us" for s in chunk)
        try:
            r = requests.get("https://stooq.com/q/l/",
                             params={"s": q, "f": "sd2t2ohlcv", "h": "1", "e": "csv"},
                             timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200: continue
            for row in csv.DictReader(io.StringIO(r.text)):
                sym = (row.get("Symbol") or "").upper().replace(".US", "")
                try:
                    c = float(row.get("Close") or 0); v = float(row.get("Volume") or 0)
                except Exception: continue
                if c > 0: out[sym] = {"close": c, "volume": v}
        except Exception: continue
        time.sleep(0.5)
    return out

# ===== تطبيع مقاييس Finnhub (دالة واحدة بدل التكرار) =====
def normalize_metrics(m):
    m = m or {}
    ff = m.get("freeFloat") or 0
    sp = m.get("shortPercentOfFloat") or 0
    mc = m.get("marketCapitalization") or 0
    return {
        "floatShares": ff * 1_000_000 if ff and ff < 1000 else ff,
        "shortPercentOfFloat": sp / 100 if sp > 1 else sp,
        "sharesShort": m.get("sharesShort") or 0,
        "marketCap": mc * 1_000_000 if mc and mc < 100000 else mc,
    }

# ===== الدوال الأساسية للتحليل =====
def _metrics_uncached(symbol):
    info = {"floatShares": 0, "shortPercentOfFloat": 0, "sharesShort": 0, "marketCap": 0}
    if not FINNHUB_KEY: return info
    finnhub_limiter.wait()
    try:
        r = requests.get("https://finnhub.io/api/v1/stock/metric",
                         params={"symbol": symbol, "metric": "all", "token": FINNHUB_KEY}, timeout=15)
        if r.status_code == 200:
            info = normalize_metrics(r.json().get("metric", {}))
    except Exception: pass
    return info

def finnhub_metrics(symbol):
    return _ttl(_f_store, _f_lock, symbol, 3600, lambda: _metrics_uncached(symbol))

@st.cache_data(ttl=86400)
def _sec_tickers_map():
    try:
        r = requests.get("https://www.sec.gov/files/company_tickers.json",
                         headers={"User-Agent": "FaisalBot contact@example.com"}, timeout=15)
        return {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in r.json().values()}
    except Exception: return {}

def check_offering(symbol):
    try:
        cik = _sec_tickers_map().get(symbol.upper())
        if not cik: return {"has_offering": False}
        time.sleep(0.15)
        r = requests.get("https://data.sec.gov/submissions/CIK" + cik + ".json",
                         headers={"User-Agent": "FaisalBot contact@example.com"}, timeout=10)
        if r.status_code != 200: return {"has_offering": False}
        recent = r.json().get("filings", {}).get("recent", {})
        cutoff = datetime.now() - timedelta(days=30)
        of = {"S-1", "S-3", "424B3", "424B5"}
        for form, date_str in zip(recent.get("form", []), recent.get("filingDate", [])):
            if form in of:
                try:
                    fdate = datetime.strptime(date_str, "%Y-%m-%d")
                    if fdate >= cutoff:
                        return {"has_offering": True, "form": form, "days": (datetime.now() - fdate).days}
                except Exception: continue
        return {"has_offering": False}
    except Exception: return {"has_offering": False}

POS_WORDS = ["approval", "approved", "contract", "award", "awarded", "partnership",
             "patent", "license", "agreement", "acquisition", "positive results",
             "phase 2", "phase 3", "regain compliance", "nasdaq compliance",
             "buyback", "insider buying", "exceeds expectations",
             "merger", "merger closed", "protocol", "strategic update",
             "regulation fd", "study initiation"]

@st.cache_data(ttl=1800)
def check_news(symbol):
    base = {"has_negative": False, "items": [], "status": "no_key",
            "critical_count": 0, "offering_count": 0, "high_count": 0,
            "positive_count": 0, "positive_items": []}
    if not FINNHUB_KEY: return base
    finnhub_limiter.wait()
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        r = requests.get("https://finnhub.io/api/v1/company-news",
                         params={"symbol": symbol, "from": week_ago, "to": today, "token": FINNHUB_KEY}, timeout=15)
        if r.status_code != 200: base["status"] = "error"; return base
        news = r.json()
        if not news: base["status"] = "no_news"; return base
        neg_critical = ["bankruptcy", "chapter 11", "chapter 7", "delisting", "delisted", "fraud",
                        "sec investigation", "sec probe", "halted", "trading halt", "going concern"]
        neg_high = ["lawsuit", "class action", "sued", "net loss", "layoffs", "layoff", "restructuring",
                    "downgrade", "downgraded", "price target cut", "misses", "missed estimates",
                    "revenue decline", "warns", "warning", "guidance cut"]
        neg_offering = ["public offering", "private placement", "registered direct", "dilution",
                        "shelf offering", "atm offering", "stock offering"]
        neg_medium = ["investigation", "probe", "subpoena", "recall", "delay", "rejected", "cancellation"]
        matches, pos_matches = [], []
        for item in news[:30]:
            head = item.get("headline") or ""
            text = (head + " " + (item.get("summary") or "")).lower()
            low_head = head.lower()
            if any(p in low_head for p in POS_WORDS): pos_matches.append(head[:120])
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
                try: date_str = datetime.fromtimestamp(item.get("datetime", 0)).strftime("%Y-%m-%d")
                except Exception: date_str = "?"
                matches.append({"headline": head[:120], "source": item.get("source", "?"),
                                "date": date_str, "level": level, "keyword": keyword, "url": item.get("url", "")})
        return {"has_negative": len(matches) > 0, "items": matches[:5], "status": "done",
                "critical_count": sum(1 for m in matches if m["level"] == "CRITICAL"),
                "offering_count": sum(1 for m in matches if m["level"] == "OFFERING"),
                "high_count": sum(1 for m in matches if m["level"] == "HIGH"),
                "positive_count": len(pos_matches), "positive_items": pos_matches[:3]}
    except Exception: return base

def atr(high, low, close, period=14):
    if len(close) < period + 1: return None
    prev = close.shift(1)
    tr = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    val = tr.rolling(period).mean().iloc[-1]
    return float(val) if pd.notna(val) else None

def rsi_series(close, period=14):
    d = close.diff()
    g = d.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    rs = g / l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

# FIX 5: RSI موحّد — طريقة Wilder نفسها في التقييم وفي كشف التراكم
def rsi(close, period=14):
    if len(close) < period + 1: return 50.0
    val = rsi_series(close, period).iloc[-1]
    return float(val) if pd.notna(val) else 50.0

def detect_rsi_build(hist):
    if len(hist) < 30: return None
    rs = rsi_series(hist["Close"]).dropna()
    if len(rs) < 15: return None
    last10 = rs.tail(10).values
    p10 = hist["Close"].tail(10).values
    rng = (p10.max() - p10.min()) / p10.min() * 100
    if rng > 12: return None
    rise = last10[-1] - last10[0]
    if rise < 8: return None
    if min(last10[-5:]) <= min(last10[-10:-5]): return None
    if last10[-1] > 72: return None
    return {"rsi_now": round(float(last10[-1]), 1), "rise": round(float(rise), 1)}

def macd(close):
    if len(close) < 26: return False, False, 0.0
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

# FIX 1: الدعم والمقاومة من الجلسات السابقة فقط (بدون اليوم)
# سابقاً كان قاع اليوم داخل الحساب فيستحيل أن يغلق السعر تحته => "الدعم مكسور" لا يتحقق أبداً
def sr(hist, w=20):
    if hist.empty or len(hist) < w + 1: return None, None
    prior = hist.iloc[:-1]
    return float(prior["Low"].tail(w).min()), float(prior["High"].tail(w).max())

def detect_trend_strength(hist):
    if len(hist) < 50: return "neutral"
    close = hist["Close"]
    s20 = close.rolling(20).mean().iloc[-1]
    s50 = close.rolling(50).mean().iloc[-1]
    cur = close.iloc[-1]
    if cur > s20 > s50: return "strong_uptrend"
    elif cur > s20: return "weak_uptrend"
    elif cur < s20 < s50: return "strong_downtrend"
    elif cur < s20: return "weak_downtrend"
    return "neutral"

def detect_distribution(hist):
    if len(hist) < 15: return False
    last = hist.tail(10)
    prev = hist.iloc[-30:-10] if len(hist) >= 30 else hist.iloc[:-10]
    if prev.empty: return False
    vol_prev = float(prev["Volume"].mean())
    vol_now = float(last["Volume"].mean())
    base = float(last["Close"].iloc[0])
    if base <= 0: return False
    move = abs(float(last["Close"].iloc[-1]) - base) / base * 100
    return vol_now > vol_prev * 1.5 and move < 2.0

def selling_volume_drying(hist):
    down = hist[hist["Close"] < hist["Open"]]
    if len(down) < 6: return False
    recent = float(down["Volume"].tail(3).mean())
    prior = float(down["Volume"].iloc[-6:-3].mean())
    return prior > 0 and recent < prior * 0.7

def geometric_wash_target(hist, splits=None):
    if not splits: return None
    sp = detect_reverse_split(splits, max_days=365)
    if not sp.get("has_split"): return None
    post_split = hist[hist.index >= pd.Timestamp(sp["date"])]
    if len(post_split) < 5: return None
    peak = float(post_split["High"].max())
    target = round(peak / 2, 3)
    days_since_split = sp["days_since"]
    if days_since_split < 30:
        return {"target": target, "reached": False,
                "status": f"⏳ انتظر {30 - days_since_split} يوم بعد التقسيم"}
    min_since = float(post_split["Low"].min())
    reached = min_since <= target
    if reached:
        first_below = post_split[post_split["Low"] <= target]
        if len(first_below) > 0:
            days_since_target = (datetime.now() - first_below.index[0]).days
            if days_since_target >= 30:
                return {"target": target, "reached": True,
                        "status": "✅ اكتمل الغسل + شهر — ابدأ المراقبة"}
            return {"target": target, "reached": True,
                    "status": f"⏳ اكتمل الغسل، انتظر {30 - days_since_target} يوم"}
        return {"target": target, "reached": True, "status": "وصل لهدف الغسل"}
    return {"target": target, "reached": False, "status": "لم يصل لهدف الغسل بعد"}

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

# FIX 4: Bull Trap حقيقي — مقاومة من ما قبل آخر 5 جلسات، ثم اختراق فعلي، ثم إغلاق دونها
# سابقاً: أي سهم قمته في آخر 5 أيام ونزل 3% كان يُقصى
def detect_bull_trap(hist):
    if len(hist) < 25: return False
    resistance = float(hist["High"].iloc[-25:-5].max())
    recent = hist.tail(5)
    broke_out = bool((recent["High"] > resistance * 1.01).any())
    if not broke_out: return False
    return float(hist["Close"].iloc[-1]) < resistance * 0.97

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

def spike_context(hist):
    fs = detect_failed_spike(hist)
    if not fs: return None
    recent = hist.tail(20)
    spike_pos = int(recent["High"].values.argmax())
    pre = hist.iloc[:len(hist) - 20 + spike_pos]
    if len(pre) < 10: pre = hist.iloc[:-20]
    if len(pre) < 10: return {"kind": "broken", "base_low": fs["peak"]}
    base_low = float(pre["Low"].tail(15).min())
    base_high = float(pre["High"].tail(15).max())
    current = float(hist["Close"].iloc[-1])
    if current < base_low:
        return {"kind": "broken", "base_low": round(base_low, 3)}
    inside = current <= base_high * 1.05
    held = bool(detect_stability(hist, base_low, 2)) or selling_volume_drying(hist)
    if inside and held:
        return {"kind": "return", "base_low": round(base_low, 3), "base_high": round(base_high, 3)}
    return {"kind": "broken", "base_low": round(base_low, 3)}

def ladder_summary(lad, n=3):
    if not lad: return ""
    parts = []
    for i, L in enumerate(lad["ladder"][:n], 1):
        parts.append(f"الدرجة {i}: مقاومة {L['tail']} → سقف {L['head']}")
    return " | ".join(parts)

def accumulation_zone(hist, lookback=40):
    if len(hist) < 20: return None
    win = hist.tail(lookback)
    lo, hi = float(win["Low"].min()), float(win["High"].max())
    if hi <= lo: return None
    bins, step = 10, (hi - lo) / 10
    vols = [0.0] * bins
    for _, c in win.iterrows():
        i0 = max(0, int((float(c["Low"]) - lo) / step))
        i1 = min(bins - 1, int((float(c["High"]) - lo) / step))
        for i in range(i0, i1 + 1):
            vols[i] += float(c["Volume"]) / (i1 - i0 + 1)
    best_i, best_v = 0, -1.0
    for i in range(bins - 2):
        v = vols[i] + vols[i+1] + vols[i+2]
        if v > best_v: best_v, best_i = v, i
    z_lo, z_hi = lo + best_i * step, lo + (best_i + 3) * step
    return {"low": round(z_lo, 3), "high": round(z_hi, 3), "avg": round((z_lo + z_hi) / 2, 3)}

def expected_sweep_zone(zone_avg):
    return round(zone_avg * 0.85, 3), round(zone_avg * 0.90, 3)

def wick_rebound_trigger(hist):
    if len(hist) < 5: return None
    for i in range(-5, 0):
        c = hist.iloc[i]
        o, cl, lo = float(c["Open"]), float(c["Close"]), float(c["Low"])
        body = abs(cl - o)
        lw = min(o, cl) - lo
        if lw > 0 and lw >= 2 * max(body, 1e-9):
            price = float(hist["Close"].iloc[-1])
            if price >= lo * 1.05:
                return {"wick_low": round(lo, 3),
                        "rebound_pct": round((price - lo) / lo * 100, 1),
                        "date": str(hist.index[i])[:10]}
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
        except Exception: continue
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
        except Exception: continue
    return patterns if patterns else None

def drop_incomplete_today(df):
    """قبل افتتاح السوق: شمعة اليوم (إن وُجدت) ناقصة — تُحذف حتى لا تشوّه الدعم والمؤشرات"""
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("America/New_York"))
        if len(df) and df.index[-1].date() == now.date() and (now.hour * 60 + now.minute) < 570:
            return df.iloc[:-1]
    except Exception:
        pass
    return df


def post_split_runup(hist, anchor_date, explode_runup=80.0, explode_day=50.0):
    """هل انفجر السهم منذ التقسيم؟
    runup = أقصى صعود من أدنى قاع سابق إلى قمة لاحقة (بعد التقسيم)، day = أكبر صعود يومي بالإغلاق.
    انفجر = runup ≥ 80% أو يوم واحد ≥ 50%."""
    try:
        post = hist[hist.index >= pd.Timestamp(anchor_date)]
    except Exception:
        return None
    if len(post) < 2:
        return {"sessions": len(post), "runup_pct": 0.0, "max_day_pct": 0.0, "exploded": False,
                "from_low_pct": 0.0}
    lows = post["Low"].cummin()
    runup = float(((post["High"] / lows) - 1).max() * 100)
    day = float((post["Close"].pct_change().max() or 0) * 100)
    low = float(post["Low"].min())
    return {"sessions": len(post), "runup_pct": round(runup, 1), "max_day_pct": round(day, 1),
            "from_low_pct": round((float(post["Close"].iloc[-1]) / low - 1) * 100, 1) if low > 0 else 0.0,
            "exploded": runup >= explode_runup or day >= explode_day}


def detect_reverse_split(splits, max_days=365):
    if not splits: return {"has_split": False, "days_since": 9999}
    cutoff = datetime.now() - timedelta(days=max_days)
    for sp in splits:
        try:
            sp_date = datetime.strptime(sp["date"], "%Y-%m-%d")
            if sp_date >= cutoff:
                num = int(sp.get("numerator", 1))
                den = int(sp.get("denominator", 1))
                return {"has_split": num < den, "date": sp["date"],
                        "ratio": f"{num}:{den}", "days_since": (datetime.now() - sp_date).days}
        except Exception: continue
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
    if sessions_held >= 3 and higher_lows: strength, color, points = "🔥 ثبات قوي", "#00b894", 10
    elif sessions_held >= 2 and higher_lows: strength, color, points = "✅ ثبات جيد", "#00b894", 7
    elif sessions_held >= 2: strength, color, points = "🟡 ثبات مقبول", "#fdcb6e", 5
    else: strength, color, points = "⚠️ ثبات ضعيف", "#fdcb6e", 0
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
            if recent["Close"].iloc[i + 1] > support: return True
    return False

PHASE_META = {
    "READY":   "🟢 جاهز (دورة)", "WATCH": "👁️ مراقبة", "RETEST": "🔄 اختبار",
    "PROOF":   "⏳ إثبات حياة", "ZONE": "🔍 منطقة", "WASHOUT": "🌪️ غسيل",
}

def post_split_phase(symbol, hist, splits, min_days=10, max_days=90):
    sp = detect_reverse_split(splits, max_days=max_days)
    if not sp.get("has_split"): return None
    D, ratio = sp["days_since"], sp["ratio"]
    try: den = int(ratio.split(":")[1])
    except Exception: den = 10
    if den >= 20: min_days = max(min_days, 20)
    if den >= 50: min_days = max(min_days, 30)
    post = hist[hist.index >= pd.Timestamp(sp["date"])]
    if len(post) < 10:
        return {"phase_key": "WASHOUT", "days": D, "ratio": ratio, "zone": None,
                "rebound": 0, "dist": None, "stability": False, "vol_dryup": False}
    price = float(hist["Close"].iloc[-1])
    early_vol = float(post["Volume"].head(10).mean())
    recent_vol = float(post["Volume"].tail(5).mean())
    vol_dryup = (recent_vol < early_vol * 0.50) if early_vol > 0 else False
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
    if D < min_days or not vol_dryup: key = "WASHOUT"
    elif not zone: key = "ZONE"
    elif not proof: key = "PROOF"
    elif not stability: key = "RETEST"
    else: key = "READY" if dist <= 15 else "WATCH"
    return {"phase_key": key, "days": D, "ratio": ratio, "zone": zone,
            "rebound": rebound, "dist": dist, "stability": stability, "vol_dryup": vol_dryup}

@st.cache_data(ttl=300)
def get_4h(symbol):
    if not PROXY_URL: return None
    try:
        r = requests.get(PROXY_URL + "/yahoo/candles",
                         params={"symbol": symbol, "period": "3mo", "interval": "1h"}, timeout=60)
        if r.status_code != 200: return None
        data = r.json()
        if not data.get("success") or not data.get("candles"): return None
        df = pd.DataFrame(data["candles"])
        if "time" in df.columns:   # صيغة faisal-proxy: ثوانٍ Unix
            df["date"] = pd.to_datetime(df["time"], unit="s"); df = df.drop(columns=["time"])
        else:
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
    except Exception: return None

def build_red_ladder(h4):
    if h4 is None or len(h4) < 10: return None
    reds = h4[h4["Close"] < h4["Open"]].tail(40)
    price = float(h4["Close"].iloc[-1])
    above = reds[reds["Low"] > price].sort_values("Low")
    if len(above) < LADDER_MIN_CANDLES: return None
    ladder = []
    for _, c in above.iterrows():
        tail, head = float(c["Low"]), float(c["High"])
        ladder.append({"tail": round(tail, 4), "head": round(head, 4),
                       "step_pct": round((head - tail) / tail * 100, 1)})
    broken = reds[reds["Low"] <= price].sort_values("Low").tail(3)
    supports = [round(float(c["Low"]), 4) for _, c in broken.iterrows()]
    return {"ladder": ladder, "supports": supports,
            "resistance": ladder[0]["tail"], "target": ladder[0]["head"]}

def ma_band_touch(hist, level, tol=MA_TOUCH_PCT):
    m1, m2 = sma(hist["Close"], 20), sma(hist["Close"], 30)
    if not m1 or not m2: return False
    lo, hi = min(m1, m2) * (1 - tol / 100), max(m1, m2) * (1 + tol / 100)
    return lo <= level <= hi

def score(symbol, hist, info, splits=None, news=None, offering=None):
    close, high, low, vol = hist["Close"], hist["High"], hist["Low"], hist["Volume"]
    price = float(close.iloc[-1])
    r = rsi(close)
    mp, mi, macd_hist = macd(close)
    macd_txt, macd_color = macd_status(mp, mi, macd_hist)
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
    elif 20 <= r < 23 or 27 < r <= 30: bd["RSI"] = 15
    elif 30 < r <= 35: bd["RSI"] = 10
    elif 35 < r <= 45: bd["RSI"] = 8
    elif 45 < r <= 57: bd["RSI"] = 5
    else: bd["RSI"] = 0

    rsi_build = detect_rsi_build(hist)
    bd["RSI_Build"] = 8 if rsi_build else 0

    # FIX 2: نافذة البحث 365 يوماً حتى تعمل درجات النقاط فعلاً (20 ثم 10)
    split_info = detect_reverse_split(splits, max_days=365)
    if split_info["has_split"]:
        d = split_info["days_since"]
        bd["Split"] = 20 if d <= 180 else 10
    else: bd["Split"] = 0

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

    # الآن "الدعم مكسور" = إغلاق اليوم تحت أدنى قاع لآخر 20 جلسة سابقة (بهامش 1%)
    support_broken = bool(sup and price < sup * 0.99)
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

    accum = selling_volume_drying(hist)
    bd["AccumVol"] = 5 if accum else 0
    bd["Catalyst"] = 10 if (news and news.get("positive_count", 0) > 0) else 0

    rebound = rebound_progress(hist, sup, res)
    if rebound is not None:
        if rebound < 30: bd["Rebound"] = 5
        elif rebound < 60: bd["Rebound"] = 0
        else: bd["Rebound"] = -10

    is_runner = detect_former_runner(hist)
    bd["Runner"] = 5 if is_runner else 0
    w_pat = detect_w_pattern(hist)
    bd["W_Pattern"] = 10 if w_pat else 0

    zone = accumulation_zone(hist)
    wick_rb = wick_rebound_trigger(hist)
    bd["WickRebound"] = 5 if wick_rb else 0

    total = max(0, min(sum(bd.values()), 100))
    if news:
        if news.get("offering_count", 0) > 0: total = max(0, total - 20)
        if news.get("high_count", 0) >= 2: total = max(0, total - 10)

    fs_ctx = spike_context(hist)
    failed_spike = bool(fs_ctx and fs_ctx["kind"] == "broken")
    spike_return = bool(fs_ctx and fs_ctx["kind"] == "return")
    distribution = detect_distribution(hist)
    bull_trap = detect_bull_trap(hist)
    hard_veto = support_broken or failed_spike or distribution or bull_trap
    if news and news.get("critical_count", 0) > 0: hard_veto = True
    if offering and offering.get("has_offering"): hard_veto = True

    if hard_veto: total, v, c = 0, "مرفوض (خطر)", "#d63031"
    elif total >= 80: v, c = "مثالي", "#00b894"
    elif total >= 65: v, c = "ممتاز", "#00b894"
    elif total >= 50: v, c = "جيد", "#fdcb6e"
    elif total >= 35: v, c = "ضعيف", "#e17055"
    else: v, c = "مرفوض", "#d63031"

    return {
        "symbol": symbol, "price": price, "rsi": round(r, 2),
        "macd_pos": mp, "macd_imp": mi, "macd_hist": round(macd_hist, 4),
        "macd_txt": macd_txt, "macd_color": macd_color, "sma20": s20, "sma50": s50,
        "support": sup, "resistance": res, "dist_sup": ds, "float": fs, "marketCap": mc,
        "rvol": rv, "short_pct": spct, "shares_short": info.get("sharesShort", 0) or 0,
        "breakdown": bd, "total": total, "verdict": v, "color": c,
        "rebound": rebound, "split_info": split_info, "stability": stability,
        "accum": accum, "distribution": distribution, "rsi_build": rsi_build,
        "spike_return": spike_return, "spike_ctx": fs_ctx,
        "zone": zone, "wick_rebound": wick_rb,
        "sweep": detect_liquidity_sweep(hist), "w_pattern": w_pat, "runner": is_runner,
        "bull_trap": bull_trap, "failed_spike": failed_spike,
        "gap": detect_gap_fill(hist), "candles": detect_candle_patterns(hist),
        "support_broken": support_broken, "hard_veto": hard_veto,
    }

def veto_reasons(r, news, offering):
    reasons = []
    if r.get("support_broken"): reasons.append("الدعم مكسور")
    if r.get("failed_spike"): reasons.append("Failed Spike (قاعدة مكسورة)")
    if r.get("distribution"): reasons.append("تصريف")
    if r.get("bull_trap"): reasons.append("Bull Trap")
    if news and news.get("critical_count", 0) > 0: reasons.append("أخبار حرجة")
    if offering and offering.get("has_offering"): reasons.append("طرح SEC")
    return reasons

def short_fuel(r):
    sp = r.get("short_pct") or 0
    sh = r.get("shares_short") or 0
    fl = r.get("float") or 0
    ratio = (sh / fl) if fl else 0
    eff = max(sp, ratio)
    if sp <= 0 and sh <= 0:
        return "missing", "❓ بيانات الشورت مفقودة — افحص IBorrowDesk يدوياً قبل أي قرار"
    if eff >= 0.30:
        return "packed", "🚀 وقود محشور ≥30% — Short Squeeze محتمل"
    if eff >= 0.10:
        return "present", "⛽ وقود موجود 10-30% — ارتكاز كلاسيكي"
    if eff < 0.05 and r.get("runner"):
        return "exhausted", "🧹 استنفاد الشورت — Former Runner بشورت منخفض = Post-Covering Rally"
    if eff < 0.10:
        return "none", "🎈 بلا وقود <10% — تضخم حر بلا غطاء: لا تُطارد، ولا ترتكز"
    return "present", "⛽ وقود متوسط"

def tradeability_veto(hist, splits=None):
    price = float(hist["Close"].iloc[-1])
    avg_vol = float(hist["Volume"].tail(20).mean())
    max_vol = float(hist["Volume"].tail(60).max()) if len(hist) >= 60 else float(hist["Volume"].max())
    if price < PRICE_MIN:
        return f"سعر تحت ${PRICE_MIN} ({price:.3f}) — خطر شطب وسبريد قاتل"
    if max_vol < 200_000:
        return "لا نشاط حيوي خلال 60 يوم — سهم زومبي"
    if avg_vol < VOLUME_MIN:
        return f"سيولة ميتة: متوسط {int(avg_vol):,} حتى مع نبضات"
    if price * avg_vol < 75_000:
        return f"قيمة التداول ${int(price * avg_vol):,} ضعيفة التنفيذ"
    if price < 1.0 and splits:
        sp = detect_reverse_split(splits, max_days=365)
        if sp.get("has_split"):
            return f"تحت $1 مع تقسيم عكسي حديث ({sp['ratio']}) — فخ استيفاء/تقسيم تسلسلي"
    return None

def manipulator_script(hist):
    if len(hist) < 40: return None
    base = hist.iloc[-20:-5]
    prev_vol = float(hist["Volume"].iloc[-40:-20].mean()) or 1.0
    base_dry = float(base["Volume"].mean()) < prev_vol * 1.2
    last_close = float(hist["Close"].iloc[-1])
    base_range = (float(base["High"].max()) - float(base["Low"].min())) / max(last_close, 0.01) < 0.25
    avg_prev = float(hist["Volume"].iloc[-30:-5].mean()) or 1.0
    probe_idx = [i for i in range(len(hist) - 5, len(hist)) if float(hist["Volume"].iloc[i]) > avg_prev * 3]
    base_low = float(base["Low"].min())
    sweep_idx = [i for i in range(len(hist) - 3, len(hist)) if float(hist["Low"].iloc[i]) <= base_low * 1.02]
    reclaim = last_close > base_low
    if base_dry and base_range and probe_idx and sweep_idx and reclaim:
        return {"base_low": round(base_low, 3),
                "probe_date": str(hist.index[probe_idx[0]])[:10],
                "sweep_date": str(hist.index[sweep_idx[0]])[:10]}
    return None

def technical_trigger(hist, r):
    triggers = []
    price = r["price"]
    wp = r.get("w_pattern")
    if wp and price > wp["neckline"] * 1.01:
        triggers.append(f"كسر عنق W عند {wp['neckline']}")
    if len(hist) >= 10:
        recent = hist.tail(10)
        for i in range(len(recent) - 1, -1, -1):
            c = recent.iloc[i]
            o = float(c["Open"])
            if o <= 0: continue
            drop = (float(c["Close"]) - o) / o * 100
            if drop < -5:
                head = float(c["High"])
                if price > head * 1.01:
                    triggers.append(f"اختراق رأس الشمعة الهابطة عند {round(head, 3)}")
                break
    if len(hist) >= 21:
        base_high = float(hist["High"].iloc[-21:-1].max())
        if base_high > 0 and price > base_high * 1.01 and (price - base_high) / base_high < 0.08:
            triggers.append(f"اختراق قمة القاعدة عند {round(base_high, 3)}")
    return triggers

def detect_families(hist, r):
    fam = []
    vol = hist["Volume"]
    price = r["price"]
    near_sup = r["dist_sup"] is not None and r["dist_sup"] <= 15
    n = len(hist)
    if 20 <= r["rsi"] <= 30: fam.append("ضغط RSI")
    if r.get("rsi_build"): fam.append("📈 تراكم RSI")
    if r.get("spike_return"): fam.append("🔁 عودة لقاعدة بعد سبايك")
    if r.get("wick_rebound"): fam.append("🕯️ ارتداد ذيل +5%")
    vol_dry = n >= 30 and float(vol.tail(30).mean()) >= 50_000 \
              and float(vol.tail(5).mean()) < float(vol.tail(30).mean()) * 0.5
    range_narrow = n >= 10 and (float(hist["High"].tail(10).max()) - float(hist["Low"].tail(10).min())) / price < 0.15
    if vol_dry and near_sup and (r["stability"] or range_narrow):
        fam.append("تجميع/قاعدة")
    if r["runner"] and near_sup: fam.append("عدّاء سابق")
    if r["sweep"]: fam.append("سحب سيولة")
    if r["w_pattern"]: fam.append("W")
    if r["gap"]: fam.append("تغطية فجوات")
    if r["split_info"].get("has_split"): fam.append("تقسيم عكسي")
    if n >= 30:
        avg_prev = float(vol.iloc[-30:-5].mean())
        probe = bool((vol.tail(5) > avg_prev * 3).any()) if avg_prev > 0 else False
        if probe and (range_narrow or r["stability"]): fam.append("جس نبض")
    if n < 70: fam.append("طرح جديد")
    if manipulator_script(hist): fam.append("سيناريو المضارب")
    if r["stability"] and r["stability"].get("higher_lows") \
       and r["stability"]["sessions_held"] >= 2 and near_sup:
        fam.append("درج ثبات")
    fuel_kind, _ = short_fuel(r)
    if fuel_kind == "packed": fam.append("🚀 وقود محشور (Squeeze)")
    elif fuel_kind == "exhausted": fam.append("🧹 استنفاد الشورت (Post-Covering)")
    elif fuel_kind == "present": fam.append("⛽ وقود متوسط")
    return fam

def _us_market_open():
    try:
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("America/New_York"))
        mins = now.hour * 60 + now.minute
        return now.weekday() < 5 and 570 <= mins < 960
    except Exception: return False

def detect_sweep_reclaim(h4, support):
    if h4 is None or len(h4) < 3 or not support: return None
    if _us_market_open() and len(h4) > 3: h4 = h4.iloc[:-1]
    last3 = h4.tail(3)
    for i in range(len(last3)):
        c = last3.iloc[i]
        if float(c["Low"]) < support * 0.99 and float(c["Close"]) > support:
            return {"sweep_low": round(float(c["Low"]), 3),
                    "reclaim": round(float(c["Close"]), 3),
                    "date": str(last3.index[i])[:16]}
    return None

def sweep_mode_candidate(h4, hist, support, dist_sup):
    if not support or dist_sup is None or dist_sup > 15: return False
    if h4 is not None and len(h4) >= 10:
        if float(h4["Low"].tail(10).min()) < support * 0.99: return True
    return bool(detect_liquidity_sweep(hist))

def load_journal():
    """تحسين 3: الدفتر يُحفظ على القرص ولا يضيع بإعادة التشغيل"""
    try:
        with open(JOURNAL_PATH, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return []

def save_journal(entries):
    try:
        with open(JOURNAL_PATH, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=1)
    except Exception as e:
        print(f"[APP] journal save failed: {e}")

def results_csv(rows, cols):
    df = pd.DataFrame([{c: (" | ".join(x[c]) if isinstance(x.get(c), list) else x.get(c)) for c in cols} for x in rows])
    return df.to_csv(index=False).encode("utf-8-sig")

def render_full_analysis(sym, hist, splits, info, news, offering, r, portfolio_size, risk_percent):
    news = news or {}
    st.markdown(f'<div class="score-card"><div class="score-big" style="color:{r["color"]}">{r["total"]}/100</div><div class="verdict">{r["verdict"]}</div></div>', unsafe_allow_html=True)

    if r.get("hard_veto"):
        st.markdown(f'<div class="danger-box">🚫 <b>إقصاء فوري:</b> {" | ".join(veto_reasons(r, news, offering))} — لا تُتداول هذه الحالة</div>', unsafe_allow_html=True)
        return

    dead = tradeability_veto(hist, splits)
    if dead:
        st.markdown(f'<div class="danger-box">🧟 <b>سهم غير قابل للتداول:</b> {dead}</div>', unsafe_allow_html=True)
        return

    live = get_realtime_price(sym)
    if live:
        chg = live.get("percent_change", 0)
        if abs(chg) >= 25:
            st.markdown(f'<div class="warn-box">⚡ <b>فجوة ≥25%:</b> حدث زخم جارٍ — مراقبة لا مطاردة (الدليل ص 5)</div>', unsafe_allow_html=True)
        st.info(f"🟢 السعر اللحظي: ${live['price']:.3f} ({chg:+.2f}%)")

    fam = detect_families(hist, r)
    if fam:
        st.markdown(f'<div class="info-box">🧬 <b>فصيلة ما قبل الانفجار:</b> {" | ".join(fam)}</div>', unsafe_allow_html=True)

    fuel_kind, fuel_txt = short_fuel(r)
    fuel_color = {"packed": "#d63031", "exhausted": "#00b894", "present": "#0984e3",
                  "none": "#fdcb6e", "missing": "#636e72"}.get(fuel_kind, "#636e72")
    st.markdown(f'<div style="background:{fuel_color}15;padding:15px;border-radius:10px;margin:10px 0;border:2px solid {fuel_color};font-size:16px"><b>🎯 وقود الشورت:</b> {fuel_txt}<br><small>Short Float: {round((r["short_pct"] or 0)*100, 2)}% | Shares Short: {int(r["shares_short"] or 0):,} | Float: {round((r["float"] or 0)/1e6, 2)}M</small></div>', unsafe_allow_html=True)

    script = manipulator_script(hist)
    if script:
        st.markdown(f'<div class="success-box">🎭 <b>سيناريو المضارب مكتمل الترتيب:</b> قاعدة جافة → جس نبض {script["probe_date"]} → سحب/اختبار {script["sweep_date"]} عند {script["base_low"]} → استرداد</div>', unsafe_allow_html=True)
    tech = technical_trigger(hist, r)
    if tech:
        st.markdown(f'<div class="success-box">🎯 <b>زناد فني تحقق:</b> {" | ".join(tech)} — الدخول بعد الثبات فوق المستوى المخترق، والوقف تحته</div>', unsafe_allow_html=True)
    if r.get("spike_return") and r.get("spike_ctx"):
        ctx = r["spike_ctx"]
        st.markdown(f'<div class="info-box">🔁 <b>سبايك ثم عودة للقاعدة:</b> الهبوط بعد السبايك اختبار دعم لا انهيار — خط الرمل: إغلاق يومي تحت {ctx["base_low"]}</div>', unsafe_allow_html=True)
    if r.get("zone"):
        sl, sh = expected_sweep_zone(r["zone"]["avg"])
        st.markdown(f'<div class="info-box">🎯 <b>منطقة تركيز المضارب:</b> {r["zone"]["low"]}–{r["zone"]["high"]} (متوسط {r["zone"]["avg"]})<br>🌀 <b>نطاق السحب المتوقع:</b> {sl}–{sh} (10–15% تحت المتوسط) — التحميل بعد السحب وليس قبل</div>', unsafe_allow_html=True)
    if r.get("wick_rebound"):
        wr = r["wick_rebound"]
        st.markdown(f'<div class="success-box">🕯️ <b>ارتداد الذيل:</b> ذيل طويل عند {wr["wick_low"]} والسعر فوقه +{wr["rebound_pct"]}% ({wr["date"]}) — إيجابي</div>', unsafe_allow_html=True)
    if r.get("rsi_build"):
        rb = r["rsi_build"]
        st.markdown(f'<div class="success-box">📈 <b>تراكم RSI (قوة خفية):</b> RSI {rb["rsi_now"]} صاعد +{rb["rise"]} نقاط خلال 10 جلسات والسعر ملتف — يد على الزناد</div>', unsafe_allow_html=True)
    gwt = geometric_wash_target(hist, splits)
    if gwt:
        st.markdown(f'<div class="warn-box">📐 <b>قاعدة MWC:</b> هدف الغسل ${gwt["target"]} — {gwt["status"]}</div>', unsafe_allow_html=True)
    if "درج ثبات" in fam:
        st.markdown(f'<div class="success-box">🪜 <b>درج ثبات:</b> جلستان متتاليتان بقيعان أعلى فوق الدعم</div>', unsafe_allow_html=True)
    if r.get("accum"):
        st.markdown('<div class="success-box">🤫 <b>تجميع هادئ:</b> فوليوم الهبوط يتناقص والسعر متماسك</div>', unsafe_allow_html=True)
    if news.get("positive_count", 0) > 0:
        st.markdown(f'<div class="success-box">📰 <b>محفز إيجابي:</b> {news["positive_count"]} خبر</div>', unsafe_allow_html=True)
    if news.get("items"):
        st.markdown("#### 📰 آخر الأخبار السلبية")
        for item in news["items"]:
            emoji, color = {"CRITICAL": ("🚨", "#d63031"), "OFFERING": ("💰", "#d63031"),
                            "HIGH": ("⚠️", "#e17055")}.get(item["level"], ("🟡", "#fdcb6e"))
            st.markdown(f'<div class="news-item" style="border-right-color:{color}">{emoji} <b>{item["headline"]}</b><br><small>{item["source"]} - {item["date"]}</small></div>', unsafe_allow_html=True)

    if r["stability"]:
        s = r["stability"]
        hl = " + قيعان أعلى ✅" if s["higher_lows"] else ""
        st.markdown(f'<div class="success-box" style="border-right-color:{s["color"]}">📊 <b>الثبات:</b> {s["strength"]} - {s["sessions_held"]} جلسات فوق الدعم{hl}</div>', unsafe_allow_html=True)
    elif r["support"]:
        st.markdown('<div class="warn-box">⚠️ <b>الثبات:</b> أقل من جلستين فوق الدعم - انتظر</div>', unsafe_allow_html=True)
    if r["bull_trap"]:
        st.markdown('<div class="danger-box">Bull Trap: كسر مقاومة ثم فشل — اشترِ الثبات لا الاختراق</div>', unsafe_allow_html=True)
    if r["split_info"].get("has_split"):
        d = r["split_info"]["days_since"]
        st.markdown(f'<div class="split-box">Reverse Split: {r["split_info"]["ratio"]} - قبل {d} يوم</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="info-box" style="border-right-color:{r["macd_color"]}"><b>MACD:</b> {r["macd_txt"]} (قيمة: {r["macd_hist"]})</div>', unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("السعر", f"${round(r['price'], 3)}")
    c2.metric("RSI", r['rsi'])
    c3.metric("RVOL", r['rvol'])
    c4.metric("Float", f"{round(r['float']/1000000, 2)}M" if r['float'] else "-")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("MA20", f"${round(r['sma20'], 2)}" if r['sma20'] else "-")
    c2.metric("MA50", f"${round(r['sma50'], 2)}" if r['sma50'] else "-")
    c3.metric("الدعم", f"${round(r['support'], 3)}" if r['support'] else "-")
    c4.metric("المقاومة", f"${round(r['resistance'], 3)}" if r['resistance'] else "-")

    if r["runner"]: st.markdown('<div class="success-box">Former Runner: سبق أن انفجر +50%</div>', unsafe_allow_html=True)
    if r["w_pattern"]:
        wp = r["w_pattern"]
        st.markdown(f'<div class="success-box">W: قاعان ${wp["bottom1"]} / ${wp["bottom2"]} - العنق ${wp["neckline"]}</div>', unsafe_allow_html=True)
    if r["sweep"]: st.markdown('<div class="success-box">سحب سيولة: كسر ثم استرداد</div>', unsafe_allow_html=True)
    if r["candles"]: st.markdown(f'<div class="success-box">شمعة إيجابية: {", ".join(r["candles"])}</div>', unsafe_allow_html=True)
    if r["gap"]:
        g = r["gap"]
        st.markdown(f'<div class="info-box">فجوة: {g["gap_pct"]}% عند ${g["gap_price"]}</div>', unsafe_allow_html=True)

    sweep = sweep_wait = None
    heads_x = []
    h4x = None
    if r["support"]:
        h4x = get_4h(sym)
        sweep = detect_sweep_reclaim(h4x, r["support"])
        sweep_wait = sweep is None and sweep_mode_candidate(h4x, hist, r["support"], r["dist_sup"])
        lad_x = build_red_ladder(h4x)
        heads_x = [L["head"] for L in lad_x["ladder"]] if lad_x else []

    if sweep:
        entry = sweep["reclaim"]
        stop = round(sweep["sweep_low"] * 0.97, 3)
        risk_ps = round(entry - stop, 3)
        cur = r["price"]
        atr_val0 = atr(hist["High"], hist["Low"], hist["Close"], 14)
        atr_pct0 = round(atr_val0 / cur * 100, 1) if atr_val0 else 0
        eff_risk0 = risk_percent / 2 if atr_pct0 > 8 else risk_percent
        max_risk0 = portfolio_size * (eff_risk0 / 100)
        shares0 = int(max_risk0 / risk_ps) if risk_ps > 0 else 0
        st1 = heads_x[0] if heads_x and heads_x[0] > entry else r["resistance"]
        st2 = heads_x[1] if len(heads_x) > 1 and heads_x[1] > st1 else round(st1 * 1.2, 3)
        st3 = heads_x[2] if len(heads_x) > 2 and heads_x[2] > st2 else round(st1 * 1.5, 3)
        rr0 = [round((t - entry) / risk_ps, 2) if risk_ps > 0 else 0 for t in (st1, st2, st3)]
        rew0 = [round((t - entry) / entry * 100, 2) if entry > 0 else 0 for t in (st1, st2, st3)]
        st.markdown("### 📊 خطة الدخول بعد السحب (الزناد تحقق)")
        st.markdown(f'<div class="success-box">🌀 <b>مسح سيولة ثم استرداد:</b> ذيل تحت {r["support"]} وإغلاق فوقه عند {entry} (شمعة {sweep["date"]})</div>', unsafe_allow_html=True)
        st.markdown(f"""
        <div class="plan-box"><table class="plan-table">
        <tr><td><b>⚡ الدخول سوقاً عند الاسترداد</b></td><td style="color:#00b894"><b>${entry}</b></td><td>{shares0} سهم</td></tr>
        <tr><td><b>🛡️ الوقف تحت قاع السحب</b></td><td style="color:#d63031"><b>${stop}</b></td><td>-{round(risk_ps/entry*100,1)}%</td></tr>
        <tr><td><b>🎯 هدف 1 (33%)</b></td><td style="color:#00b894"><b>${st1}</b> (+{rew0[0]}%)</td><td>R:R 1:{rr0[0]}</td></tr>
        <tr><td><b>🚀 هدف 2 (33%+Trailing)</b></td><td style="color:#0984e3"><b>${st2}</b> (+{rew0[1]}%)</td><td>R:R 1:{rr0[1]}</td></tr>
        <tr><td><b>🌟 هدف 3 (الباقي)</b></td><td style="color:#6c5ce7"><b>${st3}</b> (+{rew0[2]}%)</td><td>R:R 1:{rr0[2]}</td></tr>
        <tr><td><b>⚖️ المخاطرة القصوى</b></td><td style="color:#d63031">${round(max_risk0,2)}</td><td>{eff_risk0}%</td></tr>
        </table></div>""", unsafe_allow_html=True)
    elif sweep_wait:
        sup0 = r["support"]
        w1 = heads_x[0] if heads_x and heads_x[0] > r["price"] else r["resistance"]
        w2 = heads_x[1] if len(heads_x) > 1 and heads_x[1] > w1 else round(w1 * 1.2, 3)
        w3 = heads_x[2] if len(heads_x) > 2 and heads_x[2] > w2 else round(w1 * 1.5, 3)
        st.markdown("### 📊 خطة الدخول: وضع انتظار الزناد")
        st.markdown(f'<div class="warn-box">🌀 <b>نمط سحب السيولة:</b> لا تضع طلبات قبل السحب.<br>'
                    f'⏳ <b>الزناد:</b> شمعة 4H مغلقة ذيلها تحت <b>{round(sup0, 3)}</b> وإغلاقها فوقه.<br>'
                    f'📌 إن تحقق: دخول سوقاً عند الإغلاق المسترد، وقف تحت قاع السحب ×0.97، أهداف: {w1} → {w2} → {w3}.<br>'
                    f'🚫 إن أُغلق تحت {round(sup0*0.80,3)} بدون استرداد: السحب تحوّل انهياراً.</div>', unsafe_allow_html=True)

    if r["support"] and r["resistance"] and not sweep and not sweep_wait:
        sup_val, res_val, current_price = r["support"], r["resistance"], r["price"]
        atr_val = atr(hist["High"], hist["Low"], hist["Close"], 14)
        trend = detect_trend_strength(hist)
        fib1618 = round(res_val + (res_val - sup_val) * 0.618, 3)
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
        target1 = round(res_val, 3)
        target2 = round(max(t2_raw, target1 * 1.15), 3)
        target3 = round(max(fib1618, target2 * 1.5), 3)

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
            note = "⏳ فوق السلّم — ضع طلباتك في منطقة الطلب"; note_color = "#0984e3"
        elif current_price >= entry2:
            ladder = [
                {"t": "🥇 دخول فوري (40%)", "p": mkt,    "ord": "Market", "pct": 0.40},
                {"t": "🥈 تعزيز (35%)",     "p": entry2, "ord": "Limit",  "pct": 0.35},
                {"t": "🥉 دخول أخير (25%)", "p": entry3, "ord": "Limit",  "pct": 0.25},
            ]
            avg_entry = round(mkt*0.40 + entry2*0.35 + entry3*0.25, 3)
            note = "⚡ داخل السلّم — الأولى سوقاً والبقية معلقة"; note_color = "#00b894"
        elif current_price >= entry3:
            ladder = [
                {"t": "🥇 دخول فوري (40%)",  "p": mkt, "ord": "Market", "pct": 0.40},
                {"t": "🥈 تعزيز فوري (35%)", "p": mkt, "ord": "Market", "pct": 0.35},
                {"t": "🥉 دخول أخير (25%)",  "p": entry3, "ord": "Limit", "pct": 0.25},
            ]
            avg_entry = round(mkt*0.75 + entry3*0.25, 3)
            note = "⚡ عميق داخل السلّم — شريحتان سوقاً وواحدة معلقة"; note_color = "#00b894"
        else:
            ladder = [{"t": "🎯 دخول كامل (100%)", "p": mkt, "ord": "Market", "pct": 1.0}]
            avg_entry = mkt
            note = "🟢 عند منطقة الطلب — دخول كامل"; note_color = "#00b894"

        risk_ps = round(avg_entry - stop, 3)
        shares = int(max_risk / risk_ps) if risk_ps > 0 else 0
        for L in ladder: L["sh"] = int(shares * L["pct"])
        risk_pct = round(risk_ps / avg_entry * 100, 2) if avg_entry > 0 else 0
        rr = [round((t - avg_entry) / risk_ps, 2) if risk_ps > 0 else 0 for t in (target1, target2, target3)]
        rew_pct = [round((t - avg_entry) / avg_entry * 100, 2) if avg_entry > 0 else 0 for t in (target1, target2, target3)]
        pos_value = round(shares * avg_entry, 2)

        trend_map = {
            "strong_uptrend": ("✅ اتجاه صاعد قوي", "#00b894"),
            "weak_uptrend": ("🟢 اتجاه صاعد", "#00b894"),
            "neutral": ("🟡 اتجاه عرضي", "#fdcb6e"),
            "weak_downtrend": ("🟠 اتجاه هابط ضعيف", "#e17055"),
            "strong_downtrend": ("🔴 اتجاه هابط قوي", "#d63031"),
        }
        trend_txt, trend_color = trend_map[trend]

        st.markdown("### 📊 خطة التداول (نمط الثبات)")
        st.markdown(f'<div style="background:{note_color}22;padding:15px;border-radius:10px;margin:10px 0;border:2px solid {note_color};text-align:center;font-size:18px;font-weight:bold">{note}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="background:{trend_color}15;padding:10px;border-radius:8px;margin:5px 0;border-right:4px solid {trend_color}"><b>📈 الاتجاه:</b> {trend_txt}</div>', unsafe_allow_html=True)
        if vol_flag:
            st.markdown(f'<div class="warn-box">⚠️ تقلب {atr_pct}% — المخاطرة خُفّضت إلى {eff_risk}%. 📌 وقف القرار: إغلاق يومي تحت ${stop_struct} = خروج.</div>', unsafe_allow_html=True)

        rows_html = ""
        for L in ladder:
            color = "#00b894" if L["ord"] == "Market" else "#0984e3"
            rows_html += (f'<tr><td><b>{L["t"]}</b></td>'
                          f'<td style="color:{color}"><b>${L["p"]}</b> — {L["ord"]}</td>'
                          f'<td>{L["sh"]} سهم</td></tr>')
        st.markdown(f"""
        <div class="plan-box"><table class="plan-table">
        <tr><td colspan="3"><b>السعر:</b> ${mkt} | <b>المتوسط:</b> ${avg_entry}</td></tr>
        {rows_html}
        <tr><td><b>🛡️ الوقف الصلب</b></td><td style="color:#d63031"><b>${stop}</b> (-{risk_pct}%)</td><td>—</td></tr>
        <tr><td><b>📌 وقف القرار</b></td><td style="color:#e17055"><b>${stop_struct}</b></td><td>تحت الدعم</td></tr>
        <tr><td><b>🎯 هدف 1 (33%)</b></td><td style="color:#00b894"><b>${target1}</b> (+{rew_pct[0]}%)</td><td>R:R 1:{rr[0]}</td></tr>
        <tr><td><b>🚀 هدف 2 (33%+Trailing)</b></td><td style="color:#0984e3"><b>${target2}</b> (+{rew_pct[1]}%)</td><td>R:R 1:{rr[1]}</td></tr>
        <tr><td><b>🌟 هدف 3 (الباقي)</b></td><td style="color:#6c5ce7"><b>${target3}</b> (+{rew_pct[2]}%)</td><td>R:R 1:{rr[2]}</td></tr>
        <tr><td><b>💵 قيمة الصفقة</b></td><td>${pos_value:,}</td><td>{round(pos_value/portfolio_size*100, 1)}%</td></tr>
        <tr><td><b>⚖️ المخاطرة القصوى</b></td><td style="color:#d63031">${round(max_risk, 2)}</td><td>{eff_risk}%</td></tr>
        </table></div>""", unsafe_allow_html=True)

    st.markdown("### تفصيل النقاط")
    st.dataframe(pd.DataFrame(list(r["breakdown"].items()), columns=["المعيار", "النقاط"]),
                 use_container_width=True, hide_index=True)
    lad = build_red_ladder(h4x if h4x is not None else get_4h(sym))
    if lad:
        st.markdown(f'<div class="info-box">🕯️ <b>سلّم الشموع الساقطة (4H):</b> {ladder_summary(lad)}<br>🛡️ ذيول تحولت لدعم: {lad["supports"]}<br><small>الذيل = أول مقاومة تقابل الصعود، والرأس = سقف البائعين المحاصرين؛ السعر يصعد درجة درجة.</small></div>', unsafe_allow_html=True)

st.markdown("# 🎯 Stock Screener Pro — نظرية الارتكاز")

with st.sidebar:
    st.markdown("### ⚙️ إعدادات الخطة")
    portfolio_size = st.number_input("حجم المحفظة ($)", min_value=1000, value=10000, step=1000)
    risk_percent = st.slider("نسبة المخاطرة (%)", 0.5, 5.0, 2.0, 0.5)
    st.markdown("### 🔍 فلاتر المسح الحي")
    MARKET_CAP_MAX = st.number_input("القيمة السوقية القصوى ($M)", 1, 500, 20) * 1_000_000
    FLOAT_MAX      = st.number_input("العائمة القصوى (M سهم)", 1, 100, 5) * 1_000_000
    PRICE_MIN, PRICE_MAX = st.slider("نطاق السعر ($)", 0.1, 20.0, (0.5, 5.0), 0.1)
    VOLUME_MIN     = st.number_input("أدنى حجم يومي (K)", 10, 5000, 50) * 1000
    SHORT_MIN      = st.slider("أدنى نسبة شورت (%)", 0, 50, 10) / 100
    MIN_SCORE_LIVE = st.slider("أدنى نقاط للاكتشاف", 20, 80, 40, 5)
    st.markdown("### 🔌 المصادر")
    st.markdown(f"**Finnhub:** {'🟢' if FINNHUB_KEY else '🔴'}")
    st.markdown(f"**Proxy:** {'🟢' if PROXY_URL else '🔴'}")
    st.markdown(f"**CORE_LIST:** {'🟢 ' + str(len([t for t in CORE_LIST_RAW.replace(';', ',').split(',') if t.strip()])) + ' رمز' if CORE_LIST_RAW.strip() else '🔴 فارغة (اختياري)'}")

tab1, tab2, tab3 = st.tabs(["📈 تحليل سهم", "🛰️ الرادار الموحد", "🌊 مسح حي من السوق"])

if "journal" not in st.session_state:
    st.session_state["journal"] = load_journal()

with tab1:
    col1, col2 = st.columns([3, 1])
    with col1: sym = st.text_input("رمز السهم", "AEMD").upper()
    with col2:
        st.write(""); st.write("")
        btn = st.button("🔍 تحليل شامل", key="a")
    if btn and sym:
        with st.spinner("جاري تحليل " + sym):
            hist, splits, source = get_candles_unified(sym, "6mo")
            if hist.empty: st.error("❌ لا بيانات لـ " + sym)
            else:
                st.success(f"✅ المصدر: **{source}** ({len(hist)} شمعة)")
                if source == "stooq":
                    st.warning("⚠️ بيانات Stooq لا تتضمن التقسيمات العكسية — معايير التقسيم معطّلة لهذا السهم")
                st.session_state.setdefault("discovered", set()).add(sym)
                info = finnhub_metrics(sym)
                offering = check_offering(sym)
                news = check_news(sym)
                r = score(sym, hist, info, splits, news, offering=offering)
                render_full_analysis(sym, hist, splits, info, news, offering, r, portfolio_size, risk_percent)

    with st.expander("📓 دفتر المتابعة اليومي"):
        with st.form("journal_form"):
            j_sym = st.text_input("السهم").upper()
            j_type = st.selectbox("النوع", ["ارتكاز", "زخم", "Former Runner", "Gap Fill", "W", "سحب سيولة",
                                            "سيناريو المضارب", "درج ثبات", "🚀 وقود محشور", "🧹 استنفاد شورت",
                                            "📈 تراكم RSI", "🔁 عودة لقاعدة بعد سبايك", "🕯️ ارتداد ذيل", "طرح جديد"])
            j_fuel = st.selectbox("مقياس الوقود", ["وقود محشور", "وقود متوسط", "استنفاد شورت", "بلا وقود", "بيانات مفقودة", "غير مفحوص"])
            j_fee = st.text_input("رسوم الاقتراض السنوية (IBorrowDesk، إن توفرت)")
            j_levels = st.text_input("المستويات (دعم / طلب / مقاومة / هدف)")
            j_plan = st.text_input("الخطة (دخول / وقف / أهداف)")
            j_outcome = st.text_input("النتيجة والدرس")
            submitted = st.form_submit_button("💾 حفظ")
            if submitted and j_sym:
                st.session_state.setdefault("journal", []).append({
                    "التاريخ": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "السهم": j_sym, "النوع": j_type, "الوقود": j_fuel,
                    "رسوم الاقتراض": j_fee, "المستويات": j_levels,
                    "الخطة": j_plan, "النتيجة": j_outcome})
                save_journal(st.session_state["journal"])
                st.success(f"✅ حُفظ {j_sym} (على القرص: {JOURNAL_PATH})")
        if st.session_state.get("journal"):
            jdf = pd.DataFrame(st.session_state["journal"])
            st.dataframe(jdf, use_container_width=True, hide_index=True)
            st.download_button("⬇️ تصدير CSV", jdf.to_csv(index=False).encode("utf-8-sig"), file_name="faisal_journal.csv")

with tab2:
    st.markdown("### 🛰️ الرادار الموحد — نظرية الارتكاز (الشورت محوراً)")
    st.markdown(
        '<div class="filter-box"><b>🌐 الكون:</b> CORE_LIST الأساسية → ذاتية/دفتر → إضافية<br>'
        '<b>💡 للمسح الحي من السوق مباشرة:</b> استخدم تبويب <b>🌊 مسح حي من السوق</b><br>'
        '<b>🎯 وقود الشورت:</b> 🚀 محشور ≥30% | 🧹 استنفاد | ⛽ متوسط | 🎈 بلا وقود<br>'
        '<b>🎯 الزناد الفني:</b> كسر عنق W | اختراق رأس شمعة هابطة قوية | اختراق قمة القاعدة (كسر حديث ≤8%)<br>'
        '<b>📈 نافذة الإشعال:</b> RSI 45-57 + تراكم (قيعان أعلى + صعود ≥8 + سعر ملتف) = يد على الزناد<br>'
        '<b>🔁 السبايك:</b> سقط تحت قاعدته = جثة (إقصاء) | سقط داخل قاعدته وثبت = اختبار دعم (مراقبة)<br>'
        '<b>🎯 منطقة التركيز:</b> متوسط التجمع → نطاق السحب المتوقع 10-15% تحته — التحميل بعد السحب لا قبله<br>'
        '<b>الجاهزية =</b> وقود مقبول + معادلة مكتملة + نقاط ≥50 + (قرب الدعم <b>أو</b> زناد سحب <b>أو</b> زناد فني) + بلا فجوة ≥25%</div>',
        unsafe_allow_html=True)
    extra = st.text_input("➕ رموز إضافية تُدمج في هذا المسح (افصل بفاصلة): مثل SXTC, OFAL, INLF", "")

    if st.button("🛰️ امسح بالرادار الموحد", key="uni"):
        core = []
        for tok in CORE_LIST_RAW.replace(";", ",").split(","):
            s = tok.split(":")[0].strip().upper()
            if s: core.append(s)
        grown = list(st.session_state.get("discovered", set())) + \
                [e.get("السهم") for e in st.session_state.get("journal", [])]
        n_core, n_grown = len(core), len([s for s in grown if s])
        universe = list(dict.fromkeys(core + [s for s in grown if s]))
        if extra.strip():
            universe = list(dict.fromkeys([u.strip().upper() for u in extra.split(",") if u.strip()] + universe))
        n_extra = len([u for u in extra.split(",") if u.strip()]) if extra.strip() else 0
        st.info(f"🧬 تركيبة الكون: أساسية {n_core} + ذاتية/دفتر {n_grown} + إضافية {n_extra}")
        if not universe:
            st.error("🛑 **الكون فارغ.** إما اضبط CORE_LIST أو استخدم تبويب 🌊 مسح حي من السوق لاكتشاف الأسهم مباشرة من Finnhub.")
        else:
            pg = st.progress(0)
            raw = scan_parallel(universe, "6mo", progress_cb=lambda i, n: pg.progress(i / n))
            pg.empty()

            stage1, excluded = [], []
            for s, h, sps, src in raw:
                # FIX 6: لا نرمي بيانات Stooq — هي الاحتياطي؛ فقط لا تتوفر فيها تقسيمات
                try:
                    dead = tradeability_veto(h, sps)
                    if dead:
                        excluded.append({"symbol": s, "reason": dead}); continue
                    cheap = score(s, h, {}, sps)
                    if cheap["hard_veto"]:
                        excluded.append({"symbol": s, "reason": " | ".join(veto_reasons(cheap, None, None)) or "إقصاء"}); continue
                    phase = post_split_phase(s, h, sps)
                    fam = detect_families(h, cheap)
                    phase_ok = phase and phase["phase_key"] in ("READY", "WATCH", "RETEST", "PROOF")
                    if fam or phase_ok or cheap["rsi"] <= 35:
                        stage1.append((s, h, sps, phase, fam, src))
                except Exception: continue
            st.info(f"🔎 {len(stage1)} سهم حي يحمل بصمة فصيلة")

            results = []
            pg2 = st.progress(0)
            for i, (s, h, sps, phase, fam, src) in enumerate(stage1):
                pg2.progress((i + 1) / max(len(stage1), 1))
                try:
                    inf = finnhub_metrics(s)
                    news = check_news(s)
                    r = score(s, h, inf, sps, news)
                    if r["hard_veto"]:
                        excluded.append({"symbol": s, "reason": " | ".join(veto_reasons(r, news, None))}); continue
                    fam = detect_families(h, r)  # إعادة الحساب بعد توفر بيانات الشورت
                    fuel_kind, fuel_txt = short_fuel(r)
                    h4 = get_4h(s) if (r["dist_sup"] is not None and r["dist_sup"] <= 20) else None
                    lad = build_red_ladder(h4)
                    sweep = detect_sweep_reclaim(h4, r["support"])
                    sweep_wait = sweep is None and sweep_mode_candidate(h4, h, r["support"], r["dist_sup"])
                    tech = technical_trigger(h, r)
                    missing = []
                    sweep_ever = bool(sweep) or bool(r.get("sweep"))
                    if not sweep_ever and not r["stability"]:
                        missing.append("لا سحب تاريخي → إعادة الاختبار شرط أساسي (ثبات على الدعم)")
                    base_fam = ("تجميع/قاعدة" in fam) or ("سيناريو المضارب" in fam) or ("درج ثبات" in fam) \
                               or ("📈 تراكم RSI" in fam) or ("🔁 عودة لقاعدة بعد سبايك" in fam)
                    macd_flat = abs(r["macd_hist"]) / max(r["price"], 0.01) < 0.005
                    if not (20 <= r["rsi"] <= 30) and not base_fam:
                        missing.append(f"RSI {r['rsi']} خارج الضغط ولا قاعدة/درج/تراكم")
                    if not (r["macd_imp"] or (base_fam and macd_flat)):
                        missing.append("MACD لا يتحسن")
                    if not r["stability"] and not sweep and not tech and not base_fam:
                        missing.append("لا ثبات فوق الدعم")
                    near_support = r["dist_sup"] is not None and r["dist_sup"] <= 15
                    fuel_ok = fuel_kind in ("packed", "present", "exhausted", "missing")
                    guide_ready = fuel_ok and (not missing) and r["total"] >= 50 and (near_support or bool(sweep) or bool(tech))
                    tags = []
                    if src == "stooq": tags.append("📦 مصدر Stooq (بلا تقسيمات)")
                    if phase and phase["phase_key"] in ("READY", "WATCH", "RETEST"):
                        tags.append(f"تقسيم: {PHASE_META[phase['phase_key']]} {phase['ratio']}")
                    if lad: tags.append(f"سلّم 4H: {len(lad['ladder'])} شموع")
                    if r["accum"]: tags.append("تجميع هادئ")
                    if "سيناريو المضارب" in fam: tags.append("🎭 سيناريو المضارب")
                    if "درج ثبات" in fam: tags.append("🪜 درج ثبات")
                    if "📈 تراكم RSI" in fam: tags.append("📈 تراكم RSI")
                    if "🔁 عودة لقاعدة بعد سبايك" in fam: tags.append("🔁 عودة لقاعدة")
                    if "🕯️ ارتداد ذيل +5%" in fam: tags.append("🕯️ ارتداد ذيل")
                    if "🚀 وقود محشور (Squeeze)" in fam: tags.append("🚀 وقود محشور")
                    elif "🧹 استنفاد الشورت (Post-Covering)" in fam: tags.append("🧹 استنفاد الشورت")
                    elif "⛽ وقود متوسط" in fam: tags.append("⛽ وقود متوسط")
                    if fuel_kind == "none": tags.append("🎈 بلا وقود")
                    if tech: tags.append("🎯 زناد فني")
                    if sweep: tags.append("🌀 زناد السحب تحقق")
                    elif sweep_wait: tags.append("🌀 نمط سحب — انتظر الزناد")
                    if fuel_kind == "none":
                        missing.append("🎈 بلا وقود شورت — تضخم حر بلا غطاء")
                    if guide_ready or r["total"] >= 55:
                        off = check_offering(s)
                        if off.get("has_offering"):
                            excluded.append({"symbol": s, "reason": f"طرح SEC نشط ({off.get('form','?')})"}); continue
                    if guide_ready:
                        lq = get_realtime_price(s)
                        if lq and abs(lq.get("percent_change", 0)) >= 25:
                            guide_ready = False
                            tags.append("⚡ فجوة ≥25% — مراقبة لا مطاردة")
                    results.append({"symbol": s, "total": r["total"], "rsi": r["rsi"],
                                    "guide_ready": guide_ready, "missing": missing,
                                    "support": r["support"], "dist_sup": r["dist_sup"],
                                    "rvol": r["rvol"], "tags": tags, "fam": fam,
                                    "phase": phase, "ladder": lad, "sweep": sweep, "sweep_wait": sweep_wait,
                                    "tech": tech, "rsi_build": r.get("rsi_build"),
                                    "spike_return": r.get("spike_return"), "zone": r.get("zone"),
                                    "fuel_kind": fuel_kind, "fuel_txt": fuel_txt,
                                    "short_pct": r["short_pct"], "shares_short": r["shares_short"]})
                except Exception: continue
            pg2.empty()
            fuel_order = {"packed": 0, "exhausted": 1, "present": 2, "missing": 3, "none": 4}
            results.sort(key=lambda x: (not x["guide_ready"], fuel_order.get(x["fuel_kind"], 5), -x["total"]))
            st.session_state["uni_results"] = results
            st.session_state["uni_excluded"] = excluded
            st.success(f"✅ المسح اكتمل: {len(results)} مرشح | {len(excluded)} مقصيّ")

    if "uni_results" in st.session_state:
        results = st.session_state["uni_results"]
        excluded = st.session_state.get("uni_excluded", [])
        ready = [x for x in results if x["guide_ready"]]
        packed = [x for x in ready if x["fuel_kind"] == "packed"]
        exhausted = [x for x in ready if x["fuel_kind"] == "exhausted"]
        present = [x for x in ready if x["fuel_kind"] in ("present", "missing")]

        cols = st.columns(3)
        with cols[0]:
            st.markdown(f'<div style="background:#d6303115;border:1.5px solid #d63031;border-radius:12px;padding:12px;text-align:center"><div style="font-size:28px;font-weight:bold;color:#d63031">{len(packed)}</div><div>🚀 وقود محشور</div></div>', unsafe_allow_html=True)
        with cols[1]:
            st.markdown(f'<div style="background:#00b89415;border:1.5px solid #00b894;border-radius:12px;padding:12px;text-align:center"><div style="font-size:28px;font-weight:bold;color:#00b894">{len(exhausted)}</div><div>🧹 استنفاد شورت</div></div>', unsafe_allow_html=True)
        with cols[2]:
            st.markdown(f'<div style="background:#0984e315;border:1.5px solid #0984e3;border-radius:12px;padding:12px;text-align:center"><div style="font-size:28px;font-weight:bold;color:#0984e3">{len(present)}</div><div>⛽ وقود متوسط/مفقود</div></div>', unsafe_allow_html=True)

        if ready:
            names = ", ".join(x["symbol"] for x in ready[:6])
            st.markdown(f'<div style="background:#00b89422;border:2px solid #00b894;border-radius:12px;padding:12px;text-align:center;margin:8px 0;font-size:20px;font-weight:bold;color:#00b894">🟢 جاهز دخول كامل: {len(ready)} سهم — {names}</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="warn-box">⏳ لا سهم مكتمل المعادلة حالياً — الرادار يجهّز ولا يطارد</div>', unsafe_allow_html=True)

        for x in results[:20]:
            badge = "✅" if x["guide_ready"] else "⏳"
            fuel_emoji = {"packed": "🚀", "exhausted": "🧹", "present": "⛽", "none": "🎈", "missing": "❓"}.get(x["fuel_kind"], "")
            fam_txt = " | ".join(x["fam"][:2]) if x["fam"] else "—"
            sp_display = round((x["short_pct"] or 0) * 100, 2)
            with st.expander(f"{badge} {fuel_emoji} **{x['symbol']}** — {x['total']}/100 | 🧬 {fam_txt} | RSI {x['rsi']} | شورت {sp_display}%"):
                st.markdown(f'<div class="info-box">🎯 <b>{x["fuel_txt"]}</b></div>', unsafe_allow_html=True)
                if x.get("rsi_build"):
                    rb = x["rsi_build"]
                    st.markdown(f'<div class="success-box">📈 <b>تراكم RSI:</b> RSI {rb["rsi_now"]} صاعد +{rb["rise"]} خلال 10 جلسات والسعر ملتف — نافذة الإشعال</div>', unsafe_allow_html=True)
                if x.get("spike_return"):
                    st.markdown('<div class="info-box">🔁 <b>سبايك ثم عودة للقاعدة:</b> الهبوط اختبار دعم لا انهيار — خط الرمل قاع القاعدة</div>', unsafe_allow_html=True)
                if x.get("zone"):
                    sl, sh = expected_sweep_zone(x["zone"]["avg"])
                    st.markdown(f'<div class="info-box">🎯 <b>منطقة التركيز:</b> {x["zone"]["low"]}–{x["zone"]["high"]} (متوسط {x["zone"]["avg"]}) | 🌀 السحب المتوقع: {sl}–{sh}</div>', unsafe_allow_html=True)
                if x.get("tech"):
                    st.markdown(f'<div class="success-box">🎯 <b>زناد فني تحقق:</b> {" | ".join(x["tech"])} — الدخول بعد الثبات فوق المستوى، والوقف تحته</div>', unsafe_allow_html=True)
                if x["fam"]:
                    st.markdown(f'<div class="info-box">🧬 <b>فصيلة ما قبل الانفجار:</b> {" | ".join(x["fam"])}</div>', unsafe_allow_html=True)
                if x["tags"]:
                    st.markdown(f'<div class="success-box">🏷️ السلوك: {" | ".join(x["tags"])}</div>', unsafe_allow_html=True)
                if x["guide_ready"]:
                    st.markdown('<div class="success-box">✅ معادلة الدليل مكتملة — افتح تبويب التحليل للخطة الكاملة</div>', unsafe_allow_html=True)
                elif x["missing"]:
                    st.markdown(f'<div class="warn-box">⏳ ينقص: {" | ".join(x["missing"])}</div>', unsafe_allow_html=True)
                if x["ladder"]:
                    st.markdown(f'<div class="info-box">🕯️ <b>سلّم 4H:</b> {ladder_summary(x["ladder"])} | 🛡️ دعم الذيول: {x["ladder"]["supports"]}</div>', unsafe_allow_html=True)

        if results:
            st.download_button("⬇️ تصدير نتائج الرادار CSV",
                               results_csv(results, ["symbol", "total", "rsi", "guide_ready", "fuel_kind",
                                                     "support", "dist_sup", "rvol", "fam", "tags", "missing"]),
                               file_name=f"radar_{datetime.now():%Y%m%d}.csv", key="dl_radar")
        if excluded:
            with st.expander(f"🚫 المقصيون وأسبابهم ({len(excluded)})"):
                for e in excluded:
                    st.markdown(f"- **{e['symbol']}**: {e['reason']}")

# ===== TAB 3: المسح الحي من السوق =====
with tab3:
    st.markdown("### 🌊 مسح حي من السوق")
    src_mode = "Finnhub (كون كامل + عائمة/شورت/أخبار)" if FINNHUB_KEY else "SEC (كون مجاني — بلا عائمة/شورت/أخبار)"
    st.markdown(
        f'<div class="live-box"><b>🌍 المصدر:</b> {src_mode}<br>'
        f'<b>🔍 الفلترة:</b> كاب ≤${MARKET_CAP_MAX/1e6:.0f}M | سعر ${PRICE_MIN}-${PRICE_MAX} | حجم ≥{VOLUME_MIN/1000:.0f}K | عائمة ≤{FLOAT_MAX/1e6:.0f}M | شورت ≥{SHORT_MIN*100:.0f}% (أو مفقود) | نقاط ≥{MIN_SCORE_LIVE}<br>'
        '<b>⚡ الشموع تُجلب بالتوازي</b> (12 خيطاً) — الزمن يعتمد أساساً على metrics/news من Finnhub (55 طلب/دقيقة)<br>'
        '<b>💡 الميزة:</b> لا قوائم محفوظة — كل مسح يرى ما <b>في السوق الآن</b>؛ والاكتشافات تُضاف تلقائياً إلى كون الرادار</div>',
        unsafe_allow_html=True)

    if not FINNHUB_KEY:
        st.warning("🟡 بلا مفتاح Finnhub: سيعمل المسح على كون SEC وبيانات Stooq/البروكسي — العائمة والشورت ستكون مفقودة، ولن تُفحص الأخبار.")

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        max_candidates = st.slider("الحد الأقصى للمرشحين للفحص", 50, 1000, 300, 50,
                                    help="كلما زاد العدد زاد الوقت واكتُشفت فرص أكثر")
    with col2:
        skip_news = st.checkbox("تخطي الأخبار (أسرع)", value=not FINNHUB_KEY, disabled=not FINNHUB_KEY)
    with col3:
        st.write("")
        live_btn = st.button("🌊 ابدأ مسحاً حياً", key="live_scan", type="primary")
    uni_mode = st.radio("الكون", ["🔀 المقسّمة فقط (SEC)", "كل السوق"], horizontal=True, key="uni_mode")
    split_only = uni_mode.startswith("🔀")
    not_exploded_only = st.checkbox("لم ينفجر بعد التقسيم فقط (لا صعود ≥80% ولا يوم ≥50% منذ التقسيم)",
                                    value=True, disabled=not split_only, key="not_exploded")
    show_unconfirmed = st.checkbox("إظهار غير المؤكد (إفصاحات تذكر التقسيم دون دليل تنفيذ)",
                                   value=False, disabled=not split_only, key="show_unconf")

    if live_btn:
        split_map = {}
        if split_only:
            with st.spinner("بحث SEC عن التقسيمات العكسية (آخر 89 يوماً + القادمة)..."):
                split_map = sec_split_universe()
            st.session_state["split_map"] = split_map
            all_syms = [t for t, v in split_map.items()
                        if v["kind"] == "done" or (show_unconfirmed and v.get("intent"))]
            uni_src = "SEC (تقسيم عكسي منفذ)" + (" + غير المؤكد" if show_unconfirmed else "")
            if not split_map:
                st.error("❌ بحث SEC لم يُرجع نتائج — تحقق من SEC_UA أو الشبكة.")
        else:
            with st.spinner("جلب كون السوق..."):
                all_syms, uni_src = market_universe()
        if not all_syms:
            st.error("❌ فشل جلب الرموز من Finnhub وSEC معاً — تحقق من الشبكة.")
        else:
            st.info(f"📡 الكون: **{len(all_syms)}** رمزاً من {uni_src}")

            with st.spinner(f"اقتباسات مجمّعة لـ {len(all_syms)} رمز (سعر + حجم)..."):
                quotes = proxy_bulk_quotes(all_syms) if PROXY_URL else {}
                if not quotes:
                    if PROXY_URL: st.warning("⚠️ البروكسي لم يرد — نستخدم Stooq")
                    quotes = stooq_bulk(all_syms)
            st.info(f"💰 اقتباسات: {len(quotes)} رمز")

            pre_filtered = [sym for sym, q in quotes.items()
                            if PRICE_MIN <= q.get("close", 0) <= PRICE_MAX and q.get("volume", 0) >= VOLUME_MIN]
            # الأعلى حجماً أولاً حتى لا يقطع الحد الأقصى الأسهم الحيّة
            pre_filtered.sort(key=lambda x: -quotes[x].get("volume", 0))
            st.info(f"🔎 اجتاز السعر+الحجم: {len(pre_filtered)} رمز")

            if not pre_filtered:
                st.error("❌ لا رموز اجتازت التصفية الأولية — وسّع الفلاتر من الشريط الجانبي.")
            else:
                pre_filtered = pre_filtered[:max_candidates]
                excluded_live = []
                pg = st.progress(0)
                status = st.empty()

                # ---- المرحلة 1: metrics ----
                sym_with_metrics, n_short_missing = [], 0
                if FINNHUB_KEY:
                    status.info(f"📊 المرحلة 1/3: metrics لـ {len(pre_filtered)} رمز (~{len(pre_filtered)//55 + 1} دقيقة)...")
                    for i, sym in enumerate(pre_filtered):
                        pg.progress((i + 1) / len(pre_filtered) * 0.4)
                        m = finnhub_metrics_live(sym)
                        if not m:
                            if split_only:
                                sym_with_metrics.append((sym, {"floatShares": 0, "shortPercentOfFloat": 0, "sharesShort": 0, "marketCap": 0}))
                            continue
                        info_dict = normalize_metrics(m)
                        mc, fs = info_dict["marketCap"], info_dict["floatShares"]
                        sp, sh = info_dict["shortPercentOfFloat"], info_dict["sharesShort"]
                        if not split_only:   # بعد التقسيم قد تكون العائمة/الكاب في Finnhub قديمة — لا نُقصي بها
                            if not (1_000_000 <= mc <= MARKET_CAP_MAX): continue
                            if not (0 < fs <= FLOAT_MAX): continue
                        eff = max(sp, (sh / fs) if fs else 0)
                        if eff <= 0: n_short_missing += 1
                        elif eff < SHORT_MIN and not split_only: continue
                        sym_with_metrics.append((sym, info_dict))
                    st.info(f"✅ اجتاز metrics: {len(sym_with_metrics)} رمز (منها {n_short_missing} بشورت مفقود — افحصها يدوياً)")
                else:
                    empty = {"floatShares": 0, "shortPercentOfFloat": 0, "sharesShort": 0, "marketCap": 0}
                    sym_with_metrics = [(sym, dict(empty)) for sym in pre_filtered]
                    n_short_missing = len(sym_with_metrics)
                    pg.progress(0.4)

                # ---- المرحلة 2: شموع بالتوازي (تحسين 4) ----
                status.info(f"🕯️ المرحلة 2/3: شموع {len(sym_with_metrics)} رمز بالتوازي...")
                info_map = dict(sym_with_metrics)
                raw = scan_parallel([s for s, _ in sym_with_metrics], "6mo",
                                    progress_cb=lambda i, n: pg.progress(0.4 + i / max(n, 1) * 0.35))
                st.info(f"🕯️ شموع كافية (≥30): {len(raw)} رمز")

                # ---- المرحلة 3: أخبار + تقييم ----
                status.info(f"🧮 المرحلة 3/3: تقييم {len(raw)} رمز" + ("" if skip_news else " + أخبار") + "...")
                final = []
                crit = ["bankruptcy", "delisting", "delisted", "fraud", "trading halt", "chapter 11"]
                off_k = ["public offering", "private placement", "dilution", "shelf offering"]
                for i, (sym, hist, splits_raw, src) in enumerate(raw):
                    pg.progress(0.75 + (i + 1) / max(len(raw), 1) * 0.25)
                    try:
                        dead = tradeability_veto(hist, splits_raw)
                        if dead:
                            excluded_live.append({"symbol": sym, "reason": dead}); continue

                        psr, anchor_src, split_conf = None, None, None
                        if split_only:
                            rs = detect_reverse_split(splits_raw, max_days=SPLIT_MAX_DAYS)
                            ss = split_map.get(sym) or {}
                            if splits_raw and not rs.get("has_split"):
                                # البروكسي أرجع سجل تقسيمات ولا تقسيم عكسي خلال المدة → هو الحَكَم
                                excluded_live.append({"symbol": sym, "reason": f"البروكسي: لا تقسيم عكسي خلال {SPLIT_MAX_DAYS} يوماً"})
                                continue
                            if rs.get("has_split"):
                                split_conf = f"✅ مؤكد {rs['ratio']}"
                            elif ss.get("strength") == "likely":
                                split_conf = "🟡 مرجّح (SEC)"
                            else:
                                split_conf = "⚪ غير مؤكد"
                            anchor, anchor_src = (rs["date"], "البروكسي") if rs.get("has_split") else (ss.get("date"), "إفصاح SEC")
                            psr = post_split_runup(hist, anchor) if anchor else None
                            if psr is not None:
                                psr["anchor"], psr["anchor_src"] = anchor, anchor_src
                            try:
                                too_old = anchor and (datetime.now() - datetime.strptime(anchor[:10], "%Y-%m-%d")).days > SPLIT_MAX_DAYS
                            except Exception:
                                too_old = False
                            if too_old:
                                excluded_live.append({"symbol": sym, "reason": f"التقسيم أقدم من {SPLIT_MAX_DAYS} يوماً ({anchor})"})
                                continue
                            if not_exploded_only and psr and psr["exploded"]:
                                excluded_live.append({"symbol": sym, "reason": f"انفجر بعد التقسيم: أقصى صعود +{psr['runup_pct']}% · أكبر يوم +{psr['max_day_pct']}%"})
                                continue

                        if not skip_news and FINNHUB_KEY:
                            news_items = []
                            try:
                                today = datetime.now().strftime("%Y-%m-%d")
                                week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
                                nr = finnhub_get("/company-news", {"symbol": sym, "from": week_ago, "to": today})
                                if nr: news_items = nr
                            except Exception: pass
                            texts = [((it.get("headline") or "") + " " + (it.get("summary") or "")).lower() for it in news_items[:10]]
                            if any(any(k in t for k in crit) for t in texts):
                                excluded_live.append({"symbol": sym, "reason": "أخبار حرجة (إفلاس/شطب/إيقاف)"}); continue
                            if any(any(k in t for k in off_k) for t in texts):
                                excluded_live.append({"symbol": sym, "reason": "خبر طرح/تخفيف"}); continue

                        r = score(sym, hist, info_map.get(sym, {}), splits_raw, None, None)
                        if r["hard_veto"]:
                            excluded_live.append({"symbol": sym, "reason": " | ".join(veto_reasons(r, None, None)) or "إقصاء"}); continue
                        if r["total"] < MIN_SCORE_LIVE:
                            excluded_live.append({"symbol": sym, "reason": f"نقاط {r['total']} < {MIN_SCORE_LIVE}"}); continue

                        fuel_kind, fuel_txt = short_fuel(r)
                        fam = detect_families(hist, r)
                        final.append({
                            "symbol": sym, "total": r["total"], "price": r["price"],
                            "rsi": r["rsi"], "fuel_kind": fuel_kind, "fuel_txt": fuel_txt,
                            "short_pct": r["short_pct"], "float_m": round((r["float"] or 0)/1e6, 2),
                            "support": r["support"], "dist_sup": r["dist_sup"],
                            "fam": fam, "verdict": r["verdict"], "src": src,
                            "stability": bool(r["stability"]), "tech": technical_trigger(hist, r),
                            "sec_split": split_map.get(sym), "psr": psr, "split_conf": split_conf,
                        })
                    except Exception: continue

                pg.empty(); status.empty()
                final.sort(key=lambda x: (-{"packed": 3, "present": 2, "exhausted": 1, "missing": 1, "none": 0}.get(x["fuel_kind"], 0), -x["total"]))
                st.session_state["live_results"] = final
                st.session_state["live_excluded"] = excluded_live
                # تحسين 5: الاكتشافات تدخل كون الرادار تلقائياً
                st.session_state.setdefault("discovered", set()).update(x["symbol"] for x in final)
                st.success(f"🌊 المسح الحي اكتمل: **{len(final)}** اكتشاف | {len(excluded_live)} مقصيّ — أُضيفت الاكتشافات إلى كون الرادار", icon="🎯")

    if "live_results" in st.session_state:
        final = st.session_state["live_results"]
        excluded_live = st.session_state.get("live_excluded", [])

        if not final:
            st.warning("⏳ لا اكتشافات مكتملة المعادلة في هذا المسح — السوق هادئ.")
        else:
            packed = [x for x in final if x["fuel_kind"] == "packed"]
            exhausted = [x for x in final if x["fuel_kind"] == "exhausted"]
            present = [x for x in final if x["fuel_kind"] in ("present", "missing")]

            cols = st.columns(3)
            with cols[0]:
                st.markdown(f'<div style="background:#d6303115;border:1.5px solid #d63031;border-radius:12px;padding:12px;text-align:center"><div style="font-size:28px;font-weight:bold;color:#d63031">{len(packed)}</div><div>🚀 وقود محشور</div></div>', unsafe_allow_html=True)
            with cols[1]:
                st.markdown(f'<div style="background:#00b89415;border:1.5px solid #00b894;border-radius:12px;padding:12px;text-align:center"><div style="font-size:28px;font-weight:bold;color:#00b894">{len(exhausted)}</div><div>🧹 استنفاد شورت</div></div>', unsafe_allow_html=True)
            with cols[2]:
                st.markdown(f'<div style="background:#0984e315;border:1.5px solid #0984e3;border-radius:12px;padding:12px;text-align:center"><div style="font-size:28px;font-weight:bold;color:#0984e3">{len(present)}</div><div>⛽ وقود متوسط/مفقود</div></div>', unsafe_allow_html=True)

            st.markdown("### 📋 جدول الاكتشافات الحية")
            rows = []
            for x in final[:50]:
                fuel_emoji = {"packed": "🚀", "exhausted": "🧹", "present": "⛽", "none": "🎈", "missing": "❓"}.get(x["fuel_kind"], "")
                rows.append({
                    "الرمز": x["symbol"],
                    "النقاط": x["total"],
                    "السعر": f"${x['price']:.3f}",
                    "RSI": x["rsi"],
                    # FIX 3: تحويل الكسر إلى نسبة مئوية
                    "الوقود": fuel_emoji + " " + str(round((x["short_pct"] or 0) * 100, 1)) + "%",
                    "العائمة": f"{x['float_m']}M" if x['float_m'] else "؟",
                    "بُعد الدعم": f"{x['dist_sup']:+.1f}%" if x.get("dist_sup") is not None else "—",
                    "ثبات": "✅" if x.get("stability") else "—",
                    "زناد": "🎯" if x.get("tech") else "—",
                    "التقسيم": x.get("split_conf") or "—",
                    "منذ التقسيم": (f"{x['psr']['sessions']} جلسة · أقصى +{x['psr']['runup_pct']}%" if x.get("psr") else "—"),
                    "الفصيلة": " | ".join(x["fam"][:2]) if x["fam"] else "—",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, height=500)
            st.download_button("⬇️ تصدير الاكتشافات CSV",
                               results_csv(final, ["symbol", "total", "verdict", "price", "rsi", "fuel_kind",
                                                   "short_pct", "float_m", "support", "dist_sup", "stability", "tech", "fam", "src"]),
                               file_name=f"live_scan_{datetime.now():%Y%m%d}.csv", key="dl_live")

            st.markdown("### 🔬 تفاصيل الاكتشافات")
            for x in final[:20]:
                fuel_emoji = {"packed": "🚀", "exhausted": "🧹", "present": "⛽", "none": "🎈", "missing": "❓"}.get(x["fuel_kind"], "")
                sp_pct = round((x["short_pct"] or 0) * 100, 1)
                with st.expander(f"{fuel_emoji} **{x['symbol']}** — {x['total']}/100 ({x['verdict']}) | ${x['price']:.3f} | RSI {x['rsi']} | شورت {sp_pct}%"):
                    st.markdown(f'<div class="info-box">🎯 <b>{x["fuel_txt"]}</b></div>', unsafe_allow_html=True)
                    if x["fam"]:
                        st.markdown(f'<div class="info-box">🧬 <b>فصيلة ما قبل الانفجار:</b> {" | ".join(x["fam"])}</div>', unsafe_allow_html=True)
                    if x["support"]:
                        ds = x["dist_sup"]
                        st.markdown(f"📍 دعم: ${x['support']:.3f} ({ds:+.1f}%)")
                    if x.get("sec_split"):
                        ss = x["sec_split"]
                        st.markdown(f'<div class="split-box">🔀 <b>تقسيم عكسي (SEC):</b> آخر إفصاح {ss["form"]} بتاريخ {ss["date"]} · عدد الإيداعات المرتبطة {ss["filings"]}</div>', unsafe_allow_html=True)
                    if x.get("psr"):
                        ps = x["psr"]
                        st.markdown(f'<div class="info-box">⏳ <b>منذ التقسيم</b> ({ps.get("anchor")}، المرجع: {ps.get("anchor_src")}): '
                                    f'{ps["sessions"]} جلسة · أقصى صعود +{ps["runup_pct"]}% · أكبر يوم +{ps["max_day_pct"]}% · '
                                    f'السعر فوق أدنى قاع +{ps["from_low_pct"]}% — <b>لم ينفجر بعد</b></div>' if not ps["exploded"] else
                                    f'<div class="warn-box">⚠️ <b>انفجر سابقاً بعد التقسيم:</b> أقصى صعود +{ps["runup_pct"]}%</div>',
                                    unsafe_allow_html=True)
                    st.markdown(f'<div class="success-box">💡 للتحليل الكامل: افتح تبويب <b>📈 تحليل سهم</b> واكتب <code>{x["symbol"]}</code></div>', unsafe_allow_html=True)

            upc = {t: v for t, v in st.session_state.get("split_map", {}).items() if v["kind"] == "upcoming"}
            if upc:
                with st.expander(f"🗳️ تقسيمات قادمة أو محتملة ({len(upc)})"):
                    st.caption("شركات تطلب موافقة المساهمين على تقسيم عكسي. لا تحليل الآن — راقبها لتبدأ دورة ما بعد التقسيم من يومها الأول.")
                    st.dataframe(pd.DataFrame([{"الرمز": t, "النوع": "نية/إشعار" if v.get("intent") else "دعوة تصويت",
                                                "النموذج": v["form"], "التاريخ": v["date"]}
                                               for t, v in sorted(upc.items(), key=lambda kv: kv[1]["date"], reverse=True)]),
                                 use_container_width=True, hide_index=True)
            if excluded_live:
                with st.expander(f"🚫 المقصيون ({len(excluded_live)})"):
                    for e in excluded_live[:50]:
                        st.markdown(f"- **{e['symbol']}**: {e['reason']}")

st.markdown("---")
st.caption("⚠️ تعليمي فقط - ليس توصية استثمارية | نظرية الارتكاز: وايكوف + إليوت + كلاسيكي + الشورت محوراً")
