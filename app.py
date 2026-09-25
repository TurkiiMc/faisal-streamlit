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

st.set_page_config(page_title="Stock Screener Pro", page_icon="🎯", layout="wide")

PROXY_URL   = os.environ.get("PROXY_URL", "https://faisal-proxy.onrender.com").rstrip("/")
FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
ON_RENDER   = bool(os.environ.get("RENDER_SERVICE_NAME"))

MARKET_CAP_MAX = 20_000_000
FLOAT_MAX      = 5_000_000
PRICE_MAX      = 5.0
VOLUME_MIN     = 50_000
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
.plan-table{width:100%;border-collapse:collapse;margin-top:10px}
.plan-table td{padding:8px;border-bottom:1px solid #d0e4f5;font-size:16px}
</style>""", unsafe_allow_html=True)

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
                if r.status_code in (429, 503): time.sleep(3)
            except Exception: time.sleep(2)
    if not ON_RENDER:
        try:
            import yfinance as yf
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period)
            if not df.empty:
                df.columns = [c.capitalize() for c in df.columns]
                splits_list = []
                for date, ratio in ticker.splits.items():
                    num, den = (1, int(round(1/ratio))) if (ratio and ratio < 1) else (int(ratio), 1)
                    splits_list.append({"date": date.strftime("%Y-%m-%d"), "numerator": num, "denominator": den})
                return df, splits_list, "yfinance"
        except Exception: pass
    df = stooq_candles(symbol, period)
    if not df.empty: return df, [], "stooq"
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

def get_dynamic_universe(limit=500):
    cache_key = f"universe_{limit}"
    if cache_key not in st.session_state: st.session_state[cache_key] = {"data": None, "ts": 0}
    now = datetime.now().timestamp()
    cache = st.session_state[cache_key]
    if cache["data"] and (now - cache["ts"]) < 3600: return cache["data"]
    try:
        from tradingview_screener import Query, col
        q = (Query()
             .select('name', 'close', 'volume', 'market_cap_basic', 'float_shares')
             .where(col('exchange') == 'NASDAQ', col('market_cap_basic') <= MARKET_CAP_MAX,
                    col('float_shares') < FLOAT_MAX, col('close') < PRICE_MAX,
                    col('volume') > VOLUME_MIN, col('type') == 'stock')
             .order_by('market_cap_basic', ascending=True).limit(limit))
        out = q.get_scanner_data()
        df = None
        if isinstance(out, pd.DataFrame):
            df = out
        elif isinstance(out, tuple):
            for item in out:
                if isinstance(item, pd.DataFrame):
                    df = item
                    break
        if df is None or df.empty:
            st.session_state["universe_error"] = "استجابة TradingView بلا جدول — حظر IP أو تغيير واجهة"
        if df is not None and not df.empty:
            tickers = df['ticker'].tolist() if 'ticker' in df.columns else df['name'].tolist()
            tickers = [t.split(':')[-1] if ':' in t else t for t in tickers]
            tickers = list(dict.fromkeys(tickers))
            st.session_state[cache_key] = {"data": tickers, "ts": now}
            st.session_state["universe_source"] = "TradingView"
            return tickers
    except Exception as e:
        st.session_state["universe_error"] = f"{type(e).__name__}: {e}"
    st.session_state[cache_key] = {"data": FALLBACK_UNIVERSE, "ts": now}
    st.session_state["universe_source"] = "قائمة احتياطية قديمة"
    return FALLBACK_UNIVERSE

def _metrics_uncached(symbol):
    info = {"floatShares": 0, "shortPercentOfFloat": 0, "sharesShort": 0, "marketCap": 0}
    if not FINNHUB_KEY: return info
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

def rsi(close, period=14):
    if len(close) < period + 1: return 50.0
    d = close.diff()
    g = d.clip(lower=0).rolling(period).mean()
    l = (-d.clip(upper=0)).rolling(period).mean()
    rs = g / l.replace(0, np.nan)
    val = (100 - (100 / (1 + rs))).iloc[-1]
    return float(val) if pd.notna(val) else 50.0

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

def sr(hist, w=20):
    if hist.empty or len(hist) < w: return None, None
    return float(hist["Low"].rolling(w).min().iloc[-1]), float(hist["High"].rolling(w).max().iloc[-1])

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

def geometric_wash_target(hist):
    if not (30 <= len(hist) <= 260): return None
    first_low = float(hist["Low"].head(20).min())
    current = float(hist["Close"].iloc[-1])
    if first_low > 0 and current < first_low * 0.999: return round(first_low * 0.5, 3)
    return None

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

def count_reverse_splits(splits, months=36):
    if not splits: return 0
    cutoff = datetime.now() - timedelta(days=months * 30)
    cnt = 0
    for sp in splits:
        try:
            d = datetime.strptime(sp["date"], "%Y-%m-%d")
            if d >= cutoff and int(sp.get("numerator", 1)) < int(sp.get("denominator", 1)): cnt += 1
        except Exception: continue
    return cnt

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
    elif 20 <= r < 23: bd["RSI"] = 12
    elif 27 < r <= 30: bd["RSI"] = 15
    elif 30 < r <= 35: bd["RSI"] = 6
    else: bd["RSI"] = 0

    split_info = detect_reverse_split(splits, max_days=90)
    if split_info["has_split"]:
        d = split_info["days_since"]
        bd["Split"] = 20 if d <= 180 else (10 if d <= 365 else 0)
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

    total = max(0, min(sum(bd.values()), 100))
    if news:
        if news.get("offering_count", 0) > 0: total = max(0, total - 20)
        if news.get("high_count", 0) >= 2: total = max(0, total - 10)

    failed_spike = detect_failed_spike(hist)
    distribution = detect_distribution(hist)
    hard_veto = support_broken or bool(failed_spike) or distribution
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
        "accum": accum, "distribution": distribution,
        "sweep": detect_liquidity_sweep(hist), "w_pattern": w_pat, "runner": is_runner,
        "bull_trapering": detect_bull_trap(hist), "failed_spike": failed_spike,
        "gap": detect_gap_fill(hist), "candles": detect_candle_patterns(hist),
        "support_broken": support_broken, "hard_veto": hard_veto,
    }

def veto_reasons(r, news, offering):
    reasons = []
    if r.get("support_broken"): reasons.append("الدعم مكسور")
    if r.get("failed_spike"): reasons.append("Failed Spike")
    if r.get("distribution"): reasons.append("تصريف")
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
    if price < 0.50:
        return f"سعر تحت $0.50 ({price:.3f}) — خطر شطب وسبريد قاتل"
    if max_vol < 200_000:
        return "لا نشاط حيوي خلال 60 يوم — سهم زومبي"
    if avg_vol < 50_000:
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

def render_full_analysis(sym, hist, splits, info, news, offering, r):
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
    if "درج ثبات" in fam:
        st.markdown(f'<div class="success-box">🪜 <b>درج ثبات:</b> جلستان متتاليتان بقيعان أعلى فوق الدعم</div>', unsafe_allow_html=True)
    if r.get("accum"):
        st.markdown('<div class="success-box">🤫 <b>تجميع هادئ:</b> فوليوم الهبوط يتناقص والسعر متماسك</div>', unsafe_allow_html=True)
    if news and news.get("positive_count", 0) > 0:
        st.markdown(f'<div class="success-box">📰 <b>محفز إيجابي:</b> {news["positive_count"]} خبر</div>', unsafe_allow_html=True)
    gwt = geometric_wash_target(hist)
    if gwt:
        st.markdown(f'<div class="warn-box">📐 <b>قاعدة MWC:</b> القاع الأول مكسور → هدف الغسل ${gwt}</div>', unsafe_allow_html=True)
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
    if r["bull_trapering"]:
        st.markdown('<div class="danger-box">Bull Trap: كسر مقاومة ثم فشل</div>', unsafe_allow_html=True)
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
        trail_dist = round(atr_val * 1.5, 3) if atr_val else round(avg_entry * 0.05, 3)

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

st.markdown("# 🎯 Stock Screener Pro — نظرية الارتكاز")

with st.sidebar:
    st.markdown("### ⚙️ إعدادات الخطة")
    portfolio_size = st.number_input("حجم المحفظة ($)", min_value=1000, value=10000, step=1000)
    risk_percent = st.slider("نسبة المخاطرة (%)", 0.5, 5.0, 2.0, 0.5)
    st.markdown("### 🔌 المصادر")
    st.markdown(f"**Finnhub:** {'🟢' if FINNHUB_KEY else '🔴'}")
    st.markdown(f"**Proxy:** {'🟢' if PROXY_URL else '🔴'}")

tab1, tab2 = st.tabs(["📈 تحليل سهم", "🛰️ الرادار الموحد"])

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
                info = finnhub_metrics(sym)
                offering = check_offering(sym)
                news = check_news(sym)
                r = score(sym, hist, info, splits, news, offering=offering)
                render_full_analysis(sym, hist, splits, info, news, offering, r)
                h4 = get_4h(sym)
                lad = build_red_ladder(h4)
                if lad:
                    st.markdown("#### 🕯️ سلّم الشموع الساقطة (4H)")
                    st.dataframe(pd.DataFrame(lad["ladder"]), use_container_width=True, hide_index=True)
                    if lad["supports"]:
                        st.markdown(f'<div class="success-box">🛡️ ذيول تحولت لدعم: {lad["supports"]}</div>', unsafe_allow_html=True)

    with st.expander("📓 دفتر المتابعة اليومي"):
        with st.form("journal_form"):
            j_sym = st.text_input("السهم").upper()
            j_type = st.selectbox("النوع", ["ارتكاز", "زخم", "Former Runner", "Gap Fill", "W", "سحب سيولة",
                                            "سيناريو المضارب", "درج ثبات", "🚀 وقود محشور", "🧹 استنفاد شورت", "طرح جديد"])
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
                st.success(f"✅ حُفظ {j_sym}")
        if st.session_state.get("journal"):
            jdf = pd.DataFrame(st.session_state["journal"])
            st.dataframe(jdf, use_container_width=True, hide_index=True)
            st.download_button("⬇️ تصدير CSV", jdf.to_csv(index=False).encode("utf-8-sig"), file_name="faisal_journal.csv")

with tab2:
    st.markdown("### 🛰️ الرادار الموحد — نظرية الارتكاز (الشورت محوراً)")
    st.markdown(
        '<div class="filter-box"><b>🎯 وقود الشورت:</b> 🚀 محشور ≥30% | 🧹 استنفاد | ⛽ متوسط | 🎈 بلا وقود<br>'
        '<b>🎯 الزناد الفني:</b> كسر عنق W | اختراق رأس شمعة هابطة قوية | اختراق قمة القاعدة (كسر حديث ≤8%)<br>'
        '<b>الجاهزية =</b> وقود مقبول + معادلة مكتملة + نقاط ≥50 + (قرب الدعم <b>أو</b> زناد سحب <b>أو</b> زناد فني) + بلا فجوة ≥25%</div>',
        unsafe_allow_html=True)
    extra = st.text_input("➕ رموز إضافية تُدمج في هذا المسح (افصل بفاصلة): مثل SXTC, OFAL, INLF", "")

    if st.button("🛰️ امسح بالرادار الموحد", key="uni"):
        with st.spinner("تحميل القائمة..."):
            universe = get_dynamic_universe(limit=300)
        if extra.strip():
            universe = list(dict.fromkeys([u.strip().upper() for u in extra.split(",") if u.strip()] + universe))
        if st.session_state.get("universe_source") != "TradingView":
            st.warning(f"⚠️ المسح على القائمة الاحتياطية — سبب فشل المصدر الحي: {st.session_state.get('universe_error', 'غير معروف')}")
        pg = st.progress(0)
        raw = scan_parallel(universe, "6mo", progress_cb=lambda i, n: pg.progress(i / n))
        pg.empty()

        stage1, excluded = [], []
        for s, h, sps, src in raw:
            if src == "stooq": continue
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
                    stage1.append((s, h, sps, phase, fam))
            except Exception: continue
        st.info(f"🔎 {len(stage1)} سهم حي يحمل بصمة فصيلة")

        results = []
        pg2 = st.progress(0)
        for i, (s, h, sps, phase, fam) in enumerate(stage1):
            pg2.progress((i + 1) / max(len(stage1), 1))
            try:
                inf = finnhub_metrics(s)
                news = check_news(s)
                r = score(s, h, inf, sps, news)
                if r["hard_veto"]:
                    excluded.append({"symbol": s, "reason": " | ".join(veto_reasons(r, news, None))}); continue
                fuel_kind, fuel_txt = short_fuel(r)
                h4 = get_4h(s) if (r["dist_sup"] is not None and r["dist_sup"] <= 20) else None
                lad = build_red_ladder(h4)
                sweep = detect_sweep_reclaim(h4, r["support"])
                sweep_wait = sweep is None and sweep_mode_candidate(h4, h, r["support"], r["dist_sup"])
                tech = technical_trigger(h, r)
                missing = []
                base_fam = ("تجميع/قاعدة" in fam) or ("سيناريو المضارب" in fam) or ("درج ثبات" in fam)
                macd_flat = abs(r["macd_hist"]) / max(r["price"], 0.01) < 0.005
                if not (20 <= r["rsi"] <= 30) and not base_fam:
                    missing.append(f"RSI {r['rsi']} خارج الضغط ولا قاعدة/درج")
                if not (r["macd_imp"] or (base_fam and macd_flat)):
                    missing.append("MACD لا يتحسن")
                if not r["stability"] and not sweep and not tech and not ("سيناريو المضارب" in fam) and not ("درج ثبات" in fam):
                    missing.append("لا ثبات فوق الدعم")
                near_support = r["dist_sup"] is not None and r["dist_sup"] <= 15
                fuel_ok = fuel_kind in ("packed", "present", "exhausted", "missing")
                guide_ready = fuel_ok and (not missing) and r["total"] >= 50 and (near_support or bool(sweep) or bool(tech))
                tags = []
                if phase and phase["phase_key"] in ("READY", "WATCH", "RETEST"):
                    tags.append(f"تقسيم: {PHASE_META[phase['phase_key']]} {phase['ratio']}")
                if lad: tags.append(f"سلّم 4H: {len(lad['ladder'])} شموع")
                if r["accum"]: tags.append("تجميع هادئ")
                if "سيناريو المضارب" in fam: tags.append("🎭 سيناريو المضارب")
                if "درج ثبات" in fam: tags.append("🪜 درج ثبات")
                if "🚀 وقود محشور (Squeeze)" in fam: tags.append("🚀 وقود محشور")
                elif "🧹 استنفاد الشورت (Post-Covering)" in fam: tags.append("🧹 استنفاد الشورت")
                elif "⛽ وقود متوسط" in fam: tags.append("⛽ وقود متوسط")
                if fuel_kind == "none": tags.append("🎈 بلا وقود")
                if tech: tags.append("🎯 زناد فني")
                if sweep: tags.append("🌀 زناد السحب تحقق")
                elif sweep_wait: tags.append("🌀 نمط سحب — انتظر الزناد")
                if fuel_kind == "none":
                    missing.append("🎈 بلا وقود شورت — تضخم حر بلا غطاء")
                off = {"has_offering": False}
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
                                "tech": tech, "fuel_kind": fuel_kind, "fuel_txt": fuel_txt,
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
                    st.dataframe(pd.DataFrame(x["ladder"]["ladder"]), use_container_width=True, hide_index=True)

        if excluded:
            with st.expander(f"🚫 المقصيون وأسبابهم ({len(excluded)})"):
                for e in excluded:
                    st.markdown(f"- **{e['symbol']}**: {e['reason']}")

st.markdown("---")
st.caption("⚠️ تعليمي فقط - ليس توصية استثمارية | نظرية الارتكاز: وايكوف + إليوت + كلاسيكي + الشورت محوراً")
