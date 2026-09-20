import streamlit as st
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta

st.set_page_config(page_title="Stock Screener", page_icon="🎯", layout="wide")

PROXY_URL = "https://faisal-proxy.onrender.com"
FINNHUB_KEY = "demn1c9r01qnf8fq7jc0demn1c9r01qnf8fq7jcg"

st.markdown("<style>.main{direction:rtl}h1,h2,h3{direction:rtl;text-align:right}.score-card{background:linear-gradient(135deg,#f8f9fa,#e9ecef);padding:25px;border-radius:15px;text-align:center;margin:15px 0}.score-big{font-size:56px;font-weight:bold;margin:0}.verdict{font-size:22px;margin-top:10px}.stButton>button{width:100%;background:linear-gradient(90deg,#00b894,#0984e3);color:white;font-weight:bold;border-radius:10px;padding:12px;border:none}.info-box{background:#e8f4f8;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #0984e3}.warn-box{background:#fff3cd;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #fdcb6e}.success-box{background:#d4edda;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #00b894}.danger-box{background:#f8d7da;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #d63031}.split-box{background:#ffe5e5;padding:12px;border-radius:8px;margin:8px 0;border-right:4px solid #d63031}.plan-box{background:#f0f7ff;padding:15px;border-radius:10px;margin:10px 0;border:2px solid #0984e3}.news-item{background:#fff;padding:10px;border-radius:6px;margin:5px 0;border-right:3px solid #0984e3;font-size:14px}a{color:#0984e3;text-decoration:none}.plan-table{width:100%;border-collapse:collapse;margin-top:10px}.plan-table td{padding:8px;border-bottom:1px solid #d0e4f5;font-size:16px}</style>", unsafe_allow_html=True)

LOCAL_UNIVERSE = [
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
    "ZJYL","SOPA","PRSO","ELYM","ALLR","AGRI","ALZN","AMST","APRE","AUID",
    "AUUD","AVTX","AXLA","BCDA","BCLI","BEAT","BIVI","BLBX","BMEA","BNOX",
    "BOLT","BRTX","BTBT","BTCS","BTTR","BYSI","CANF","CARV","CASI","CBIH",
    "CCCC","CELZ","CFRX","CGEN","CHRS","CLVR","CNTB","CNTX","COCP","COEP",
    "CRBP","CREX","CRGE","CTSO","CUEN","CVKD","CVM","CYCC","CYTO","DBGI",
    "DCTH","DFLI","DMAC","DRMA","DRRX","EFTR","EIGR","ELDN","ENG","ENSC",
    "EPIX","ERNA","EVFM","EVLO","EXPR","FBRX","FGEN","FHTX","FLGC","FPAY",
    "FRES","FREQ","FRGE","GANX","GENE","GHSI","GLMD","GLTO","GMBL","GOVX",
    "GRNA","GRTS","GTBP","GTHX","HCWB","HGEN","HOLO","HOOK","HOWL","HPCO",
    "ICCM","ICU","IGC","IMNN","IMPL","IMRX","INAB","INCR","INM","INOD",
    "INVO","IPHA","IPSC","ISPC","ISUN","JAGX","JSPR","KALA","KAVL","KERN",
    "KPRX","KRON","KTRA","KTTA","LASE","LBPH","LCTX","LFLY","LIPO","LIXT",
    "LNSR","LPTH","LPTX","LRMR","LSTA","LTBR","LTRN","LUNR","LVLU","LVO",
    "LXEO","LYEL","LYRA","MAIA","MBIO","MBOT","MDGL","MDNA","MEIP","METX",
    "MGRM","MIGI","MIST","MKUL","MNOV","MOVE","MPLN","MRKR","MRM","MTEM",
    "MURA","MYMD","NAOV","NBRV","NCPL","NDRA","NKLA","NKTR","NLSP","NMTR",
    "NRIX","NSYS","NURO","NUVB","NVAX","NVIV","NVNO","NXGL","NXTC","OBLG",
    "OCEA","OCGN","OCUL","OGEN","OGI","OMER","OMGA","ONCT","ONCY","ONVO",
    "ORGN","ORGS","ORMP","OTLK","OTMO","OTRK","PALI","PASG","PBLA","PCSA",
    "PDSB","PEV","PIRS","PLAB"
]


def yahoo_candles(symbol, period="6mo"):
    headers = {"User-Agent": "Mozilla/5.0"}
    if PROXY_URL:
        try:
            url = PROXY_URL + "/yahoo/candles"
            r = requests.get(url, params={"symbol": symbol, "period": period}, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if data.get("success") and data.get("candles"):
                    df = pd.DataFrame(data["candles"])
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.set_index("date").sort_index()
                    df.columns = [c.capitalize() for c in df.columns]
                    return df, data.get("splits", [])
        except Exception:
            pass
    return pd.DataFrame(), []


def finnhub_metrics(symbol):
    info = {"floatShares": 0, "shortPercentOfFloat": 0, "sharesShort": 0}
    if not FINNHUB_KEY:
        return info
    try:
        url = "https://finnhub.io/api/v1/stock/metric"
        r = requests.get(url, params={"symbol": symbol, "metric": "all", "token": FINNHUB_KEY}, timeout=15)
        if r.status_code == 200:
            m = r.json().get("metric", {})
            ff = m.get("freeFloat") or 0
            info["floatShares"] = ff * 1000000 if ff and ff < 1000 else ff
            sp = m.get("shortPercentOfFloat") or 0
            info["shortPercentOfFloat"] = sp / 100 if sp > 1 else sp
            info["sharesShort"] = m.get("sharesShort") or 0
    except Exception:
        pass
    return info


def check_offering(symbol):
    try:
        cik_map_url = "https://www.sec.gov/files/company_tickers.json"
        headers = {"User-Agent": "FaisalBot contact@example.com"}
        r = requests.get(cik_map_url, headers=headers, timeout=10)
        if r.status_code != 200:
            return {"has_offering": False}
        data = r.json()
        cik = None
        for v in data.values():
            if v["ticker"].upper() == symbol.upper():
                cik = str(v["cik_str"]).zfill(10)
                break
        if not cik:
            return {"has_offering": False}
        import time
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
                        days = (datetime.now() - fdate).days
                        return {"has_offering": True, "form": form, "days": days}
                except Exception:
                    continue
        return {"has_offering": False}
    except Exception:
        return {"has_offering": False}


def check_negative_news(symbol):
    """فحص الأخبار السلبية من Finnhub"""
    if not FINNHUB_KEY:
        return {"has_negative": False, "items": [], "status": "no_key"}
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        url = "https://finnhub.io/api/v1/company-news"
        r = requests.get(url, params={
            "symbol": symbol, "from": week_ago, "to": today, "token": FINNHUB_KEY
        }, timeout=15)
        if r.status_code != 200:
            return {"has_negative": False, "items": [], "status": "error"}
        news = r.json()
        if not news:
            return {"has_negative": False, "items": [], "status": "no_news"}
        
        neg_critical = ["bankruptcy", "chapter 11", "chapter 7", "delisting", "delisted", "fraud", "sec investigation", "sec probe", "halted", "trading halt", "going concern"]
        neg_high = ["lawsuit", "class action", "sued", "net loss", "layoffs", "layoff", "restructuring", "downgrade", "downgraded", "price target cut", "misses", "missed estimates", "revenue decline", "warns", "warning", "guidance cut"]
        neg_offering = ["public offering", "private placement", "registered direct", "dilution", "shelf offering", "atm offering", "stock offering"]
        neg_medium = ["investigation", "probe", "subpoena", "recall", "delay", "rejected", "cancellation"]
        
        matches = []
        for item in news[:30]:
            headline = (item.get("headline") or "").lower()
            summary = (item.get("summary") or "").lower()
            text = headline + " " + summary
            level = None
            keyword = None
            for kw in neg_critical:
                if kw in text: level = "CRITICAL"; keyword = kw; break
            if not level:
                for kw in neg_offering:
                    if kw in text: level = "OFFERING"; keyword = kw; break
            if not level:
                for kw in neg_high:
                    if kw in text: level = "HIGH"; keyword = kw; break
            if not level:
                for kw in neg_medium:
                    if kw in text: level = "MEDIUM"; keyword = kw; break
            if level:
                ts = item.get("datetime", 0)
                try:
                    date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                except Exception:
                    date_str = "?"
                matches.append({
                    "headline": item.get("headline", "")[:120],
                    "source": item.get("source", "?"),
                    "date": date_str,
                    "level": level,
                    "keyword": keyword,
                    "url": item.get("url", "")
                })
        return {
            "has_negative": len(matches) > 0,
            "items": matches[:5],
            "status": "done",
            "critical_count": sum(1 for m in matches if m["level"] == "CRITICAL"),
            "offering_count": sum(1 for m in matches if m["level"] == "OFFERING"),
            "high_count": sum(1 for m in matches if m["level"] == "HIGH"),
        }
    except Exception:
        return {"has_negative": False, "items": [], "status": "error"}


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
    if mp and mi:
        return "إيجابي ويتحسن ✅", "#00b894"
    elif mp:
        return "إيجابي لكن يضعف 🟡", "#fdcb6e"
    elif mi:
        return "سلبي لكن يتحسن 🟡", "#fdcb6e"
    else:
        return "سلبي ويضعف ❌", "#d63031"


def sma(close, p):
    return float(close.rolling(p).mean().iloc[-1]) if len(close) >= p else None


def stoch(high, low, close, kp=14, dp=3):
    if len(close) < kp:
        return 50.0
    ll = low.rolling(kp).min()
    hh = high.rolling(kp).max()
    d = (hh - ll).replace(0, np.nan)
    k = 100 * ((close - ll) / d)
    v = k.rolling(dp).mean().iloc[-1]
    return float(v) if pd.notna(v) else 50.0


def sr(hist, w=20):
    if hist.empty or len(hist) < w:
        return None, None
    return float(hist["Low"].rolling(w).min().iloc[-1]), float(hist["High"].rolling(w).max().iloc[-1])


def detect_former_runner(hist):
    if len(hist) < 20:
        return False
    returns = hist["Close"].pct_change()
    return bool((returns > 0.5).any())


def detect_w_pattern(hist):
    if len(hist) < 40:
        return None
    h = hist["Close"].tail(60)
    lows = h[(h.shift(1) > h) & (h.shift(-1) > h)]
    if len(lows) < 2:
        return None
    last_two = lows.tail(2)
    diff = abs(last_two.iloc[0] - last_two.iloc[1]) / last_two.iloc[0] * 100
    if diff < 4:
        neckline = float(h.loc[last_two.index[0]:last_two.index[1]].max())
        return {"bottom1": round(float(last_two.iloc[0]), 3), "bottom2": round(float(last_two.iloc[1]), 3), "neckline": round(neckline, 3)}
    return None


def detect_bull_trap(hist):
    if len(hist) < 25:
        return False
    resistance = hist["High"].tail(20).max()
    recent = hist.tail(5)
    broke = (recent["High"] > resistance * 0.98).any()
    if not broke:
        return False
    current = hist["Close"].iloc[-1]
    return current < resistance * 0.97


def detect_gap_fill(hist):
    if len(hist) < 10:
        return None
    for i in range(len(hist) - 10, len(hist) - 1):
        try:
            prev_close = hist["Close"].iloc[i]
            curr_open = hist["Open"].iloc[i + 1]
            gap_pct = (curr_open - prev_close) / prev_close * 100
            if abs(gap_pct) > 5:
                gap_price = curr_open
                current = hist["Close"].iloc[-1]
                if gap_pct < 0 and current < gap_price:
                    return {"direction": "down", "gap_price": round(gap_price, 3), "gap_pct": round(gap_pct, 1)}
                elif gap_pct > 0 and current > gap_price:
                    return {"direction": "up", "gap_price": round(gap_price, 3), "gap_pct": round(gap_pct, 1)}
        except Exception:
            continue
    return None


def detect_candle_patterns(hist):
    if len(hist) < 5:
        return None
    patterns = []
    for i in range(-3, 0):
        try:
            o = hist["Open"].iloc[i]
            c = hist["Close"].iloc[i]
            hi = hist["High"].iloc[i]
            lo = hist["Low"].iloc[i]
            body = abs(c - o)
            rt = hi - lo
            if rt == 0:
                continue
            uw = hi - max(o, c)
            lw = min(o, c) - lo
            if lw > body * 2 and uw < body * 0.5 and body < rt * 0.3:
                patterns.append("Hammer")
            if i > -len(hist):
                po = hist["Open"].iloc[i - 1]
                pc = hist["Close"].iloc[i - 1]
                if pc < po and c > o and c > po and o < pc:
                    patterns.append("Bullish Engulfing")
        except Exception:
            continue
    return patterns if patterns else None


def detect_reverse_split(splits):
    if not splits:
        return {"has_split": False, "days_since": 9999}
    cutoff = datetime.now() - timedelta(days=365)
    for sp in splits:
        try:
            sp_date = datetime.strptime(sp["date"], "%Y-%m-%d")
            if sp_date >= cutoff:
                num = int(sp.get("numerator", 1))
                den = int(sp.get("denominator", 1))
                ds = (datetime.now() - sp_date).days
                return {"has_split": num < den, "date": sp["date"], "ratio": str(num) + ":" + str(den), "days_since": ds}
        except Exception:
            continue
    return {"has_split": False, "days_since": 9999}


def detect_stability(hist, support, min_sessions=2):
    if not support or len(hist) < min_sessions + 2:
        return None
    recent = hist.tail(min_sessions + 3)
    threshold = support * 0.98
    closes = recent["Close"].values
    lows = recent["Low"].values
    sessions_held = 0
    for c in reversed(closes):
        if c >= threshold:
            sessions_held += 1
        else:
            break
    if sessions_held < min_sessions:
        return None
    recent_lows = lows[-sessions_held:]
    higher_lows = all(recent_lows[i] >= recent_lows[i-1] * 0.99 for i in range(1, len(recent_lows))) if len(recent_lows) > 1 else False
    if sessions_held >= 3 and higher_lows:
        strength = "🔥 ثبات قوي"
        color = "#00b894"
        points = 10
    elif sessions_held >= 2 and higher_lows:
        strength = "✅ ثبات جيد"
        color = "#00b894"
        points = 7
    elif sessions_held >= 2:
        strength = "🟡 ثبات مقبول"
        color = "#fdcb6e"
        points = 5
    else:
        strength = "⚠️ ثبات ضعيف"
        color = "#fdcb6e"
        points = 0
    return {"sessions_held": sessions_held, "higher_lows": higher_lows, "strength": strength, "color": color, "points": points}


def rebound_progress(hist, support, resistance):
    if not support or not resistance or resistance == support:
        return None
    price = hist["Close"].iloc[-1]
    return round((price - support) / (resistance - support) * 100, 1)


def detect_liquidity_sweep(hist):
    if len(hist) < 15:
        return False
    support = hist["Low"].tail(20).min()
    recent = hist.tail(5)
    for i in range(1, len(recent) - 1):
        if recent["Low"].iloc[i] < support * 1.02:
            if recent["Close"].iloc[i + 1] > support:
                return True
    return False


def detect_spring(hist):
    if len(hist) < 40:
        return None
    support = hist["Low"].tail(40).min()
    last10 = hist.tail(10)
    broke = last10["Low"].min() < support * 0.97
    recovered = last10["Close"].iloc[-1] > support
    return {"detected": True} if (broke and recovered) else None


def detect_lps(hist):
    if len(hist) < 40:
        return None
    resistance = hist["High"].tail(30).max()
    recent = hist.tail(15)
    if not (recent["High"].max() > resistance * 0.98):
        return None
    pullback = recent["Low"].min()
    price = hist["Close"].iloc[-1]
    return {"detected": True} if (pullback > resistance * 0.95 and price > pullback) else None


def score(symbol, hist, info, splits=None, news=None):
    close = hist["Close"]
    high = hist["High"]
    low = hist["Low"]
    vol = hist["Volume"]
    price = float(close.iloc[-1])
    r = rsi(close)
    mp, mi, macd_hist = macd(close)
    macd_txt, macd_color = macd_status(mp, mi, macd_hist)
    sk = stoch(high, low, close)
    s20 = sma(close, 20)
    s30 = sma(close, 30)
    s50 = sma(close, 50)
    sup, res = sr(hist, 20)
    fs = info.get("floatShares", 0) or 0
    cv = int(vol.iloc[-1]) if len(vol) else 0
    avg_vol = float(vol.tail(20).mean()) if len(vol) >= 20 else 0
    rv = round(cv / avg_vol, 2) if avg_vol > 0 else 0

    bd = {}
    if 23 <= r <= 27:
        bd["RSI"] = 25
    elif 20 <= r < 23:
        bd["RSI"] = 12
    elif 27 < r <= 30:
        bd["RSI"] = 15
    elif 30 < r <= 35:
        bd["RSI"] = 6
    else:
        bd["RSI"] = 0

    split_info = detect_reverse_split(splits)
    if split_info["has_split"]:
        d = split_info["days_since"]
        if d <= 180:
            bd["Split"] = 20
        elif d <= 365:
            bd["Split"] = 10
        else:
            bd["Split"] = 0
    else:
        bd["Split"] = 0

    if fs:
        if fs < 1000000:
            bd["Float"] = 15
        elif fs < 5000000:
            bd["Float"] = 12
        elif fs < 10000000:
            bd["Float"] = 8
        elif fs < 20000000:
            bd["Float"] = 5
        else:
            bd["Float"] = 0
    else:
        bd["Float"] = 0

    if mp and mi:
        bd["MACD"] = 15
    elif mi:
        bd["MACD"] = 8
    else:
        bd["MACD"] = 0

    if s20 and s30 and s50:
        b = sum(1 for x in [s20, s30, s50] if price < x)
        bd["MA"] = {3: 15, 2: 8, 1: 5}.get(b, 0)
    else:
        bd["MA"] = 0

    if sk < 20:
        bd["Stoch"] = 10
    elif sk < 30:
        bd["Stoch"] = 8
    elif sk < 40:
        bd["Stoch"] = 6
    elif sk < 50:
        bd["Stoch"] = 4
    else:
        bd["Stoch"] = 0

    ds = None
    if sup:
        ds = round((price - sup) / price * 100, 2)
        if ds < 3:
            bd["Support"] = 10
        elif ds < 5:
            bd["Support"] = 7
        elif ds < 8:
            bd["Support"] = 3
        elif ds < 15:
            bd["Support"] = 0
        else:
            bd["Support"] = -15
    else:
        bd["Support"] = 0

    if rv > 5:
        bd["RVOL"] = 5
    elif rv >= 2:
        bd["RVOL"] = 3
    else:
        bd["RVOL"] = 0

    stability = detect_stability(hist, sup, min_sessions=2)
    bd["Stability"] = stability["points"] if stability else 0

    rebound = rebound_progress(hist, sup, res)
    if rebound is not None:
        if rebound < 30:
            bd["Rebound"] = 5
        elif rebound < 60:
            bd["Rebound"] = 0
        else:
            bd["Rebound"] = -10

    is_runner = detect_former_runner(hist)
    bd["Runner"] = 5 if is_runner else 0

    w_pat = detect_w_pattern(hist)
    bd["W_Pattern"] = 10 if w_pat else 0

    total = max(0, min(sum(bd.values()), 100))

    # عقوبات الأخبار
    if news:
        if news.get("critical_count", 0) > 0:
            total = max(0, total - 30)
        if news.get("offering_count", 0) > 0:
            total = max(0, total - 20)
        if news.get("high_count", 0) >= 2:
            total = max(0, total - 10)

    if total >= 80:
        v = "مثالي"
        c = "#00b894"
    elif total >= 65:
        v = "ممتاز"
        c = "#00b894"
    elif total >= 50:
        v = "جيد"
        c = "#fdcb6e"
    elif total >= 35:
        v = "ضعيف"
        c = "#e17055"
    else:
        v = "مرفوض"
        c = "#d63031"

    return {
        "symbol": symbol, "price": price, "rsi": round(r, 2), "stoch": round(sk, 2),
        "macd_pos": mp, "macd_imp": mi, "macd_hist": round(macd_hist, 4),
        "macd_txt": macd_txt, "macd_color": macd_color,
        "sma20": s20, "sma50": s50,
        "support": sup, "resistance": res, "dist_sup": ds, "float": fs, "rvol": rv,
        "short_pct": info.get("shortPercentOfFloat", 0) or 0,
        "breakdown": bd, "total": total, "verdict": v, "color": c,
        "rebound": rebound, "split_info": split_info, "stability": stability,
        "sweep": detect_liquidity_sweep(hist), "spring": detect_spring(hist),
        "lps": detect_lps(hist), "w_pattern": w_pat, "runner": is_runner,
        "bull_trap": detect_bull_trap(hist), "gap": detect_gap_fill(hist),
        "candles": detect_candle_patterns(hist)
    }


st.markdown("# Stock Screener")
st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["تحليل سهم", "أسهم التقسيم", "أفضل 10", "رادار الاكتشاف"])

with tab1:
    col1, col2 = st.columns([3, 1])
    with col1:
        sym = st.text_input("رمز السهم", "AEMD").upper()
    with col2:
        st.write("")
        st.write("")
        btn = st.button("تحليل", key="a")

    if btn and sym:
        with st.spinner("جاري تحليل " + sym):
            hist, splits = yahoo_candles(sym, "6mo")
            if hist.empty:
                st.error("لا بيانات لـ " + sym)
            else:
                info = finnhub_metrics(sym)
                offering = check_offering(sym)
                news = check_negative_news(sym)
                r = score(sym, hist, info, splits, news)

                st.markdown('<div class="score-card"><div class="score-big" style="color:' + r['color'] + '">' + str(r['total']) + '/100</div><div class="verdict">' + r['verdict'] + '</div></div>', unsafe_allow_html=True)

                # ====== الأخبار السلبية ======
                if news.get("critical_count", 0) > 0:
                    st.markdown('<div class="danger-box">🚨 <b>أخبار حرجة!</b> ' + str(news["critical_count"]) + ' خبر خطير - تجنب السهم!</div>', unsafe_allow_html=True)
                if news.get("offering_count", 0) > 0:
                    st.markdown('<div class="danger-box">⚠️ <b>طرح جديد محتمل!</b> ' + str(news["offering_count"]) + ' خبر عن طرح/إضعاف - خطر!</div>', unsafe_allow_html=True)

                if offering.get("has_offering"):
                    st.markdown('<div class="danger-box">📋 <b>طرح في SEC:</b> ' + offering.get("form", "") + ' قبل ' + str(offering.get("days", 0)) + ' يوم - تجنب!</div>', unsafe_allow_html=True)

                if news.get("items"):
                    st.markdown("#### 📰 آخر الأخبار السلبية")
                    for item in news["items"]:
                        if item["level"] == "CRITICAL":
                            emoji = "🚨"
                            color = "#d63031"
                        elif item["level"] == "OFFERING":
                            emoji = "💰"
                            color = "#d63031"
                        elif item["level"] == "HIGH":
                            emoji = "⚠️"
                            color = "#e17055"
                        else:
                            emoji = "🟡"
                            color = "#fdcb6e"
                        st.markdown(
                            '<div class="news-item" style="border-right-color:' + color + '">'
                            + emoji + ' <b>' + item["headline"] + '</b><br>'
                            '<small>' + item["source"] + ' - ' + item["date"] + '</small>'
                            '</div>',
                            unsafe_allow_html=True
                        )

                # ====== الثبات ======
                if r["stability"]:
                    s = r["stability"]
                    hl = " + قيعان أعلى ✅" if s["higher_lows"] else ""
                    st.markdown('<div class="success-box" style="border-right-color:' + s["color"] + '">📊 <b>نموذج الثبات:</b> ' + s["strength"] + ' - ' + str(s["sessions_held"]) + ' جلسات فوق الدعم' + hl + '</div>', unsafe_allow_html=True)
                elif r["support"]:
                    st.markdown('<div class="warn-box">⚠️ <b>نموذج الثبات:</b> أقل من جلستين فوق الدعم - انتظر</div>', unsafe_allow_html=True)

                if r["bull_trap"]:
                    st.markdown('<div class="danger-box">Bull Trap: كسر مقاومة ثم فشل - خطر!</div>', unsafe_allow_html=True)

                if r["split_info"].get("has_split"):
                    d = r["split_info"]["days_since"]
                    label = " - حديث!" if d <= 180 else ""
                    st.markdown('<div class="split-box">Reverse Split: ' + r["split_info"]["ratio"] + ' - قبل ' + str(d) + ' يوم' + label + '</div>', unsafe_allow_html=True)

                if r["short_pct"] > 0:
                    short_warn = " - مرتفع! Squeeze محتمل" if r["short_pct"] > 0.20 else ""
                    st.markdown('<div class="info-box">Short Float: ' + str(round(r["short_pct"]*100, 2)) + '%' + short_warn + '</div>', unsafe_allow_html=True)

                st.markdown('<div class="info-box" style="border-right-color:' + r["macd_color"] + '"><b>MACD:</b> ' + r["macd_txt"] + ' (قيمة: ' + str(r["macd_hist"]) + ')</div>', unsafe_allow_html=True)

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("السعر", "$" + str(round(r['price'], 3)))
                c2.metric("RSI", str(r['rsi']))
                c3.metric("Stoch", str(r['stoch']))
                c4.metric("RVOL", str(r['rvol']))

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("MA20", "$" + str(round(r['sma20'], 2)) if r['sma20'] else "-")
                c2.metric("MA50", "$" + str(round(r['sma50'], 2)) if r['sma50'] else "-")
                c3.metric("الدعم", "$" + str(round(r['support'], 3)) if r['support'] else "-")
                c4.metric("المقاومة", "$" + str(round(r['resistance'], 3)) if r['resistance'] else "-")

                if r["runner"]:
                    st.markdown('<div class="success-box">Former Runner: سبق أن انفجر +50% في يوم!</div>', unsafe_allow_html=True)
                if r["w_pattern"]:
                    wp = r["w_pattern"]
                    st.markdown('<div class="success-box">W Pattern: قاعان $' + str(wp['bottom1']) + ' / $' + str(wp['bottom2']) + ' - خط العنق: $' + str(wp['neckline']) + '</div>', unsafe_allow_html=True)
                if r["sweep"]:
                    st.markdown('<div class="success-box">سحب سيولة: تم كشفه!</div>', unsafe_allow_html=True)
                if r["spring"]:
                    st.markdown('<div class="success-box">Spring (وايكوف): كسر ثم استرداد!</div>', unsafe_allow_html=True)
                if r["lps"]:
                    st.markdown('<div class="success-box">LPS (وايكوف): اختراق ثم اختبار ناجح!</div>', unsafe_allow_html=True)
                if r["candles"]:
                    st.markdown('<div class="success-box">شموع انعكاسية: ' + ", ".join(r["candles"]) + '</div>', unsafe_allow_html=True)
                if r["gap"]:
                    g = r["gap"]
                    direction = "هبوط" if g["direction"] == "down" else "صعود"
                    st.markdown('<div class="info-box">فجوة ' + direction + ': ' + str(g["gap_pct"]) + '% عند $' + str(g["gap_price"]) + '</div>', unsafe_allow_html=True)
                if r["rebound"] is not None:
                    if r["rebound"] > 60:
                        st.markdown('<div class="warn-box">الارتداد: ' + str(r['rebound']) + '% - متأخر</div>', unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="info-box">تقدم الارتداد: ' + str(r['rebound']) + '% - مبكر</div>', unsafe_allow_html=True)

                if r["float"]:
                    st.info("Free Float: " + str(round(r['float']/1000000, 2)) + "M سهم")
                else:
                    st.info("Free Float: غير متوفر")

                if r["support"] and r["resistance"]:
                    sup_val = r["support"]
                    res_val = r["resistance"]
                    current_price = r["price"]
                    entry = round(sup_val * 1.02, 3)
                    stop = round(sup_val * 0.94, 3)
                    target1 = round(res_val, 3)
                    target2 = round(res_val * 1.20, 3)
                    target3 = round(res_val * 1.50, 3)
                    risk = round((entry - stop) / entry * 100, 2)
                    reward1 = round((target1 - entry) / entry * 100, 2)
                    rr = round((target1 - entry) / (entry - stop), 2) if entry > stop else 0
                    if current_price > entry * 1.03:
                        entry_status = " - السعر أعلى من الدخول، انتظر النزول"
                        entry_color = "#d63031"
                    elif current_price < entry * 0.98:
                        entry_status = " - السعر تحت الدخول، فرصة!"
                        entry_color = "#00b894"
                    else:
                        entry_status = " - السعر عند منطقة الدخول"
                        entry_color = "#0984e3"

                    st.markdown("### خطة الدخول والخروج")
                    st.markdown(
                        '<div class="plan-box"><table class="plan-table">'
                        '<tr><td><b>السعر الحالي</b></td><td>$' + str(round(current_price, 3)) + '</td></tr>'
                        '<tr><td><b>الدخول المقترح</b></td><td style="color:' + entry_color + '"><b>$' + str(entry) + '</b>' + entry_status + '</td></tr>'
                        '<tr><td><b>الوقف</b></td><td style="color:#d63031"><b>$' + str(stop) + '</b> (مخاطرة ' + str(risk) + '%)</td></tr>'
                        '<tr><td><b>هدف 1</b></td><td style="color:#00b894"><b>$' + str(target1) + '</b> (+' + str(reward1) + '%)</td></tr>'
                        '<tr><td><b>هدف 2</b></td><td style="color:#00b894">$' + str(target2) + '</td></tr>'
                        '<tr><td><b>هدف 3</b></td><td style="color:#00b894">$' + str(target3) + '</td></tr>'
                        '<tr><td><b>نسبة المخاطرة/المكافأة</b></td><td><b>1 : ' + str(rr) + '</b></td></tr>'
                        '<tr><td><b>حجم المخاطرة</b></td><td>1-2% من المحفظة</td></tr>'
                        '</table></div>',
                        unsafe_allow_html=True
                    )

                st.markdown("### تفصيل النقاط")
                st.dataframe(pd.DataFrame(list(r["breakdown"].items()), columns=["المعيار", "النقاط"]), use_container_width=True, hide_index=True)

                st.markdown('<a href="https://fintel.io/ss/us/' + sym.lower() + '" target="_blank">عرض تفاصيل Short Interest على Fintel</a>', unsafe_allow_html=True)

with tab2:
    st.markdown("### أسهم Reverse Split حديثة")
    st.caption("فحص " + str(len(LOCAL_UNIVERSE)) + " سهماً")
    if st.button("ابدأ البحث", key="sp"):
        pg = st.progress(0)
        res = []
        for i, s in enumerate(LOCAL_UNIVERSE):
            pg.progress((i + 1) / len(LOCAL_UNIVERSE))
            try:
                h, sps = yahoo_candles(s, "1y")
                if h.empty or len(h) < 30:
                    continue
                sp_info = detect_reverse_split(sps)
                if sp_info.get("has_split"):
                    inf = finnhub_metrics(s)
                    r = score(s, h, inf, sps)
                    r["split_details"] = sp_info
                    res.append(r)
            except Exception:
                continue
        pg.empty()
        if res:
            res.sort(key=lambda x: (x["split_details"]["days_since"], -x["total"]))
            st.success(str(len(res)) + " سهم بتقسيم عكسي")
            for r in res[:20]:
                sp = r["split_details"]
                with st.expander("**" + r['symbol'] + "** - " + str(r['total']) + "/100 " + r['verdict']):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("النسبة", sp["ratio"])
                    c2.metric("قبل", str(sp["days_since"]) + " يوم")
                    c3.metric("النقاط", str(r['total']))
                    c1, c2, c3 = st.columns(3)
                    c1.metric("السعر", "$" + str(round(r['price'], 3)))
                    c2.metric("RSI", str(r['rsi']))
                    c3.metric("Stoch", str(r['stoch']))
                    st.markdown('<a href="https://fintel.io/ss/us/' + r['symbol'].lower() + '" target="_blank">Fintel Short Interest</a>', unsafe_allow_html=True)
        else:
            st.warning("لا توجد أسهم بتقسيم عكسي.")

with tab3:
    st.markdown("### أفضل 10 أسهم")
    st.caption("فحص " + str(len(LOCAL_UNIVERSE)) + " سهماً")
    if st.button("ابدأ الفحص", key="t"):
        pg = st.progress(0)
        res = []
        for i, s in enumerate(LOCAL_UNIVERSE):
            pg.progress((i + 1) / len(LOCAL_UNIVERSE))
            try:
                h, sps = yahoo_candles(s, "3mo")
                if h.empty or len(h) < 30:
                    continue
                inf = finnhub_metrics(s)
                r = score(s, h, inf, sps)
                if r["total"] >= 30:
                    res.append(r)
            except Exception:
                continue
        pg.empty()
        if res:
            res.sort(key=lambda x: x["total"], reverse=True)
            for i, r in enumerate(res[:10], 1):
                m = "1." if i == 1 else "2." if i == 2 else "3." if i == 3 else str(i) + "."
                with st.expander(m + " **" + r['symbol'] + "** - " + str(r['total']) + "/100 " + r['verdict']):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("السعر", "$" + str(round(r['price'], 3)))
                    c2.metric("RSI", str(r['rsi']))
                    c3.metric("Stoch", str(r['stoch']))
                    if r["support"]:
                        st.write("الدعم: $" + str(round(r['support'], 3)) + " (على بعد " + str(r['dist_sup']) + "%)")
                    if r["split_info"].get("has_split"):
                        st.write("Reverse Split: " + r["split_info"]["ratio"] + " قبل " + str(r["split_info"]["days_since"]) + " يوم")
                    if r["stability"]:
                        st.write("الثبات: " + r["stability"]["strength"] + " - " + str(r["stability"]["sessions_held"]) + " جلسات")
        else:
            st.warning("لا نتائج.")

with tab4:
    st.markdown("### رادار الاكتشاف المبكر")
    st.caption("أسهم Squeeze محتملة")
    if st.button("ابحث", key="h"):
        pg = st.progress(0)
        res = []
        for i, s in enumerate(LOCAL_UNIVERSE):
            pg.progress((i + 1) / len(LOCAL_UNIVERSE))
            try:
                h, sps = yahoo_candles(s, "3mo")
                if h.empty or len(h) < 30:
                    continue
                inf = finnhub_metrics(s)
                r = score(s, h, inf, sps)
                if r["rsi"] > 35 or r["total"] < 30:
                    continue
                res.append(r)
            except Exception:
                continue
        pg.empty()
        if res:
            res.sort(key=lambda x: x["total"], reverse=True)
            for r in res[:10]:
                with st.expander("**" + r['symbol'] + "** - " + str(r['total']) + "/100"):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("السعر", "$" + str(round(r['price'], 3)))
                    c2.metric("RSI", str(r['rsi']))
                    c3.metric("Stoch", str(r['stoch']))
                    if r["short_pct"] > 0:
                        st.write("Short Float: " + str(round(r["short_pct"]*100, 2)) + "%")
        else:
            st.info("لا فرص حالياً.")

st.markdown("---")
st.caption("تعليمي فقط - ليس توصية استثمارية")
