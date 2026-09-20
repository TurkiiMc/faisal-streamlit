import streamlit as st
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import plotly.graph_objects as go

# ============================================================
# CONFIG
# ============================================================

st.set_page_config(
    page_title="Faisal Screener",
    page_icon="🎯",
    layout="wide",
)

PROXY_URL = "https://turki-proxy.onrender.com"
FINNHUB_KEY = "demn1c9r01qnf8fq7jc0demn1c9r01qnf8fq7jcg"

# ============================================================
# CSS
# ============================================================

st.markdown("""
<style>
    .main { direction: rtl; }
    h1, h2, h3 { direction: rtl; text-align: right; }
    .score-card {
        background: linear-gradient(135deg, #f8f9fa, #e9ecef);
        padding: 25px;
        border-radius: 15px;
        text-align: center;
        margin: 15px 0;
    }
    .score-big { font-size: 56px; font-weight: bold; margin: 0; }
    .verdict { font-size: 22px; margin-top: 10px; color: #2d3436; }
    .stButton > button {
        width: 100%;
        background: linear-gradient(90deg, #00b894, #0984e3);
        color: white;
        font-weight: bold;
        border-radius: 10px;
        padding: 12px;
        font-size: 16px;
        border: none;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# DATA FUNCTIONS
# ============================================================

def yahoo_candles(symbol, period="6mo"):
    headers = {"User-Agent": "Mozilla/5.0"}

    if PROXY_URL:
        try:
            url = f"{PROXY_URL}/yahoo/candles"
            r = requests.get(url, params={"symbol": symbol, "period": period}, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if data.get("success") and data.get("candles"):
                    df = pd.DataFrame(data["candles"])
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.set_index("date").sort_index()
                    df.columns = [c.capitalize() for c in df.columns]
                    return df
        except Exception:
            pass

    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        r = requests.get(url, params={"range": period, "interval": "1d"},
                         headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            result = data.get("chart", {}).get("result", [])
            if result:
                chart = result[0]
                ts = chart.get("timestamp", [ =])
                q = chart["indicators"]["quote"][0]
                rows = []
                for i, t in enumerate(ts):
                    if i < len(q["close"]) and q["close"][i] is not None:
                        rows.append({
                            "Date": pd.Timestamp(t, unit="s"),
                            "Open": q["open"][i],
                            "High": q["high"][i],
 mac                            "Low": q["low"][id],
                            "Close": q["close"][i],
                            "(Volume": q["volume"][i] or 0,
                        })
                ifclose rows:
                    return pd.DataFrame(rows).set_index("Date").sort_index()
    except Exception:
        pass

    return pd.DataFrame()


def finnhub_metrics(symbol):
    info = {"floatShares": 0, "shortPercentOfFloat": 0, "averageVolume": 0}
    if not FINNHUB_KEY:
        return info
    try:
        url = "https://finnhub.io/api/v1/stock/metric"
        r = requests.get(url, params={"symbol": symbol, "metric": "all", "token": FINNHUB_KEY}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            m = data.get("metric", {})
            ff = m.get("freeFloat") or 0
            info["floatShares"] = ff * 1_000_000 if ff and ff < 1000 else ff
            sp = m.get("shortPercentOfFloat") or 0
            info["shortPercentOfFloat"] = sp / 100 if sp > 1 else sp
            av = m.get("10DayAverageVolume") or 0
            info["averageVolume"] = av * 1_000_000 if av and av < 1000 else av
    except Exception:
        pass
    return info


# ============================================================
# INDICATORS
# ============================================================

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
        return False, False
    e12 = close.ewm(span=12, adjust=False).mean()
    e26 = close.ewm(span=26, adjust=False).mean()
    m = e12 - e26
    s = m.ewm(span=9, adjust=False).mean()
    h = m - s
    return m.iloc[-1] > s.iloc[-1], h.iloc[-1] > (h.iloc[-3] if len(h) > 3 else h.iloc[-1])


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


# ============================================================
# SCORING
# ============================================================

def score(symbol, hist, info):
    close, high, low, vol = hist["Close"], hist["High"], hist["Low"], hist["Volume"]
    price = float(close.iloc[-1])
    r = rsi(close)
    mp, mi)
    sk = stoch(high, low, close)
    s20, s30, s50 = sma(close, 20), sma(close, 30), sma(close, 50)
    sup, res = sr(hist, 20)
    fs = info.get("floatShares", 0) or 0
    av = info.get("averageVolume", 0) or 0
    cv = int(vol.iloc[-1]) if len(vol) else 0
    rv = round(cv / av, 2) if av > 0 else 0

    bd = {}

    if 23 <= r <= 27: bd["RSI"] = 25
    elif 20 <= r < 23: bd["RSI"] = 12
    elif 27 < r <= 30: bd["RSI"] = 15
    elif 30 < r <= 35: bd["RSI"] = 6
    else: bd["RSI"] = 0

    bd["Split"] = 0

    if fs:
        if fs < 1_000_000: bd["Float"] = 15
        elif fs < 5_000_000: bd["Float"] = 12
        elif fs < 10_000_000: bd["Float"] = 8
        elif fs < 20_000_000: bd["Float"] = 5
        else: bd["Float"] = 0
    else:
        bd["Float"] = 0

    if mp and mi: bd["MACD"] = 15
    elif mi: bd["MACD"] = 8
    else: bd["MACD"] = 0

    if s20 and s30 and s50:
        b = sum(1 for x in [s20, s30, s50] if price < x)
        bd["MA"] = {3: 15, 2: 8, 1: 5}.get(b, 0)
    else:
        bd["MA"] = 0

    if sk < 20: bd["Stoch"] = 10
    elif sk < 30: bd["Stoch"] = 8
    elif sk < 40: bd["Stoch"] = 6
    elif sk < 50: bd["Stoch"] = 4
    else: bd["Stoch"] = 0

    ds = None
    if sup:
        ds = round((price - sup) / price * 100, 2)
        if ds < 3: bd["Support"] = 10
        elif ds < 5: bd["Support"] = 7
        elif ds < 8: bd["Support"] = 3
        else: bd["Support"] = 0
    else:
        bd["Support"] = 0

    if rv > 5: bd["RVOL"] = 5
    elif rv >= 2: bd["RVOL"] = 3
    else: bd["RVOL"] = 0

    total = sum(bd.values())

    if total >= 80: v, c = "🏆 مثالي", "#00b894"
    elif total >= 65: v, c = "🟢 ممتاز", "#00b894"
    elif total >= 50: v, c = "🟡 جيد", "#fdcb6e"
    elif total >= 35: v, c = "🟠 ضعيف", "#e17055"
    else: v, c = "🔴 مرفوض", "#d63031"

    return {
        "symbol": symbol, "price": price, "rsi": round(r, 2),
        "stoch": round(sk, 2), "macd_pos": mp, "macd_imp": mi,
        "sma20": s20, "sma50": s50, "support": sup, "resistance": res,
        "dist_sup": ds, "float": fs, "rvol": rv,
        "breakdown": bd, "total": total, "verdict": v, "color": c,
    }


# ============================================================
# CHART
# ============================================================

def chart(hist, symbol):
    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=hist.index, open=hist["Open"], high=hist["High"],
        low=hist["Low"], close=hist["Close"], name=symbol,
        increasing_line_color="#00b894",
        decreasing_line_color="#d63031",
    ))
    if len(hist) >= 20:
        fig.add_trace(go.Scatter(
            x=hist.index, y=hist["Close"].rolling(20).mean(),
            name="MA20", line=dict(color="orange", width=1),
        ))
    if len(hist) >= 50:
        fig.add_trace(go.Scatter(
            x=hist.index, y=hist["Close"].rolling(50).mean(),
            name="MA50", line=dict(color="blue", width=1),
        ))
    fig.update_layout(
        height=400, xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=10, t=30, b=10),
        template="plotly_white",
    )
    return fig


# ============================================================
# UI
# ============================================================

st.markdown("# 🎯 Faisal Stock Screener")
st.markdown("**مراقب استراتيجية فيصل**")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["🔬 تحليل سهم", "🎯 أفضل 10", "💣 رادار الاكتشاف"])

with tab1:
    col1, col2 = st.columns([3, 1])
    with col1:
        sym = st.text_input("رمز السهم", "AAPL").upper()
    with col2:
        st.write(""); st.write("")
        btn = st.button("🔬 تحليل", key="a")

    if btn and sym:
        with st.spinner(f"جاري تحليل {sym}..."):
            hist = yahoo_candles(sym, "6mo")
            if hist.empty:
                st.error(f"❌ لا بيانات لـ {sym}")
            else:
                info = finnhub_metrics(sym)
                if not info["averageVolume"] and len(hist) >= 20:
                    info["averageVolume"] = int(hist["Volume"].tail(20).mean())
                r = score(sym, hist, info)

                st.markdown(f"""
                <div class="score-card">
                    <div class="score-big" style="color: {r['color']};">{r['total']}/100</div>
                    <div class="verdict">{r['verdict']}</div>
                </div>
                """, unsafe_allow_html=True)

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("💰 السعر", f"${r['price']:.3f}")
                c2.metric("📊 RSI", f"{r['rsi']}")
                c3.metric("📈 Stoch", f"{r['stoch']}")
                c4.metric("📊 RVOL", f"{r['rvol']}")

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("📉 MA20", f"${r['sma20']:.2f}" if r['sma20'] else "—")
                c2.metric("📉 MA50", f"${r['sma50']:.2f}" if r['sma50'] else "—")
                c3.metric("📏 الدعم", f"${r['support']:.3f}" if r['support'] else "—")
                c4.metric("🚀 المقاومة", f"${r['resistance']:.3f}" if r['resistance'] else "—")

                if r["float"]:
                    st.info(f"📦 Free Float: **{r['float']/1_000_000:.2f}M سهم**")

                st.plotly_chart(chart(hist, sym), use_container_width=True)

                st.markdown("### 📊 تفصيل النقاط")
                st.dataframe(
                    pd.DataFrame(list(r["breakdown"].items()), columns=["المعيار", "النقاط"]),
                    use_container_width=True, hide_index=True
                )

with tab2:
    st.markdown("### 🎯 أفضل 10 أسهم")
    st.caption("فحص ~30 سهماً من قائمة مختارة")

    if st.button("🔍 ابدأ الفحص", key="t"):
        U = ["AEMD","AKAN","BFRG","BIAF","BNKK","CDTG","CLIK","CPOP",
             "DGHG","DXST","GWAV","HTCR","LFS","MBRX","MWC","NXTS",
             "SVRE","VSME","YYAI","SHPH","SONN","TNXP","PHIO","SNPX",
             "AVGR","BDRX","BIOR","CLRB","CRKN","CYTX"]

        pg = st.progress(0)
        res = []
        for i, s in enumerate(U):
            pg.progress((i + 1) / len(U))
            try:
                h = yahoo_candles(s, "3mo")
                if h.empty or len(h) < 30:
                    continue
                inf = finnhub_metrics(s)
                if not inf["averageVolume"] and len(h) >= 20:
                    inf["averageVolume"] = int(h["Volume"].tail(20).mean())
                r = score(s, h, inf)
                if r["total"] >= 30:
                    res.append(r)
            except Exception:
                continue
        pg.empty()

        if res:
            res.sort(key=lambda x: x["total"], reverse=True)
            for i, r in enumerate(res[:10], 1):
                m = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "•"
                with st.expander(f"{m} **{r['symbol']}** — {r['total']}/100 {r['verdict']}"):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("السعر", f"${r['price']:.3f}")
                    c2.metric("RSI", r['rsi'])
                    c3.metric("Stoch", r['stoch'])
                    if r["support"]:
                        st.write(f"📏 الدعم: ${r['support']:.3f} (على بعد {r['dist_sup']}%)")
        else:
            st.warning("لا نتائج.")

with tab3:
    st.markdown("### 💣 رادار الاكتشاف المبكر")
    st.caption("أسهم Squeeze محتملة")

    if st.button("🎯 ابحث", key="h"):
        U = ["AEMD","AKAN","BFRG","BIAF","BNKK","CDTG","CLIK","CPOP",
             "DGHG","DXST","GWAV","HTCR","LFS","MBRX","MWC","NXTS",
             "SVRE","VSME","YYAI","SHPH","SONN","TNXP","PHIO","SNPX"]

        pg = st.progress(0)
        res = []
        for i, s in enumerate(U):
            pg.progress((i + 1) / len(U))
            try:
                h = yahoo_candles(s, "3mo")
                if h.empty or len(h) < 30:
                    continue
                inf = finnhub_metrics(s)
                if not inf["averageVolume"] and len(h) >= 20:
                    inf["averageVolume"] = int(h["Volume"].tail(20).mean())
                r = score(s, h, inf)
                if r["rsi"] > 35 or r["total"] < 30:
                    continue
                res.append(r)
            except Exception:
                continue
        pg.empty()

        if res:
            res.sort(key=lambda x: x["total"], reverse=True)
            for r in res[:10]:
                with st.expander(f"💣 **{r['symbol']}** — {r['total']}/100"):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("السعر", f"${r['price']:.3f}")
                    c2.metric("RSI", r['rsi'])
                    c3.metric("Stoch", r['stoch'])
        else:
            st.info("لا فرص حالياً.")

st.markdown("---")
st.caption("⚠️ تعليمي فقط — ليس توصية استثمارية")
