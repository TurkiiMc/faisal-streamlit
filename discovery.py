"""
Finnhub Discovery Engine v2 — مسح استباقي كل 6 ساعات + نتائج فورية عبر /latest
"""
import os
import time
import json
import requests
import threading
import io
import csv
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

# ===== الإعدادات =====
FINNHUB = os.environ.get("FINNHUB_KEY", "")
PROXY = os.environ.get("PROXY_URL", "https://faisal-proxy.onrender.com").rstrip("/")
TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")
BOT_URL = os.environ.get("BOT_URL", "").rstrip("/")
CORE_LIST_RAW = os.environ.get("CORE_LIST", "")

POOL_FILE = Path("universe_pool.json")
DISC_FILE = Path("discoveries.json")
LAST_SCAN = Path("last_scan.json")
ET = ZoneInfo("America/New_York")
SCAN_EVERY_HOURS = 6

# ===== Rate Limiter =====
class Limiter:
    def __init__(self, rpm=55):
        self.rpm = rpm
        self.times = []
        self.lock = threading.Lock()
    
    def wait(self):
        with self.lock:
            now = time.time()
            self.times = [t for t in self.times if now - t < 60]
            if len(self.times) >= self.rpm:
                time.sleep(60 - (now - self.times[0]) + 0.5)
            self.times.append(time.time())

lim = Limiter(55)

# ===== Finnhub API =====
def fh_get(path, params=None):
    if not FINNHUB:
        return None
    lim.wait()
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1{path}",
            params={**(params or {}), "token": FINNHUB},
            timeout=20
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def all_us_symbols():
    """جلب كل الرموز الأمريكية من Finnhub"""
    data = fh_get("/stock/symbol", {"exchange": "US"})
    if not data:
        return []
    ok_mic = {"XNAS", "XNYS", "ARCX", "BATS", "XASE"}
    out = []
    for s in data:
        if s.get("type") != "Common Stock":
            continue
        if s.get("mic") not in ok_mic:
            continue
        sym = s.get("symbol")
        if sym:
            out.append(sym)
    return out

def sec_tickers():
    """بديل: جلب الرموز من SEC"""
    try:
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "FaisalDiscovery faisal@example.com"},
            timeout=30
        )
        if r.status_code == 200:
            return [v["ticker"].upper() for v in r.json().values()]
    except Exception:
        pass
    return []

def metrics(sym):
    m = fh_get("/stock/metric", {"symbol": sym, "metric": "all"})
    return (m or {}).get("metric", {})

def quote(sym):
    return fh_get("/quote", {"symbol": sym}) or {}

def news(sym, days=7):
    today = datetime.now().strftime("%Y-%m-%d")
    frm = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return fh_get("/company-news", {"symbol": sym, "from": frm, "to": today}) or []

# ===== البروكسي =====
def candles(sym, period="6mo"):
    try:
        r = requests.get(
            PROXY + "/yahoo/candles",
            params={"symbol": sym, "period": period},
            timeout=60
        )
        if r.status_code == 200:
            d = r.json()
            if d.get("success") and d.get("candles"):
                return d["candles"]
    except Exception:
        pass
    return []

def stooq_bulk(symbols):
    """اقتباسات مجمعة من stooq"""
    out = {}
    for i in range(0, len(symbols), 150):
        chunk = symbols[i:i+150]
        q = ",".join(s.lower() + ".us" for s in chunk)
        try:
            r = requests.get(
                "https://stooq.com/q/l/",
                params={"s": q, "f": "sd2t2ohlcv", "h": "1", "e": "csv"},
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0"}
            )
            if r.status_code != 200:
                continue
            for row in csv.DictReader(io.StringIO(r.text)):
                sym = (row.get("Symbol") or "").upper().replace(".US", "")
                try:
                    c = float(row.get("Close") or 0)
                    v = float(row.get("Volume") or 0)
                except Exception:
                    continue
                if c > 0:
                    out[sym] = {"close": c, "volume": v}
        except Exception:
            continue
        time.sleep(0.5)
    return out

def proxy_bulk(symbols):
    """اقتباسات مجمعة من البروكسي"""
    out = {}
    for i in range(0, len(symbols), 200):
        chunk = symbols[i:i+200]
        try:
            r = requests.get(
                PROXY + "/yahoo/last",
                params={"symbols": ",".join(chunk)},
                timeout=120
            )
            if r.status_code == 200:
                out.update(r.json().get("quotes", {}))
        except Exception:
            pass
        time.sleep(0.5)
    return out

# ===== الحسابات الفنية =====
def rsi(closes, p=14):
    if len(closes) < p + 1:
        return 50.0
    d = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    g = [max(x, 0) for x in d[-p:]]
    l = [max(-x, 0) for x in d[-p:]]
    ag = sum(g) / p
    al = sum(l) / p
    if al == 0:
        return 100.0
    return 100 - (100 / (1 + ag / al))

def sma(closes, p):
    return sum(closes[-p:]) / p if len(closes) >= p else None

def support(cs, w=20):
    if len(cs) < w:
        return None
    return min(c["low"] for c in cs[-w:])

def detect_failed_spike(cs):
    if len(cs) < 20:
        return False
    rec = cs[-20:]
    mx = max(c["high"] for c in rec)
    mn = min(c["low"] for c in rec)
    cur = cs[-1]["close"]
    if mn <= 0:
        return False
    return (mx - mn) / mn * 100 > 50 and (mx - cur) / mx * 100 > 40

def detect_bull_trap(cs):
    if len(cs) < 25:
        return False
    res = max(c["high"] for c in cs[-20:])
    if not any(c["high"] > res * 0.98 for c in cs[-5:]):
        return False
    return cs[-1]["close"] < res * 0.97

def stability(cs, sup):
    if not sup or len(cs) < 5:
        return 0
    th = sup * 0.98
    held = 0
    for c in reversed(cs[-5:]):
        if c["close"] >= th:
            held += 1
        else:
            break
    return held

def _txt(items):
    return [
        ((it.get("headline") or "") + " " + (it.get("summary") or "")).lower()
        for it in items[:10]
    ]

def is_critical_news(items):
    k = ["bankruptcy", "delisting", "delisted", "fraud", "sec investigation",
         "trading halt", "chapter 11", "going concern"]
    return any(any(w in t for w in k) for t in _txt(items))

def has_offering_news(items):
    k = ["public offering", "private placement", "dilution", "shelf offering", "atm offering"]
    return any(any(w in t for w in k) for t in _txt(items))

def is_positive_news(items):
    k = ["approval", "contract", "award", "partnership", "patent", "acquisition",
         "phase 2", "phase 3", "positive results", "buyback", "insider buying"]
    return any(
        any(w in t for w in k)
        for t in [(it.get("headline") or "").lower() for it in items[:10]]
    )

# ===== التقييم =====
def score(sym, m, q, cs, nws):
    price = q.get("c", 0) or 0
    if price <= 0 or not cs or len(cs) < 30:
        return None
    
    fs_raw = m.get("freeFloat") or 0
    fs = fs_raw * 1e6 if fs_raw and fs_raw < 1000 else fs_raw
    sp_raw = m.get("shortPercentOfFloat") or 0
    sp = sp_raw / 100 if sp_raw > 1 else sp_raw
    mc_raw = m.get("marketCapitalization") or 0
    mc = mc_raw * 1e6 if mc_raw and mc_raw < 100000 else mc_raw

    if not (1_000_000 <= mc <= 20_000_000):
        return None
    if not (0.5 <= price <= 5.0):
        return None
    if not (0 < fs <= 5_000_000):
        return None
    if sp < 0.10:
        return None

    closes = [c["close"] for c in cs]
    r = rsi(closes)
    sup = support(cs)
    s20 = sma(closes, 20)
    s50 = sma(closes, 50)
    
    if detect_failed_spike(cs) or detect_bull_trap(cs):
        return None
    if sup and price < sup * 0.97:
        return None
    if is_critical_news(nws) or has_offering_news(nws):
        return None

    pts = 0
    if 23 <= r <= 27:
        pts += 25
    elif 20 <= r < 23 or 27 < r <= 30:
        pts += 15
    elif 30 < r <= 35:
        pts += 10
    elif 45 <= r <= 57:
        pts += 5
    
    if fs < 1e6:
        pts += 15
    elif fs < 5e6:
        pts += 12
    
    if sp > 0.40:
        pts += 20
    elif sp > 0.30:
        pts += 15
    elif sp > 0.20:
        pts += 10
    
    if s20 and price < s20:
        pts += 5
    if s50 and price < s50:
        pts += 5
    
    stab = stability(cs, sup) if sup else 0
    if stab >= 3:
        pts += 10
    elif stab >= 2:
        pts += 7
    
    if is_positive_news(nws):
        pts += 10
    
    if pts < 50:
        return None

    eff = max(sp, (m.get("sharesShort") or 0) / fs if fs else 0)
    fuel = "packed" if eff >= 0.30 else ("present" if eff >= 0.10 else "none")
    
    return {
        "sym": sym,
        "price": round(price, 3),
        "rsi": round(r, 1),
        "score": pts,
        "fuel": fuel,
        "sup": round(sup, 3) if sup else None,
        "dist_sup": round((price - sup) / sup * 100, 1) if sup else None,
        "float_m": round(fs / 1e6, 2),
        "short_pct": round(eff * 100, 1),
        "stab": stab,
        "positive_news": is_positive_news(nws)
    }

# ===== بناء الـ Pool =====
def build_pool(force=False):
    if POOL_FILE.exists() and not force:
        age_h = (time.time() - POOL_FILE.stat().st_mtime) / 3600
        if age_h < 144:
            return json.loads(POOL_FILE.read_text())
    
    print("[pool] بناء قائمة أسبوعية...")
    
    syms = all_us_symbols()
    if not syms:
        print("[pool] Finnhub symbols فارغ — SEC بديلاً")
        syms = sec_tickers()
    
    print(f"[pool] {len(syms)} رمز خام")
    
    quotes = stooq_bulk(syms)
    if not quotes:
        print("[pool] stooq صامت — البروكسي بديلاً")
        q2 = proxy_bulk(syms)
        quotes = {
            k: {"close": v.get("price", 0), "volume": v.get("volume", 0)}
            for k, v in q2.items()
        }
    
    print(f"[pool] {len(quotes)} اقتباس")
    
    if quotes:
        pre = [
            s for s, q in quotes.items()
            if 0.5 <= q.get("close", 0) <= 5.0 and q.get("volume", 0) >= 50_000
        ]
    else:
        pre = []
    
    print(f"[pool] {len(pre)} اجتاز السعر+الحجم")
    
    pool = []
    for i, s in enumerate(pre):
        if i % 50 == 0:
            print(f"[pool] metrics {i}/{len(pre)}")
        m = metrics(s)
        if not m:
            continue
        mc_raw = m.get("marketCapitalization") or 0
        mc = mc_raw * 1e6 if mc_raw and mc_raw < 100000 else mc_raw
        if 1_000_000 <= mc <= 20_000_000:
            pool.append(s)
    
    core = [
        t.strip().upper()
        for t in CORE_LIST_RAW.replace(";", ",").split(",")
        if t.strip()
    ]
    pool = list(dict.fromkeys(pool + core))
    
    print(f"[pool] نهائي مع CORE_LIST: {len(pool)}")
    POOL_FILE.write_text(json.dumps(pool))
    
    return pool

# ===== المسح اليومي =====
def daily_scan():
    pool = build_pool()
    print(f"[scan] فحص {len(pool)} مرشح...")
    
    disc = []
    for i, sym in enumerate(pool):
        if i % 20 == 0:
            print(f"[scan] {i}/{len(pool)}")
        
        m = metrics(sym)
        q = quote(sym)
        if not m or not q:
            continue
        
        cs = candles(sym, "6mo")
        nws = news(sym, 7)
        r = score(sym, m, q, cs, nws)
        if r:
            disc.append(r)
    
    disc.sort(
        key=lambda x: (
            -x["score"],
            -{"packed": 3, "present": 2, "none": 0}.get(x["fuel"], 1)
        )
    )
    disc = disc[:30]
    
    DISC_FILE.write_text(
        json.dumps({
            "date": datetime.now(ET).isoformat(),
            "count": len(disc),
            "items": disc
        }, indent=2, ensure_ascii=False)
    )
    LAST_SCAN.write_text(json.dumps({"ts": time.time()}))
    
    print(f"[scan] {len(disc)} اكتشاف")
    return disc

# ===== إرسال لتليجرام =====
def send_tg(msg):
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=15
        )
    except Exception as e:
        print("[tg]", e)

def auto_watchlist(disc):
    if not BOT_URL:
        return
    added = 0
    for d in disc[:15]:
        sup = d.get("sup")
        if not sup:
            continue
        try:
            r = requests.post(
                BOT_URL + "/add",
                json={"sym": d["sym"], "sup": sup, "res": round(sup * 1.3, 3)},
                timeout=15
            )
            if r.status_code == 200 and r.json().get("ok"):
                added += 1
        except Exception:
            pass
        time.sleep(0.5)
    if added:
        print(f"[watch] أُضيف {added} سهماً لقائمة البوت")

def notify(disc):
    if not disc:
        send_tg("🛰️ <b>مسح اليوم</b>: لا اكتشافات مكتملة المعادلة")
        return
    
    packed = [d for d in disc if d["fuel"] == "packed"]
    present = [d for d in disc if d["fuel"] == "present"]
    
    lines = [f"🛰️ <b>اكتشافات اليوم ({len(disc)})</b>", ""]
    
    if packed:
        lines.append(f"🚀 <b>وقود محشور ({len(packed)}):</b>")
        for d in packed[:8]:
            ne = "+" if d["positive_news"] else ""
            lines.append(
                f"• <b>{d['sym']}</b>: {d['score']}/100 | ${d['price']} | "
                f"RSI {d['rsi']} | شورت {d['short_pct']}% {ne}"
            )
    
    if present:
        lines.append(f"\n⛽ <b>وقود متوسط ({len(present)}):</b>")
        for d in present[:8]:
            ne = "+" if d["positive_news"] else ""
            lines.append(
                f"• <b>{d['sym']}</b>: {d['score']}/100 | ${d['price']} | "
                f"RSI {d['rsi']} | شورت {d['short_pct']}% {ne}"
            )
    
    lines.append("\n💡 أرسل /s SYM للتحليل الكامل")
    send_tg("\n".join(lines))

# ===== الجدولة =====
def last_scan_age_hours():
    if not LAST_SCAN.exists():
        return 999
    try:
        return (time.time() - json.loads(LAST_SCAN.read_text())["ts"]) / 3600
    except Exception:
        return 999

def should_scan():
    now = datetime.now(ET)
    # ✅ تم إزالة شرط عطلة نهاية الأسبوع — يعمل كل يوم
    if now.hour < 6 or now.hour >= 22:
        return False
    return last_scan_age_hours() >= SCAN_EVERY_HOURS

def scheduler_loop():
    while True:
        try:
            if should_scan():
                print("[sched] بدء المسح الدوري")
                disc = daily_scan()
                notify(disc)
                auto_watchlist(disc)
                print("[sched] اكتمل المسح")
        except Exception as e:
            print("[sched] error:", e)
        time.sleep(900)

def run_once():
    disc = daily_scan()
    notify(disc)
    auto_watchlist(disc)
    return disc

# ===== FastAPI =====
app = FastAPI()

@app.get("/ping", response_class=PlainTextResponse)
def ping():
    return "pong"

@app.get("/health")
def health():
    return {
        "ok": True,
        "last_scan_hours_ago": round(last_scan_age_hours(), 1)
    }

@app.get("/")
def root():
    n = 0
    if POOL_FILE.exists():
        try:
            n = len(json.loads(POOL_FILE.read_text()))
        except Exception:
            pass
    return {
        "engine": "finnhub-discovery-v2",
        "pool": n,
        "last_scan_hours_ago": round(last_scan_age_hours(), 1)
    }

@app.get("/latest")
def latest():
    if not DISC_FILE.exists():
        return {
            "ok": False,
            "msg": "لا مسح سابق بعد — أول مسح تلقائي يعمل",
            "items": []
        }
    try:
        data = json.loads(DISC_FILE.read_text())
        return {
            "ok": True,
            "date": data.get("date"),
            "count": data.get("count", 0),
            "age_hours": round(last_scan_age_hours(), 1),
            "items": data.get("items", [])
        }
    except Exception:
        return {"ok": False, "items": []}

@app.post("/scan")
def trigger_scan():
    disc = run_once()
    return {"discoveries": len(disc), "items": disc[:10]}

# ===== بدء الجدولة التلقائية =====
if os.environ.get("START_SCHEDULER", "1") == "1":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    print("[init] الجدولة التلقائية مفعّلة")
