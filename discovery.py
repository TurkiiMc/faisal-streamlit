"""
Finnhub Discovery Engine — مسح ذاتي للسوق الأمريكي
يُشغَّل يومياً عبر cronjob داخلي ويُرسل الاكتشافات للبوت والرادار.
"""
import os, time, json, requests, threading
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

FINNHUB = os.environ.get("FINNHUB_KEY", "")
PROXY   = os.environ.get("PROXY_URL", "https://faisal-proxy.onrender.com").rstrip("/")
TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TG_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")

POOL_FILE = Path("universe_pool.json")
DISC_FILE = Path("discoveries.json")
LAST_SCAN = Path("last_scan.json")
ET = ZoneInfo("America/New_York")

# ===== Rate limiter =====
class Limiter:
    def __init__(self, rpm=55): self.rpm, self.times, self.lock = rpm, [], threading.Lock()
    def wait(self):
        with self.lock:
            now = time.time()
            self.times = [t for t in self.times if now - t < 60]
            if len(self.times) >= self.rpm:
                time.sleep(60 - (now - self.times[0]) + 0.5)
            self.times.append(time.time())
lim = Limiter(55)

# ===== Finnhub primitives =====
def fh_get(path, params=None):
    if not FINNHUB: return None
    lim.wait()
    try:
        r = requests.get(f"https://finnhub.io/api/v1{path}",
                         params={**(params or {}), "token": FINNHUB}, timeout=20)
        if r.status_code == 200: return r.json()
    except Exception: pass
    return None

def all_us_symbols():
    """قائمة كل الرموز الأمريكية (~8000)"""
    data = fh_get("/stock/symbol", {"exchange": "US"})
    if not data: return []
    return [s.get("symbol") for s in data if s.get("symbol")]

def metrics(sym):
    m = fh_get("/stock/metric", {"symbol": sym, "metric": "all"})
    return (m or {}).get("metric", {})

def quote(sym):
    return fh_get("/quote", {"symbol": sym}) or {}

def news(sym, days=7):
    today = datetime.now().strftime("%Y-%m-%d")
    frm = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return fh_get("/company-news", {"symbol": sym, "from": frm, "to": today}) or []

# ===== الشموع من البروكسي =====
def candles(sym, period="6mo"):
    try:
        r = requests.get(PROXY + "/yahoo/candles",
                         params={"symbol": sym, "period": period}, timeout=60)
        if r.status_code == 200:
            d = r.json()
            if d.get("success") and d.get("candles"):
                return d["candles"]
    except Exception: pass
    return []

# ===== الحسابات =====
def rsi(closes, p=14):
    if len(closes) < p + 1: return 50.0
    d = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    g = [max(x, 0) for x in d[-p:]]; l = [max(-x, 0) for x in d[-p:]]
    ag = sum(g) / p; al = sum(l) / p
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag / al))

def sma(closes, p):
    return sum(closes[-p:]) / p if len(closes) >= p else None

def support(candles, w=20):
    if len(candles) < w: return None
    return min(c["low"] for c in candles[-w:])

def detect_failed_spike(candles):
    if len(candles) < 20: return False
    rec = candles[-20:]
    mx = max(c["high"] for c in rec); mn = min(c["low"] for c in rec)
    cur = candles[-1]["close"]
    if mn <= 0: return False
    sp = (mx - mn) / mn * 100
    dr = (mx - cur) / mx * 100
    return sp > 50 and dr > 40

def detect_bull_trap(candles):
    if len(candles) < 25: return False
    res = max(c["high"] for c in candles[-20:])
    rec = candles[-5:]
    if not any(c["high"] > res * 0.98 for c in rec): return False
    return candles[-1]["close"] < res * 0.97

def stability(candles, sup):
    if not sup or len(candles) < 5: return 0
    th = sup * 0.98
    held = 0
    for c in reversed(candles[-5:]):
        if c["close"] >= th: held += 1
        else: break
    return held

def is_critical_news(items):
    crit = ["bankruptcy", "delisting", "delisted", "fraud", "sec investigation",
            "trading halt", "chapter 11", "going concern"]
    for it in items[:10]:
        t = ((it.get("headline") or "") + " " + (it.get("summary") or "")).lower()
        if any(k in t for k in crit): return True
    return False

def has_offering_news(items):
    off = ["public offering", "private placement", "dilution", "shelf offering", "atm offering"]
    for it in items[:10]:
        t = ((it.get("headline") or "") + " " + (it.get("summary") or "")).lower()
        if any(k in t for k in off): return True
    return False

def is_positive_news(items):
    pos = ["approval", "contract", "award", "partnership", "patent", "acquisition",
           "phase 2", "phase 3", "positive results", "buyback", "insider buying"]
    for it in items[:10]:
        t = (it.get("headline") or "").lower()
        if any(k in t for k in pos): return True
    return False

# ===== النقاط =====
def score(sym, m, q, cs, nws):
    price = q.get("c", 0) or 0
    if price <= 0 or not cs or len(cs) < 30: return None

    fs_raw = m.get("freeFloat") or 0
    fs = fs_raw * 1e6 if fs_raw and fs_raw < 1000 else fs_raw
    sp_raw = m.get("shortPercentOfFloat") or 0
    sp = sp_raw / 100 if sp_raw > 1 else sp_raw
    mc_raw = m.get("marketCapitalization") or 0
    mc = mc_raw * 1e6 if mc_raw and mc_raw < 100000 else mc_raw

    # البوابات الصلبة
    if not (1_000_000 <= mc <= 20_000_000): return None
    if not (0.5 <= price <= 5.0): return None
    if not (0 < fs <= 5_000_000): return None
    if sp < 0.10: return None  # وقود مقبول على الأقل

    closes = [c["close"] for c in cs]
    r = rsi(closes)
    sup = support(cs)
    s20 = sma(closes, 20)
    s50 = sma(closes, 50)
    failed = detect_failed_spike(cs)
    bull = detect_bull_trap(cs)
    stab = stability(cs, sup) if sup else 0

    # الإقصاءات
    if failed or bull: return None
    if sup and price < sup * 0.97: return None
    if is_critical_news(nws) or has_offering_news(nws): return None

    pts = 0
    if 23 <= r <= 27: pts += 25
    elif 20 <= r < 23 or 27 < r <= 30: pts += 15
    elif 30 < r <= 35: pts += 10
    elif 45 <= r <= 57: pts += 5

    if fs < 1e6: pts += 15
    elif fs < 5e6: pts += 12
    if sp > 0.40: pts += 20
    elif sp > 0.30: pts += 15
    elif sp > 0.20: pts += 10
    if s20 and price < s20: pts += 5
    if s50 and price < s50: pts += 5
    if stab >= 3: pts += 10
    elif stab >= 2: pts += 7
    if is_positive_news(nws): pts += 10

    if pts < 50: return None

    # الوقود
    eff = max(sp, (m.get("sharesShort") or 0) / fs if fs else 0)
    if eff >= 0.30: fuel = "packed"
    elif eff >= 0.10: fuel = "present"
    else: fuel = "none"

    dist_sup = round((price - sup) / sup * 100, 1) if sup else None

    return {
        "sym": sym, "price": round(price, 3), "rsi": round(r, 1),
        "score": pts, "fuel": fuel, "sup": round(sup, 3) if sup else None,
        "dist_sup": dist_sup, "float_m": round(fs / 1e6, 2),
        "short_pct": round(eff * 100, 1), "stab": stab,
        "positive_news": is_positive_news(nws),
    }

# ===== بناء pool أسبوعي =====
def build_pool(force=False):
    if POOL_FILE.exists() and not force:
        age_h = (time.time() - POOL_FILE.stat().st_mtime) / 3600
        if age_h < 144:  # أقل من 6 أيام
            return json.loads(POOL_FILE.read_text())
    print("[pool] بناء قائمة أسبوعية جديدة...")
    syms = all_us_symbols()
    print(f"[pool] {len(syms)} رمز من Finnhub")
    pool = []
    for i, s in enumerate(syms):
        if i % 100 == 0: print(f"[pool] {i}/{len(syms)}")
        m = metrics(s); q = quote(s)
        if not m or not q: continue
        p = q.get("c", 0) or 0
        if not (0.5 <= p <= 5.0): continue
        mc_raw = m.get("marketCapitalization") or 0
        mc = mc_raw * 1e6 if mc_raw and mc_raw < 100000 else mc_raw
        if not (1_000_000 <= mc <= 20_000_000): continue
        pool.append(s)
    POOL_FILE.write_text(json.dumps(pool))
    print(f"[pool] حفظ {len(pool)} مرشح")
    return pool

# ===== المسح اليومي =====
def daily_scan():
    pool = build_pool()
    print(f"[scan] فحص {len(pool)} مرشح...")
    discoveries = []
    for i, sym in enumerate(pool):
        if i % 20 == 0: print(f"[scan] {i}/{len(pool)}")
        m = metrics(sym)
        q = quote(sym)
        if not m or not q: continue
        cs = candles(sym, "6mo")
        nws = news(sym, 7)
        r = score(sym, m, q, cs, nws)
        if r: discoveries.append(r)
    discoveries.sort(key=lambda x: (-x["score"], -({"packed": 3, "present": 2, "none": 0}.get(x["fuel"], 1))))
    discoveries = discoveries[:30]
    DISC_FILE.write_text(json.dumps({
        "date": datetime.now(ET).isoformat(),
        "count": len(discoveries),
        "items": discoveries,
    }, indent=2, ensure_ascii=False))
    LAST_SCAN.write_text(json.dumps({"ts": time.time()}))
    print(f"[scan] {len(discoveries)} اكتشاف")
    return discoveries

# ===== إرسال للبوت =====
def send_tg(msg):
    if not (TG_TOKEN and TG_CHAT): return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
                      timeout=15)
    except Exception as e: print("[tg]", e)

def auto_watchlist(disc):
    """إضافة تلقائية للقائمة بـ /w"""
    if not TG_TOKEN: return
    for d in disc[:15]:
        sup = d.get("sup")
        if not sup: continue
        msg = f"/w {d['sym']}:{sup}:{round(sup * 1.3, 3)}"
        # إرسال كأمر عادي — البوت سيُعالجه ويضيفه
        send_tg(msg)
        time.sleep(1)

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
            ne = "📰+" if d["positive_news"] else ""
            lines.append(f"• <b>{d['sym']}</b>: {d['score']}/100 | ${d['price']} | RSI {d['rsi']} | شورت {d['short_pct']}% {ne}")
    if present:
        lines.append(f"\n⛽ <b>وقود متوسط ({len(present)}):</b>")
        for d in present[:8]:
            ne = "📰+" if d["positive_news"] else ""
            lines.append(f"• <b>{d['sym']}</b>: {d['score']}/100 | ${d['price']} | RSI {d['rsi']} | شورت {d['short_pct']}% {ne}")
    lines.append(f"\n💡 أرسل /s SYM للتحليل الكامل لأي رمز")
    send_tg("\n".join(lines))

# ===== الجدولة =====
def last_scan_age_hours():
    if not LAST_SCAN.exists(): return 999
    try: return (time.time() - json.loads(LAST_SCAN.read_text())["ts"]) / 3600
    except Exception: return 999

def should_scan():
    now = datetime.now(ET)
    if now.weekday() >= 5: return False  # weekend
    if now.hour < 6 or now.hour >= 20: return False
    return last_scan_age_hours() >= 18

def scheduler_loop():
    while True:
        try:
            if should_scan():
                print("[sched] بدء المسح اليومي")
                disc = daily_scan()
                notify(disc)
                auto_watchlist(disc)
                print("[sched] اكتمل المسح")
        except Exception as e:
            print("[sched] error:", e)
        time.sleep(900)  # 15 دقيقة

def run_once():
    """للتشغيل اليدوي أو الاختبار"""
    disc = daily_scan()
    notify(disc)
    auto_watchlist(disc)
    return disc

# ===== FastAPI =====
from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"engine": "finnhub-discovery", "pool": len(build_pool(force=False)),
            "last_scan_hours_ago": round(last_scan_age_hours(), 1)}

@app.get("/health")
def health():
    return {"ok": True, "last_scan_hours_ago": round(last_scan_age_hours(), 1)}

@app.post("/scan")
def trigger_scan():
    disc = run_once()
    return {"discoveries": len(disc), "items": disc[:10]}

# بدء الخيط
if os.environ.get("START_SCHEDULER", "1") == "1":
    threading.Thread(target=scheduler_loop, daemon=True).start()
