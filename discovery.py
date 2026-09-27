"""
Finnhub Discovery Engine v4 — تشخيصي + تسامحي + سريع
"""
import os, time, json, requests, threading, io, csv
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

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
POOL_CAP = 400
MIN_SCORE = 40  # ✅ خُفِّض من 50

class Limiter:
    def __init__(self, rpm=55):
        self.rpm, self.times, self.lock = rpm, [], threading.Lock()
    def wait(self):
        with self.lock:
            now = time.time()
            self.times = [t for t in self.times if now - t < 60]
            if len(self.times) >= self.rpm:
                time.sleep(60 - (now - self.times[0]) + 0.5)
            self.times.append(time.time())
lim = Limiter(55)

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
    data = fh_get("/stock/symbol", {"exchange": "US"})
    if not data: return []
    ok_mic = {"XNAS", "XNYS", "ARCX", "BATS", "XASE"}
    return [s["symbol"] for s in data
            if s.get("type") == "Common Stock"
            and s.get("mic") in ok_mic and s.get("symbol")]

def sec_tickers():
    try:
        r = requests.get("https://www.sec.gov/files/company_tickers.json",
                         headers={"User-Agent": "FaisalDiscovery f@example.com"}, timeout=30)
        if r.status_code == 200:
            return [v["ticker"].upper() for v in r.json().values()]
    except Exception: pass
    return []

def metrics(sym):
    m = fh_get("/stock/metric", {"symbol": sym, "metric": "all"})
    return (m or {}).get("metric", {})

def news(sym, days=7):
    today = datetime.now().strftime("%Y-%m-%d")
    frm = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return fh_get("/company-news", {"symbol": sym, "from": frm, "to": today}) or []

def candles(sym, period="6mo"):
    """جلب الشموع مع إعادة محاولة تلقائية"""
    for attempt in range(2):
        try:
            r = requests.get(PROXY + "/yahoo/candles",
                             params={"symbol": sym, "period": period}, timeout=60)
            if r.status_code == 200:
                d = r.json()
                if d.get("success") and d.get("candles"):
                    return d["candles"]
        except Exception: pass
        if attempt == 0: time.sleep(2)
    return []

# ===== تطبيع آمن للوحدات =====
def normalize_metrics(m):
    """يعيد metrics مُطبّعة بوحدات ثابتة (أرقام مطلقة)"""
    if not m: return None
    fs_raw = m.get("freeFloat") or 0
    # Finnhub freeFloat = نسبة مئوية (0-100) أو عدد أسهم — نحدد بالأصل
    if 0 < fs_raw < 100:
        fs = fs_raw * 1e6  # نسبة → أسهم
    elif fs_raw > 100:
        fs = fs_raw  # عدد أسهم مباشرة
    else:
        fs = 0

    sp_raw = m.get("shortPercentOfFloat") or 0
    sp = sp_raw / 100 if sp_raw > 1 else sp_raw

    mc_raw = m.get("marketCapitalization") or 0
    if mc_raw > 0 and mc_raw < 100000:
        mc = mc_raw * 1e6  # مليون → دولارات
    else:
        mc = mc_raw

    return {"fs": fs, "sp": sp, "sh": m.get("sharesShort") or 0, "mc": mc}

# ===== اقتباسات =====
def stooq_probe(symbols_150):
    q = ",".join(s.lower() + ".us" for s in symbols_150)
    try:
        r = requests.get("https://stooq.com/q/l/",
                         params={"s": q, "f": "sd2t2ohlcv", "h": "1", "e": "csv"},
                         timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200: return {}
        out = {}
        for row in csv.DictReader(io.StringIO(r.text)):
            sym = (row.get("Symbol") or "").upper().replace(".US", "")
            try:
                c = float(row.get("Close") or 0)
                v = float(row.get("Volume") or 0)
            except Exception: continue
            if c > 0: out[sym] = {"close": c, "volume": v}
        return out
    except Exception:
        return {}

def proxy_bulk(symbols):
    out = {}
    chunks = [symbols[i:i+150] for i in range(0, len(symbols), 150)]
    for ci, chunk in enumerate(chunks):
        try:
            r = requests.get(PROXY + "/yahoo/last",
                             params={"symbols": ",".join(chunk)}, timeout=120)
            if r.status_code == 200:
                out.update(r.json().get("quotes", {}))
        except Exception: pass
        if ci % 4 == 0 or ci == len(chunks) - 1:
            print(f"[pool] بروكسي دفعة {ci+1}/{len(chunks)} — {len(out)} اقتباس")
        time.sleep(0.3)
    return out

# ===== حسابات فنية =====
def rsi(closes, p=14):
    if len(closes) < p + 1: return 50.0
    d = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    g = [max(x, 0) for x in d[-p:]]; l = [max(-x, 0) for x in d[-p:]]
    ag = sum(g) / p; al = sum(l) / p
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag / al))

def sma(closes, p):
    return sum(closes[-p:]) / p if len(closes) >= p else None

def support(cs, w=20):
    return min(c["low"] for c in cs[-w:]) if len(cs) >= w else None

def detect_failed_spike(cs):
    if len(cs) < 20: return False
    rec = cs[-20:]
    mx = max(c["high"] for c in rec); mn = min(c["low"] for c in rec)
    cur = cs[-1]["close"]
    if mn <= 0: return False
    return (mx - mn) / mn * 100 > 50 and (mx - cur) / mx * 100 > 40

def detect_bull_trap(cs):
    if len(cs) < 25: return False
    res = max(c["high"] for c in cs[-20:])
    if not any(c["high"] > res * 0.98 for c in cs[-5:]): return False
    return cs[-1]["close"] < res * 0.97

def stability(cs, sup):
    if not sup or len(cs) < 5: return 0
    th = sup * 0.98
    held = 0
    for c in reversed(cs[-5:]):
        if c["close"] >= th: held += 1
        else: break
    return held

def _txt(items):
    return [((it.get("headline") or "") + " " + (it.get("summary") or "")).lower()
            for it in items[:10]]

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
    return any(any(w in t for w in k)
               for t in [(it.get("headline") or "").lower() for it in items[:10]])

# ===== التقييم مع تشخيص السبب =====
def score(sym, nm, price, cs, nws, verbose=False):
    """
    يعيد (result_dict, rejection_reason)
    verbose=True يطبع سبب استبعاد أول 30 مرشح
    """
    if not cs or len(cs) < 30:
        if verbose: print(f"[diag] {sym}: شموع غير كافية ({len(cs) if cs else 0})")
        return None, "no_candles"
    if price <= 0:
        if verbose: print(f"[diag] {sym}: سعر غير صالح")
        return None, "bad_price"

    fs, sp, mc, sh = nm["fs"], nm["sp"], nm["mc"], nm["sh"]

    # فلتر الكب
    if not (1_000_000 <= mc <= 20_000_000):
        if verbose: print(f"[diag] {sym}: كاب خارج النطاق ({mc/1e6:.1f}M)")
        return None, "market_cap"

    # فلتر السعر
    if not (0.5 <= price <= 5.0):
        if verbose: print(f"[diag] {sym}: سعر خارج النطاق (${price:.2f})")
        return None, "price_range"

    # فلتر العائمة
    if not (0 < fs <= 5_000_000):
        if verbose: print(f"[diag] {sym}: عائمة خارج النطاق ({fs/1e6:.2f}M)")
        return None, "float_range"

    # فلتر الشورت
    if sp < 0.10:
        if verbose: print(f"[diag] {sym}: شورت منخفض ({sp*100:.1f}%)")
        return None, "short_low"

    closes = [c["close"] for c in cs]
    r = rsi(closes)
    sup = support(cs)
    s20 = sma(closes, 20); s50 = sma(closes, 50)

    if detect_failed_spike(cs):
        if verbose: print(f"[diag] {sym}: Failed Spike")
        return None, "failed_spike"
    if detect_bull_trap(cs):
        if verbose: print(f"[diag] {sym}: Bull Trap")
        return None, "bull_trap"
    if sup and price < sup * 0.97:
        if verbose: print(f"[diag] {sym}: دعم مكسور")
        return None, "support_broken"
    if nws and is_critical_news(nws):
        if verbose: print(f"[diag] {sym}: أخبار حرجة")
        return None, "critical_news"
    if nws and has_offering_news(nws):
        if verbose: print(f"[diag] {sym}: طرح")
        return None, "offering"

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
    stab = stability(cs, sup) if sup else 0
    if stab >= 3: pts += 10
    elif stab >= 2: pts += 7
    pos = is_positive_news(nws) if nws else False
    if pos: pts += 10

    if pts < MIN_SCORE:
        if verbose: print(f"[diag] {sym}: نقاط {pts} < {MIN_SCORE} (RSI={r:.0f}, stab={stab})")
        return None, f"score_{pts}"

    eff = max(sp, sh / fs if fs else 0)
    fuel = "packed" if eff >= 0.30 else ("present" if eff >= 0.10 else "none")
    return {"sym": sym, "price": round(price, 3), "rsi": round(r, 1), "score": pts,
            "fuel": fuel, "sup": round(sup, 3) if sup else None,
            "dist_sup": round((price - sup) / sup * 100, 1) if sup else None,
            "float_m": round(fs / 1e6, 2), "short_pct": round(eff * 100, 1),
            "stab": stab, "positive_news": pos}, "ok"

# ===== بناء الـ Pool =====
def build_pool(force=False):
    if POOL_FILE.exists() and not force:
        age_h = (time.time() - POOL_FILE.stat().st_mtime) / 3600
        if age_h < 144:
            return json.loads(POOL_FILE.read_text())

    print("[pool] بناء قائمة أسبوعية...")
    syms = all_us_symbols()
    if not syms:
        print("[pool] Finnhub فارغ — SEC بديلاً")
        syms = sec_tickers()
    print(f"[pool] {len(syms)} رمز خام")

    probe = stooq_probe(syms[:150])
    if probe:
        print("[pool] stooq يستجيب — استخدامه")
        quotes = dict(probe)
        for i in range(150, len(syms), 150):
            quotes.update(stooq_probe(syms[i:i+150]))
            if (i // 150) % 6 == 0:
                print(f"[pool] stooq {i}/{len(syms)} — {len(quotes)} اقتباس")
    else:
        print("[pool] stooq صامت — البروكسي مباشرة")
        quotes = proxy_bulk(syms)
    print(f"[pool] {len(quotes)} اقتباس")

    pre = [s for s, q in quotes.items()
           if 0.5 <= q.get("close", 0) <= 5.0 and q.get("volume", 0) >= 50_000]
    pre.sort(key=lambda s: -quotes[s].get("volume", 0))
    pre = pre[:POOL_CAP]
    print(f"[pool] {len(pre)} اجتاز السعر+الحجم (الأعلى حجماً)")

    pool = []
    for i, s in enumerate(pre):
        m = metrics(s)
        nm = normalize_metrics(m)
        if nm and 1_000_000 <= nm["mc"] <= 20_000_000 and 0 < nm["fs"] <= 5_000_000:
            pool.append({"sym": s, **nm})
        if i % 40 == 0:
            print(f"[pool] metrics {i}/{len(pre)} — مقبول {len(pool)}")
    print(f"[pool] اجتاز الكب/العائمة: {len(pool)}")

    for t in CORE_LIST_RAW.replace(";", ",").split(","):
        s = t.split(":")[0].strip().upper()
        if s and not any(p["sym"] == s for p in pool):
            m = metrics(s)
            nm = normalize_metrics(m)
            if nm:
                pool.append({"sym": s, **nm})
    print(f"[pool] نهائي مع CORE_LIST: {len(pool)}")
    POOL_FILE.write_text(json.dumps(pool))
    return pool

# ===== المسح مع تشخيص مفصّل =====
def daily_scan():
    pool = build_pool()
    print(f"[scan] جلب شموع متوازي لـ {len(pool)} مرشح...")
    candles_map = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(candles, p["sym"], "6mo"): p["sym"] for p in pool}
        done = 0
        for f in as_completed(futs):
            done += 1
            if done % 50 == 0:
                print(f"[scan] شموع {done}/{len(pool)}")
            try: candles_map[futs[f]] = f.result()
            except Exception: pass
    print(f"[scan] شموع جاهزة: {len(candles_map)}/{len(pool)}")

    disc = []
    rejections = {}
    for i, p in enumerate(pool):
        cs = candles_map.get(p["sym"])
        if not cs: continue
        price = cs[-1]["close"]
        nm = {"fs": p["fs"], "sp": p["sp"], "sh": p["sh"], "mc": p["mc"]}

        # أول 30 مرشح فقط مع تشخيص تفصيلي
        verbose = i < 30
        r, reason = score(p["sym"], nm, price, cs, [], verbose=verbose)
        if not r:
            rejections[reason] = rejections.get(reason, 0) + 1
            continue
        nws = news(p["sym"], 7)
        if is_critical_news(nws) or has_offering_news(nws): continue
        r, reason = score(p["sym"], nm, price, cs, nws)
        if r: disc.append(r)
        if i % 20 == 0:
            print(f"[scan] تحليل {i}/{len(pool)} — اكتشافات {len(disc)}")

    # 🎯 طباعة ملخص أسباب الرفض — مفتاح التشخيص
    print(f"[scan] ===== ملخص الرفض =====")
    for reason, count in sorted(rejections.items(), key=lambda x: -x[1]):
        print(f"[scan]   {reason}: {count} سهم")
    print(f"[scan] =====================")

    disc.sort(key=lambda x: (-x["score"],
                             -{"packed": 3, "present": 2, "none": 0}.get(x["fuel"], 1)))
    disc = disc[:30]
    DISC_FILE.write_text(json.dumps({"date": datetime.now(ET).isoformat(),
                                     "count": len(disc), "items": disc},
                                    indent=2, ensure_ascii=False))
    LAST_SCAN.write_text(json.dumps({"ts": time.time()}))
    print(f"[scan] {len(disc)} اكتشاف")
    return disc

def send_tg(msg):
    if not (TG_TOKEN and TG_CHAT): return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}, timeout=15)
    except Exception as e: print("[tg]", e)

def auto_watchlist(disc):
    if not BOT_URL: return
    added = 0
    for d in disc[:15]:
        sup = d.get("sup")
        if not sup: continue
        try:
            r = requests.post(BOT_URL + "/add",
                              json={"sym": d["sym"], "sup": sup, "res": round(sup * 1.3, 3)},
                              timeout=15)
            if r.status_code == 200 and r.json().get("ok"): added += 1
        except Exception: pass
        time.sleep(0.5)
    if added: print(f"[watch] أُضيف {added} سهماً لقائمة البوت")

def notify(disc):
    if not disc:
        send_tg("🛰️ <b>مسح اليوم</b>: لا اكتشافات مكتملة المعادلة — راجع السجلات للتشخيص")
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
    lines.append("\n💡 أرسل /s SYM للتحليل الكامل")
    send_tg("\n".join(lines))

def last_scan_age_hours():
    if not LAST_SCAN.exists(): return 999
    try: return (time.time() - json.loads(LAST_SCAN.read_text())["ts"]) / 3600
    except Exception: return 999

def should_scan():
    now = datetime.now(ET)
    if now.hour < 6 or now.hour >= 22: return False
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
        time.sleep(300)

def run_once():
    disc = daily_scan()
    notify(disc)
    auto_watchlist(disc)
    return disc

app = FastAPI()

@app.get("/ping", response_class=PlainTextResponse)
def ping():
    return "pong"

@app.get("/health")
def health():
    return {"ok": True, "last_scan_hours_ago": round(last_scan_age_hours(), 1)}

@app.get("/")
def root():
    n = 0
    if POOL_FILE.exists():
        try: n = len(json.loads(POOL_FILE.read_text()))
        except Exception: pass
    return {"engine": "finnhub-discovery-v4", "pool": n,
            "last_scan_hours_ago": round(last_scan_age_hours(), 1)}

@app.get("/latest")
def latest():
    if not DISC_FILE.exists():
        return {"ok": False, "msg": "لا مسح سابق بعد", "items": []}
    try:
        data = json.loads(DISC_FILE.read_text())
        return {"ok": True, "date": data.get("date"), "count": data.get("count", 0),
                "age_hours": round(last_scan_age_hours(), 1), "items": data.get("items", [])}
    except Exception:
        return {"ok": False, "items": []}

@app.get("/scan")
def trigger_scan_browser():
    threading.Thread(target=run_once, daemon=True).start()
    return {"started": True, "msg": "المسح بدأ في الخلفية"}

@app.post("/scan")
def trigger_scan():
    disc = run_once()
    return {"discoveries": len(disc), "items": disc[:10]}

if os.environ.get("START_SCHEDULER", "1") == "1":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    print("[init] الجدولة التلقائية مفعّلة")
