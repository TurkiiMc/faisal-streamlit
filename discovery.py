"""
Finnhub Discovery Engine v10.1 — شكل نقي + متوسطات حيّة
- universe = بورصة ناسداك (XNAS) + بورصة نيويورك (XNYS)
- لا شورت، لا CORE_LIST، لا ذاكرة — شكل نقي فقط
- نطاق الصيد 0.50-6.00 | كاب 1M-20M
- v10.1: المتوسطات كسياق ارتكاز (جيب SMA20 + هيكل سليم)
         + هدف فني واقعي (أقرب SMA فوق السعر) يذهب للبوت وتليجرام
- دفعات 60 + تباعد 2s + تبريد 180s قبل الشموع + عمال 3
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

POOL_FILE = Path("universe_pool.json")      # تشخيص فقط — لا يُقرأ
DISC_FILE = Path("discoveries.json")
LAST_SCAN = Path("last_scan.json")
QUOTE_CACHE = Path("quotes_cache.json")
METRICS_CACHE = Path("metrics_cache.json")
ET = ZoneInfo("America/New_York")
SCAN_EVERY_HOURS = 6
POOL_CAP = 150
PRE_CAP = 300
PRICE_CEIL = 6.0
PENNY_DANGER = 0.50
NEWS_CAP = 60
MIN_SCORE = 40
MC_MIN, MC_MAX = 1_000_000, 50_000_000
COOLDOWN_SEC = 90
CANDLE_COOLDOWN_SEC = 180
CANDLE_WORKERS = 3

_scanning = False
_scan_lock = threading.Lock()

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
    # v10: universe = أسهم بورصة ناسداك (XNAS) + بورصة نيويورك (XNYS)
    data = fh_get("/stock/symbol", {"exchange": "US"})
    if not data: return []
    return [s["symbol"] for s in data
            if s.get("type") == "Common Stock"
            and s.get("mic") in {"XNAS", "XNYS"} and s.get("symbol")]

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
    for attempt in range(2):
        try:
            r = requests.get(PROXY + "/yahoo/candles",
                             params={"symbol": sym, "period": period}, timeout=60)
            if r.status_code == 200:
                d = r.json()
                if d.get("success") and d.get("candles"):
                    return d["candles"]
        except Exception: pass
        if attempt == 0: time.sleep(3)
    return []

def _q_price(q): return float(q.get("price") or q.get("close") or 0)
def _q_vol(q):   return float(q.get("volume") or 0)

def normalize_metrics(m):
    # v10: الكاب فقط (بوابة) — لا شورت ولا فلوت
    if not m: return None
    mc_raw = m.get("marketCapitalization") or 0
    mc = mc_raw * 1e6 if 0 < mc_raw < 100000 else mc_raw
    return {"mc": mc}

def load_metrics_cache():
    if METRICS_CACHE.exists():
        age = (time.time() - METRICS_CACHE.stat().st_mtime) / 3600
        if age < 24:
            try:
                d = json.loads(METRICS_CACHE.read_text())
                if isinstance(d, dict): return d
            except Exception: pass
    return {}

def save_metrics_cache(d):
    try: METRICS_CACHE.write_text(json.dumps(d))
    except Exception as e: print("[cache] فشل حفظ metrics:", e)

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
                c = float(row.get("Close") or 0); v = float(row.get("Volume") or 0)
            except Exception: continue
            if c > 0: out[sym] = {"close": c, "volume": v}
        return out
    except Exception:
        return {}

def proxy_bulk(symbols):
    out = {}
    chunks = [symbols[i:i+60] for i in range(0, len(symbols), 60)]
    for ci, chunk in enumerate(chunks):
        try:
            r = requests.get(PROXY + "/yahoo/last",
                             params={"symbols": ",".join(chunk)}, timeout=120)
            if r.status_code == 200:
                out.update(r.json().get("quotes", {}))
        except Exception: pass
        if ci % 8 == 0 or ci == len(chunks) - 1:
            print(f"[pool] بروكسي دفعة {ci+1}/{len(chunks)} — {len(out)} اقتباس")
        time.sleep(2.0)
    return out

def load_quotes_cache():
    if QUOTE_CACHE.exists():
        age = (time.time() - QUOTE_CACHE.stat().st_mtime) / 3600
        if age < 24:
            try:
                d = json.loads(QUOTE_CACHE.read_text())
                if isinstance(d, dict) and len(d) > 500: return d
            except Exception: pass
    return None

# ===== فنية (شكل نقي + متوسطات) =====
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
    rec = cs[-60:] if len(cs) >= 60 else cs[-20:]
    mx = max(c["high"] for c in rec); mn = min(c["low"] for c in rec)
    cur = cs[-1]["close"]
    if mn <= 0: return False
    return (mx - mn) / mn * 100 > 40 and (mx - cur) / mx * 100 > 35

def detect_broken_base(cs):
    if len(cs) < 60: return False
    base = min(c["low"] for c in cs[-60:-20])
    mx = max(c["high"] for c in cs[-40:])
    cur = cs[-1]["close"]
    if base <= 0: return False
    return mx > base * 1.8 and cur < base * 1.05

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

def score(sym, nm, price, cs, nws, verbose=False):
    if not cs or len(cs) < 30:
        if verbose: print(f"[diag] {sym}: شموع غير كافية ({len(cs) if cs else 0})")
        return None, "no_candles"
    if price <= 0:
        if verbose: print(f"[diag] {sym}: سعر غير صالح"); return None, "bad_price"
    if price < PENNY_DANGER:
        if verbose: print(f"[diag] {sym}: تحت $0.50"); return None, "penny_danger"
    mc = nm["mc"]
    if not (MC_MIN <= mc <= MC_MAX):
        if verbose: print(f"[diag] {sym}: كاب خارج النطاق ({mc/1e6:.1f}M)")
        return None, "market_cap"
    closes = [c["close"] for c in cs]
    r = rsi(closes); sup = support(cs)
    s20 = sma(closes, 20); s50 = sma(closes, 50)
    if detect_failed_spike(cs):
        if verbose: print(f"[diag] {sym}: Failed Spike"); return None, "failed_spike"
    if detect_bull_trap(cs):
        if verbose: print(f"[diag] {sym}: Bull Trap"); return None, "bull_trap"
    if detect_broken_base(cs):
        if verbose: print(f"[diag] {sym}: قاعدة مكسورة"); return None, "broken_base"
    if sup and price < sup * 0.97:
        if verbose: print(f"[diag] {sym}: دعم مكسور"); return None, "support_broken"
    if nws and is_critical_news(nws):
        if verbose: print(f"[diag] {sym}: أخبار حرجة"); return None, "critical_news"
    if nws and has_offering_news(nws):
        if verbose: print(f"[diag] {sym}: طرح"); return None, "offering"
    pts = 0
    if 23 <= r <= 27: pts += 25
    elif 20 <= r < 23 or 27 < r <= 30: pts += 15
    elif 30 < r <= 35: pts += 10
    elif 45 <= r <= 57: pts += 5
    if PENNY_DANGER <= price <= PRICE_CEIL: pts += 8
    # ===== v10.1: المتوسطات كسياق ارتكاز =====
    if s20:
        if price < s20: pts += 5                      # في منطقة البيع تحت المتوسط
        if s20 * 0.92 <= price < s20: pts += 4        # لمس SMA20 من الأسفل = ارتداد وشيك
    if s50 and price < s50: pts += 3                  # تحت المتوسط الطويل (قاع)
    if s20 and s50 and s20 > s50: pts += 3            # هيكل أعلى سليم رغم القاع
    # الهدف الفني: أقرب متوسط فوق السعر، وإلا sup*1.3
    cands = [x for x in (s20, s50) if x and x > price * 1.02]
    res_target = round(min(cands), 3) if cands else (round(sup * 1.3, 3) if sup else None)
    stab = stability(cs, sup) if sup else 0
    if stab >= 3: pts += 10
    elif stab >= 2: pts += 7
    pos = is_positive_news(nws) if nws else False
    if pos: pts += 10
    if pts < MIN_SCORE:
        if verbose: print(f"[diag] {sym}: نقاط {pts} < {MIN_SCORE} (RSI={r:.0f}, stab={stab})")
        return None, f"score_{pts}"
    return {"sym": sym, "price": round(price, 3), "rsi": round(r, 1), "score": pts,
            "sup": round(sup, 3) if sup else None,
            "res": res_target,
            "sma20": round(s20, 3) if s20 else None,
            "sma50": round(s50, 3) if s50 else None,
            "dist_sup": round((price - sup) / sup * 100, 1) if sup else None,
            "stab": stab, "positive_news": pos}, "ok"

def build_pool():
    print("[pool] بناء قائمة من السوق الحي (Nasdaq + NYSE)...")
    syms = all_us_symbols()
    if not syms:
        print("[pool] Finnhub فارغ — SEC بديلاً"); syms = sec_tickers()
    print(f"[pool] {len(syms)} رمز خام")

    quotes = load_quotes_cache()
    if quotes:
        print(f"[pool] اقتباسات من كاش اليوم ({len(quotes)})")
    else:
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
        try:
            QUOTE_CACHE.write_text(json.dumps(quotes))
            print(f"[pool] كُتب كاش الاقتباسات ({len(quotes)})")
        except Exception as e: print("[pool] فشل كتابة الكاش:", e)
        print(f"[pool] تبريد {COOLDOWN_SEC} ثانية...")
        time.sleep(COOLDOWN_SEC)
    print(f"[pool] {len(quotes)} اقتباس")

    cand = [(s, _q_vol(q)) for s, q in quotes.items()
            if PENNY_DANGER <= _q_price(q) <= PRICE_CEIL and _q_vol(q) >= 50_000]
    cand.sort(key=lambda x: -x[1])
    cand = cand[:PRE_CAP]
    print(f"[pool] {len(cand)} مرشح أولي (سعر 0.50-6 + حجم)")

    mcache = load_metrics_cache()
    survivors = []; misses = []; hits = 0
    for s, vol in cand:
        if s in mcache:
            hits += 1
            nm = mcache[s]
            if nm and MC_MIN <= nm["mc"] <= MC_MAX:
                survivors.append((s, vol, nm))
        else:
            misses.append((s, vol))
    print(f"[pool] metrics كاش: {hits} مخبأ، {len(misses)} مطلوب")
    for i, (s, vol) in enumerate(misses):
        nm = normalize_metrics(metrics(s))
        if nm: mcache[s] = nm
        if nm and MC_MIN <= nm["mc"] <= MC_MAX:
            survivors.append((s, vol, nm))
        if i % 50 == 0:
            print(f"[pool] metrics {i}/{len(misses)} — ناجٍ {len(survivors)}")
    save_metrics_cache(mcache)
    survivors.sort(key=lambda x: -x[1])
    pool = [{"sym": s, **nm} for s, vol, nm in survivors[:POOL_CAP]]
    print(f"[pool] اجتاز الكب: {len(survivors)} → نهائي: {len(pool)}")
    POOL_FILE.write_text(json.dumps(pool))
    return pool

def daily_scan():
    pool = build_pool()
    print(f"[scan] تبريد {CANDLE_COOLDOWN_SEC} ثانية قبل الشموع...")
    time.sleep(CANDLE_COOLDOWN_SEC)
    print(f"[scan] شموع متوازي (workers={CANDLE_WORKERS}) لـ {len(pool)}...")
    candles_map = {}
    with ThreadPoolExecutor(max_workers=CANDLE_WORKERS) as ex:
        futs = {ex.submit(candles, p["sym"], "6mo"): p["sym"] for p in pool}
        done = 0
        for f in as_completed(futs):
            done += 1
            if done % 50 == 0: print(f"[scan] شموع {done}/{len(pool)}")
            try:
                res = f.result()
                if res: candles_map[futs[f]] = res
            except Exception: pass
    got = len(candles_map)
    print(f"[scan] شموع جاهزة: {got}/{len(pool)}")
    if len(pool) > 0 and got / len(pool) < 0.5:
        print(f"[warn] ⚠️ خنق Yahoo محتمل: {got}/{len(pool)}")

    prelim = []
    rejections = {}
    for i, p in enumerate(pool):
        cs = candles_map.get(p["sym"])
        if not cs:
            rejections["no_candles"] = rejections.get("no_candles", 0) + 1
            continue
        price = cs[-1]["close"]
        nm = {"mc": p["mc"]}
        verbose = i < 30
        r, reason = score(p["sym"], nm, price, cs, [], verbose=verbose)
        if r: prelim.append((p, price, cs, r))
        else: rejections[reason] = rejections.get(reason, 0) + 1
        if i % 20 == 0:
            print(f"[scan] أولي {i}/{len(pool)} — مرشحون {len(prelim)}")

    prelim.sort(key=lambda x: -x[3]["score"])
    prelim = prelim[:NEWS_CAP]   # v10: لا بوابة وقود — الشكل مباشرة للأخبار

    print(f"[scan] أخبار لـ {len(prelim)} مرشح نهائي...")
    disc = []
    for p, price, cs, _ in prelim:
        nws = news(p["sym"], 7)
        nm = {"mc": p["mc"]}
        r, reason = score(p["sym"], nm, price, cs, nws)
        if r: disc.append(r)
        else: rejections[reason] = rejections.get(reason, 0) + 1

    print(f"[scan] ===== ملخص الرفض =====")
    for reason, count in sorted(rejections.items(), key=lambda x: -x[1]):
        print(f"[scan]   {reason}: {count} سهم")
    print(f"[scan] =====================")

    disc.sort(key=lambda x: -x["score"])
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
    # v10.1: الهدف الفني من score (res) بدل sup*1.3 الاعتباطي
    if not BOT_URL: return
    added = 0
    for d in disc[:15]:
        sup = d.get("sup"); res = d.get("res")
        if not sup or not res: continue          # بلا هدف فني لا نضيف
        try:
            r = requests.post(BOT_URL + "/add",
                              json={"sym": d["sym"], "sup": sup, "res": res},
                              timeout=15)
            if r.status_code == 200 and r.json().get("ok"): added += 1
        except Exception: pass
        time.sleep(0.5)
    if added: print(f"[watch] أُضيف {added} سهماً بأهداف فنية")

def notify(disc):
    if not disc:
        send_tg("🛰️ <b>مسح اليوم</b>: لا اكتشافات (سوق هادئ/مغلق) — راجع ملخص الرفض")
        return
    lines = [f"🛰️ <b>اكتشافات اليوم ({len(disc)})</b>", ""]
    for d in disc[:12]:
        ne = " 📰+" if d["positive_news"] else ""
        ds = f" | دعم {d['dist_sup']}%" if d.get("dist_sup") is not None else ""
        tg = f" | 🎯 هدف ${d['res']}" if d.get("res") else ""
        lines.append(f"• <b>{d['sym']}</b>: {d['score']}/100 | ${d['price']} | RSI {d['rsi']} | ثبات {d['stab']}{ds}{tg}{ne}")
    lines.append("\n💡 أرسل /s SYM للتحليل الكامل")
    send_tg("\n".join(lines))

def last_scan_age_hours():
    if not LAST_SCAN.exists(): return 999
    try: return (time.time() - json.loads(LAST_SCAN.read_text())["ts"]) / 3600
    except Exception: return 999

def should_scan():
    now = datetime.now(ET)
    if now.weekday() >= 5: return False
    if now.hour < 6 or now.hour >= 19: return False
    return last_scan_age_hours() >= SCAN_EVERY_HOURS

def run_once():
    global _scanning
    with _scan_lock:
        if _scanning:
            print("[scan] مسح جارٍ — تجاهل"); return []
        _scanning = True
    try:
        disc = daily_scan(); notify(disc); auto_watchlist(disc); return disc
    finally:
        with _scan_lock: _scanning = False

def scheduler_loop():
    while True:
        try:
            if should_scan():
                print("[sched] بدء المسح الدوري"); run_once(); print("[sched] اكتمل")
        except Exception as e: print("[sched] error:", e)
        time.sleep(300)

app = FastAPI()

@app.get("/ping", response_class=PlainTextResponse)
def ping(): return "pong"

@app.get("/health")
def health(): return {"ok": True, "scanning": _scanning, "last_scan_hours_ago": round(last_scan_age_hours(), 1)}

@app.get("/")
def root():
    n = qc = mc = 0
    if POOL_FILE.exists():
        try: n = len(json.loads(POOL_FILE.read_text()))
        except Exception: pass
    if QUOTE_CACHE.exists():
        try: qc = len(json.loads(QUOTE_CACHE.read_text()))
        except Exception: pass
    if METRICS_CACHE.exists():
        try: mc = len(json.loads(METRICS_CACHE.read_text()))
        except Exception: pass
    return {"engine": "finnhub-discovery-v10.1", "pool_diag": n, "quotes_cached": qc,
            "metrics_cached": mc, "scanning": _scanning,
            "last_scan_hours_ago": round(last_scan_age_hours(), 1)}

@app.get("/latest")
def latest():
    if not DISC_FILE.exists():
        return {"ok": False, "msg": "no prior scan", "items": []}
    try:
        data = json.loads(DISC_FILE.read_text())
        return {"ok": True, "date": data.get("date"), "count": data.get("count", 0),
                "age_hours": round(last_scan_age_hours(), 1), "items": data.get("items", [])}
    except Exception:
        return {"ok": False, "items": []}

@app.get("/scan")
def trigger_scan_browser():
    threading.Thread(target=run_once, daemon=True).start()
    return {"started": True, "msg": "scan started in background"}

@app.post("/scan")
def trigger_scan():
    disc = run_once()
    return {"discoveries": len(disc), "items": disc[:10]}

@app.post("/reset")
def reset_memory():
    removed = []
    for f in (POOL_FILE, DISC_FILE, LAST_SCAN, QUOTE_CACHE, METRICS_CACHE):
        try:
            if f.exists():
                f.unlink(); removed.append(f.name)
        except Exception: pass
    return {"ok": True, "removed": removed}

if os.environ.get("START_SCHEDULER", "1") == "1":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    print("[init] الجدولة مفعّلة")
