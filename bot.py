import os, time, threading, requests
from datetime import datetime
from zoneinfo import ZoneInfo

TOKEN = os.environ.get("TELEGRAM_TOKEN", "8910866855:AAHxH8DSy15nXEhCRCta8WYJdhbmHXyZSUc")
CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "8910866855")
PROXY = os.environ.get("PROXY_URL", "https://faisal-proxy.onrender.com").rstrip("/")
FINNHUB = os.environ.get("FINNHUB_KEY", "")
WATCHLIST = os.environ.get("WATCHLIST", "NTCL:1.735:2.223;CIIT:2.26:3.0")

ET = ZoneInfo("America/New_York")
SCHEDULE = [
    ("03:45", "pre",  "🌅 قبل البري-ماركت"),
    ("08:15", "news", "📰 قبل موجة الأخبار"),
    ("09:20", "open", "🔔 قبل افتتاح الجلسة"),
    ("15:50", "after","🌆 قبل الأفتر-اورز"),
]
NOTES = {
    "pre":  "🌅 افحص فجوات البري؛ أي فجوة ≥25% = مراقبة لا مطاردة.",
    "news": "📰 تحقق من الإعلانات لكل رمز؛ بوابة الإعلانات قبل أي زر.",
    "open": "🔔 أول 15 دقيقة فخ تقلب؛ انتظر إغلاق شمعة 9:45.",
    "after":"🌆 الأفتر سيولة رفيعة؛ لا دخول إلا بزناد مؤكد.",
}

_alerted, _fired = {}, {}

# ===== Telegram =====
def tg_send(chat, msg):
    if TOKEN and chat:
        try:
            r = requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                              json={"chat_id": chat, "text": msg, "parse_mode": "HTML"},
                              timeout=15)
            if r.status_code == 400 and len(msg) > 3900:
                for i in range(0, len(msg), 3900):
                    tg_send(chat, msg[i:i+3900])
        except Exception as e: print("tg error", e)

def tg_get_updates(offset=0):
    try:
        r = requests.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates",
                         params={"offset": offset, "timeout": 30}, timeout=35)
        if r.status_code == 200:
            return r.json().get("result", [])
    except Exception: pass
    return []

# ===== بيانات السوق =====
def candles(sym, period="6mo", interval="1d"):
    try:
        r = requests.get(PROXY + "/yahoo/candles",
                         params={"symbol": sym, "period": period, "interval": interval}, timeout=60)
        if r.status_code == 200:
            d = r.json()
            if d.get("success") and d.get("candles"):
                return d["candles"]
    except Exception: pass
    return []

def finnhub_metrics(sym):
    if not FINNHUB: return {}
    try:
        r = requests.get("https://finnhub.io/api/v1/stock/metric",
                         params={"symbol": sym, "metric": "all", "token": FINNHUB}, timeout=15)
        if r.status_code == 200: return r.json().get("metric", {})
    except Exception: pass
    return {}

def quote(sym):
    if not FINNHUB: return None
    try:
        r = requests.get("https://finnhub.io/api/v1/quote",
                         params={"symbol": sym, "token": FINNHUB}, timeout=10)
        if r.status_code == 200: return r.json()
    except Exception: pass
    return None

# ===== حسابات =====
def rsi_val(closes, p=14):
    if len(closes) < p + 1: return 50.0
    d = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    g = [max(x, 0) for x in d]
    l = [max(-x, 0) for x in d]
    ag = sum(g[-p:]) / p
    al = sum(l[-p:]) / p
    if al == 0: return 100.0
    rs = ag / al
    return 100 - (100 / (1 + rs))

def sma_val(closes, p):
    if len(closes) < p: return None
    return sum(closes[-p:]) / p

def support_resistance(candles, w=20):
    if len(candles) < w: return None, None
    lows = [c["low"] for c in candles[-w:]]
    highs = [c["high"] for c in candles[-w:]]
    return min(lows), max(highs)

def ladder_summary(lad, n=3):
    if not lad: return "—"
    return " | ".join(f"{L['tail']}→{L['head']}" for L in lad["ladder"][:n])

def build_ladder(h4, price):
    if not h4 or len(h4) < 10: return None
    reds = [c for c in h4 if c["close"] < c["open"]][-40:]
    above = [c for c in reds if c["low"] > price]
    if len(above) < 2: return None
    above = sorted(above, key=lambda x: x["low"])
    ladder = [{"tail": round(c["low"], 3), "head": round(c["high"], 3)} for c in above[:6]]
    supports = [round(c["low"], 3) for c in reds if c["low"] <= price][-3:]
    return {"ladder": ladder, "supports": supports}

def detect_failed_spike(candles):
    if len(candles) < 20: return None
    rec = candles[-20:]
    mx = max(c["high"] for c in rec)
    mn = min(c["low"] for c in rec)
    cur = candles[-1]["close"]
    if mn <= 0: return None
    sp = (mx - mn) / mn * 100
    dr = (mx - cur) / mx * 100
    if sp > 50 and dr > 40:
        return {"spike_pct": round(sp, 1), "drop_pct": round(dr, 1), "peak": mx}
    return None

def detect_bull_trap(candles):
    if len(candles) < 25: return False
    res = max(c["high"] for c in candles[-20:])
    rec = candles[-5:]
    if not any(c["high"] > res * 0.98 for c in rec): return False
    return candles[-1]["close"] < res * 0.97

def detect_distribution(candles):
    if len(candles) < 30: return False
    last = candles[-10:]; prev = candles[-30:-10]
    vp = sum(c["volume"] for c in prev) / len(prev)
    vl = sum(c["volume"] for c in last) / len(last)
    base = last[0]["close"]
    if base <= 0: return False
    move = abs(last[-1]["close"] - base) / base * 100
    return vl > vp * 1.5 and move < 2.0

def stability(candles, sup, min_sess=2):
    if not sup or len(candles) < min_sess + 2: return None
    rec = candles[-(min_sess + 3):]
    th = sup * 0.98
    held = 0
    for c in reversed(rec):
        if c["close"] >= th: held += 1
        else: break
    if held < min_sess: return None
    return held

def rsi_build(candles):
    if len(candles) < 30: return None
    closes = [c["close"] for c in candles]
    rs = []
    for i in range(14, len(closes)):
        rs.append(rsi_val(closes[:i+1], 14))
    if len(rs) < 10: return None
    last10 = rs[-10:]
    p10 = closes[-10:]
    rng = (max(p10) - min(p10)) / min(p10) * 100
    if rng > 12: return None
    rise = last10[-1] - last10[0]
    if rise < 8: return None
    if min(last10[-5:]) <= min(last10[-10:-5]): return None
    return round(last10[-1], 1)

# ===== التحليل الشامل =====
def analyze(sym):
    sym = sym.upper().strip()
    d = candles(sym, "6mo", "1d")
    if not d or len(d) < 30:
        return f"❌ <b>{sym}</b>: لا بيانات كافية"
    
    closes = [c["close"] for c in d]
    price = closes[-1]
    prev = closes[-2] if len(closes) > 1 else price
    chg = (price - prev) / prev * 100 if prev else 0
    
    rsi = rsi_val(closes)
    s20 = sma_val(closes, 20)
    s50 = sma_val(closes, 50)
    sup, res = support_resistance(d)
    m = finnhub_metrics(sym)
    q = quote(sym)
    
    fs = (m.get("freeFloat") or 0)
    if fs and fs < 1000: fs *= 1000000
    sp = m.get("shortPercentOfFloat") or 0
    if sp and sp > 1: sp = sp / 100
    sh = m.get("sharesShort") or 0
    mc = m.get("marketCapitalization") or 0
    if mc and mc < 100000: mc *= 1000000
    
    eff = max(sp, sh / fs if fs else 0)
    if eff >= 0.30: fuel_t, fuel_e = "🚀 وقود محشور", f"{round(eff*100,1)}%"
    elif eff >= 0.10: fuel_t, fuel_e = "⛽ وقود متوسط", f"{round(eff*100,1)}%"
    elif eff < 0.05: fuel_t, fuel_e = "🎈 بلا وقود", f"{round(eff*100,1)}%"
    else: fuel_t, fuel_e = "❓ بيانات ناقصة", "—"
    
    # النقاط
    pts = 0
    if 23 <= rsi <= 27: pts += 25
    elif 20 <= rsi < 23 or 27 < rsi <= 30: pts += 15
    elif 30 < rsi <= 35: pts += 10
    elif 35 < rsi <= 45: pts += 8
    elif 45 < rsi <= 57: pts += 5
    if fs and fs < 1e6: pts += 15
    elif fs and fs < 5e6: pts += 12
    elif fs and fs < 10e6: pts += 8
    if sp > 0.40: pts += 20
    elif sp > 0.30: pts += 15
    elif sp > 0.20: pts += 10
    elif sp > 0.10: pts += 5
    if sup and abs(price - sup) / price * 100 < 5: pts += 7
    stab = stability(d, sup) if sup else None
    if stab and stab >= 3: pts += 10
    elif stab and stab >= 2: pts += 7
    
    # الفصائل
    fams = []
    if 20 <= rsi <= 30: fams.append("ضغط RSI")
    rb = rsi_build(d)
    if rb: fams.append("📈 تراكم RSI")
    failed = detect_failed_spike(d)
    bull = detect_bull_trap(d)
    dist = detect_distribution(d)
    sup_broken = sup and price < sup
    hard_veto = sup_broken or failed or bull or dist
    
    if hard_veto:
        verdict = "🚫 مرفوض"
        color = "#d63031"
    elif pts >= 65:
        verdict = "✅ ممتاز"
        color = "#00b894"
    elif pts >= 50:
        verdict = "🟡 جيد"
        color = "#fdcb6e"
    else:
        verdict = "⏳ ضعيف"
        color = "#e17055"
    
    if failed: fams.append("Failed Spike")
    if bull: fams.append("Bull Trap")
    if dist: fams.append("تصريف")
    if stab and stab >= 2: fams.append(f"ثبات {stab} جلسات")
    
    # 4H + سلّم
    h4 = candles(sym, "3mo", "1h")
    h4_agg = []
    for i in range(0, len(h4) - 3, 4):
        ch = h4[i:i+4]
        h4_agg.append({"low": min(x["low"] for x in ch), "high": max(x["high"] for x in ch),
                       "close": ch[-1]["close"], "open": ch[0]["open"]})
    lad = build_ladder(h4_agg, price)
    
    # الزناد
    trigger = None
    if sup:
        dist_pct = (price - sup) / sup * 100
        if dist_pct <= 5 and stab and stab >= 2:
            trigger = f"ثبات فوق الدعم {sup} — دخول Limit قرب الدعم، وقف {round(sup*0.97,3)}"
        elif sup and price > res and (res / sup - 1) < 0.3:
            trigger = f"اختراق للمقاومة {round(res,3)} — تأكيد بإغلاق ثانٍ فوقها"
        elif dist_pct <= 15:
            trigger = f"قرب الدعم {sup} ({round(dist_pct,1)}%) — انتظار ثبات أو سحب"
    
    # بناء الرسالة
    live_p = q.get("c", price) if q else price
    live_chg = q.get("dp", chg) if q else chg
    
    lines = []
    lines.append(f"📊 <b>{sym}</b> @ ${round(live_p, 3)} ({live_chg:+.1f}%)")
    lines.append("─" * 20)
    lines.append(f"🎯 <b>{verdict}</b> — {pts}/100")
    if fams: lines.append(f"🧬 الفصيلة: {' | '.join(fams[:3])}")
    lines.append(f"⛽ {fuel_t}: {fuel_e}")
    if fs: lines.append(f"📏 عائمة: {round(fs/1e6,2)}M" + (f" | كاب: {round(mc/1e6,1)}M" if mc else ""))
    if sup and res:
        ds = (price - sup) / sup * 100
        lines.append(f"📍 دعم: {round(sup,3)} ({round(ds,1)}%) | مقاومة: {round(res,3)}")
    if s20 and s50:
        lines.append(f"📈 SMA20: {round(s20,3)} | SMA50: {round(s50,3)}")
    if rb: lines.append(f"📈 تراكم RSI: {rb} — نافذة إشعال")
    if lad: lines.append(f"🕯️ السلّم: {ladder_summary(lad)}")
    if trigger:
        lines.append(f"🎯 <b>الزناد:</b> {trigger}")
    else:
        lines.append(f"⏳ الزناد: لم يتحقق بعد")
    if hard_veto:
        lines.append(f"🚫 <b>إقصاء فوري</b> — لا تتداول هذه الحالة")
    
    return "\n".join(lines)

# ===== معالجة الأوامر =====
def get_watchlist():
    return [p.split(":") for p in WATCHLIST.split(";") if ":" in p]

def handle_cmd(text, chat_id):
    text = text.strip()
    parts = text.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1] if len(parts) > 1 else ""
    
    if cmd == "/help":
        return ("📋 <b>الأوامر:</b>\n"
                "/s SYMBOL — تحليل شامل\n"
                "/w SYM:sup:res — أضف للقائمة\n"
                "/d SYM — احذف من القائمة\n"
                "/list — عرض القائمة\n"
                "/now — لقطة فورية للجميع\n\n"
                "أو اكتب اسم السهم مباشرة للتحليل السريع.")
    
    elif cmd == "/s" or (not cmd.startswith("/") and text.isalnum()):
        sym = arg if cmd == "/s" else text
        return analyze(sym.upper())
    
    elif cmd == "/list":
        wl = get_watchlist()
        if not wl: return "📋 القائمة فارغة"
        lines = ["📋 <b>القائمة المراقبة:</b>"]
        for p in wl:
            if len(p) >= 3:
                lines.append(f"• {p[0]}: دعم {p[1]} | مقاومة {p[2]}")
        lines.append(f"\nلتعديل القائمة: عدّل WATCHLIST في Render")
        return "\n".join(lines)
    
    elif cmd == "/w":
        return ("⚠️ لإضافة سهم للقائمة الدائمة:\n"
                "عدّل WATCHLIST في Render\n"
                f"الصيغة الحالية: {WATCHLIST}\n"
                "للتحليل الفوري استخدم: /s SYMBOL")
    
    elif cmd == "/d":
        return "⚠️ للحذف: عدّل WATCHLIST في Render"
    
    elif cmd == "/now":
        wl = get_watchlist()
        if not wl: return "📋 القائمة فارغة"
        lines = [f"📋 <b>لقطة {datetime.now(ET).strftime('%H:%M ET')}</b>"]
        for p in wl:
            if len(p) >= 3:
                d = candles(p[0], "6mo", "1d")
                if d and len(d) >= 2:
                    pr = d[-1]["close"]; pp = d[-2]["close"]
                    chg = (pr - pp) / pp * 100 if pp else 0
                    ds = (pr - float(p[1])) / float(p[1]) * 100
                    state = "👀 عند الدعم" if abs(pr - float(p[1])) / float(p[1]) <= 0.02 else (
                        "🎯 فوق المقاومة" if pr > float(p[2]) else "⏳ بين")
                    lines.append(f"• <b>{p[0]}</b>: {round(pr,3)} ({chg:+.1f}%) | {state}")
        return "\n".join(lines)
    
    return "❓ أمر غير معروف — أرسل /help"

# ===== Polling loop =====
def polling():
    offset = 0
    while True:
        try:
            updates = tg_get_updates(offset)
            for u in updates:
                offset = u["update_id"] + 1
                msg = u.get("message", {})
                text = msg.get("text", "")
                chat_id = msg.get("chat", {}).get("id")
                if text and chat_id:
                    reply = handle_cmd(text, chat_id)
                    tg_send(chat_id, reply)
        except Exception as e:
            print("polling error", e)
        time.sleep(2)

# ===== Scheduler =====
def snapshot(sym, sup, res):
    d = candles(sym, "6mo", "1d")
    if not d or len(d) < 2: return f"• {sym}: لا بيانات"
    last = d[-1]["close"]; prev = d[-2]["close"]
    chg = (last - prev) / prev * 100 if prev else 0
    ds = (last - sup) / sup * 100
    if last < sup * 0.97: state = "🚫 مُبطَل"
    elif abs(last - sup) / sup <= 0.02: state = "👀 عند الدعم"
    elif last > res: state = "🎯 فوق المقاومة"
    else: state = "⏳ بين المستويين"
    return f"• <b>{sym}</b>: {round(last,3)} ({chg:+.1f}%) | عن الدعم {round(ds,1)}% | {state}"

def build_message(title, key):
    now = datetime.now(ET)
    lines = [f"📋 <b>{title}</b> — {now.strftime('%A %H:%M ET')}"]
    for item in WATCHLIST.split(";"):
        p = item.strip().split(":")
        if len(p) >= 3:
            lines.append(snapshot(p[0].upper(), float(p[1]), float(p[2])))
    lines.append(NOTES[key])
    return "\n".join(lines)

def scheduler():
    while True:
        try:
            now = datetime.now(ET)
            if now.weekday() < 5:
                hm = now.strftime("%H:%M")
                today = now.strftime("%Y-%m-%d")
                for slot_hm, key, title in SCHEDULE:
                    if hm == slot_hm and _fired.get(key) != today:
                        _fired[key] = today
                        tg_send(CHAT, build_message(title, key))
        except Exception as e:
            print("sched error", e)
        time.sleep(30)

def alert_once(sym, kind, msg):
    key = sym + kind
    now = time.time()
    if key in _alerted and now - _alerted[key] < 86400: return
    _alerted[key] = now
    tg_send(CHAT, msg)

def agg4h(hc):
    out = []
    for i in range(0, len(hc) - 3, 4):
        ch = hc[i:i+4]
        out.append({"low": min(x["low"] for x in ch), "close": ch[-1]["close"]})
    return out

def us_market_open():
    try:
        now = datetime.now(ET)
        mins = now.hour * 60 + now.minute
        return now.weekday() < 5 and 570 <= mins < 960
    except Exception: return False

def check_symbol(sym, sup, res):
    d = candles(sym, "6mo", "1d")
    if not d or len(d) < 3: return
    price = d[-1]["close"]; prev = d[-2]["close"]
    if prev <= res and price > res:
        alert_once(sym, "break", f"🎯 <b>{sym}</b>: إغلاق يومي فوق المقاومة {res}")
    if price < sup * 0.97:
        alert_once(sym, "inv", f"🚫 <b>{sym}</b>: إغلاق تحت {round(sup*0.97,3)} — الدورة مُبطلة")
    if abs(price - sup) / sup <= 0.02:
        alert_once(sym, "zone", f"👀 <b>{sym}</b>: لمس الدعم {sup}")
    h = candles(sym, "3mo", "1h")
    if h:
        bars = agg4h(h)
        if us_market_open() and len(bars) > 1: bars = bars[:-1]
        if bars:
            last4 = bars[-1]
            if last4["low"] < sup * 0.99 and last4["close"] > sup:
                alert_once(sym, "sweep", f"🌀 <b>{sym}</b>: سحب ثم استرداد 4H (ذيل {round(last4['low'],3)} إغلاق {round(last4['close'],3)})")

def monitor():
    while True:
        try:
            for item in WATCHLIST.split(";"):
                p = item.strip().split(":")
                if len(p) >= 3:
                    check_symbol(p[0].upper(), float(p[1]), float(p[2]))
        except Exception as e:
            print("monitor error", e)
        time.sleep(600)

# ===== FastAPI =====
from fastapi import FastAPI
app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True, "watching": WATCHLIST}

@app.get("/")
def root():
    return {"bot": "faisal-alerts-v3", "features": ["schedule", "alerts", "commands"]}

# ===== بدء الخيوط =====
threading.Thread(target=scheduler, daemon=True).start()
threading.Thread(target=monitor, daemon=True).start()
threading.Thread(target=polling, daemon=True).start()
