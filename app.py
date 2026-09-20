from fastapi import FastAPI, HTTPException, Query
import httpx
from datetime import datetime, timezone

VERSION = "8.0"
app = FastAPI()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


@app.get("/")
async def root():
    return {"message": "Proxy is working", "version": VERSION}


@app.get("/health")
async def health():
    return {"status": "ok", "version": VERSION}


@app.get("/yahoo/candles")
async def yahoo_candles(symbol: str = Query(...), period: str = Query("6mo")):
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + symbol
    params = {
        "range": period,
        "interval": "1d",
        "includePrePost": "false",
        "events": "splits,div",
    }
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            r = await client.get(url, params=params, headers=HEADERS)
            if r.status_code != 200:
                raise HTTPException(r.status_code, "Yahoo error")
            data = r.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                raise HTTPException(404, "No data")
            chart = result[0]
            ts = chart.get("timestamp", [])
            q = chart["indicators"]["quote"][0]

            candles = []
            for i, t in enumerate(ts):
                if i < len(q["close"]) and q["close"][i] is not None:
                    candles.append({
                        "date": datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d"),
                        "open": q["open"][i],
                        "high": q["high"][i],
                        "low": q["low"][i],
                        "close": q["close"][i],
                        "volume": q["volume"][i] or 0,
                    })

            splits = []
            events = chart.get("events", {})
            split_events = events.get("splits", {})
            for ts_key, sp in split_events.items():
                splits.append({
                    "date": datetime.fromtimestamp(int(ts_key), tz=timezone.utc).strftime("%Y-%m-%d"),
                    "numerator": sp.get("numerator", 1),
                    "denominator": sp.get("denominator", 1),
                    "ratio": sp.get("splitRatio", ""),
                })

            return {
                "success": True,
                "symbol": symbol,
                "count": len(candles),
                "candles": candles,
                "splits": splits,
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
