from fastapi import FastAPI, Query
from pymongo import MongoClient
from datetime import datetime
from typing import List

# ==============================
# CONFIG
# ==============================
MONGO_URI = "..."
DB_NAME = "BigData"

RAW_COLLECTION = "raw_trade"
TUMBLING_COLLECTION = "agg_tumbling_1m"
SLIDING_COLLECTION = "agg_sliding_30s"

# ==============================
# APP INIT
# ==============================
app = FastAPI(
    title="Crypto Analytics API",
    description="REST API for Grafana visualization",
    version="1.0"
)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# ==============================
# HELPERS
# ==============================
def serialize(doc):
    doc["_id"] = str(doc["_id"])
    return doc

# ==============================
# HEALTH CHECK
# ==============================
@app.get("/")
def health():
    return {"status": "ok"}

# ==============================
# SLIDING WINDOW (BEST FOR CHARTS)
# ==============================
@app.get("/crypto/sliding")
def get_sliding(
    symbol: str = Query(..., example="BTCUSDT"),
    limit: int = Query(200, le=1000)
):
    """
    Used by Grafana (Time Series)
    """
    cursor = (
        db[SLIDING_COLLECTION]
        .find({"symbol": symbol})
        .sort("start_time", 1)
        .limit(limit)
    )

    return [
        {
            "time": doc["start_time"],
            "price": doc["close"],
            "volume": doc.get("volume", 0),
            "trades": doc.get("trade_count", 0)
        }
        for doc in cursor
    ]

# ==============================
# TUMBLING WINDOW (OHLC)
# ==============================
@app.get("/crypto/tumbling")
def get_tumbling(
    symbol: str = Query(..., example="BTCUSDT"),
    limit: int = Query(200, le=1000)
):
    cursor = (
        db[TUMBLING_COLLECTION]
        .find({"symbol": symbol})
        .sort("start_time", 1)
        .limit(limit)
    )

    return [
        {
            "time": doc["start_time"],
            "open": doc["open"],
            "high": doc["high"],
            "low": doc["low"],
            "close": doc["close"],
            "volume": doc["volume"]
        }
        for doc in cursor
    ]

# ==============================
# RAW TRADES (TABLE VIEW)
# ==============================
@app.get("/crypto/raw")
def get_raw(
    symbol: str = Query(...),
    limit: int = Query(100, le=1000)
):
    cursor = (
        db[RAW_COLLECTION]
        .find({"symbol": symbol})
        .sort("timestamp", -1)
        .limit(limit)
    )

    return [serialize(doc) for doc in cursor]

@app.get("/grafana/sliding")
def grafana_sliding(
    symbol: str = Query(...),
    limit: int = Query(200)
):
    """
    Grafana-compatible time series endpoint
    """
    cursor = (
        db["agg_sliding_30s"]
        .find({"symbol": symbol})
        .sort("start_time", -1)
        .limit(limit)
    )

    data = []
    for doc in cursor:
        data.append({
            "time": doc["start_time"],
            "open": doc["open"],
            "high": doc["high"],
            "low": doc["low"],
            "close": doc["close"],
            "volume": doc["volume"]
        })

    return list(reversed(data))