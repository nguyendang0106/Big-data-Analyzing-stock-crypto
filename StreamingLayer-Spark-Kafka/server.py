from fastapi import FastAPI, Query, HTTPException
from pymongo import MongoClient
from datetime import datetime
from typing import List
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np

load_dotenv()

# ==============================
# CONFIG
# ==============================
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")

TUMBLING_COLLECTION = os.getenv("MONGO_TUMBLING_COLLECTION", "agg_tumbling_1m")
SLIDING_COLLECTION = os.getenv("MONGO_SLIDING_COLLECTION", "agg_sliding_30s")

GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_RAW_PATH = os.getenv("GCS_RAW_PATH")
GCS_KEY_PATH = os.getenv("GCS_KEY_PATH")

if GCS_KEY_PATH:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_KEY_PATH

# ==============================
# APP INIT
# ==============================
app = FastAPI(
    title="Crypto Analytics API",
    version="2.0"
)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# ==============================
# HELPERS
# ==============================
def serialize(doc):
    """Convert Mongo types -> JSON safe"""
    doc["_id"] = str(doc["_id"])
    doc["start_time"] = doc["start_time"].isoformat()
    doc["end_time"] = doc["end_time"].isoformat()
    return doc

def calculate_technical_indicators(data):
    if not data:
        return []

    df = pd.DataFrame(data)

    # 1. Ensure Datetime & Sort
    # Chuyển đổi sang datetime objects để pandas tính toán
    df["start_time"] = pd.to_datetime(df["start_time"])
    
    # Sắp xếp Cũ -> Mới để tính toán indicator (QUAN TRỌNG)
    df = df.sort_values("start_time", ascending=True)

    # 2. Calculate Indicators
    # SMA 7
    df["sma_7"] = df["close"].rolling(window=7).mean()

    # Bollinger Bands (20, 2)
    df["bb_middle"] = df["close"].rolling(window=20).mean()
    std = df["close"].rolling(window=20).std()
    df["bb_upper"] = df["bb_middle"] + (2 * std)
    df["bb_lower"] = df["bb_middle"] - (2 * std)

    # RSI 14
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # 3. XỬ LÝ DỮ LIỆU ĐỂ TRẢ VỀ API (FIX LỖI GRAFANA)
    
    # FIX QUAN TRỌNG 1: Không dùng fillna(0). 
    # Thay NaN bằng None (Python None -> JSON null).
    # Grafana gặp null sẽ ngắt nét vẽ thay vì kéo xuống 0.
    df = df.replace({np.nan: None})

    # FIX QUAN TRỌNG 2: Format Time chuẩn ISO 8601 (Có chữ T)
    df["start_time"] = df["start_time"].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Xử lý ObjectId
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    return df.to_dict(orient="records")

# ==============================
# HEALTH
# ==============================
@app.get("/")
def health():
    return {"status": "ok"}

# ==============================
# TUMBLING (OHLC)
# ==============================
# @app.get("/grafana/tumbling")
# def tumbling(
#     symbol: str,
#     limit: int = Query(200, le=1000)
# ):
#     docs = list(
#         db[TUMBLING_COLLECTION]
#         .find({"symbol": symbol})
#         .sort("start_time", 1)
#         .limit(limit)
#     )

#     return [
#         {
#             "time": d["start_time"].isoformat(),
#             "open": d["open"],
#             "high": d["high"],
#             "low": d["low"],
#             "close": d["close"],
#             "volume": d["volume"]
#         }
#         for d in docs
#     ]
@app.get("/grafana/tumbling")
def tumbling(
    symbol: str,
    limit: int = Query(200, le=1000)
):
    try:
        docs = list(
            db[TUMBLING_COLLECTION]
            .find({"symbol": symbol})
            .sort("start_time", 1)   # cũ -> mới (Grafana friendly)
            .limit(limit)
        )

        if not docs:
            return []

        result = []
        for d in docs:
            # FIX 1: đảm bảo start_time là datetime
            start_time = d.get("start_time")
            if isinstance(start_time, str):
                start_time = pd.to_datetime(start_time)

            result.append({
                "time": start_time.isoformat() if start_time else None,
                "open": d.get("open"),
                "high": d.get("high"),
                "low": d.get("low"),
                "close": d.get("close"),
                "volume": d.get("volume")
            })

        return result

    except Exception as e:
        # FIX 2: log lỗi chi tiết cho debug Grafana
        raise HTTPException(
            status_code=500,
            detail=f"/grafana/tumbling error: {str(e)}"
        )


# ==============================
# SLIDING + INDICATORS
# ==============================
# @app.get("/grafana/sliding")
# def sliding(
#     symbol: str,
#     limit: int = Query(200, le=1000)
# ):
#     fetch_limit = limit + 50

#     docs = list(
#         db[SLIDING_COLLECTION]
#         .find({"symbol": symbol})
#         .sort("start_time", -1)
#         .limit(fetch_limit)
#     )

#     if not docs:
#         return []

#     processed = calculate_technical_indicators(docs)

#     return processed[-limit:]

@app.get("/grafana/sliding")
def sliding(
    symbol: str,
    limit: int = Query(200, le=1000)
):
    try:
        fetch_limit = limit + 50

        docs = list(
            db[SLIDING_COLLECTION]
            .find({"symbol": symbol})
            .sort("start_time", 1)
            .limit(fetch_limit)
        )

        if not docs:
            return []

        processed = calculate_technical_indicators(docs)

        return processed[-limit:]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))