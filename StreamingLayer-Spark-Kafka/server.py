# from fastapi import FastAPI, Query
# from pymongo import MongoClient
# from datetime import datetime
# from typing import List
# from dotenv import load_dotenv
# import os
# load_dotenv()
# # ==============================
# # CONFIG
# # ==============================
# MONGO_URI = os.getenv("MONGO_URI")
# DB_NAME = "BigData"

# RAW_COLLECTION = "raw_trade"
# TUMBLING_COLLECTION = "agg_tumbling_1m"
# SLIDING_COLLECTION = "agg_sliding_30s"

# # ==============================
# # APP INIT
# # ==============================
# app = FastAPI(
#     title="Crypto Analytics API",
#     description="REST API for Grafana visualization",
#     version="1.0"
# )

# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]

# # ==============================
# # HELPERS
# # ==============================
# def serialize(doc):
#     doc["_id"] = str(doc["_id"])
#     return doc

# # ==============================
# # HEALTH CHECK
# # ==============================
# @app.get("/")
# def health():
#     return {"status": "ok"}

# # ==============================
# # SLIDING WINDOW (BEST FOR CHARTS)
# # ==============================
# @app.get("/crypto/sliding")
# def get_sliding(
#     symbol: str = Query(..., example="BTCUSDT"),
#     limit: int = Query(200, le=1000)
# ):
#     """
#     Used by Grafana (Time Series)
#     """
#     cursor = (
#         db[SLIDING_COLLECTION]
#         .find({"symbol": symbol})
#         .sort("start_time", 1)
#         .limit(limit)
#     )

#     return [
#         {
#             "time": doc["start_time"],
#             "price": doc["close"],
#             "volume": doc.get("volume", 0),
#             "trades": doc.get("trade_count", 0)
#         }
#         for doc in cursor
#     ]

# # ==============================
# # TUMBLING WINDOW (OHLC)
# # ==============================
# @app.get("/crypto/tumbling")
# def get_tumbling(
#     symbol: str = Query(..., example="BTCUSDT"),
#     limit: int = Query(200, le=1000)
# ):
#     cursor = (
#         db[TUMBLING_COLLECTION]
#         .find({"symbol": symbol})
#         .sort("start_time", 1)
#         .limit(limit)
#     )

#     return [
#         {
#             "time": doc["start_time"],
#             "open": doc["open"],
#             "high": doc["high"],
#             "low": doc["low"],
#             "close": doc["close"],
#             "volume": doc["volume"]
#         }
#         for doc in cursor
#     ]

# # ==============================
# # RAW TRADES (TABLE VIEW)
# # ==============================
# @app.get("/crypto/raw")
# def get_raw(
#     symbol: str = Query(...),
#     limit: int = Query(100, le=1000)
# ):
#     cursor = (
#         db[RAW_COLLECTION]
#         .find({"symbol": symbol})
#         .sort("timestamp", -1)
#         .limit(limit)
#     )

#     return [serialize(doc) for doc in cursor]

# @app.get("/grafana/sliding")
# def grafana_sliding(
#     symbol: str = Query(...),
#     limit: int = Query(200)
# ):
#     """
#     Grafana-compatible time series endpoint
#     """
#     cursor = (
#         db["agg_sliding_30s"]
#         .find({"symbol": symbol})
#         .sort("start_time", -1)
#         .limit(limit)
#     )

#     data = []
#     for doc in cursor:
#         data.append({
#             "time": doc["start_time"],
#             "open": doc["open"],
#             "high": doc["high"],
#             "low": doc["low"],
#             "close": doc["close"],
#             "volume": doc["volume"]
#         })

#     return list(reversed(data))

from fastapi import FastAPI, Query, HTTPException
from pymongo import MongoClient
from datetime import datetime, date
from typing import List
from dotenv import load_dotenv
import os
import pandas as pd
import json

load_dotenv()

# ==============================
# CONFIG
# ==============================
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")

# Collections
TUMBLING_COLLECTION = os.getenv("MONGO_TUMBLING_COLLECTION", "agg_tumbling_1m")
SLIDING_COLLECTION = os.getenv("MONGO_SLIDING_COLLECTION", "agg_sliding_30s")

# GCS Config
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_RAW_PATH = os.getenv("GCS_RAW_PATH")
GCS_KEY_PATH = os.getenv("GCS_KEY_PATH")

# Setup Google Auth cho Pandas/GCSFS
if GCS_KEY_PATH:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_KEY_PATH

# ==============================
# APP INIT
# ==============================
app = FastAPI(
    title="Crypto Analytics API",
    description="REST API reading Aggregates from MongoDB and Raw Data from GCS",
    version="2.0"
)

# MongoDB Connection
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print("Connected to MongoDB")
except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")

# ==============================
# HELPERS
# ==============================
def serialize_mongo_doc(doc):
    """Serialize MongoDB ObjectId and Dates"""
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

# ==============================
# HEALTH CHECK
# ==============================
@app.get("/")
def health():
    return {"status": "ok", "source_raw": "GCS (Parquet)", "source_agg": "MongoDB"}

# ==============================
# SLIDING WINDOW (BEST FOR CHARTS) - FROM MONGODB
# ==============================
@app.get("/crypto/sliding")
def get_sliding(
    symbol: str = Query(..., example="BTCUSDT"),
    limit: int = Query(200, le=1000)
):
    """
    Lấy dữ liệu Sliding Window từ MongoDB (Dùng cho Realtime Chart)
    """
    cursor = (
        db[SLIDING_COLLECTION]
        .find({"symbol": symbol})
        .sort("start_time", 1) # Sắp xếp tăng dần theo thời gian để vẽ biểu đồ
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
# TUMBLING WINDOW (OHLC) - FROM MONGODB
# ==============================
@app.get("/crypto/tumbling")
def get_tumbling(
    symbol: str = Query(..., example="BTCUSDT"),
    limit: int = Query(200, le=1000)
):
    """
    Lấy dữ liệu Candlestick (OHLC) từ MongoDB
    """
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
# RAW TRADES (TABLE VIEW) - FROM GCS (PARQUET)
# ==============================
@app.get("/crypto/raw")
def get_raw(
    symbol: str = Query(..., example="BTCUSDT"),
    limit: int = Query(100, le=1000)
):
    """
    Lấy dữ liệu Raw Trade trực tiếp từ file Parquet trên GCS.
    Spark lưu theo cấu trúc partition: symbol=.../date=.../hour=...
    """
    if not GCS_BUCKET or not GCS_RAW_PATH:
        raise HTTPException(status_code=500, detail="GCS Configuration missing on server")

    try:
        # Đường dẫn gốc tới thư mục chứa dữ liệu raw trên GCS
        # Spark partition: gs://bucket/raw_path/symbol=BTCUSDT/...
        # Chúng ta dùng filters của PyArrow để chỉ đọc partition của symbol cần tìm (Partition Pruning)
        
        gcs_path = f"gs://{GCS_BUCKET}/{GCS_RAW_PATH}"
        
        # Đọc dữ liệu với Filter để tối ưu tốc độ (tránh đọc toàn bộ bucket)
        # Lưu ý: Việc đọc Parquet từ GCS sẽ chậm hơn Mongo, nên giới hạn cột cần lấy
        df = pd.read_parquet(
            gcs_path, 
            filters=[('symbol', '==', symbol)],
            columns=['event_time', 'price', 'quantity', 'is_buyer_maker', 'timestamp', 'symbol']
        )
        
        if df.empty:
            return []

        # Sắp xếp giảm dần theo thời gian và lấy limit
        df = df.sort_values(by='timestamp', ascending=False).head(limit)

        # Chuyển đổi Timestamp sang string để JSON serialize được
        # Spark lưu timestamp dạng microsecond/millisecond, pandas đọc thành datetime64[ns]
        df['timestamp'] = df['timestamp'].astype(str)
        
        # Chuyển thành List of Dicts
        result = df.to_dict(orient="records")
        return result

    except Exception as e:
        print(f"Error reading GCS: {e}")
        # Trong trường hợp chưa có dữ liệu hoặc lỗi kết nối GCS
        return {"error": str(e), "message": "Could not fetch data from GCS"}

# ==============================
# GRAFANA ENDPOINT
# ==============================
@app.get("/grafana/sliding")
def grafana_sliding(
    symbol: str = Query(...),
    limit: int = Query(200)
):
    """
    Grafana-compatible time series endpoint (From MongoDB)
    """
    cursor = (
        db[SLIDING_COLLECTION]
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