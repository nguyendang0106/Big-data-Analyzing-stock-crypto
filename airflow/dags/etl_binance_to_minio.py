import os
import requests
import pandas as pd
import s3fs
import sys
from datetime import datetime, date, timedelta, timezone

MINIO_ENDPOINT = "http://host.docker.internal:9000"
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")
MINIO_BUCKET = "crypto-data"


# Danh sach cac dong coin muon lay
COIN_LIST = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT", 
    "XRPUSDT", "PEPEUSDT", "TONUSDT", "SHIBUSDT", "DOTUSDT",
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TRXUSDT", "MATICUSDT",
    "BCHUSDT", "ICPUSDT", "NEARUSDT", "LTCUSDT", "LEOUSD",
    "UNIUSDT", "FETUSDT", "RNDRUSDT", "ATOMUSDT", "INJUSDT",
    "WIFUSDT", "OPUSDT", "ARBUSDT", "ETCUSDT", "FILUSDT"
]


BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

KLINE_COLUMNS = ["open_time", "open", "high", "low", "close", "volume", 
                 "close_time", "quote_asset_volume", "number_of_trades", 
                 "taker_buy_base", "taker_buy_quote", "ignore"]


# Ham goi API Binance
def fetch_binance_klines(symbol, start_time_ms, end_time_ms):
    all_klines = []
    mid_time_ms = start_time_ms + (12 * 60 * 60 * 1000)
    time_ranges = [(start_time_ms, mid_time_ms - 1), (mid_time_ms, end_time_ms)]

    for start, end in time_ranges:
        params = {"symbol": symbol, "interval": "1m", "startTime": start, "endTime": end, "limit": 1000}
        try:
            response = requests.get(BINANCE_API_URL, params=params)
            response.raise_for_status()
            klines = response.json()
            all_klines.extend(klines)
        except Exception as e:
            print(f"  Lỗi khi gọi API Binance cho {symbol}: {e}")
            return None
    return all_klines



def run_etl(date_to_process):
    print(f"--- BẮT ĐẦU ETL CHO NGÀY: {date_to_process.strftime('%Y-%m-%d')} ---")
    
    # Kết nối MinIO
    try:
        minio_fs = s3fs.S3FileSystem(
            client_kwargs={'endpoint_url': MINIO_ENDPOINT},
            key=MINIO_USER,
            secret=MINIO_PASSWORD
        )
        print("Đã kết nối MinIO thành công.")
    except Exception as e:
        print(f"LỖI: Không thể kết nối MinIO tại {MINIO_ENDPOINT}. {e}")
        return
    start_of_day = datetime(date_to_process.year, date_to_process.month, date_to_process.day, 0, 0, 0, tzinfo=timezone.utc)
    end_of_day = datetime(date_to_process.year, date_to_process.month, date_to_process.day, 23, 59, 59, 999999, tzinfo=timezone.utc)
    start_ms = int(start_of_day.timestamp() * 1000)
    end_ms = int(end_of_day.timestamp() * 1000)

    all_data_for_day = [] # Gom tất cả coin vào list này

    for coin in COIN_LIST:
        print(f"[Processing Coin: {coin}]")
        klines_data = fetch_binance_klines(coin, start_ms, end_ms)
        
        if klines_data and len(klines_data) == 1440:
            for kline in klines_data:
                kline_dict = dict(zip(KLINE_COLUMNS, kline))
                kline_dict['symbol'] = coin # Thêm cột symbol
                all_data_for_day.append(kline_dict)
        else:
            print(f"  -> Không lấy đủ 1440 nến cho {coin}. Bỏ qua.")

    if not all_data_for_day:
        print("Không có dữ liệu nào được tải về. Kết thúc.")
        return

    # --- TRANSFORM (Chuyển đổi) ---
    print(f"Đã tải về {len(all_data_for_day)} dòng. Đang xử lý...")
    df = pd.DataFrame(all_data_for_day)
    
    # Chuyển đổi kiểu dữ liệu
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    # Thêm cột phân vùng (partition)
    df['year'] = df['open_time'].dt.year
    df['month'] = df['open_time'].dt.month
    df['day'] = df['open_time'].dt.day
    
    # Bỏ các cột không cần thiết
    df_final = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 
                   'quote_asset_volume', 'number_of_trades', 'symbol', 
                   'year', 'month', 'day']]

    # --- LOAD (Lưu vào MinIO) ---
    s3_path = f"{MINIO_BUCKET}/crypto_klines" # Cùng thư mục với Giai đoạn 1
    
    print(f"Đang ghi {len(df_final)} dòng vào MinIO (Parquet)...")
    try:
        df_final.to_parquet(
            f"s3://{s3_path}",
            engine='pyarrow',
            filesystem=minio_fs,
            partition_cols=['symbol', 'year', 'month', 'day'],
            compression='snappy',
            existing_data_behavior='overwrite_or_ignore' # Thêm file mới vào partition
        )
        print(f"--- HOÀN TẤT GHI DỮ LIỆU CHO NGÀY: {date_to_process.strftime('%Y-%m-%d')} ---")
    except Exception as e:
        print(f"LỖI khi ghi vào MinIO: {e}")



if __name__ == "__main__":
    # Kịch bản này nhận ngày từ tham số dòng lệnh (ví dụ: "2025-10-01") de xu ly phan du lieu con thieu khi dung data.binance.vision
    if len(sys.argv) > 1:
        date_str = sys.argv[1]

    # Neu khong thi se tu dong lay du lieu tu hom qua
    else:
        date_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        run_etl(date_obj)
    except ValueError:
        print(f"Lỗi: Ngày '{date_str}' không hợp lệ. Hãy dùng định dạng YYYY-MM-DD.")

    
    