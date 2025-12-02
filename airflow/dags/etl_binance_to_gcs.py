import os
import requests
import pandas as pd
import gcsfs
import sys
from datetime import datetime, date, timedelta, timezone
from dotenv import load_dotenv
import time
# Load dotenv
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/gcs_key.json"
# Bucket name
GCS_BUCKET_NAME="my-project-binance-data-lake"
gcs_cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# List of coins
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
            print(f"Lỗi khi gọi API Binance cho {symbol}: {e}")
            return None
    return all_klines

def run_etl(date_to_process):
    time.sleep(10)
    if isinstance(date_to_process, str):
        date_obj = datetime.strptime(date_to_process, '%Y-%m-%d').date()
    else:
        date_obj = date_to_process

    print(f"--- BẮT ĐẦU ETL (GCS) CHO NGÀY: {date_to_process} ---")
    start_of_day = datetime(date_obj.year, date_obj.month, date_obj.day, 0, 0, 0, tzinfo=timezone.utc)
    try:
        # Tự động xác thực bằng Service Account của VM
        gcs_fs = gcsfs.GCSFileSystem(token=gcs_cred_path) 
        print("Đã kết nối GCS thành công.")
    except Exception as e:
        print(f"LỖI: Không thể kết nối GCS. {e}")
        return

    start_of_day = datetime(date_obj.year, date_obj.month, date_obj.day, 0, 0, 0, tzinfo=timezone.utc)
    end_of_day = datetime(date_obj.year, date_obj.month, date_obj.day, 23, 59, 59, 999999, tzinfo=timezone.utc)
    start_ms = int(start_of_day.timestamp() * 1000)
    end_ms = int(end_of_day.timestamp() * 1000)

    all_data_for_day = []
    for coin_raw in COIN_LIST:
        coin = coin_raw.strip() 
        print(f"[Processing Coin: {coin}]")
        klines_data = fetch_binance_klines(coin, start_ms, end_ms)
        
        if klines_data and len(klines_data) == 1440:
            for kline in klines_data:
                kline_dict = dict(zip(KLINE_COLUMNS, kline))
                kline_dict['symbol'] = coin
                all_data_for_day.append(kline_dict)
        else:
            print(f"  -> Không lấy đủ 1440 nến cho {coin}. Bỏ qua.")

    if not all_data_for_day:
        print("Không có dữ liệu nào được tải về. Kết thúc.")
        return


    print(f"Đã tải về {len(all_data_for_day)} dòng. Đang xử lý...")
    df = pd.DataFrame(all_data_for_day)
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df['year'] = df['open_time'].dt.year
    df['month'] = df['open_time'].dt.month
    df['day'] = df['open_time'].dt.day
    df_final = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 
                   'quote_asset_volume', 'number_of_trades', 'symbol', 
                   'year', 'month', 'day']]


    gcs_path = f"{GCS_BUCKET_NAME}/crypto_klines" 
    
    print(f"Đang ghi {len(df_final)} dòng vào GCS (Parquet)...")
    try:
        df_final.to_parquet(
            gcs_path,
            engine='pyarrow',
            filesystem=gcs_fs,
            partition_cols=['symbol', 'year', 'month', 'day']
)
        print(f"--- HOÀN TẤT GHI DỮ LIỆU CHO NGÀY: {date_obj.strftime('%Y-%m-%d')} ---")
    except Exception as e:
        print(f"LỖI khi ghi vào GCS: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        run_etl(date_obj)
    except ValueError:
        print(f"Lỗi: Ngày '{date_str}' không hợp lệ. Hãy dùng định dạng YYYY-MM-DD.")