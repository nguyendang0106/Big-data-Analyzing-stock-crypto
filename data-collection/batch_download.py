import os
import requests
import pandas as pd
import s3fs
import io
import zipfile
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = "http://host.docker.internal:9000"
MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")
MINIO_BUCKET = "crypto-data"

# Danh sach cac dong tien ma chung ta muon tai ve
COIN_LIST = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT", 
    "XRPUSDT", "PEPEUSDT", "TONUSDT", "SHIBUSDT", "DOTUSDT",
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TRXUSDT", "MATICUSDT",
    "BCHUSDT", "ICPUSDT", "NEARUSDT", "LTCUSDT", "LEOUSD",
    "UNIUSDT", "FETUSDT", "RNDRUSDT", "ATOMUSDT", "INJUSDT",
    "WIFUSDT", "OPUSDT", "ARBUSDT", "ETCUSDT", "FILUSDT"
]

# Cac cot trong du lieu kline tu Binance API
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

# Link goc tai du lieu tu Binance Vision
BASE_DOWNLOAD_URL = "https://data.binance.vision/data/spot/monthly/klines"

START_DATE = date(2023, 1, 1)  # Ngay bat dau co du lieu tu Binance
END_DATE = date.today()  # Ngay ket thuc la ngay hien tai

# Su dung s3fs de ket noi den MinIO
fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': MINIO_ENDPOINT},
    key=MINIO_USER,
    secret=MINIO_PASSWORD
)


S3_PATH = f"{MINIO_BUCKET}/crypto_klines/"

def get_monthly_range(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield (current_date.year, str(current_date.month).zfill(2))
        current_date += relativedelta(months=1)

        
def run_historical_load():
    print("Starting historical data load...")

    months_to_fetch = list(get_monthly_range(START_DATE, END_DATE))
    for coin in COIN_LIST:
        print(f"\n[Processing Coin: {coin}]")
        
        for year, month in months_to_fetch:
            # Tên file ZIP (ví dụ: BTCUSDT-1m-2023-01.zip)
            zip_file_name = f"{coin}-1m-{year}-{month}.zip"
            # Tên file CSV bên trong ZIP
            csv_file_name = f"{coin}-1m-{year}-{month}.csv"
            
            # Tạo URL tải về
            download_url = f"{BASE_DOWNLOAD_URL}/{coin}/1m/{zip_file_name}"
            
            print(f"  Đang tải: {download_url}...")
            
            try:
                # Tải file ZIP về bộ nhớ
                response = requests.get(download_url, stream=True)
                
                # Kiểm tra xem file có tồn tại không (ví dụ: tháng này chưa có)
                if response.status_code == 404:
                    print(f"  -> File không tồn tại (404). Bỏ qua.")
                    continue
                
                response.raise_for_status() # Báo lỗi nếu có (ví dụ: 500, 403)
                
                # Mở file ZIP từ bộ nhớ (bytes)
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    # Đọc file CSV bên trong ZIP
                    with z.open(csv_file_name) as f:
                        df = pd.read_csv(f, header=None, names=KLINE_COLUMNS)

                print(f"  -> Đã tải và đọc {len(df)} dòng.")

                # Xử lý dữ liệu
                df['open_time'] = pd.to_numeric(df['open_time'], errors='coerce')
                df = df.dropna(subset=['open_time']) # Xóa dòng không phải là số
                
                if df.empty:
                    print("  -> File rỗng sau khi lọc dữ liệu hỏng. Bỏ qua.")
                    continue
                
                # 2. Tìm timestamp lớn nhất để "đoán" đơn vị
                # 13 chữ số (ms) có giá trị max ~ 9.999.999.999.999
                # 16 chữ số (us) có giá trị max ~ 9.999.999.999.999.999
                # Ta dùng mốc 14 chữ số (100.000.000.000.000)
                
                if df['open_time'].max() > 100000000000000: # Nếu lớn hơn 14 chữ số
                    print("  -> Phát hiện timestamp (microseconds). Đang chuyển đổi...")
                    df['open_time'] = df['open_time'] // 1000 # Chuyển từ us -> ms
                else:
                    print("  -> Phát hiện timestamp (milliseconds).")
                    
                # 3. Bây giờ, MỌI DỮ LIỆU đều là milliseconds (ms), ta có thể convert an toàn
                df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', errors='coerce')

                # 4. Xóa các dòng hỏng (vẫn có thể có)
                original_rows = len(df)
                df = df.dropna(subset=['open_time'])
                new_rows = len(df)
                
                if new_rows < original_rows:
                    print(f"  -> Đã phát hiện và xóa {original_rows - new_rows} dòng dữ liệu hỏng (ngoài phạm vi).")

                if new_rows < original_rows:
                    print(f"  -> Đã phát hiện và xóa {original_rows - new_rows} dòng dữ liệu hỏng.")
                # Tạo các cột để phân vùng (partition)
                df['symbol'] = coin
                df['year'] = df['open_time'].dt.year
                df['month'] = df['open_time'].dt.month
                df['day'] = df['open_time'].dt.day
                
                # Bỏ các cột không cần thiết
                df = df.drop(columns=['close_time', 'ignore'])

                # Ghi ra MinIO với phân vùng
                # Pandas sẽ tự động tạo cấu trúc thư mục
                # ví dụ: s3://binance-kline-1m/symbol=BTCUSDT/year=2023/month=1/day=1/...
                # và mỗi thư mục con "day=..." sẽ chứa 1 file parquet
                print(f"  -> Đang lưu vào MinIO (phân vùng theo ngày)...")
                df.to_parquet(
                    S3_PATH,
                    engine='pyarrow',
                    filesystem=fs,
                    partition_cols=['symbol', 'year', 'month', 'day'],
                    compression='snappy'
                )
                print(f"  -> Đã lưu thành công {coin} - {year}-{month}.")

            except requests.exceptions.RequestException as e:
                print(f"  Lỗi khi tải file: {e}")
            except zipfile.BadZipFile:
                print(f"  Lỗi: File tải về không phải là file ZIP hợp lệ.")
            except KeyError:
                print(f"  Lỗi: Không tìm thấy file {csv_file_name} bên trong file ZIP.")
            except Exception as e:
                print(f"  Lỗi không xác định: {e}")

    print("\n--- HOÀN TẤT TẢI DỮ LIỆU LỊCH SỬ ---")

if __name__ == "__main__":
    run_historical_load()

