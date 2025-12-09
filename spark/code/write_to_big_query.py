import os
import traceback
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime, date, timedelta
from google.cloud import storage
from pyspark.sql.functions import (
    year, month, dayofmonth, to_date, col, lit, concat, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType, LongType
)

# --- CẤU HÌNH CHUNG VÀ BIẾN MÔI TRƯỜNG ---
load_dotenv()
START_DATE_FILE_PATH = "/opt/spark/start_date.txt"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_PROJECT_ID = "decoded-tribute-474915-u9"
GCS_BUCKET_NAME = "my-project-binance-data-lake"
GCS_INPUT_PATH = f"gs://{GCS_BUCKET_NAME}/crypto_klines"

COIN_LIST = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT", 
    "XRPUSDT", "PEPEUSDT", "TONUSDT", "SHIBUSDT", "DOTUSDT",
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TRXUSDT", "MATICUSDT",
    "BCHUSDT", "ICPUSDT", "NEARUSDT", "LTCUSDT", "LEOUSD",
    "UNIUSDT", "FETUSDT", "RNDRUSDT", "ATOMUSDT", "INJUSDT",
    "WIFUSDT", "OPUSDT", "ARBUSDT", "ETCUSDT", "FILUSDT"
]

if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("Khong tim thay GAC, kiem tra lai .env")

# --- ĐƯỜNG DẪN JAR TRONG CONTAINER ---
GCS_CONNECTOR_JAR = "/opt/spark/jars/gcs-connector-hadoop3-latest.jar"
BIGQUERY_JAR = "/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar"

# --- KHỞI TẠO HÀM ĐỌC/GHI NGÀY ---
def read_start_date(default_days_ago=7):
    try:
        with open(START_DATE_FILE_PATH, "r") as f:
            date_str = f.read().strip()
            return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return date.today() - timedelta(days=default_days_ago)

def write_next_start_date(date_to_save):
    next_date = date_to_save + timedelta(days=1)
    try:
        with open(START_DATE_FILE_PATH, "w") as f:
            f.write(next_date.strftime("%Y-%m-%d"))
        print(f"Đã lưu ngày tiếp theo cần xử lý: {next_date.strftime('%Y-%m-%d')}")
    except Exception as e:
        print(f"Không thể ghi ngày tiếp theo vào file {START_DATE_FILE_PATH}. {e}")

# --- KHỞI TẠO SPARK SESSION ---
spark = SparkSession.builder \
    .appName("CryptoToBigQueryETL") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
    .config("spark.hadoop.google.cloud.project.id", GCP_PROJECT_ID) \
    .config("spark.jars", f"{GCS_CONNECTOR_JAR},{BIGQUERY_JAR}") \
    .config("spark.driver.extraClassPath", GCS_CONNECTOR_JAR) \
    .config("spark.executor.extraClassPath", GCS_CONNECTOR_JAR) \
    .config("spark.sql.parquet.int96AsTimestamp", "true") \
    .config("spark.sql.parquet.timestampConversion", "false") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.writeLegacyFormat", "false") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
    .getOrCreate()

# --- KHỞI TẠO GCS CLIENT ---
storage_client = storage.Client()
target_bucket = storage_client.bucket(GCS_BUCKET_NAME)

# Hằng số xử lý timestamp (nanoseconds)
MIN_VALID_TIMESTAMP_NS = 946684800000000000    # 2000-01-01 00:00:00
MAX_VALID_TIMESTAMP_NS = 4102444800000000000   # 2100-01-01 00:00:00

# --- SCHEMA: Đọc open_time như LongType (INT64) ---
crypto_kline_schema = StructType([
    StructField("open_time", LongType(), True),  # INT64 milliseconds
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("quote_asset_volume", DoubleType(), True),
    StructField("number_of_trades", LongType(), True),
    StructField("symbol", StringType(), False),
    StructField("taker_buy_base_asset_volume", DoubleType(), True), 
    StructField("taker_buy_quote_asset_volume", DoubleType(), True),
])

# --- HÀM ETL CHÍNH ---
def run_spark_etl_from_gcs(execution_date):
    
    execution_date_str = execution_date.strftime('%Y-%m-%d')
    exec_year = execution_date.year
    exec_month = execution_date.month
    exec_day = execution_date.day
    success_on_day = True

    for coin in COIN_LIST:
        print(f"\n--- Bắt đầu xử lý Coin: {coin} cho ngày {execution_date_str} ---")
        
        gcs_prefix = (
            f"crypto_klines/symbol={coin}/year={exec_year}/month={exec_month}/day={exec_day}/"
        )
        full_gcs_path = f"gs://{GCS_BUCKET_NAME}/{gcs_prefix}"

        # Kiểm tra sự tồn tại của thư mục
        if not next(target_bucket.list_blobs(prefix=gcs_prefix, max_results=1), None):
            if coin == "LEOUSD":
                print(f"LEOUSD không phải là cặp USDT. Bỏ qua.")
            else:
                print(f"Thư mục không tồn tại hoặc rỗng tại: {full_gcs_path}. Bỏ qua coin này.")
            continue 

        try:
            # 1. Đọc dữ liệu từ GCS (open_time là LongType - milliseconds)
            df = spark.read.schema(crypto_kline_schema).parquet(full_gcs_path)
            
            original_count = df.count()
            print(f"Đọc được {original_count} dòng từ GCS")
            
            if original_count == 0:
                print(f"Dữ liệu rỗng cho {coin}. Bỏ qua.")
                continue
            
            # 2. Debug: Xem giá trị open_time thô
            print(f"Dữ liệu mẫu open_time (nanoseconds):")
            df.select("open_time").show(5, truncate=False)
            
            # 3. Lọc các giá trị timestamp hợp lệ
            # Chỉ lọc NULL và giá trị ngoài khoảng hợp lý (nanoseconds)
            df_cleaned = df.filter(
                col("open_time").isNotNull() & 
                (col("open_time") >= MIN_VALID_TIMESTAMP_NS) &
                (col("open_time") <= MAX_VALID_TIMESTAMP_NS)
            )
            
            count = df_cleaned.count()
            
            if count == 0:
                print(f"Dữ liệu rỗng sau khi lọc cho {coin}. Bỏ qua.")
                continue
            
            if count < original_count:
                filtered = original_count - count
                print(f"Đã lọc bỏ {filtered} dòng có timestamp không hợp lệ ({filtered/original_count*100:.2f}%)")
            
            # 4. Convert open_time (nanoseconds) sang open_datetime (timestamp)
            # Chia cho 1,000,000,000 để đổi từ nanoseconds sang seconds
            df_with_timestamp = df_cleaned.withColumn(
                "open_datetime",
                from_unixtime(col("open_time") / 1000000000).cast(TimestampType())
            )
            
            # 5. Lọc các dòng có open_datetime NULL (nếu conversion thất bại)
            df_with_valid_timestamp = df_with_timestamp.filter(col("open_datetime").isNotNull())
            
            valid_count = df_with_valid_timestamp.count()
            
            if valid_count == 0:
                print(f"Không còn dữ liệu sau khi convert timestamp cho {coin}. Bỏ qua.")
                continue
            
            if valid_count < count:
                print(f"Đã loại bỏ thêm {count - valid_count} dòng sau conversion")
            
            # 6. Thêm cột phân vùng
            df_with_partitions = df_with_valid_timestamp.withColumn("symbol", lit(coin)) \
                                                        .withColumn("year", lit(str(exec_year))) \
                                                        .withColumn("month", lit(str(exec_month))) \
                                                        .withColumn("day", lit(str(exec_day)))
            
            # 7. Tạo partition_date từ open_datetime
            df_transformed = df_with_partitions.withColumn(
                "partition_date", 
                to_date(col("open_datetime"))
            )
            
            # 8. Cache để tránh tính toán lại
            df_transformed.cache()
            
            final_count = df_transformed.count()
            
            # 9. Debug: Hiển thị schema và mẫu dữ liệu
            print(f"Schema trước khi ghi:")
            df_transformed.printSchema()
            print(f"Dữ liệu mẫu (3 dòng đầu):")
            df_transformed.select("open_time", "open_datetime", "open", "close", "symbol", "partition_date").show(3, truncate=False)
            
            # 10. Ghi vào BigQuery
            BIGQUERY_FULL_TABLE = f"{GCP_PROJECT_ID}.crypto_data.binance_klines"
            print(f"Đang ghi {final_count} dòng vào BigQuery cho {coin}...")

            df_transformed.write.format("bigquery") \
                .option("table", BIGQUERY_FULL_TABLE) \
                .option("temporaryGcsBucket", GCS_BUCKET_NAME) \
                .option("partitionField", "partition_date") \
                .option("partitionType", "DAY") \
                .option("allowFieldAddition", "true") \
                .option("createDisposition", "CREATE_IF_NEEDED") \
                .mode("append") \
                .save()
            
            # 11. Giải phóng cache
            df_transformed.unpersist()
            
            print(f"Ghi dữ liệu {coin} thành công! ({final_count} dòng)")
        
        except Exception as e:
            print(f"LỖI XỬ LÝ DỮ LIỆU {coin} cho ngày {execution_date_str}.")
            print(f"Lỗi: {e}")
            print("----- CHI TIẾT LỖI (TRACEBACK) -----")
            traceback.print_exc()
            print("-----------------------------------")
            success_on_day = False
            return success_on_day 
            
    return success_on_day

# --- CHU TRÌNH CHÍNH ---
if __name__ == "__main__":
    start_processing_date = read_start_date()
    end_processing_date = date.today() - timedelta(days=1)
    
    current_date = start_processing_date
    overall_success_flag = True

    while current_date <= end_processing_date:
        print(f"\n==================================================")
        print(f"Bắt đầu xử lý TẤT CẢ COIN cho ngày: {current_date.strftime('%Y-%m-%d')}")
        
        try:
            if run_spark_etl_from_gcs(current_date):
                write_next_start_date(current_date)
                current_date += timedelta(days=1)
            else:
                print(f"LỖI XỬ LÝ MỘT HOẶC NHIỀU COIN cho ngày {current_date.strftime('%Y-%m-%d')}. Dừng lại.")
                overall_success_flag = False
                break
            
        except Exception as e:
            print(f"LỖI TOÀN BỘ CHU TRÌNH cho ngày {current_date.strftime('%Y-%m-%d')}. Dừng lại. Lỗi: {e}")
            overall_success_flag = False
            break
            
    if overall_success_flag:
        print("\nHoàn tất xử lý tăng dần.")
    
    spark.stop()