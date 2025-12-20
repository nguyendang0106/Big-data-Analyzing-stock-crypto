# ============================================================
# Batch Feature ETL for Crypto Analytics
# Reads from BigQuery: crypto_data.binance_klines
# Writes to BigQuery: crypto_data.binance_features
# ============================================================

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, avg, stddev, lag, log,
    hour, broadcast
)

# ------------------------------------------------------------
# ENV & CONFIG
# ------------------------------------------------------------
load_dotenv()

GCP_PROJECT_ID = "decoded-tribute-474915-u9"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCS_BUCKET_NAME = "my-project-binance-data-lake"

SOURCE_TABLE = f"{GCP_PROJECT_ID}.crypto_data.binance_klines"
TARGET_TABLE = f"{GCP_PROJECT_ID}.crypto_data.binance_features"

GCS_CONNECTOR_JAR = "/opt/spark/jars/gcs-connector-hadoop3-latest.jar"
BIGQUERY_JAR = "/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar"

if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("Missing GOOGLE_APPLICATION_CREDENTIALS")

# ------------------------------------------------------------
# SPARK SESSION
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("CryptoBatchFeatureETL") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
    .config("spark.hadoop.google.cloud.project.id", GCP_PROJECT_ID) \
    .config("spark.jars", f"{GCS_CONNECTOR_JAR},{BIGQUERY_JAR}") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "200")

# ------------------------------------------------------------
# 1. READ DATA WITH PARTITION PRUNING
# ------------------------------------------------------------
print("Reading source data with partition pruning...")

df = spark.read.format("bigquery") \
    .option("table", SOURCE_TABLE) \
    .load() \
    .filter(col("partition_date") >= "2024-01-01")   # <-- partition pruning

df.cache()
print(f"Source count: {df.count()}")

# ------------------------------------------------------------
# 2. WINDOW FUNCTIONS (SMA, RETURNS, VOLATILITY)
# ------------------------------------------------------------
window_1h = Window.partitionBy("symbol").orderBy("open_datetime")
window_24h = window_1h.rowsBetween(-24, 0)

df_features = df \
    .withColumn("log_return",
                log(col("close")) - log(lag("close", 1).over(window_1h))) \
    .withColumn("sma_24h",
                avg("close").over(window_24h)) \
    .withColumn("volatility_24h",
                stddev("log_return").over(window_24h))

# ------------------------------------------------------------
# 3. AGGREGATION + PIVOT (Hour × Symbol)
# ------------------------------------------------------------
df_hourly = df_features \
    .withColumn("hour", hour("open_datetime")) \
    .groupBy("partition_date", "hour") \
    .pivot("symbol") \
    .avg("close")

# ------------------------------------------------------------
# 4. BROADCAST JOIN – COIN METADATA
# ------------------------------------------------------------
coin_metadata = spark.createDataFrame([
    ("BTCUSDT", "Bitcoin", "PoW"),
    ("ETHUSDT", "Ethereum", "PoS"),
    ("SOLUSDT", "Solana", "PoS"),
    ("BNBUSDT", "BNB", "PoSA"),
], ["symbol", "coin_name", "consensus"])

df_with_meta = df_features.join(
    broadcast(coin_metadata),
    on="symbol",
    how="left"
)

# ------------------------------------------------------------
# 5. SORT-MERGE JOIN – SENTIMENT DATA
# ------------------------------------------------------------
sentiment_schema = ["partition_date", "symbol", "sentiment_score"]
sentiment_data = [
    ("2024-01-01", "BTCUSDT", 0.35),
    ("2024-01-01", "ETHUSDT", 0.21),
]

df_sentiment = spark.createDataFrame(sentiment_data, sentiment_schema) \
    .withColumn("partition_date", col("partition_date").cast("date"))

df_final = df_with_meta.join(
    df_sentiment,
    on=["partition_date", "symbol"],
    how="left"
)

# ------------------------------------------------------------
# 6. CACHE + EXPLAIN PLAN
# ------------------------------------------------------------
df_final.cache()

print("\n===== EXECUTION PLAN =====")
df_final.explain(mode="formatted")

print(f"Final feature count: {df_final.count()}")

# ------------------------------------------------------------
# 7. WRITE TO BIGQUERY (SERVING / ANALYTICS LAYER)
# ------------------------------------------------------------
print("Writing features to BigQuery...")

df_final.write.format("bigquery") \
    .option("table", TARGET_TABLE) \
    .option("temporaryGcsBucket", GCS_BUCKET_NAME) \
    .option("partitionField", "partition_date") \
    .option("partitionType", "DAY") \
    .mode("overwrite") \
    .save()

print("Batch feature ETL completed successfully.")

spark.stop()
