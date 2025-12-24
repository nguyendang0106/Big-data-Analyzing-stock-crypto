"""
Streaming inference:
- Read Kafka trades (topic binance)
- Aggregate tumbling 1m and sliding 1m/30s
- Compute features (requires short history) using a small state per symbol (kept in-memory)
- Score with latest GBT model from GCS
- Write to Mongo collections ml_signals_tumbling and ml_signals_sliding
"""
import json
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List

from pymongo import MongoClient
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
    TimestampType,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.ml import PipelineModel

from mlspark.config import load_streaming_config
from mlspark.feature_engineering import add_bar_features, forward_fill, feature_columns
from mlspark.model_io import latest_model_uri, load_pipeline


# Keep short history per symbol for feature windows
HISTORY: Dict[str, Deque[dict]] = defaultdict(lambda: deque(maxlen=200))
MODEL_CACHE = {"path": None, "model": None, "last_check": datetime.utcnow()}


def load_model_if_needed(cfg, spark: SparkSession) -> PipelineModel:
    now = datetime.utcnow()
    if MODEL_CACHE["model"] is None or (now - MODEL_CACHE["last_check"]) > timedelta(minutes=cfg.refresh_minutes):
        path = latest_model_uri(cfg.gcs_bucket, cfg.model_base_path, cfg.model_name)
        MODEL_CACHE["last_check"] = now
        if path and path != MODEL_CACHE["path"]:
            MODEL_CACHE["model"] = load_pipeline(path)
            MODEL_CACHE["path"] = path
            print(f"[Model] Loaded {path}")
    if MODEL_CACHE["model"] is None:
        raise RuntimeError("Model not found in GCS; ensure training ran.")
    return MODEL_CACHE["model"]


def update_history(rows: List[dict]):
    for r in rows:
        HISTORY[r["symbol"]].append(r)


def build_feature_df(spark, new_rows: List[dict]):
    if not new_rows:
        return None
    update_history(new_rows)
    data = []
    for _, dq in HISTORY.items():
        data.extend(list(dq))
    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), False),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("trade_count", LongType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema=schema)
    df = df.withColumn("event_time", F.col("end_time"))
    df = add_bar_features(df)
    df = forward_fill(df, feature_columns())
    newest_times = {r["end_time"] for r in new_rows}
    return df.filter(F.col("end_time").isin(list(newest_times)))


def write_mongo(cfg, docs: List[dict], collection: str):
    if not docs:
        return
    client = MongoClient(cfg.mongo_uri)
    coll = client[cfg.mongo_db][collection]
    coll.insert_many(docs, ordered=False)
    client.close()


def score_and_write(df_feat, cfg, model: PipelineModel, collection: str):
    if df_feat is None or df_feat.rdd.isEmpty():
        return
    scored = model.transform(df_feat)
    scored = scored.withColumn("pred_up_prob", F.col("probability").getItem(1))
    scored = scored.withColumn("pred_return", F.lit(None).cast("double"))
    scored = scored.withColumn("pred_horizon_sec", F.lit(300))
    scored = scored.withColumn("model_version", F.lit(MODEL_CACHE["path"]))
    scored = scored.withColumn("feature_version", F.lit("v1"))
    scored = scored.withColumn("generated_at", F.lit(datetime.utcnow()))
    scored = scored.withColumn("event_time", F.col("end_time"))
    out_cols = [
        "symbol",
        "start_time",
        "end_time",
        "close",
        "volume",
        "trade_count",
        "pred_horizon_sec",
        "pred_up_prob",
        "pred_return",
        "anomaly_score",
        "model_version",
        "feature_version",
        "generated_at",
        "event_time",
    ]
    docs = [json.loads(r.toJSON()) for r in scored.select(out_cols).collect()]
    write_mongo(cfg, docs, collection)
    print(f"[Mongo] wrote {len(docs)} docs -> {collection}")


def foreach_batch_writer(cfg, collection: str):
    def _fn(df, batch_id):
        if df.isEmpty():
            return
        rows = [r.asDict() for r in df.collect()]
        feat_df = build_feature_df(df.sparkSession, rows)
        if feat_df is None or feat_df.rdd.isEmpty():
            return
        model = load_model_if_needed(cfg, df.sparkSession)
        score_and_write(feat_df, cfg, model, collection)

    return _fn


def build_trades_df(spark, cfg):
    trade_schema = StructType(
        [
            StructField("e", StringType()),
            StructField("E", LongType()),
            StructField("s", StringType()),
            StructField("t", LongType()),
            StructField("p", StringType()),
            StructField("q", StringType()),
            StructField("T", LongType()),
            StructField("m", BooleanType()),
            StructField("M", BooleanType()),
        ]
    )
    root = StructType([StructField("stream", StringType()), StructField("data", trade_schema)])
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", cfg.kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", root).alias("r"))
        .select("r.data.*")
    )
    trades = (
        parsed.select(
            F.col("s").alias("symbol"),
            F.col("p").cast("double").alias("price"),
            F.col("q").cast("double").alias("quantity"),
            (F.col("T") / F.lit(1000)).cast("timestamp").alias("timestamp"),
        )
        .withWatermark("timestamp", "5 seconds")
    )
    return trades


def aggregate_bars(trades, slide: str = None):
    w = F.window("timestamp", "1 minute", slide) if slide else F.window("timestamp", "1 minute")
    return (
        trades.groupBy(w, "symbol")
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("*").alias("trade_count"),
        )
        .select(
            "symbol",
            F.col("window.start").alias("start_time"),
            F.col("window.end").alias("end_time"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
        )
    )


def main():
    cfg = load_streaming_config()
    spark = (
        SparkSession.builder.appName("CryptoML-Streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    if cfg.gcs_key_path:
        spark.sparkContext._jsc.hadoopConfiguration().set(
            "google.cloud.auth.service.account.json.keyfile", cfg.gcs_key_path
        )

    trades = build_trades_df(spark, cfg)
    tumbling = aggregate_bars(trades)
    sliding = aggregate_bars(trades, slide="30 seconds")

    # preload model
    load_model_if_needed(cfg, spark)

    q1: StreamingQuery = (
        tumbling.writeStream.outputMode("update")
        .foreachBatch(foreach_batch_writer(cfg, cfg.mongo_tumbling_collection))
        .option("checkpointLocation", f"{cfg.checkpoint_path}/tumbling")
        .start()
    )
    q2: StreamingQuery = (
        sliding.writeStream.outputMode("update")
        .foreachBatch(foreach_batch_writer(cfg, cfg.mongo_sliding_collection))
        .option("checkpointLocation", f"{cfg.checkpoint_path}/sliding")
        .start()
    )

    q1.awaitTermination()
    q2.awaitTermination()


if __name__ == "__main__":
    sys.exit(main())
