"""
Daily Spark ML training job.
Reads 1m klines from BigQuery, builds features, trains a GBTClassifier to predict next 5m direction,
and writes model artifacts + metadata to GCS (add-on pipeline, no change to existing jobs).
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder

from mlspark.config import load_training_config
from mlspark.feature_engineering import (
    add_bar_features,
    add_labels,
    add_time_order,
    forward_fill,
    feature_columns,
)
from mlspark.model_io import save_metadata


HORIZON_MINUTES = 5


def normalize_ts(df):
    """Handle ms/ns/seconds open_time."""
    df = df.withColumn(
        "ts_seconds",
        F.when(F.col("open_time") > 1e14, F.col("open_time") / 1e9)
        .when(F.col("open_time") > 1e11, F.col("open_time") / 1e3)
        .otherwise(F.col("open_time").cast("double")),
    )
    return df.withColumn("event_time", F.to_timestamp("ts_seconds"))


def load_klines(spark: SparkSession, table_fqn: str):
    df = (
        spark.read.format("bigquery")
        .option("table", table_fqn)
        .load()
        .select("symbol", "open_time", "open", "high", "low", "close", "volume", "number_of_trades")
    )
    df = normalize_ts(df)
    df = df.withColumnRenamed("number_of_trades", "trade_count")
    return df


def build_dataset(df_raw, label_epsilon: float, max_gap_minutes: int):
    df = add_time_order(df_raw, "event_time")
    df = df.filter(F.col("event_time").isNotNull())
    df = add_bar_features(df)
    df = add_labels(df, horizon=HORIZON_MINUTES, epsilon=label_epsilon)
    feats = feature_columns()
    df = forward_fill(df, feats, max_gap_minutes=max_gap_minutes)
    df = df.dropna(subset=list(feats) + ["label", "close", "volume", "trade_count"])
    return df


def split_time_per_symbol(df, validation_days: int):
    """Time split per symbol to avoid distribution shift."""
    max_by_symbol = df.groupBy("symbol").agg(F.max("event_time").alias("max_event_time"))
    cut_df = max_by_symbol.withColumn("cut_time", F.col("max_event_time") - F.expr(f"INTERVAL {validation_days} DAYS"))
    df_with_cut = df.join(cut_df.select("symbol", "cut_time"), on="symbol", how="inner")
    train_df = df_with_cut.filter(F.col("event_time") < F.col("cut_time")).drop("cut_time")
    val_df = df_with_cut.filter(F.col("event_time") >= F.col("cut_time")).drop("cut_time")
    return train_df, val_df, cut_df


def train(df_train, df_val, feature_version: str, seed: int):
    feats = list(feature_columns())
    indexer = StringIndexer(inputCol="symbol", outputCol="symbol_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["symbol_idx"], outputCols=["symbol_ohe"])
    assembler = VectorAssembler(inputCols=feats + ["symbol_ohe"], outputCol="features", handleInvalid="keep")
    clf = GBTClassifier(labelCol="label", featuresCol="features", maxIter=50, maxDepth=5, seed=seed)
    pipe = Pipeline(stages=[indexer, encoder, assembler, clf])
    model = pipe.fit(df_train)
    evalr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    metrics = {"feature_version": feature_version, "train_auc": evalr.evaluate(model.transform(df_train)), "val_auc": evalr.evaluate(model.transform(df_val))}
    return model, metrics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", help="project.dataset.table")
    parser.add_argument("--max_days", type=int, default=None, help="Limit to last N days for faster runs")
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbols to keep")
    args = parser.parse_args()

    cfg = load_training_config()
    table_fqn = args.table or f"{cfg.bq_project}.{cfg.bq_dataset}.{cfg.bq_table}"

    gcs_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    gcs_connector = "/opt/spark/jars/gcs-connector-hadoop3-latest.jar"
    builder = SparkSession.builder.appName("CryptoML-Training").config("spark.sql.session.timeZone", "UTC")
    if gcs_key:
        builder = (
            builder.config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_key)
        )
    if os.path.exists(gcs_connector):
        builder = (
            builder.config("spark.jars", gcs_connector)
            .config("spark.driver.extraClassPath", gcs_connector)
            .config("spark.executor.extraClassPath", gcs_connector)
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_raw = load_klines(spark, table_fqn)
    if args.symbols:
        syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
        df_raw = df_raw.filter(F.col("symbol").isin(syms))
    if args.max_days:
        mx = df_raw.agg(F.max("event_time")).collect()[0][0]
        if mx:
            df_raw = df_raw.filter(F.col("event_time") >= F.lit(mx - timedelta(days=args.max_days)))

    df_dataset = build_dataset(df_raw, label_epsilon=cfg.label_epsilon, max_gap_minutes=cfg.ffill_max_gap_minutes)
    train_df, val_df, cut_df = split_time_per_symbol(df_dataset, cfg.validation_days)
    if train_df.count() == 0 or val_df.count() == 0:
        raise ValueError("Not enough data after filtering.")

    model, metrics = train(train_df, val_df, cfg.feature_version, cfg.seed)

    today = date.today().isoformat()
    model_path = f"gs://{cfg.gcs_bucket}/{cfg.model_base_path.rstrip('/')}/{cfg.model_name}/{today}/sparkml"
    model.write().overwrite().save(model_path)

    metadata = {
        "model_version": today,
        "model_name": cfg.model_name,
        "feature_version": cfg.feature_version,
        "train_range_start": train_df.agg(F.min("event_time")).collect()[0][0].isoformat(),
        "train_range_end": train_df.agg(F.max("event_time")).collect()[0][0].isoformat(),
        "validation_range_start": val_df.agg(F.min("event_time")).collect()[0][0].isoformat(),
        "validation_range_end": val_df.agg(F.max("event_time")).collect()[0][0].isoformat(),
        "split_strategy": "per_symbol_time",
        "split_points": {r["symbol"]: r["cut_time"].isoformat() for r in cut_df.collect() if r["cut_time"]},
        "metrics": metrics,
        "created_at": datetime.utcnow().isoformat(),
        "horizon_sec": HORIZON_MINUTES * 60,
        "label_epsilon": cfg.label_epsilon,
        "ffill_max_gap_minutes": cfg.ffill_max_gap_minutes,
    }
    save_metadata(cfg.gcs_bucket, cfg.model_base_path, cfg.model_name, today, metadata)
    print(f"Saved model to {model_path}")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
