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
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
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


def build_dataset(df_raw, epsilon: float, label_mode: str, gap_minutes: int):
    df = add_time_order(df_raw, "event_time")
    df = add_bar_features(df)
    df = add_labels(df, horizon=5, epsilon=epsilon, mode=label_mode)
    feats = feature_columns()
    df = forward_fill(df, feats, gap_minutes=gap_minutes)
    df = df.dropna(subset=list(feats) + ["label", "close", "volume", "trade_count"])
    return df


def split_time(df, validation_days: int):
    max_ts = df.agg(F.max("event_time")).collect()[0][0]
    if max_ts is None:
        raise ValueError("No data available.")
    cut = max_ts - timedelta(days=validation_days)
    return df.filter(F.col("event_time") < cut), df.filter(F.col("event_time") >= cut), cut


def make_pipeline(label_mode: str, feature_version: str, seed: int):
    feats = list(feature_columns())
    indexer = StringIndexer(inputCol="symbol", outputCol="symbol_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["symbol_idx"], outputCols=["symbol_ohe"])
    assembler = VectorAssembler(inputCols=feats + ["symbol_ohe"], outputCol="features", handleInvalid="keep")
    if label_mode == "ternary":
        clf = GBTClassifier(labelCol="label", featuresCol="features", maxIter=60, maxDepth=6, seed=seed)
    else:
        clf = GBTClassifier(labelCol="label", featuresCol="features", maxIter=50, maxDepth=5, seed=seed)
    pipe = Pipeline(stages=[indexer, encoder, assembler, clf])
    return pipe


def eval_metrics(model, df_train, df_val, label_mode: str):
    metrics = {}
    if label_mode == "ternary":
        f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1").evaluate(
            model.transform(df_val)
        )
        metrics["val_f1"] = f1
    else:
        evalr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        metrics["train_auc"] = evalr.evaluate(model.transform(df_train))
        metrics["val_auc"] = evalr.evaluate(model.transform(df_val))
    return metrics


def walk_forward(df, folds: int, label_mode: str, feature_version: str, seed: int):
    """
    Simple walk-forward: split time into folds, train on all before cutoff, eval on next slice.
    """
    if folds < 1 or df.rdd.isEmpty():
        return []
    times = [r[0] for r in df.select("event_time").distinct().orderBy("event_time").collect()]
    if len(times) < folds + 1:
        return []
    min_ts, max_ts = min(times), max(times)
    total_secs = (max_ts - min_ts).total_seconds()
    results = []
    for i in range(1, folds + 1):
        cutoff = min_ts + timedelta(seconds=total_secs * i / (folds + 1))
        train_df = df.filter(F.col("event_time") < cutoff)
        val_df = df.filter(
            (F.col("event_time") >= cutoff)
            & (F.col("event_time") < cutoff + timedelta(days=1))
        )
        if train_df.count() == 0 or val_df.count() == 0:
            continue
        pipe = make_pipeline(label_mode, feature_version, seed + i)
        model = pipe.fit(train_df)
        metrics = eval_metrics(model, train_df, val_df, label_mode)
        results.append({"cutoff": cutoff.isoformat(), **metrics})
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", help="project.dataset.table")
    parser.add_argument("--max_days", type=int, default=None, help="Limit to last N days for faster runs")
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbols to keep")
    parser.add_argument("--epsilon", type=float, default=0.0002, help="Epsilon band for flat/denoise labels")
    parser.add_argument(
        "--label_mode",
        type=str,
        default="binary_drop_flat",
        choices=["binary_drop_flat", "ternary"],
        help="Label strategy: drop flat points for binary or use ternary classes",
    )
    parser.add_argument("--per_symbol", action="store_true", help="Train one model per symbol instead of global")
    parser.add_argument("--walk_folds", type=int, default=3, help="Number of walk-forward folds for eval")
    parser.add_argument("--gap_minutes", type=int, default=0, help="Reset fill if gap > minutes to avoid leakage")
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

    symbols_to_train = (
        [row[0] for row in df_raw.select("symbol").distinct().collect()] if args.per_symbol else [None]
    )

    today = date.today().isoformat()
    for sym in symbols_to_train:
        df_sym = df_raw.filter(F.col("symbol") == sym) if sym else df_raw
        df_dataset = build_dataset(df_sym, epsilon=args.epsilon, label_mode=args.label_mode, gap_minutes=args.gap_minutes)
        train_df, val_df, cut = split_time(df_dataset, cfg.validation_days)
        if train_df.count() == 0 or val_df.count() == 0:
            print(f"[Skip] Not enough data for symbol {sym or 'ALL'}")
            continue

        pipe = make_pipeline(args.label_mode, cfg.feature_version, cfg.seed)
        model = pipe.fit(train_df)
        metrics = eval_metrics(model, train_df, val_df, args.label_mode)
        wf_metrics = walk_forward(df_dataset, args.walk_folds, args.label_mode, cfg.feature_version, cfg.seed)

        model_suffix = sym if sym else cfg.model_name
        model_path = f"gs://{cfg.gcs_bucket}/{cfg.model_base_path.rstrip('/')}/{model_suffix}/{today}/sparkml"
        model.write().overwrite().save(model_path)

        metadata = {
            "model_version": today,
            "model_name": model_suffix,
            "feature_version": cfg.feature_version,
            "train_range_start": train_df.agg(F.min("event_time")).collect()[0][0].isoformat(),
            "train_range_end": train_df.agg(F.max("event_time")).collect()[0][0].isoformat(),
            "validation_range_start": val_df.agg(F.min("event_time")).collect()[0][0].isoformat(),
            "validation_range_end": val_df.agg(F.max("event_time")).collect()[0][0].isoformat(),
            "split_point": cut.isoformat(),
            "metrics": metrics,
            "walk_forward": wf_metrics,
            "created_at": datetime.utcnow().isoformat(),
            "horizon_sec": 300,
            "label_mode": args.label_mode,
            "epsilon": args.epsilon,
        }
        save_metadata(cfg.gcs_bucket, cfg.model_base_path, model_suffix, today, metadata)
        print(f"Saved model to {model_path}")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
