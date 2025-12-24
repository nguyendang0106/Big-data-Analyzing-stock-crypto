from typing import Tuple
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_time_order(df: DataFrame, time_col: str = "event_time") -> DataFrame:
    """Ensure event_time column exists for ordering."""
    return df.withColumn("event_time", F.col(time_col))


def add_bar_features(df: DataFrame) -> DataFrame:
    """Add returns, rolling stats, RSI, MACD-like, volume/trade features."""
    w = Window.partitionBy("symbol").orderBy("event_time")
    w5 = w.rowsBetween(-4, 0)
    w15 = w.rowsBetween(-14, 0)
    w30 = w.rowsBetween(-29, 0)

    df = df.withColumn("prev_close", F.lag("close").over(w))
    df = df.withColumn("return_1m", (F.col("close") - F.col("prev_close")) / F.col("prev_close"))
    df = df.withColumn(
        "log_return_1m", F.when(F.col("close") > 0, F.log(F.col("close")) - F.log(F.col("prev_close")))
    )
    df = df.withColumn("rolling_mean_5", F.avg("close").over(w5))
    df = df.withColumn("rolling_mean_15", F.avg("close").over(w15))
    df = df.withColumn("rolling_vol_15", F.stddev_pop("close").over(w15))
    df = df.withColumn("macd_like", F.avg("close").over(w5) - F.avg("close").over(w30))

    change = F.col("close") - F.col("prev_close")
    gain = F.when(change > 0, change).otherwise(0.0)
    loss = F.when(change < 0, -change).otherwise(0.0)
    df = df.withColumn("avg_gain_14", F.avg(gain).over(w15))
    df = df.withColumn("avg_loss_14", F.avg(loss).over(w15))
    df = df.withColumn(
        "rsi_14",
        F.when(F.col("avg_loss_14") == 0, 100.0).otherwise(
            100 - (100 / (1 + (F.col("avg_gain_14") / F.col("avg_loss_14"))))
        ),
    )
    df = df.withColumn("vol_sma_15", F.avg("volume").over(w15))
    df = df.withColumn("trades_sma_15", F.avg("trade_count").over(w15))
    df = df.withColumn("return_mean_30", F.avg("return_1m").over(w30))
    df = df.withColumn("return_std_30", F.stddev_pop("return_1m").over(w30))
    df = df.withColumn(
        "anomaly_score",
        F.when(F.col("return_std_30") > 0, (F.col("return_1m") - F.col("return_mean_30")) / F.col("return_std_30")),
    )
    return df


def add_labels(df: DataFrame, horizon: int = 5) -> DataFrame:
    """Label: next_horizon return sign."""
    w = Window.partitionBy("symbol").orderBy("event_time")
    df = df.withColumn("future_close", F.lead("close", horizon).over(w))
    df = df.withColumn("next_return", (F.col("future_close") - F.col("close")) / F.col("close"))
    df = df.withColumn(
        "label",
        F.when(F.col("next_return") > 0, F.lit(1.0))
        .when(F.col("next_return") < 0, F.lit(0.0))
        .otherwise(F.lit(0.0)),
    )
    return df


def forward_fill(df: DataFrame, cols: Tuple[str, ...]) -> DataFrame:
    """Forward-fill selected columns within symbol."""
    w = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(Window.unboundedPreceding, 0)
    out = df
    for c in cols:
        out = out.withColumn(c, F.last(F.col(c), ignorenulls=True).over(w))
    return out


def feature_columns() -> Tuple[str, ...]:
    return (
        "return_1m",
        "log_return_1m",
        "rolling_mean_5",
        "rolling_mean_15",
        "rolling_vol_15",
        "macd_like",
        "rsi_14",
        "vol_sma_15",
        "trades_sma_15",
        "anomaly_score",
    )
