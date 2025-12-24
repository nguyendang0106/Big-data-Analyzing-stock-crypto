import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class TrainingConfig:
    bq_project: str
    bq_dataset: str
    bq_table: str
    gcs_bucket: str
    model_base_path: str = "ml/models"
    model_name: str = "gbt_direction"
    feature_version: str = "v1"
    validation_days: int = 7
    seed: int = 1234


@dataclass
class StreamingConfig:
    kafka_bootstrap: str
    kafka_topic: str = "binance"
    mongo_uri: str = ""
    mongo_db: str = "BigData"
    mongo_sliding_collection: str = "ml_signals_sliding"
    mongo_tumbling_collection: str = "ml_signals_tumbling"
    gcs_bucket: str = ""
    model_base_path: str = "ml/models"
    model_name: str = "gbt_direction"
    checkpoint_path: str = "/checkpoint-ml"
    gcs_key_path: Optional[str] = None
    refresh_minutes: int = 15


def _req(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise ValueError(f"Missing required env var {key}")
    return val


def load_training_config() -> TrainingConfig:
    return TrainingConfig(
        bq_project=_req("BQ_PROJECT"),
        bq_dataset=_req("BQ_DATASET"),
        bq_table=_req("BQ_TABLE"),
        gcs_bucket=_req("GCS_BUCKET"),
        model_base_path=os.getenv("MODEL_BASE_PATH", "ml/models"),
        model_name=os.getenv("MODEL_NAME", "gbt_direction"),
        feature_version=os.getenv("FEATURE_VERSION", "v1"),
        validation_days=int(os.getenv("VALIDATION_DAYS", "7")),
        seed=int(os.getenv("SEED", "1234")),
    )


def load_streaming_config() -> StreamingConfig:
    return StreamingConfig(
        kafka_bootstrap=_req("KAFKA_BOOTSTRAP_SERVERS"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "binance"),
        mongo_uri=_req("MONGO_URI"),
        mongo_db=os.getenv("MONGO_DB", "BigData"),
        mongo_sliding_collection=os.getenv("ML_SLIDING_COLLECTION", "ml_signals_sliding"),
        mongo_tumbling_collection=os.getenv("ML_TUMBLING_COLLECTION", "ml_signals_tumbling"),
        gcs_bucket=_req("GCS_BUCKET"),
        model_base_path=os.getenv("MODEL_BASE_PATH", "ml/models"),
        model_name=os.getenv("MODEL_NAME", "gbt_direction"),
        checkpoint_path=os.getenv("CHECKPOINT_PATH", "/checkpoint-ml"),
        gcs_key_path=os.getenv("GCS_KEY_PATH"),
        refresh_minutes=int(os.getenv("MODEL_REFRESH_MINUTES", "15")),
    )
