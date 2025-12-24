import json
from datetime import datetime
from typing import Optional

from google.cloud import storage
from pyspark.ml import PipelineModel


def _client():
    return storage.Client()


def latest_model_uri(bucket: str, base_path: str, model_name: str) -> Optional[str]:
    client = _client()
    prefix = f"{base_path.rstrip('/')}/{model_name}/"
    dates = []
    for page in client.list_blobs(bucket, prefix=prefix, delimiter="/").pages:
        for p in page.prefixes:
            try:
                dates.append(datetime.strptime(p.rstrip("/").split("/")[-1], "%Y-%m-%d").date())
            except Exception:
                continue
    if not dates:
        return None
    latest = max(dates)
    return f"gs://{bucket}/{prefix}{latest.isoformat()}/sparkml"


def load_pipeline(path: str) -> PipelineModel:
    return PipelineModel.load(path)


def save_metadata(bucket: str, base_path: str, model_name: str, date_str: str, metadata: dict) -> str:
    client = _client()
    blob = client.bucket(bucket).blob(f"{base_path.rstrip('/')}/{model_name}/{date_str}/metadata.json")
    blob.upload_from_string(json.dumps(metadata, indent=2))
    return blob.public_url
