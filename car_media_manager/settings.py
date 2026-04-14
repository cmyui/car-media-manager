from pathlib import Path

from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CMM_")

    storage_dir: Path
    db_path: Path
    web_port: int = 8000

    s3_endpoint_url: str
    s3_region_name: str
    s3_bucket_name: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_prefix: str = "car-footage"

    ingest_interval_seconds: int = 300
    upload_interval_seconds: int = 60
