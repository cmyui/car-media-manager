from pathlib import Path

from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CMM_")

    storage_dir: Path
    db_path: Path
    rclone_remote: str
    web_port: int = 8000

    ingest_interval_seconds: int = 300
    upload_interval_seconds: int = 60

    gopro_volume_name: str = "HERO13 BLACK"
    insta360_volume_name: str = "Insta360 X4"
