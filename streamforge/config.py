"""Environment-based application configuration."""
from __future__ import annotations
from enum import Enum
from pydantic_settings import BaseSettings, SettingsConfigDict


class StateBackend(str, Enum):
    IN_MEMORY = "in_memory"
    ROCKSDB = "rocksdb"
    KAFKA = "kafka"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STREAMFORGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    app_name: str = "StreamForge API"
    version: str = "1.0.0"
    debug: bool = False

    state_backend: StateBackend = StateBackend.ROCKSDB
    window_size_seconds: int = 300
    recover_state_from_changelog: bool = True

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_telemetry_topic: str = "telemetry-events"
    kafka_consumer_group: str = "streamforge-workers"
    kafka_changelog_topic: str = "streamforge-state-changelog"
    kafka_partitions: int = 20

    rocksdb_path: str = "./data/rocksdb"

    worker_id: str = "api-worker"
    worker_count: int = 20
    worker_heartbeat_ttl_seconds: int = 30

    host: str = "0.0.0.0"
    port: int = 8000


def get_settings() -> Settings:
    return Settings()
