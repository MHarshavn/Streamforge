"""Environment-based application configuration using pydantic-settings."""

from __future__ import annotations

from enum import Enum

from pydantic_settings import BaseSettings, SettingsConfigDict


class StateBackend(str, Enum):
    """Supported state-store backends."""

    IN_MEMORY = "in_memory"
    ROCKSDB = "rocksdb"  # Future integration
    KAFKA = "kafka"  # Future integration


class Settings(BaseSettings):
    """StreamForge application settings.

    All values can be overridden via environment variables prefixed with
    ``STREAMFORGE_``.  See ``.env.example`` for the full list.
    """

    model_config = SettingsConfigDict(
        env_prefix="STREAMFORGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Application ──────────────────────────────────────────────────────
    app_name: str = "StreamForge API"
    version: str = "0.1.0"
    debug: bool = False

    # ── State backend ────────────────────────────────────────────────────
    state_backend: StateBackend = StateBackend.IN_MEMORY

    # ── Windowing ────────────────────────────────────────────────────────
    window_size_seconds: int = 300  # 5-minute rolling window

    # ── Kafka (future) ───────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_telemetry_topic: str = "streamforge.telemetry"
    kafka_consumer_group: str = "streamforge-api"

    # ── RocksDB (future) ─────────────────────────────────────────────────
    rocksdb_path: str = "/var/lib/streamforge/state"

    # ── Server ───────────────────────────────────────────────────────────
    host: str = "0.0.0.0"
    port: int = 8000


def get_settings() -> Settings:
    """Return a cached :class:`Settings` instance."""
    return Settings()
