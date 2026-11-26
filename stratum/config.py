"""配置：全部来自环境变量，便于容器化部署。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass
class Settings:
    data_dir: str = "./data"
    db_path: str = ""
    admin_token: str = "stratum-admin-token"
    chunk_size: int = 1024 * 1024
    gc_grace_seconds: int = 900
    max_object_bytes: int = 256 * 1024 * 1024
    presign_ttl: int = 900
    worker_interval: int = 5
    public_base_url: str = "http://127.0.0.1:8080"
    webhook_max_attempts: int = 6
    multipart_max_parts: int = 10000
    fsync_chunks: bool = True
    sqlite_cache_kb: int = 16384
    extra: dict = field(default_factory=dict)

    # 派生路径
    @property
    def chunk_root(self) -> str:
        return os.path.join(self.data_dir, "chunks")

    @property
    def part_root(self) -> str:
        return os.path.join(self.data_dir, "parts")

    @property
    def meta_path(self) -> str:
        return self.db_path or os.path.join(self.data_dir, "stratum.db")

    def normalize(self) -> "Settings":
        self.data_dir = os.path.abspath(self.data_dir)
        if not self.db_path:
            self.db_path = os.path.join(self.data_dir, "stratum.db")
        else:
            self.db_path = os.path.abspath(self.db_path)
        if self.chunk_size < 64 * 1024:
            self.chunk_size = 64 * 1024
        return self


def load_settings(**overrides) -> Settings:
    cfg = Settings(
        data_dir=os.getenv("STRATUM_DATA_DIR", "./data"),
        db_path=os.getenv("STRATUM_DB_PATH", ""),
        admin_token=os.getenv("STRATUM_ADMIN_TOKEN", "stratum-admin-token"),
        chunk_size=_int("STRATUM_CHUNK_SIZE", 1024 * 1024),
        gc_grace_seconds=_int("STRATUM_GC_GRACE_SECONDS", 900),
        max_object_bytes=_int("STRATUM_MAX_OBJECT_BYTES", 256 * 1024 * 1024),
        presign_ttl=_int("STRATUM_PRESIGN_TTL", 900),
        worker_interval=_int("STRATUM_WORKER_INTERVAL", 5),
        public_base_url=os.getenv("STRATUM_PUBLIC_BASE_URL", "http://127.0.0.1:8080"),
        webhook_max_attempts=_int("STRATUM_WEBHOOK_MAX_ATTEMPTS", 6),
        multipart_max_parts=_int("STRATUM_MULTIPART_MAX_PARTS", 10000),
        fsync_chunks=os.getenv("STRATUM_FSYNC_CHUNKS", "1").lower() not in ("0", "false", "no"),
        sqlite_cache_kb=_int("STRATUM_SQLITE_CACHE_KB", 16384),
    )
    for key, value in overrides.items():
        if value is not None and hasattr(cfg, key):
            setattr(cfg, key, value)
    return cfg.normalize()
