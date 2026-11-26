"""依赖装配：把配置、存储、服务显式串起来。

不用全局单例：测试需要并行创建多个独立容器（各自的临时目录与数据库），
依赖注入让「一个 app 实例 = 一套独立存储」这件事成立。
"""

from __future__ import annotations

from ..config import Settings
from ..storage.cas import ChunkStore
from ..storage.db import Database
from ..storage.metadata import MetadataStore
from ..storage.parts import PartStore
from ..util import ensure_dir
from .auth import AuthService
from .buckets import BucketService
from .lifecycle import LifecycleService
from .maintenance import MaintenanceService
from .multipart import MultipartService
from .objects import ObjectService
from .presign import PresignService
from .webhook import OutboxDispatcher


class Container:
    def __init__(self, settings: Settings):
        self.settings = settings
        ensure_dir(settings.chunk_root)
        ensure_dir(settings.part_root)
        self.db = Database(settings.meta_path, cache_kb=settings.sqlite_cache_kb)
        self.db.migrate()

        self.meta = MetadataStore()
        self.cas = ChunkStore(settings.chunk_root, fsync=settings.fsync_chunks)
        self.parts = PartStore(settings.part_root)

        self.auth = AuthService(self.db, self.meta, settings.admin_token)
        self.buckets = BucketService(self.db, self.meta)
        self.objects = ObjectService(self.db, self.meta, self.cas, settings)
        self.multipart = MultipartService(self.db, self.meta, self.cas, self.parts,
                                          settings, self.objects)
        self.presign = PresignService(self.db, self.meta, settings.public_base_url,
                                      settings.presign_ttl)
        self.lifecycle = LifecycleService(self.db, self.meta, self.buckets, self.objects,
                                          self.multipart)
        self.maintenance = MaintenanceService(self.db, self.meta, self.cas, settings)
        self.outbox = OutboxDispatcher(self.db, self.meta,
                                       max_attempts=settings.webhook_max_attempts)
