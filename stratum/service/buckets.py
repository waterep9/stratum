"""Bucket 生命周期与策略。

策略（lifecycle_json）字段：
  expire_after_days              : 当前版本超过 N 天 -> 过期（版本化桶生成删除标记）
  noncurrent_expire_after_days   : 非当前版本超过 N 天 -> 物理删除
  abort_incomplete_days          : 未完成的分片上传超过 N 天 -> 中止
  delete_marker_expire_after_days: 孤立删除标记超过 N 天 -> 清理
"""

from __future__ import annotations

from ..errors import BucketNotFound, Conflict, InvalidRequest
from ..metrics import METRICS
from ..storage.metadata import Bucket, MetadataStore
from ..util import utcnow, validate_bucket_name

ALLOWED_LIFECYCLE_KEYS = {
    "expire_after_days",
    "noncurrent_expire_after_days",
    "abort_incomplete_days",
    "delete_marker_expire_after_days",
}


class BucketService:
    def __init__(self, db, meta: MetadataStore):
        self.db = db
        self.meta = meta

    def create(self, name: str, *, versioning: bool = False, quota_bytes: int = 0,
               max_object_bytes: int = 0, default_content_type: str = "application/octet-stream",
               lifecycle: dict | None = None, webhook_url: str | None = None,
               labels: dict | None = None, actor: str = "anonymous") -> dict:
        try:
            validate_bucket_name(name)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from exc
        policy = self._validate_lifecycle(lifecycle or {})
        bucket = Bucket(
            name=name, created_at=utcnow(), versioning=versioning,
            quota_bytes=max(0, int(quota_bytes)), max_object_bytes=max(0, int(max_object_bytes)),
            default_content_type=default_content_type or "application/octet-stream",
            lifecycle=policy, webhook_url=webhook_url or None, labels=labels or {},
            created_by=actor,
        )
        with self.db.tx() as conn:
            if self.meta.get_bucket(conn, name):
                raise Conflict(f"bucket 已存在: {name}")
            self.meta.create_bucket(conn, bucket)
            self.meta.add_audit(conn, actor=actor, action="bucket.create", status=201,
                                bucket=name, detail={"versioning": versioning,
                                                     "quota_bytes": bucket.quota_bytes})
        METRICS.inc("stratum_bucket_operations_total", action="create")
        return bucket.to_dict()

    def get(self, name: str) -> Bucket:
        with self.db.read() as conn:
            bucket = self.meta.get_bucket(conn, name)
        if not bucket:
            raise BucketNotFound(f"bucket 不存在: {name}", detail={"bucket": name})
        return bucket

    def list(self) -> list[dict]:
        with self.db.read() as conn:
            return [b.to_dict() for b in self.meta.list_buckets(conn)]

    def update(self, name: str, *, versioning: bool | None = None, quota_bytes: int | None = None,
               max_object_bytes: int | None = None, default_content_type: str | None = None,
               lifecycle: dict | None = None, webhook_url: str | None = None,
               labels: dict | None = None, actor: str = "anonymous") -> dict:
        bucket = self.get(name)
        if versioning is not None:
            bucket.versioning = bool(versioning)
        if quota_bytes is not None:
            bucket.quota_bytes = max(0, int(quota_bytes))
        if max_object_bytes is not None:
            bucket.max_object_bytes = max(0, int(max_object_bytes))
        if default_content_type:
            bucket.default_content_type = default_content_type
        if lifecycle is not None:
            bucket.lifecycle = self._validate_lifecycle(lifecycle)
        if webhook_url is not None:
            bucket.webhook_url = webhook_url or None
        if labels is not None:
            bucket.labels = labels
        with self.db.tx() as conn:
            self.meta.update_bucket(conn, bucket)
            self.meta.add_audit(conn, actor=actor, action="bucket.update", status=200,
                                bucket=name, detail={"versioning": bucket.versioning,
                                                     "quota_bytes": bucket.quota_bytes,
                                                     "lifecycle": bucket.lifecycle})
        METRICS.inc("stratum_bucket_operations_total", action="update")
        return bucket.to_dict()

    def delete(self, name: str, *, force: bool = False, actor: str = "anonymous") -> dict:
        self.get(name)  # 不存在则抛 NoSuchBucket
        with self.db.tx() as conn:
            versions = conn.execute(
                "SELECT COUNT(*) FROM object_versions WHERE bucket=?", (name,)
            ).fetchone()[0]
            uploads = conn.execute(
                "SELECT COUNT(*) FROM multipart_uploads WHERE bucket=? AND state='OPEN'", (name,)
            ).fetchone()[0]
            if (versions or uploads) and not force:
                raise Conflict(f"bucket 非空（versions={versions}, open_uploads={uploads}），"
                               "使用 force=true 强制清空")
            if force:
                rows = conn.execute(
                    "SELECT * FROM object_versions WHERE bucket=?", (name,)
                ).fetchall()
                for row in rows:
                    refs = self.meta.chunk_refs(conn, name, row["key"], row["version_id"])
                    self.meta.drop_chunk_refs(conn, name, row["key"], row["version_id"])
                    from collections import Counter
                    for digest, count in Counter(d for _s, d, _z in refs).items():
                        self.meta.dec_refcount(conn, digest, count)
                conn.execute("DELETE FROM object_versions WHERE bucket=?", (name,))
                conn.execute("UPDATE multipart_uploads SET state='ABORTED' WHERE bucket=?", (name,))
            self.meta.drop_bucket(conn, name)
            self.meta.add_audit(conn, actor=actor, action="bucket.delete", status=200,
                                bucket=name, detail={"force": force, "versions": versions})
        METRICS.inc("stratum_bucket_operations_total", action="delete")
        return {"bucket": name, "deleted": True, "versions_removed": versions if force else 0}

    @staticmethod
    def _validate_lifecycle(policy: dict) -> dict:
        if not isinstance(policy, dict):
            raise InvalidRequest("lifecycle 必须是对象")
        unknown = set(policy) - ALLOWED_LIFECYCLE_KEYS
        if unknown:
            raise InvalidRequest(f"未知的生命周期字段: {sorted(unknown)}")
        clean = {}
        for key, value in policy.items():
            try:
                days = float(value)
            except (TypeError, ValueError):
                raise InvalidRequest(f"生命周期字段 {key} 必须是数字（天）")
            if days < 0:
                raise InvalidRequest(f"生命周期字段 {key} 不能为负")
            clean[key] = days
        return clean
