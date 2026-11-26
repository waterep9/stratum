"""分片上传（multipart）。

为什么不用「把部件直接写进 CAS」的简化实现：
部件允许被同名覆盖、允许乱序、允许放弃，这与内容寻址的不可变语义冲突；
因此部件先落在可变的暂存区，Complete 时按序流式切块写入 CAS —— 也顺带把
「客户端分片」与「服务端分片」这两个不同粒度解耦（部件可以远大于 1 MiB 分片）。

幂等性：Complete 是重试高发点。版本化桶里我们直接用 upload_id 作为 version_id，
重复 Complete 命中「已提交」分支后返回同一个版本，不会产生重复对象。
"""

from __future__ import annotations

import hashlib
from typing import Iterable, Optional

from ..config import Settings
from ..errors import (BucketNotFound, Conflict, InvalidRequest, NotFound, PayloadTooLarge,
                      QuotaExceeded)
from ..metrics import METRICS
from ..storage.cas import ChunkStore
from ..storage.metadata import MetadataStore, ObjectVersion, Upload
from ..storage.parts import PartStore
from ..util import new_id, utcnow, validate_key


class MultipartService:
    def __init__(self, db, meta: MetadataStore, cas: ChunkStore, parts: PartStore,
                 settings: Settings, objects):
        self.db = db
        self.meta = meta
        self.cas = cas
        self.parts = parts
        self.settings = settings
        self.objects = objects  # ObjectService：复用提交与引用计数逻辑

    def _bucket(self, name: str):
        with self.db.read() as conn:
            bucket = self.meta.get_bucket(conn, name)
        if not bucket:
            raise BucketNotFound(f"bucket 不存在: {name}")
        return bucket

    def get_upload(self, upload_id: str) -> Upload:
        with self.db.read() as conn:
            upload = self.meta.get_upload(conn, upload_id)
        if not upload:
            raise NotFound(f"上传任务不存在: {upload_id}", code="NoSuchUpload")
        return upload

    def create(self, bucket_name: str, key: str, *, content_type: Optional[str] = None,
               metadata: Optional[dict] = None, actor: str = "anonymous") -> dict:
        try:
            validate_key(key)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from exc
        bucket = self._bucket(bucket_name)
        upload_id = new_id()
        upload = Upload(
            upload_id=upload_id, bucket=bucket_name, key=key, created_at=utcnow(),
            content_type=content_type or bucket.default_content_type,
            metadata=metadata or {}, state="OPEN", created_by=actor,
        )
        with self.db.tx() as conn:
            self.meta.create_upload(conn, upload)
            self.meta.add_audit(conn, actor=actor, action="multipart.create", status=201,
                                bucket=bucket_name, key=key, detail={"upload_id": upload_id})
        METRICS.inc("stratum_multipart_created_total")
        return {"upload_id": upload_id, "bucket": bucket_name, "key": key,
                "created_at": upload.created_at,
                "part_size_hint": self.settings.chunk_size,
                "recommended_part_size": 8 * self.settings.chunk_size}

    def upload_part(self, upload_id: str, part_no: int, body: Iterable[bytes]) -> dict:
        if part_no < 1 or part_no > self.settings.multipart_max_parts:
            raise InvalidRequest(f"part_no 必须在 1..{self.settings.multipart_max_parts}")
        upload = self.get_upload(upload_id)
        if upload.state != "OPEN":
            raise Conflict(f"上传任务状态为 {upload.state}，不可写入部件")
        digest, size = self.parts.write_part(upload_id, part_no, body)
        with self.db.tx() as conn:
            old = self.meta.upsert_part(conn, upload_id, part_no, digest, size, utcnow())
            self.meta.bump_upload_bytes(conn, upload_id, size - old)
            self.meta.add_audit(conn, actor=upload.created_by or "anonymous",
                                action="multipart.part", status=200, bucket=upload.bucket,
                                key=upload.key, detail={"upload_id": upload_id,
                                                        "part_no": part_no, "size": size})
        METRICS.inc("stratum_multipart_parts_total")
        return {"upload_id": upload_id, "part_no": part_no, "etag": digest, "size": size}

    def list_parts(self, upload_id: str) -> dict:
        self.get_upload(upload_id)  # 校验存在性
        with self.db.read() as conn:
            parts = self.meta.list_parts(conn, upload_id)
        return {"upload_id": upload_id,
                "parts": [{"part_no": p.part_no, "etag": p.hash, "size": p.size,
                           "created_at": p.created_at} for p in parts]}

    def list_uploads(self, bucket_name: str, prefix: str = "") -> dict:
        self._bucket(bucket_name)
        with self.db.read() as conn:
            uploads = self.meta.list_uploads(conn, bucket_name, prefix)
        return {"bucket": bucket_name,
                "uploads": [{"upload_id": u.upload_id, "key": u.key, "created_at": u.created_at,
                             "parts_bytes": u.parts_bytes} for u in uploads]}

    def abort(self, upload_id: str, *, actor: str = "anonymous") -> dict:
        upload = self.get_upload(upload_id)
        with self.db.tx() as conn:
            changed = self.meta.update_upload_state(conn, upload_id, "ABORTED")
            if not changed:
                raise Conflict(f"上传任务已处于 {upload.state} 状态")
            self.meta.add_audit(conn, actor=actor, action="multipart.abort", status=200,
                                bucket=upload.bucket, key=upload.key,
                                detail={"upload_id": upload_id})
        self.parts.drop_upload(upload_id)
        METRICS.inc("stratum_multipart_aborted_total")
        return {"upload_id": upload_id, "aborted": True}

    def complete(self, upload_id: str, *, actor: str = "anonymous") -> dict:
        upload = self.get_upload(upload_id)
        bucket = self._bucket(upload.bucket)

        if upload.state != "OPEN":
            if upload.state == "COMPLETED" and bucket.versioning:
                with self.db.read() as conn:
                    version = self.meta.get_version(conn, upload.bucket, upload.key, upload_id)
                if version:
                    return self._result(version, upload_id, reused=True)
            raise Conflict(f"上传任务状态为 {upload.state}，不可完成")

        with self.db.read() as conn:
            parts = self.meta.list_parts(conn, upload_id)
        if not parts:
            raise InvalidRequest("没有已上传的部件")
        numbers = [p.part_no for p in parts]
        if numbers != list(range(1, len(parts) + 1)):
            raise InvalidRequest("部件号必须从 1 开始连续", detail={"parts": numbers})
        min_part = self.settings.chunk_size
        for part in parts[:-1]:
            if part.size < min_part:
                raise InvalidRequest(
                    f"非末尾部件 {part.part_no} 小于最小部件大小 {min_part} 字节",
                    detail={"part_no": part.part_no, "size": part.size, "min": min_part},
                )
        total = sum(p.size for p in parts)
        limit = bucket.max_object_bytes or self.settings.max_object_bytes
        if limit and total > limit:
            raise PayloadTooLarge(f"对象超过大小上限 {limit} 字节")

        refs, etag = self._assemble(upload_id, parts)
        version_id = upload_id if bucket.versioning else "null"
        now = utcnow()
        version = ObjectVersion(
            bucket=upload.bucket, key=upload.key, version_id=version_id, is_delete_marker=False,
            is_current=True, size=total, chunk_count=len(refs), etag=etag,
            content_type=upload.content_type, storage_class="hot", metadata=upload.metadata,
            created_at=now, created_by=actor,
        )
        with self.db.tx() as conn:
            live = self.meta.get_upload(conn, upload_id)
            if not live or live.state != "OPEN":
                raise Conflict("上传任务已被并发完成或中止")
            current = self.meta.get_current(conn, upload.bucket, upload.key)
            if current and not bucket.versioning:
                self.objects._drop_version_locked(conn, current)
            else:
                self.meta.clear_current(conn, upload.bucket, upload.key)
            self.meta.insert_version(conn, version)
            self.objects._attach_chunks_locked(conn, upload.bucket, upload.key, version_id, refs, now)
            if not self.meta.add_usage(conn, upload.bucket, total, 1):
                raise QuotaExceeded("bucket 配额不足")
            self.meta.update_upload_state(conn, upload_id, "COMPLETED")
            self.meta.add_audit(conn, actor=actor, action="multipart.complete", status=200,
                                bucket=upload.bucket, key=upload.key, version_id=version_id,
                                detail={"upload_id": upload_id, "parts": len(parts),
                                        "size": total, "etag": etag})
            self.meta.add_event(conn, upload.bucket, "multipart.completed", {
                "bucket": upload.bucket, "key": upload.key, "version_id": version_id,
                "size": total, "etag": etag, "parts": len(parts), "at": now,
            }, bucket.webhook_url, now)
        self.parts.drop_upload(upload_id)
        METRICS.inc("stratum_multipart_completed_total")
        METRICS.inc("stratum_bytes_written_total", total)
        return self._result(version, upload_id, reused=False)

    def _assemble(self, upload_id: str, parts) -> tuple[list[tuple[int, str, int]], str]:
        """把暂存部件流式切成 CAS 分片；返回引用列表与 multipart ETag。"""
        chunk_size = self.settings.chunk_size
        hasher = hashlib.sha256()
        refs: list[tuple[int, str, int]] = []
        seq = 0
        written = deduped = 0
        for part in parts:
            hasher.update(bytes.fromhex(part.hash))
            with self.parts.open_part(upload_id, part.part_no) as fh:
                while True:
                    buf = fh.read(chunk_size)
                    if not buf:
                        break
                    digest, is_new = self.cas.put(buf)
                    refs.append((seq, digest, len(buf)))
                    seq += 1
                    written += int(is_new)
                    deduped += int(not is_new)
        etag = hasher.hexdigest()[:32] + f"-{len(parts)}"
        METRICS.inc("stratum_chunks_written_total", written)
        METRICS.inc("stratum_chunks_deduped_total", deduped)
        return refs, etag

    @staticmethod
    def _result(version: ObjectVersion, upload_id: str, reused: bool) -> dict:
        return {
            "bucket": version.bucket, "key": version.key, "version_id": version.version_id,
            "etag": version.etag, "size": version.size, "upload_id": upload_id,
            "chunks": version.chunk_count, "idempotent_replay": reused,
        }
