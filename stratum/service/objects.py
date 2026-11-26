"""对象读写核心。

关键设计
--------
1. **先写 CAS，后写元数据**：正文写入不持数据库锁，只有最终提交的几毫秒在事务里。
   上传中途失败/取消只会留下无引用的分片，由 GC 依据「引用计数 + 宽限期」回收。
2. **条件写在锁内复核**：If-Match/If-None-Match 先做一次快速失败（省掉整段 IO），
   进入事务后再检查一次，消除 TOCTOU。
3. **删除即解引用**：删除版本只减引用计数，物理回收交给后台 GC，删除路径是 O(1)。
4. **复制不搬数据**：metadata-only copy 直接复用分片引用，100 MB 对象复制近乎零成本。
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

from ..config import Settings
from ..errors import (BucketNotFound, ChunkCorrupted, InvalidRequest, NotFound,
                      PreconditionFailed, QuotaExceeded, PayloadTooLarge)
from ..metrics import METRICS
from ..storage.cas import ChunkStore
from ..storage.metadata import Bucket, MetadataStore, ObjectVersion
from ..util import new_id, utcnow, validate_key

@dataclass
class IngestResult:
    refs: list[tuple[int, str, int]]
    size: int
    etag: str
    written: int
    deduped: int


@dataclass
class ObjectHandle:
    version: ObjectVersion
    chunks: list[tuple[int, str, int]]
    start: int
    end: int          # inclusive，空对象时为 -1
    partial: bool

    @property
    def length(self) -> int:
        return 0 if self.end < self.start else self.end - self.start + 1


def encode_token(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_token(token: str) -> dict:
    pad = "=" * (-len(token) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(token + pad))
    except Exception:
        raise InvalidRequest("分页令牌非法")


def parse_range(header: Optional[str], total: int) -> tuple[int, int, bool]:
    """解析 Range 头，返回 (start, end 含端点, 是否部分响应)。"""
    if not header:
        return 0, total - 1, False
    if not header.startswith("bytes="):
        raise InvalidRequest("仅支持 bytes= 形式的 Range")
    spec = header[6:].strip()
    if "," in spec:
        raise InvalidRequest("不支持多区间 Range")
    left, _, right = spec.partition("-")
    if total == 0:
        raise RangeNotSatisfiable("空对象不支持 Range 请求", total)
    try:
        if left == "":
            suffix = int(right)
            if suffix <= 0:
                raise ValueError
            start = max(0, total - suffix)
            end = total - 1
        else:
            start = int(left)
            end = int(right) if right else total - 1
    except ValueError:
        raise InvalidRequest("Range 头格式非法")
    if start >= total or start > end:
        raise RangeNotSatisfiable("请求区间超出对象长度", total)
    return start, min(end, total - 1), True


class RangeNotSatisfiable(InvalidRequest):
    status = 416
    code = "InvalidRange"

    def __init__(self, message: str, total: int):
        super().__init__(message, detail={"size": total})
        self.total = total


class ObjectService:
    def __init__(self, db, meta: MetadataStore, cas: ChunkStore, settings: Settings):
        self.db = db
        self.meta = meta
        self.cas = cas
        self.settings = settings

    # ------------------------------------------------------------------ 工具
    def require_bucket(self, bucket: str) -> Bucket:
        with self.db.read() as conn:
            found = self.meta.get_bucket(conn, bucket)
        if not found:
            raise BucketNotFound(f"bucket 不存在: {bucket}", detail={"bucket": bucket})
        return found

    def _limit(self, bucket: Bucket) -> int:
        return bucket.max_object_bytes or self.settings.max_object_bytes or 0

    def _check_preconditions(self, current: Optional[ObjectVersion], if_match: Optional[str],
                             if_none_match: Optional[str]) -> None:
        alive = current if (current and not current.is_delete_marker) else None
        if if_none_match == "*":
            if alive:
                raise PreconditionFailed("对象已存在（If-None-Match: *）",
                                         detail={"etag": alive.etag})
            return
        if if_match:
            if not alive:
                raise PreconditionFailed("对象不存在，If-Match 无法满足")
            if _strip_quotes(if_match) != alive.etag:
                raise PreconditionFailed("ETag 不匹配", detail={"expected": alive.etag,
                                                                "provided": _strip_quotes(if_match)})

    def _ingest(self, body: Iterable[bytes], max_bytes: int) -> IngestResult:
        chunk_size = self.settings.chunk_size
        hasher = hashlib.sha256()
        buf = bytearray()
        refs: list[tuple[int, str, int]] = []
        seq = 0
        size = 0
        written = deduped = 0
        for piece in body:
            if not piece:
                continue
            size += len(piece)
            if max_bytes and size > max_bytes:
                raise PayloadTooLarge(f"对象超过大小上限 {max_bytes} 字节",
                                      detail={"size": size, "limit": max_bytes})
            hasher.update(piece)
            buf.extend(piece)
            while len(buf) >= chunk_size:
                chunk = bytes(buf[:chunk_size])
                del buf[:chunk_size]
                digest, is_new = self.cas.put(chunk)
                refs.append((seq, digest, len(chunk)))
                seq += 1
                written += int(is_new)
                deduped += int(not is_new)
        if buf:
            chunk = bytes(buf)
            digest, is_new = self.cas.put(chunk)
            refs.append((seq, digest, len(chunk)))
            written += int(is_new)
            deduped += int(not is_new)
        METRICS.inc("stratum_chunks_written_total", written)
        METRICS.inc("stratum_chunks_deduped_total", deduped)
        return IngestResult(refs=refs, size=size, etag=hasher.hexdigest(),
                            written=written, deduped=deduped)

    def _drop_version_locked(self, conn, version: ObjectVersion) -> None:
        """删除一个版本并释放引用（调用方必须已持有写事务）。"""
        refs = self.meta.chunk_refs(conn, version.bucket, version.key, version.version_id)
        self.meta.drop_chunk_refs(conn, version.bucket, version.key, version.version_id)
        for digest, count in Counter(digest for _seq, digest, _size in refs).items():
            self.meta.dec_refcount(conn, digest, count)
        self.meta.delete_version_row(conn, version.bucket, version.key, version.version_id)
        self.meta.add_usage(
            conn, version.bucket, -version.size,
            -1 if (version.is_current and not version.is_delete_marker) else 0,
        )

    def _attach_chunks_locked(self, conn, bucket: str, key: str, version_id: str,
                              refs: list[tuple[int, str, int]], now: str) -> None:
        self.meta.add_chunk_refs(conn, bucket, key, version_id, refs)
        agg: Counter = Counter()
        sizes: dict[str, int] = {}
        for _seq, digest, size in refs:
            agg[digest] += 1
            sizes[digest] = size
        for digest, count in agg.items():
            self.meta.inc_refcount(conn, digest, sizes[digest], count, now)

    def _emit(self, conn, bucket: Bucket, event: str, payload: dict, now: str) -> None:
        self.meta.add_event(conn, bucket.name, event, payload, bucket.webhook_url, now)

    # ------------------------------------------------------------------ 写入
    def put_object(self, bucket_name: str, key: str, body: Iterable[bytes], *,
                   content_type: Optional[str] = None, metadata: Optional[dict] = None,
                   if_match: Optional[str] = None, if_none_match: Optional[str] = None,
                   actor: str = "anonymous", request_id: Optional[str] = None,
                   storage_class: str = "hot") -> dict:
        _validate_key(key)
        bucket = self.require_bucket(bucket_name)
        with self.db.read() as conn:
            current = self.meta.get_current(conn, bucket_name, key)
        self._check_preconditions(current, if_match, if_none_match)

        ingested = self._ingest(body, self._limit(bucket))
        version_id = new_id() if bucket.versioning else "null"
        now = utcnow()
        version = ObjectVersion(
            bucket=bucket_name, key=key, version_id=version_id, is_delete_marker=False,
            is_current=True, size=ingested.size, chunk_count=len(ingested.refs),
            etag=ingested.etag, content_type=content_type or bucket.default_content_type,
            storage_class=storage_class, metadata=metadata or {}, created_at=now,
            created_by=actor,
        )
        with self.db.tx() as conn:
            current = self.meta.get_current(conn, bucket_name, key)
            self._check_preconditions(current, if_match, if_none_match)
            if current and not bucket.versioning:
                self._drop_version_locked(conn, current)
            else:
                self.meta.clear_current(conn, bucket_name, key)
            self.meta.insert_version(conn, version)
            self._attach_chunks_locked(conn, bucket_name, key, version_id, ingested.refs, now)
            if not self.meta.add_usage(conn, bucket_name, ingested.size, 1):
                raise QuotaExceeded("bucket 配额不足", detail={"bucket": bucket_name})
            self.meta.add_audit(conn, actor=actor, action="object.put", status=200,
                                bucket=bucket_name, key=key, version_id=version_id,
                                request_id=request_id,
                                detail={"size": ingested.size, "etag": ingested.etag,
                                        "chunks": len(ingested.refs),
                                        "deduped": ingested.deduped})
            self._emit(conn, bucket, "object.created", {
                "bucket": bucket_name, "key": key, "version_id": version_id,
                "size": ingested.size, "etag": ingested.etag, "at": now,
            }, now)
        METRICS.inc("stratum_objects_written_total")
        METRICS.inc("stratum_bytes_written_total", ingested.size)
        return {
            "bucket": bucket_name, "key": key, "version_id": version_id,
            "etag": ingested.etag, "size": ingested.size,
            "chunks": len(ingested.refs), "chunks_written": ingested.written,
            "chunks_deduped": ingested.deduped, "created_at": now,
        }

    def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str, *,
                    if_match: Optional[str] = None, actor: str = "anonymous",
                    request_id: Optional[str] = None) -> dict:
        _validate_key(dst_key)
        self.require_bucket(src_bucket)
        dst = self.require_bucket(dst_bucket)
        with self.db.read() as conn:
            source = self.meta.get_current(conn, src_bucket, src_key)
            if not source or source.is_delete_marker:
                raise NotFound(f"源对象不存在: {src_bucket}/{src_key}")
            refs = self.meta.chunk_refs(conn, src_bucket, src_key, source.version_id)
            target = self.meta.get_current(conn, dst_bucket, dst_key)
        self._check_preconditions(target, if_match, None)
        version_id = new_id() if dst.versioning else "null"
        now = utcnow()
        with self.db.tx() as conn:
            target = self.meta.get_current(conn, dst_bucket, dst_key)
            self._check_preconditions(target, if_match, None)
            if target and not dst.versioning:
                self._drop_version_locked(conn, target)
            else:
                self.meta.clear_current(conn, dst_bucket, dst_key)
            version = ObjectVersion(
                bucket=dst_bucket, key=dst_key, version_id=version_id, is_delete_marker=False,
                is_current=True, size=source.size, chunk_count=source.chunk_count,
                etag=source.etag, content_type=source.content_type,
                storage_class=source.storage_class, metadata=dict(source.metadata),
                created_at=now, created_by=actor,
            )
            self.meta.insert_version(conn, version)
            self._attach_chunks_locked(conn, dst_bucket, dst_key, version_id, refs, now)
            if not self.meta.add_usage(conn, dst_bucket, source.size, 1):
                raise QuotaExceeded("目标 bucket 配额不足", detail={"bucket": dst_bucket})
            self.meta.add_audit(conn, actor=actor, action="object.copy", status=200,
                                bucket=dst_bucket, key=dst_key, version_id=version_id,
                                request_id=request_id,
                                detail={"source": f"{src_bucket}/{src_key}",
                                        "version_id": source.version_id})
            self._emit(conn, dst, "object.copied", {
                "bucket": dst_bucket, "key": dst_key, "version_id": version_id,
                "size": source.size, "etag": source.etag, "at": now,
            }, now)
        return {"bucket": dst_bucket, "key": dst_key, "version_id": version_id,
                "etag": source.etag, "size": source.size, "created_at": now}

    # ------------------------------------------------------------------ 读取
    def head_object(self, bucket_name: str, key: str,
                    version_id: Optional[str] = None) -> ObjectVersion:
        self.require_bucket(bucket_name)
        with self.db.read() as conn:
            version = (self.meta.get_version(conn, bucket_name, key, version_id)
                       if version_id else self.meta.get_current(conn, bucket_name, key))
        if not version:
            raise NotFound(f"对象不存在: {bucket_name}/{key}", detail={"bucket": bucket_name,
                                                                      "key": key})
        if version.is_delete_marker:
            raise NotFound(f"对象已被删除标记覆盖: {bucket_name}/{key}",
                           detail={"delete_marker": version.version_id})
        return version

    def open_object(self, bucket_name: str, key: str, range_header: Optional[str] = None,
                    version_id: Optional[str] = None) -> ObjectHandle:
        version = self.head_object(bucket_name, key, version_id)
        with self.db.read() as conn:
            refs = self.meta.chunk_refs(conn, bucket_name, key, version.version_id)
            chunk_rows = {r["hash"]: r for r in
                          (self.meta.get_chunk(conn, d) for _s, d, _z in refs) if r}
        for _seq, digest, _size in refs:
            row = chunk_rows.get(digest)
            if row is None:
                raise ChunkCorrupted("分片元数据缺失，对象不可读",
                                     detail={"hash": digest, "key": key})
            if row["corrupt"]:
                raise ChunkCorrupted("分片校验失败，对象不可读", detail={"hash": digest})
            if not self.cas.exists(digest):
                raise ChunkCorrupted("分片文件缺失，对象不可读", detail={"hash": digest})
        start, end, partial = parse_range(range_header, version.size)
        return ObjectHandle(version=version, chunks=refs, start=start, end=end, partial=partial)

    def iter_chunks(self, handle: ObjectHandle) -> Iterator[bytes]:
        if handle.end < handle.start:
            return
        offset = 0
        for _seq, digest, size in handle.chunks:
            chunk_start = offset
            chunk_end = offset + size - 1
            offset += size
            if chunk_end < handle.start or chunk_start > handle.end:
                continue
            local_start = max(handle.start - chunk_start, 0)
            local_end = min(handle.end - chunk_start, size - 1)
            yield from self.cas.iter_range(digest, local_start, local_end - local_start + 1)

    # ------------------------------------------------------------------ 删除
    def delete_object(self, bucket_name: str, key: str, version_id: Optional[str] = None, *,
                      actor: str = "anonymous",
                      request_id: Optional[str] = None) -> dict:
        bucket = self.require_bucket(bucket_name)
        now = utcnow()
        if version_id:
            with self.db.tx() as conn:
                version = self.meta.get_version(conn, bucket_name, key, version_id)
                if not version:
                    raise NotFound(f"版本不存在: {key}@{version_id}")
                was_current = version.is_current
                self._drop_version_locked(conn, version)
                promoted = None
                if was_current:
                    # 删掉当前版本（尤其是删除标记）后，让最近的历史版本重新成为当前版本，
                    # 这正是「撤销删除标记即恢复对象」的语义来源。
                    latest = self.meta.latest_version(conn, bucket_name, key)
                    if latest:
                        self.meta.set_current(conn, bucket_name, key, latest.version_id)
                        if not latest.is_delete_marker:
                            self.meta.add_usage(conn, bucket_name, 0, 1)
                        promoted = latest.version_id
                self.meta.add_audit(conn, actor=actor, action="object.version.delete", status=200,
                                    bucket=bucket_name, key=key, version_id=version_id,
                                    request_id=request_id,
                                    detail={"promoted": promoted})
                self._emit(conn, bucket, "object.version.deleted", {
                    "bucket": bucket_name, "key": key, "version_id": version_id,
                    "promoted": promoted, "at": now,
                }, now)
            return {"key": key, "version_id": version_id, "deleted": True,
                    "promoted": promoted}

        with self.db.tx() as conn:
            current = self.meta.get_current(conn, bucket_name, key)
            if not current:
                raise NotFound(f"对象不存在: {bucket_name}/{key}")
            if bucket.versioning:
                marker_id = new_id()
                self.meta.clear_current(conn, bucket_name, key)
                marker = ObjectVersion(
                    bucket=bucket_name, key=key, version_id=marker_id, is_delete_marker=True,
                    is_current=True, size=0, chunk_count=0, etag="", content_type=None,
                    storage_class="hot", metadata={}, created_at=now, created_by=actor,
                )
                self.meta.insert_version(conn, marker)
                if not current.is_delete_marker:
                    self.meta.add_usage(conn, bucket_name, 0, -1)
                self.meta.add_audit(conn, actor=actor, action="object.delete_marker.create",
                                    status=200, bucket=bucket_name, key=key,
                                    version_id=marker_id, request_id=request_id,
                                    detail={"replaced": current.version_id})
                self._emit(conn, bucket, "object.delete_marker.created", {
                    "bucket": bucket_name, "key": key, "version_id": marker_id, "at": now,
                }, now)
                return {"key": key, "delete_marker": marker_id}
            self._drop_version_locked(conn, current)
            self.meta.add_audit(conn, actor=actor, action="object.delete", status=200,
                                bucket=bucket_name, key=key, version_id=current.version_id,
                                request_id=request_id, detail={"size": current.size})
            self._emit(conn, bucket, "object.deleted", {
                "bucket": bucket_name, "key": key, "version_id": current.version_id,
                "size": current.size, "at": now,
            }, now)
        METRICS.inc("stratum_objects_deleted_total")
        return {"key": key, "version_id": current.version_id, "deleted": True}

    # ------------------------------------------------------------------ 列表
    def list_objects(self, bucket_name: str, prefix: str = "", limit: int = 100,
                     continuation: Optional[str] = None) -> dict:
        self.require_bucket(bucket_name)
        limit = max(1, min(limit, 1000))
        start_after = None
        if continuation:
            start_after = decode_token(continuation).get("key")
        with self.db.read() as conn:
            rows = self.meta.list_objects(conn, bucket_name, prefix, limit + 1, start_after)
        truncated = len(rows) > limit
        rows = rows[:limit]
        items = [{"key": v.key, "size": v.size, "etag": v.etag,
                  "content_type": v.content_type, "created_at": v.created_at,
                  "version_id": v.version_id, "storage_class": v.storage_class}
                 for v in rows]
        return {
            "bucket": bucket_name, "prefix": prefix, "items": items,
            "is_truncated": truncated,
            "next_token": encode_token({"key": rows[-1].key}) if truncated and rows else None,
            "count": len(items),
        }

    def list_versions(self, bucket_name: str, prefix: str = "", limit: int = 100,
                      continuation: Optional[str] = None) -> dict:
        self.require_bucket(bucket_name)
        limit = max(1, min(limit, 1000))
        start_after = None
        if continuation:
            payload = decode_token(continuation)
            start_after = (payload["key"], payload["ts"])
        with self.db.read() as conn:
            rows = self.meta.list_versions(conn, bucket_name, prefix, limit + 1, start_after)
        truncated = len(rows) > limit
        rows = rows[:limit]
        return {
            "bucket": bucket_name, "items": [v.to_dict() for v in rows],
            "is_truncated": truncated,
            "next_token": (encode_token({"key": rows[-1].key, "ts": rows[-1].created_at})
                           if truncated and rows else None),
            "count": len(rows),
        }


def _validate_key(key: str) -> None:
    try:
        validate_key(key)
    except ValueError as exc:
        raise InvalidRequest(str(exc)) from exc


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    return value
