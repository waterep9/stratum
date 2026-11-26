"""元数据仓库：所有 SQL 集中在此，服务层只做编排与业务不变量。

约定
----
* 所有写操作都在调用方给出的 ``conn`` 上执行，事务边界由服务层用 ``Database.tx()`` 掌控；
* 「引用计数 / 用量」是缓存型计数器，权威数据是 ``chunk_refs`` 与 ``object_versions``；
  ``maintenance.reconcile`` 可以随时把计数器与权威数据对齐（压测与故障恢复都靠它兜底）。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from ..util import json_dumps, json_loads, utcnow


# --------------------------------------------------------------------------- #
# 领域对象
# --------------------------------------------------------------------------- #
@dataclass
class Bucket:
    name: str
    created_at: str
    versioning: bool = False
    quota_bytes: int = 0
    max_object_bytes: int = 0
    used_bytes: int = 0
    object_count: int = 0
    default_content_type: str = "application/octet-stream"
    lifecycle: dict = field(default_factory=dict)
    webhook_url: Optional[str] = None
    labels: dict = field(default_factory=dict)
    created_by: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "created_at": self.created_at,
            "versioning": self.versioning,
            "quota_bytes": self.quota_bytes,
            "max_object_bytes": self.max_object_bytes,
            "used_bytes": self.used_bytes,
            "object_count": self.object_count,
            "default_content_type": self.default_content_type,
            "lifecycle": self.lifecycle,
            "webhook_url": self.webhook_url,
            "labels": self.labels,
        }


@dataclass
class ObjectVersion:
    bucket: str
    key: str
    version_id: str
    is_delete_marker: bool
    is_current: bool
    size: int
    chunk_count: int
    etag: str
    content_type: Optional[str]
    storage_class: str
    metadata: dict
    created_at: str
    created_by: Optional[str] = None

    @property
    def id(self) -> str:
        return f"{self.bucket}/{self.key}@{self.version_id}"

    def to_dict(self) -> dict:
        return {
            "bucket": self.bucket,
            "key": self.key,
            "version_id": self.version_id,
            "is_delete_marker": self.is_delete_marker,
            "is_current": self.is_current,
            "size": self.size,
            "chunk_count": self.chunk_count,
            "etag": self.etag,
            "content_type": self.content_type,
            "storage_class": self.storage_class,
            "metadata": self.metadata,
            "created_at": self.created_at,
        }


@dataclass
class Upload:
    upload_id: str
    bucket: str
    key: str
    created_at: str
    content_type: Optional[str]
    metadata: dict
    state: str
    created_by: Optional[str]
    parts_bytes: int = 0


@dataclass
class Part:
    part_no: int
    hash: str
    size: int
    created_at: str


def _bucket(row: sqlite3.Row) -> Bucket:
    return Bucket(
        name=row["name"],
        created_at=row["created_at"],
        versioning=bool(row["versioning"]),
        quota_bytes=row["quota_bytes"],
        max_object_bytes=row["max_object_bytes"],
        used_bytes=row["used_bytes"],
        object_count=row["object_count"],
        default_content_type=row["default_content_type"],
        lifecycle=json_loads(row["lifecycle_json"], {}),
        webhook_url=row["webhook_url"],
        labels=json_loads(row["labels_json"], {}),
        created_by=row["created_by"],
    )


def _version(row: sqlite3.Row) -> ObjectVersion:
    return ObjectVersion(
        bucket=row["bucket"],
        key=row["key"],
        version_id=row["version_id"],
        is_delete_marker=bool(row["is_delete_marker"]),
        is_current=bool(row["is_current"]),
        size=row["size"],
        chunk_count=row["chunk_count"],
        etag=row["etag"],
        content_type=row["content_type"],
        storage_class=row["storage_class"],
        metadata=json_loads(row["metadata_json"], {}),
        created_at=row["created_at"],
        created_by=row["created_by"],
    )


def _upload(row: sqlite3.Row) -> Upload:
    return Upload(
        upload_id=row["upload_id"],
        bucket=row["bucket"],
        key=row["key"],
        created_at=row["created_at"],
        content_type=row["content_type"],
        metadata=json_loads(row["metadata_json"], {}),
        state=row["state"],
        created_by=row["created_by"],
        parts_bytes=row["parts_bytes"],
    )


def _prefix_range(prefix: str) -> tuple[str, Optional[str]]:
    if not prefix:
        return "", None
    upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    return prefix, upper


class MetadataStore:
    """薄仓库层。方法不抛领域异常，返回值交给服务层判断。"""

    # ---------------------------------------------------------------- buckets
    def create_bucket(self, conn, bucket: Bucket) -> None:
        conn.execute(
            "INSERT INTO buckets (name, created_at, versioning, quota_bytes, max_object_bytes,"
            " used_bytes, object_count, default_content_type, lifecycle_json, webhook_url,"
            " labels_json, created_by) VALUES (?,?,?,?,?,0,0,?,?,?,?,?)",
            (
                bucket.name, bucket.created_at, int(bucket.versioning), bucket.quota_bytes,
                bucket.max_object_bytes, bucket.default_content_type,
                json_dumps(bucket.lifecycle), bucket.webhook_url,
                json_dumps(bucket.labels), bucket.created_by,
            ),
        )

    def get_bucket(self, conn, name: str) -> Optional[Bucket]:
        row = conn.execute("SELECT * FROM buckets WHERE name = ?", (name,)).fetchone()
        return _bucket(row) if row else None

    def list_buckets(self, conn) -> list[Bucket]:
        rows = conn.execute("SELECT * FROM buckets ORDER BY name").fetchall()
        return [_bucket(r) for r in rows]

    def update_bucket(self, conn, bucket: Bucket) -> None:
        conn.execute(
            "UPDATE buckets SET versioning=?, quota_bytes=?, max_object_bytes=?,"
            " default_content_type=?, lifecycle_json=?, webhook_url=?, labels_json=?"
            " WHERE name=?",
            (
                int(bucket.versioning), bucket.quota_bytes, bucket.max_object_bytes,
                bucket.default_content_type, json_dumps(bucket.lifecycle), bucket.webhook_url,
                json_dumps(bucket.labels), bucket.name,
            ),
        )

    def drop_bucket(self, conn, name: str) -> None:
        conn.execute("DELETE FROM buckets WHERE name = ?", (name,))

    def add_usage(self, conn, name: str, delta_bytes: int, delta_objects: int) -> bool:
        """原子地累加用量并校验配额。返回 False 表示超出配额（调用方抛 QuotaExceeded）。

        这里把「配额校验」放进 UPDATE 的 WHERE 里，让 SQLite 的写锁替我们做 CAS，
        避免「先 SELECT 再 UPDATE」的典型竞态。
        """
        cur = conn.execute(
            "UPDATE buckets SET used_bytes = used_bytes + ?, object_count = object_count + ?"
            " WHERE name = ? AND (quota_bytes = 0 OR used_bytes + ? <= quota_bytes)",
            (delta_bytes, delta_objects, name, delta_bytes),
        )
        return cur.rowcount == 1

    def recompute_usage(self, conn) -> list[dict]:
        """把用量/计数计数器与权威数据对齐，返回发生漂移的 bucket。"""
        rows = conn.execute(
            "SELECT b.name, b.used_bytes, b.object_count,"
            " COALESCE(v.total, 0) AS real_bytes, COALESCE(v.current, 0) AS real_objects"
            " FROM buckets b LEFT JOIN ("
            "   SELECT bucket, SUM(size) AS total,"
            "     SUM(CASE WHEN is_current = 1 AND is_delete_marker = 0 THEN 1 ELSE 0 END) AS current"
            "   FROM object_versions GROUP BY bucket"
            " ) v ON v.bucket = b.name"
        ).fetchall()
        drift = []
        for row in rows:
            if row["used_bytes"] != row["real_bytes"] or row["object_count"] != row["real_objects"]:
                drift.append({
                    "bucket": row["name"],
                    "used_bytes": [row["used_bytes"], row["real_bytes"]],
                    "object_count": [row["object_count"], row["real_objects"]],
                })
                conn.execute(
                    "UPDATE buckets SET used_bytes = ?, object_count = ? WHERE name = ?",
                    (row["real_bytes"], row["real_objects"], row["name"]),
                )
        return drift

    # ---------------------------------------------------------------- objects
    def get_current(self, conn, bucket: str, key: str) -> Optional[ObjectVersion]:
        row = conn.execute(
            "SELECT * FROM object_versions WHERE bucket=? AND key=? AND is_current=1",
            (bucket, key),
        ).fetchone()
        return _version(row) if row else None

    def get_version(self, conn, bucket: str, key: str, version_id: str) -> Optional[ObjectVersion]:
        row = conn.execute(
            "SELECT * FROM object_versions WHERE bucket=? AND key=? AND version_id=?",
            (bucket, key, version_id),
        ).fetchone()
        return _version(row) if row else None

    def count_versions(self, conn, bucket: str, key: str) -> int:
        return conn.execute(
            "SELECT COUNT(*) FROM object_versions WHERE bucket=? AND key=?", (bucket, key)
        ).fetchone()[0]

    def clear_current(self, conn, bucket: str, key: str) -> None:
        conn.execute(
            "UPDATE object_versions SET is_current=0 WHERE bucket=? AND key=? AND is_current=1",
            (bucket, key),
        )

    def insert_version(self, conn, version: ObjectVersion) -> None:
        conn.execute(
            "INSERT INTO object_versions (bucket, key, version_id, is_delete_marker, is_current,"
            " size, chunk_count, etag, content_type, storage_class, metadata_json, created_at,"
            " created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                version.bucket, version.key, version.version_id, int(version.is_delete_marker),
                int(version.is_current), version.size, version.chunk_count, version.etag,
                version.content_type, version.storage_class, json_dumps(version.metadata),
                version.created_at, version.created_by,
            ),
        )

    def latest_version(self, conn, bucket: str, key: str) -> Optional[ObjectVersion]:
        row = conn.execute(
            "SELECT * FROM object_versions WHERE bucket=? AND key=?"
            " ORDER BY created_at DESC, id DESC LIMIT 1", (bucket, key)
        ).fetchone()
        return _version(row) if row else None

    def set_current(self, conn, bucket: str, key: str, version_id: str) -> None:
        conn.execute(
            "UPDATE object_versions SET is_current=1 WHERE bucket=? AND key=? AND version_id=?",
            (bucket, key, version_id),
        )

    def delete_version_row(self, conn, bucket: str, key: str, version_id: str) -> int:
        cur = conn.execute(
            "DELETE FROM object_versions WHERE bucket=? AND key=? AND version_id=?",
            (bucket, key, version_id),
        )
        return cur.rowcount

    # --------------------------------------------------------------- lifecycle
    def expired_candidates(self, conn, bucket: str, cutoff: str, limit: int) -> list[ObjectVersion]:
        rows = conn.execute(
            "SELECT * FROM object_versions WHERE bucket=? AND is_current=1"
            " AND is_delete_marker=0 AND created_at < ? ORDER BY created_at LIMIT ?",
            (bucket, cutoff, limit),
        ).fetchall()
        return [_version(r) for r in rows]

    def noncurrent_candidates(self, conn, bucket: str, cutoff: str, limit: int) -> list[ObjectVersion]:
        rows = conn.execute(
            "SELECT * FROM object_versions WHERE bucket=? AND is_current=0 AND created_at < ?"
            " ORDER BY created_at LIMIT ?", (bucket, cutoff, limit)
        ).fetchall()
        return [_version(r) for r in rows]

    def marker_candidates(self, conn, bucket: str, cutoff: str, limit: int) -> list[ObjectVersion]:
        rows = conn.execute(
            "SELECT * FROM object_versions WHERE bucket=? AND is_delete_marker=1"
            " AND created_at < ? ORDER BY created_at LIMIT ?", (bucket, cutoff, limit)
        ).fetchall()
        return [_version(r) for r in rows]

    # ---------------------------------------------------------------- 列表
    def list_objects(self, conn, bucket: str, prefix: str = "", limit: int = 100,
                     start_after: str | None = None) -> list[ObjectVersion]:
        lo, hi = _prefix_range(prefix)
        sql = ("SELECT * FROM object_versions WHERE bucket=? AND is_current=1"
               " AND is_delete_marker=0 AND key >= ?")
        params: list[Any] = [bucket, lo]
        if hi is not None:
            sql += " AND key < ?"
            params.append(hi)
        if start_after:
            sql += " AND key > ?"
            params.append(start_after)
        sql += " ORDER BY key LIMIT ?"
        params.append(limit)
        return [_version(r) for r in conn.execute(sql, params).fetchall()]

    def list_versions(self, conn, bucket: str, prefix: str = "", limit: int = 100,
                      start_after: tuple[str, str] | None = None) -> list[ObjectVersion]:
        lo, hi = _prefix_range(prefix)
        sql = "SELECT * FROM object_versions WHERE bucket=? AND key >= ?"
        params: list[Any] = [bucket, lo]
        if hi is not None:
            sql += " AND key < ?"
            params.append(hi)
        if start_after:
            sql += " AND (key > ? OR (key = ? AND created_at > ?))"
            params.extend([start_after[0], start_after[0], start_after[1]])
        sql += " ORDER BY key ASC, created_at ASC LIMIT ?"
        params.append(limit)
        return [_version(r) for r in conn.execute(sql, params).fetchall()]

    # ----------------------------------------------------------------- chunks
    def chunk_refs(self, conn, bucket: str, key: str, version_id: str) -> list[tuple[int, str, int]]:
        rows = conn.execute(
            "SELECT seq, hash, size FROM chunk_refs WHERE bucket=? AND key=? AND version_id=?"
            " ORDER BY seq",
            (bucket, key, version_id),
        ).fetchall()
        return [(r["seq"], r["hash"], r["size"]) for r in rows]

    def add_chunk_refs(self, conn, bucket: str, key: str, version_id: str,
                       refs: Iterable[tuple[int, str, int]]) -> None:
        conn.executemany(
            "INSERT INTO chunk_refs (bucket, key, version_id, seq, hash, size) VALUES (?,?,?,?,?,?)",
            [(bucket, key, version_id, seq, digest, size) for seq, digest, size in refs],
        )

    def drop_chunk_refs(self, conn, bucket: str, key: str, version_id: str) -> int:
        cur = conn.execute(
            "DELETE FROM chunk_refs WHERE bucket=? AND key=? AND version_id=?",
            (bucket, key, version_id),
        )
        return cur.rowcount

    def inc_refcount(self, conn, digest: str, size: int, count: int, now: str) -> None:
        """引用计数 +count；分片行不存在则登记（可能新写入，也可能复用了已回收前的分片）。"""
        conn.execute(
            "INSERT INTO chunks (hash, size, refcount, created_at, last_ref_at)"
            " VALUES (?,?,?,?,?)"
            " ON CONFLICT(hash) DO UPDATE SET refcount = refcount + excluded.refcount,"
            " last_ref_at = excluded.last_ref_at",
            (digest, size, count, now, now),
        )

    def dec_refcount(self, conn, digest: str, count: int) -> None:
        conn.execute(
            "UPDATE chunks SET refcount = MAX(refcount - ?, 0) WHERE hash = ?", (count, digest)
        )

    def get_chunk(self, conn, digest: str) -> Optional[sqlite3.Row]:
        return conn.execute("SELECT * FROM chunks WHERE hash=?", (digest,)).fetchone()

    def gc_candidates(self, conn, cutoff: str, limit: int) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM chunks WHERE refcount <= 0 AND last_ref_at < ? ORDER BY last_ref_at LIMIT ?",
            (cutoff, limit),
        ).fetchall()

    def drop_chunk_row(self, conn, digest: str) -> None:
        conn.execute("DELETE FROM chunks WHERE hash=?", (digest,))

    def mark_corrupt(self, conn, digest: str, corrupt: bool = True) -> None:
        conn.execute("UPDATE chunks SET corrupt=? WHERE hash=?", (int(corrupt), digest))

    def all_chunks(self, conn, limit: int = 100000, offset: int = 0) -> list[sqlite3.Row]:
        return conn.execute(
            "SELECT * FROM chunks ORDER BY hash LIMIT ? OFFSET ?", (limit, offset)
        ).fetchall()

    def refcount_drift(self, conn) -> list[dict]:
        """引用计数与 chunk_refs 不一致的分片（reconcile 用）。"""
        rows = conn.execute(
            "SELECT c.hash, c.refcount, COALESCE(r.cnt, 0) AS real_cnt"
            " FROM chunks c LEFT JOIN ("
            "   SELECT hash, COUNT(*) AS cnt FROM chunk_refs GROUP BY hash"
            " ) r ON r.hash = c.hash"
            " WHERE c.refcount != COALESCE(r.cnt, 0)"
        ).fetchall()
        return [{"hash": r["hash"], "refcount": r["refcount"], "real": r["real_cnt"]} for r in rows]

    def fix_refcount(self, conn, digest: str, real: int) -> None:
        conn.execute("UPDATE chunks SET refcount=? WHERE hash=?", (real, digest))

    def chunk_stats(self, conn) -> dict:
        row = conn.execute(
            "SELECT COUNT(*) AS total, COALESCE(SUM(size),0) AS bytes,"
            " COALESCE(SUM(CASE WHEN refcount > 0 THEN size ELSE 0 END),0) AS live_bytes,"
            " COALESCE(SUM(CASE WHEN refcount > 0 THEN 1 ELSE 0 END),0) AS live,"
            " COALESCE(SUM(CASE WHEN corrupt = 1 THEN 1 ELSE 0 END),0) AS corrupt"
            " FROM chunks"
        ).fetchone()
        return {
            "chunks_total": row["total"],
            "chunks_live": row["live"],
            "chunk_bytes_total": row["bytes"],
            "chunk_bytes_live": row["live_bytes"],
            "chunks_corrupt": row["corrupt"],
        }

    # -------------------------------------------------------------- multipart
    def create_upload(self, conn, upload: Upload) -> None:
        conn.execute(
            "INSERT INTO multipart_uploads (upload_id, bucket, key, created_at, content_type,"
            " metadata_json, state, created_by, parts_bytes) VALUES (?,?,?,?,?,?,?,?,0)",
            (upload.upload_id, upload.bucket, upload.key, upload.created_at, upload.content_type,
             json_dumps(upload.metadata), upload.state, upload.created_by),
        )

    def get_upload(self, conn, upload_id: str) -> Optional[Upload]:
        row = conn.execute("SELECT * FROM multipart_uploads WHERE upload_id=?", (upload_id,)).fetchone()
        return _upload(row) if row else None

    def list_uploads(self, conn, bucket: str, prefix: str = "") -> list[Upload]:
        lo, hi = _prefix_range(prefix)
        sql = "SELECT * FROM multipart_uploads WHERE bucket=? AND state='OPEN' AND key >= ?"
        params: list[Any] = [bucket, lo]
        if hi is not None:
            sql += " AND key < ?"
            params.append(hi)
        sql += " ORDER BY created_at"
        return [_upload(r) for r in conn.execute(sql, params).fetchall()]

    def stale_uploads(self, conn, cutoff: str, limit: int = 100) -> list[Upload]:
        rows = conn.execute(
            "SELECT * FROM multipart_uploads WHERE state='OPEN' AND created_at < ?"
            " ORDER BY created_at LIMIT ?", (cutoff, limit)
        ).fetchall()
        return [_upload(r) for r in rows]

    def update_upload_state(self, conn, upload_id: str, state: str) -> int:
        cur = conn.execute("UPDATE multipart_uploads SET state=? WHERE upload_id=?", (state, upload_id))
        return cur.rowcount

    def bump_upload_bytes(self, conn, upload_id: str, delta: int) -> None:
        conn.execute(
            "UPDATE multipart_uploads SET parts_bytes = MAX(parts_bytes + ?, 0) WHERE upload_id=?",
            (delta, upload_id),
        )

    def upsert_part(self, conn, upload_id: str, part_no: int, digest: str, size: int,
                    now: str) -> int:
        """返回被覆盖的旧部件大小（用于修正 parts_bytes）。"""
        row = conn.execute(
            "SELECT size FROM multipart_parts WHERE upload_id=? AND part_no=?", (upload_id, part_no)
        ).fetchone()
        old = row["size"] if row else 0
        conn.execute(
            "INSERT INTO multipart_parts (upload_id, part_no, hash, size, created_at)"
            " VALUES (?,?,?,?,?) ON CONFLICT(upload_id, part_no) DO UPDATE SET"
            " hash=excluded.hash, size=excluded.size, created_at=excluded.created_at",
            (upload_id, part_no, digest, size, now),
        )
        return old

    def list_parts(self, conn, upload_id: str) -> list[Part]:
        rows = conn.execute(
            "SELECT part_no, hash, size, created_at FROM multipart_parts"
            " WHERE upload_id=? ORDER BY part_no", (upload_id,)
        ).fetchall()
        return [Part(r["part_no"], r["hash"], r["size"], r["created_at"]) for r in rows]

    # ------------------------------------------------------------------ audit
    def add_audit(self, conn, *, actor: str, action: str, status: int, bucket: str | None = None,
                  key: str | None = None, version_id: str | None = None,
                  request_id: str | None = None, detail: dict | None = None) -> None:
        conn.execute(
            "INSERT INTO audit_log (ts, actor, action, bucket, key, version_id, status,"
            " request_id, detail_json) VALUES (?,?,?,?,?,?,?,?,?)",
            (utcnow(), actor, action, bucket, key, version_id, status, request_id,
             json_dumps(detail or {})),
        )

    def list_audit(self, conn, bucket: str | None = None, action: str | None = None,
                   limit: int = 50, before_id: int | None = None) -> list[dict]:
        sql = "SELECT * FROM audit_log WHERE 1=1"
        params: list[Any] = []
        if bucket:
            sql += " AND bucket = ?"
            params.append(bucket)
        if action:
            sql += " AND action = ?"
            params.append(action)
        if before_id:
            sql += " AND id < ?"
            params.append(before_id)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        out = []
        for row in conn.execute(sql, params).fetchall():
            item = dict(row)
            item["detail"] = json_loads(item.pop("detail_json"), {})
            out.append(item)
        return out

    # ----------------------------------------------------------------- outbox
    def add_event(self, conn, bucket: str, event_type: str, payload: dict,
                  target_url: str | None, now: str) -> int:
        cur = conn.execute(
            "INSERT INTO outbox_events (created_at, bucket, event_type, payload_json,"
            " target_url, state, attempts, next_at) VALUES (?,?,?,?,?, 'PENDING', 0, ?)",
            (now, bucket, event_type, json_dumps(payload), target_url, now),
        )
        return int(cur.lastrowid)

    def due_events(self, conn, now: str, limit: int = 20) -> list[dict]:
        rows = conn.execute(
            "SELECT * FROM outbox_events WHERE state='PENDING' AND next_at <= ?"
            " ORDER BY id LIMIT ?", (now, limit)
        ).fetchall()
        events = []
        for row in rows:
            item = dict(row)
            item["payload"] = json_loads(item.pop("payload_json"), {})
            events.append(item)
        return events

    def lease_event(self, conn, event_id: int, expected_next_at: str, lease_until: str) -> bool:
        """抢占一条事件（CAS）：只有 next_at 仍等于读到的值时才成功。

        这样两个 worker 同时拿到同一条 due 事件时，只有一个能把租约推到未来，
        另一个的 UPDATE 影响 0 行，直接跳过 —— 无需额外中间件即可实现工作窃取式互斥。
        """
        cur = conn.execute(
            "UPDATE outbox_events SET attempts = attempts + 1, next_at = ?"
            " WHERE id = ? AND state = 'PENDING' AND next_at = ?",
            (lease_until, event_id, expected_next_at),
        )
        return cur.rowcount == 1

    def mark_delivered(self, conn, event_id: int, now: str) -> None:
        conn.execute(
            "UPDATE outbox_events SET state='DELIVERED', delivered_at=?, last_error=NULL"
            " WHERE id=?", (now, event_id)
        )

    def mark_failed(self, conn, event_id: int, next_at: str, error: str) -> None:
        conn.execute(
            "UPDATE outbox_events SET next_at=?, last_error=? WHERE id=?", (next_at, error[:500], event_id)
        )

    def mark_dead(self, conn, event_id: int, error: str) -> None:
        conn.execute(
            "UPDATE outbox_events SET state='DEAD', last_error=? WHERE id=?", (error[:500], event_id)
        )

    def outbox_stats(self, conn) -> dict:
        rows = conn.execute(
            "SELECT state, COUNT(*) AS cnt FROM outbox_events GROUP BY state"
        ).fetchall()
        stats = {"PENDING": 0, "DELIVERED": 0, "DEAD": 0}
        for row in rows:
            stats[row["state"]] = row["cnt"]
        return stats

    # ------------------------------------------------------------- access keys
    def create_access_key(self, conn, access_key: str, secret_hash: str, label: str,
                          grants: list, is_admin: bool, now: str) -> None:
        conn.execute(
            "INSERT INTO access_keys (access_key, secret_hash, label, grants_json, is_admin,"
            " created_at) VALUES (?,?,?,?,?,?)",
            (access_key, secret_hash, label, json_dumps(grants), int(is_admin), now),
        )

    def get_access_key(self, conn, access_key: str) -> Optional[dict]:
        row = conn.execute("SELECT * FROM access_keys WHERE access_key=?", (access_key,)).fetchone()
        if not row:
            return None
        item = dict(row)
        item["grants"] = json_loads(item.pop("grants_json"), [])
        item["is_admin"] = bool(item["is_admin"])
        return item

    def list_access_keys(self, conn) -> list[dict]:
        out = []
        for row in conn.execute("SELECT * FROM access_keys ORDER BY created_at").fetchall():
            item = dict(row)
            item["grants"] = json_loads(item.pop("grants_json"), [])
            item["is_admin"] = bool(item["is_admin"])
            item.pop("secret_hash", None)  # 密钥摘要不外泄
            out.append(item)
        return out

    def revoke_access_key(self, conn, access_key: str, now: str) -> int:
        cur = conn.execute(
            "UPDATE access_keys SET revoked_at=? WHERE access_key=? AND revoked_at IS NULL",
            (now, access_key),
        )
        return cur.rowcount

    # ----------------------------------------------------------- maintenance
    def start_run(self, conn, job: str, now: str) -> int:
        cur = conn.execute(
            "INSERT INTO maintenance_runs (job, started_at, status) VALUES (?,?, 'RUNNING')",
            (job, now),
        )
        return int(cur.lastrowid)

    def finish_run(self, conn, run_id: int, status: str, result: dict, now: str) -> None:
        conn.execute(
            "UPDATE maintenance_runs SET finished_at=?, status=?, result_json=? WHERE id=?",
            (now, status, json_dumps(result), run_id),
        )

    def recent_runs(self, conn, limit: int = 20) -> list[dict]:
        rows = conn.execute(
            "SELECT * FROM maintenance_runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["result"] = json_loads(item.pop("result_json"), {})
            out.append(item)
        return out

    def add_scrub_report(self, conn, run_id: int, started_at: str, finished_at: str,
                         checked: int, bad: int, bytes_read: int, detail: list) -> None:
        conn.execute(
            "INSERT INTO scrub_reports (run_id, started_at, finished_at, chunks_checked,"
            " chunks_bad, bytes_read, detail_json) VALUES (?,?,?,?,?,?,?)",
            (run_id, started_at, finished_at, checked, bad, bytes_read, json_dumps(detail)),
        )

    def recent_scrubs(self, conn, limit: int = 10) -> list[dict]:
        rows = conn.execute(
            "SELECT * FROM scrub_reports ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["detail"] = json_loads(item.pop("detail_json"), [])
            out.append(item)
        return out

    # ------------------------------------------------------------------ stats
    def overview(self, conn) -> dict:
        buckets = conn.execute("SELECT COUNT(*) FROM buckets").fetchone()[0]
        versions = conn.execute("SELECT COUNT(*) FROM object_versions").fetchone()[0]
        objects = conn.execute(
            "SELECT COUNT(*) FROM object_versions WHERE is_current=1 AND is_delete_marker=0"
        ).fetchone()[0]
        markers = conn.execute("SELECT COUNT(*) FROM object_versions WHERE is_delete_marker=1").fetchone()[0]
        logical = conn.execute("SELECT COALESCE(SUM(size),0) FROM object_versions").fetchone()[0]
        uploads = conn.execute(
            "SELECT COUNT(*) FROM multipart_uploads WHERE state='OPEN'"
        ).fetchone()[0]
        return {
            "buckets": buckets,
            "objects": objects,
            "versions": versions,
            "delete_markers": markers,
            "logical_bytes": logical,
            "open_uploads": uploads,
            "audit_events": conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0],
            **self.chunk_stats(conn),
            "outbox": self.outbox_stats(conn),
        }
