from __future__ import annotations

import hashlib
import os

from conftest import get, put


def _stats(container):
    with container.db.read() as conn:
        return container.meta.chunk_stats(conn)


def test_chunk_dedup_across_objects(client, bucket, container):
    data = os.urandom(200 * 1024)
    first = put(client, bucket, "a.bin", data).json()
    second = put(client, bucket, "b.bin", data).json()

    # 第二份完全相同的内容：所有分片都命中去重，不产生新的物理写入
    assert first["chunks_written"] == 4
    assert second["chunks_deduped"] == 4
    assert second["chunks_written"] == 0
    stats = _stats(container)
    assert stats["chunks_total"] == 4
    assert stats["chunk_bytes_total"] == len(data)

    with container.db.read() as conn:
        rows = conn.execute("SELECT refcount FROM chunks").fetchall()
    assert {row["refcount"] for row in rows} == {2}


def test_delete_releases_reference(client, bucket, container):
    data = os.urandom(70 * 1024)
    put(client, bucket, "x.bin", data)
    assert _stats(container)["chunks_total"] == 2

    client.delete("/v1/buckets/demo/objects/x.bin")
    with container.db.read() as conn:
        refs = conn.execute("SELECT COUNT(*) FROM chunk_refs").fetchone()[0]
    assert refs == 2  # 版本化桶：留下删除标记，但旧版本的分片引用仍在（2 个分片）

    versions = client.get("/v1/buckets/demo/versions").json()["items"]
    for item in versions:
        client.delete(f"/v1/buckets/demo/objects/x.bin?versionId={item['version_id']}")
    with container.db.read() as conn:
        rows = conn.execute("SELECT refcount FROM chunks").fetchall()
    assert all(row["refcount"] == 0 for row in rows)


def test_range_requests(client, bucket):
    payload = b"0123456789abcdef"
    put(client, bucket, "r.txt", payload)

    partial = client.get("/v1/buckets/demo/objects/r.txt", headers={"Range": "bytes=0-3"})
    assert partial.status_code == 206
    assert partial.content == b"0123"
    assert partial.headers["content-range"] == f"bytes 0-3/{len(payload)}"

    suffix = client.get("/v1/buckets/demo/objects/r.txt", headers={"Range": "bytes=-4"})
    assert suffix.content == b"cdef"

    open_end = client.get("/v1/buckets/demo/objects/r.txt", headers={"Range": "bytes=10-"})
    assert open_end.content == b"abcdef"

    bad = client.get("/v1/buckets/demo/objects/r.txt", headers={"Range": "bytes=99-100"})
    assert bad.status_code == 416
    assert bad.headers["content-range"] == f"bytes */{len(payload)}"


def test_conditional_writes(client, bucket):
    put(client, bucket, "c.txt", b"v1")
    blocked = put(client, bucket, "c.txt", b"v2", **{"If-None-Match": "*"})
    assert blocked.status_code == 412

    current = client.head("/v1/buckets/demo/objects/c.txt").headers["etag"]
    ok = client.put("/v1/buckets/demo/objects/c.txt", content=b"v3",
                    headers={"If-Match": current})
    assert ok.status_code == 200

    stale = client.put("/v1/buckets/demo/objects/c.txt", content=b"v4",
                       headers={"If-Match": current})
    assert stale.status_code == 412


def test_copy_object_shares_chunks(client, bucket, container):
    data = os.urandom(150 * 1024)
    put(client, bucket, "src.bin", data)
    before = _stats(container)["chunk_bytes_total"]

    copied = client.post("/v1/buckets/demo/objects/dst.bin",
                         headers={"X-Stratum-Copy-Source": "demo/src.bin"})
    assert copied.status_code == 201, copied.text
    assert copied.json()["size"] == len(data)
    # 复制不搬数据：物理占用不变，只有引用计数增加
    assert _stats(container)["chunk_bytes_total"] == before
    with container.db.read() as conn:
        max_ref = conn.execute("SELECT MAX(refcount) FROM chunks").fetchone()[0]
    assert max_ref == 2
    assert get(client, bucket, "dst.bin").content == data


def test_quota_enforced(client):
    client.post("/v1/buckets", json={"name": "small", "quota_bytes": 100})
    assert put(client, "small", "a.bin", b"x" * 60).status_code == 200
    rejected = put(client, "small", "b.bin", b"y" * 60)
    assert rejected.status_code == 507
    assert rejected.json()["error"]["code"] == "QuotaExceeded"
    # 失败回滚后用量不变
    assert client.get("/v1/buckets/small").json()["used_bytes"] == 60


def test_object_size_limit(client, bucket):
    client.patch("/v1/buckets/demo", json={"max_object_bytes": 1024})
    res = put(client, bucket, "big.bin", b"z" * 2048)
    assert res.status_code == 413


def test_empty_object(client, bucket):
    res = put(client, bucket, "empty.txt", b"")
    assert res.status_code == 200
    assert res.json()["size"] == 0
    body = get(client, bucket, "empty.txt")
    assert body.status_code == 200
    assert body.content == b""
    assert body.headers["content-length"] == "0"


def test_invalid_keys_rejected(client, bucket):
    # URL 中的 "." / ".." 段会被 HTTP 客户端规范化，因此这里验证服务端能拦下的形态，
    # 路径穿越类键在服务层单测里直接校验（见下方）。
    for key in ["a//b", "ends/", "x//y"]:
        res = client.put(f"/v1/buckets/demo/objects/{key}", content=b"x")
        assert res.status_code in (400, 404), (key, res.status_code)

    import pytest

    from stratum.util import validate_key

    for key in ["../etc/passwd", "/abs", "a/./b", ""]:
        with pytest.raises(ValueError):
            validate_key(key)


def test_metadata_and_content_type(client, bucket):
    res = client.put("/v1/buckets/demo/objects/meta.json", content=b"{}",
                     headers={"Content-Type": "application/json",
                              "X-Stratum-Metadata": '{"owner":"data-team","tier":"bronze"}'})
    assert res.status_code == 200
    head = client.head("/v1/buckets/demo/objects/meta.json")
    assert head.headers["content-type"] == "application/json"
    versions = client.get("/v1/buckets/demo/versions").json()["items"]
    assert versions[0]["metadata"]["owner"] == "data-team"


def test_etag_is_content_addressed(client, bucket):
    payload = b"deterministic"
    res = put(client, bucket, "e.txt", payload)
    assert res.json()["etag"] == hashlib.sha256(payload).hexdigest()
    # 同内容不同键 -> 同 ETag
    other = put(client, bucket, "e2.txt", payload)
    assert other.json()["etag"] == res.json()["etag"]


def test_missing_object_and_bucket(client, bucket):
    assert get(client, bucket, "nope.txt").status_code == 404
    assert client.get("/v1/buckets/ghost/objects/x").status_code == 404
    assert client.get("/v1/buckets/ghost").json()["error"]["code"] == "NoSuchBucket"


def test_multichunk_roundtrip(client, bucket):
    data = os.urandom(64 * 1024 * 5 + 123)
    put(client, bucket, "multi.bin", data)
    got = get(client, bucket, "multi.bin")
    assert got.content == data
    assert got.headers["etag"] == f'"{hashlib.sha256(data).hexdigest()}"'
