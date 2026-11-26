from __future__ import annotations

import os

from conftest import get, put


def test_gc_reclaims_unreferenced_chunks(client, container):
    client.post("/v1/buckets", json={"name": "plain"})
    put(client, "plain", "a.bin", os.urandom(70 * 1024))
    client.delete("/v1/buckets/plain/objects/a.bin")

    with container.db.read() as conn:
        rows = conn.execute("SELECT hash, refcount FROM chunks").fetchall()
    assert rows and all(r["refcount"] == 0 for r in rows)
    paths = [container.cas.path_for(r["hash"]) for r in rows]
    assert all(os.path.exists(p) for p in paths)

    report = container.maintenance.collect_garbage(grace_seconds=0)
    assert report["deleted"] == 2
    assert not any(os.path.exists(p) for p in paths)
    with container.db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 0


def test_gc_respects_grace_period(client, container):
    client.post("/v1/buckets", json={"name": "plain"})
    put(client, "plain", "a.bin", os.urandom(70 * 1024))
    client.delete("/v1/buckets/plain/objects/a.bin")

    report = container.maintenance.collect_garbage(grace_seconds=3600)
    assert report["deleted"] == 0
    assert report["skipped"] == 0
    with container.db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 2


def test_gc_keeps_live_chunks(client, bucket, container):
    put(client, bucket, "live.bin", os.urandom(70 * 1024))
    report = container.maintenance.collect_garbage(grace_seconds=0)
    assert report["deleted"] == 0
    with container.db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 2


def test_scrub_detects_corruption(client, bucket, container):
    payload = os.urandom(70 * 1024)
    put(client, bucket, "c.bin", payload)
    with container.db.read() as conn:
        rows = conn.execute("SELECT hash, size FROM chunks ORDER BY size DESC").fetchall()
    biggest = rows[0]
    chunk_size = biggest["size"]

    # 模拟磁盘静默损坏：直接改写分片文件内容
    with open(container.cas.path_for(biggest["hash"]), "r+b") as fh:
        fh.write(b"\x00" * 128)

    report = container.maintenance.scrub(limit=10)
    assert report["chunks_bad"] == 1
    assert report["status"] == "DEGRADED"

    res = get(client, bucket, "c.bin")
    assert res.status_code == 500
    assert res.json()["error"]["code"] == "ChunkCorrupted"

    # 修复后再次巡检应清除 corrupt 标记
    with open(container.cas.path_for(biggest["hash"]), "wb") as fh:
        fh.write(payload[:chunk_size])
    again = container.maintenance.scrub(limit=10)
    assert again["status"] == "OK"
    assert get(client, bucket, "c.bin").content == payload


def test_scrub_missing_file(client, bucket, container):
    put(client, bucket, "m.bin", os.urandom(70 * 1024))
    with container.db.read() as conn:
        digest = conn.execute("SELECT hash FROM chunks LIMIT 1").fetchone()["hash"]
    container.cas.delete(digest)
    report = container.maintenance.scrub(limit=10)
    assert report["chunks_bad"] == 1
    assert any(d["issue"] == "missing_file" for d in report["details"])


def test_reconcile_fixes_refcount_drift(client, bucket, container):
    put(client, bucket, "r.bin", os.urandom(70 * 1024))
    with container.db.tx() as conn:
        conn.execute("UPDATE chunks SET refcount = 99")

    report = container.maintenance.reconcile()
    assert report["summary"]["refcount_fixed"] == 2
    with container.db.read() as conn:
        assert {r["refcount"] for r in conn.execute("SELECT refcount FROM chunks")} == {1}


def test_reconcile_fixes_usage_drift(client, bucket, container):
    put(client, bucket, "u.bin", b"x" * 1000)
    with container.db.tx() as conn:
        conn.execute("UPDATE buckets SET used_bytes = 999999, object_count = 42")

    report = container.maintenance.reconcile()
    assert report["summary"]["usage_buckets_fixed"] == 1
    bucket_info = client.get("/v1/buckets/demo").json()
    assert bucket_info["used_bytes"] == 1000
    assert bucket_info["object_count"] == 1


def test_orphan_scan(client, bucket, container):
    put(client, bucket, "o.bin", b"payload")
    orphan_dir = os.path.join(container.cas.root, "ff", "ff")
    os.makedirs(orphan_dir, exist_ok=True)
    orphan_path = os.path.join(orphan_dir, "deadbeef")
    with open(orphan_path, "wb") as fh:
        fh.write(b"orphan")
    report = container.maintenance.orphan_files()
    assert report["orphans"] == 1
    assert orphan_path in report["samples"]


def test_sweep_temp_files(container):
    tmp_dir = os.path.join(container.cas.root, "aa", "bb")
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_file = os.path.join(tmp_dir, ".tmp-crashed")
    with open(tmp_file, "wb") as fh:
        fh.write(b"partial")
    old = 1
    os.utime(tmp_file, (old, old))
    assert container.maintenance.sweep_temp_files()["removed"] == 1
