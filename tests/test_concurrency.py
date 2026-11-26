from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

from stratum.errors import PreconditionFailed, QuotaExceeded
from stratum.storage.metadata import ObjectVersion  # noqa: F401  (类型提示用)


def test_parallel_identical_puts_dedup_and_refcount(client, bucket, container):
    data = os.urandom(150 * 1024)

    def worker(idx: int):
        return container.objects.put_object("demo", f"f{idx}.bin", iter([data]),
                                            actor=f"w{idx}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(worker, range(8)))

    assert len({r["etag"] for r in results}) == 1
    with container.db.read() as conn:
        stats = container.meta.chunk_stats(conn)
        refs = [r["refcount"] for r in conn.execute("SELECT refcount FROM chunks")]
    assert stats["chunks_total"] == 3
    assert sorted(refs) == [8, 8, 8]

    # 引用计数与 chunk_refs 完全一致，说明并发提交没有丢更新
    report = container.maintenance.reconcile()
    assert report["summary"]["refcount_fixed"] == 0
    assert report["summary"]["usage_buckets_fixed"] == 0


def test_parallel_conditional_put_only_one_wins(client, bucket, container):
    successes = []
    failures = []

    def worker(idx: int):
        try:
            result = container.objects.put_object("demo", "race.txt", iter([b"payload"]),
                                                  if_none_match="*", actor=f"w{idx}")
            successes.append(result)
        except PreconditionFailed:
            failures.append(idx)

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(worker, range(6)))

    assert len(successes) == 1
    assert len(failures) == 5
    with container.db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM object_versions").fetchone()[0] == 1


def test_parallel_puts_never_exceed_quota(client, container):
    client.post("/v1/buckets", json={"name": "tight", "quota_bytes": 100})
    ok, rejected = [], []

    def worker(idx: int):
        try:
            container.objects.put_object("tight", f"k{idx}.bin", iter([b"x" * 30]))
            ok.append(idx)
        except QuotaExceeded:
            rejected.append(idx)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(8)))

    assert len(ok) == 3
    assert len(rejected) == 5
    info = client.get("/v1/buckets/tight").json()
    assert info["used_bytes"] == 90
    assert info["object_count"] == 3


def test_concurrent_delete_same_version_is_safe(client, bucket, container):
    from stratum.errors import NotFound

    version = container.objects.put_object("demo", "x.bin", iter([b"data"]))
    outcomes = []

    def worker(_idx: int):
        try:
            container.objects.delete_object("demo", "x.bin", version["version_id"])
            outcomes.append("deleted")
        except NotFound:
            outcomes.append("gone")

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(worker, range(4)))

    assert outcomes.count("deleted") == 1
    assert outcomes.count("gone") == 3
    with container.db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM object_versions").fetchone()[0] == 0
