"""后台维护：分片 GC、完整性巡检、计数器对账。

三者的关系
----------
* **GC**：按引用计数回收物理分片，解决「删除版本」的延迟物理清理；
* **巡检（scrub）**：按内容哈希复算每个分片，发现磁盘静默损坏并隔离（标记 corrupt，
  读路径会拒绝返回坏数据而不是返回错误内容——宁可 500 也不返回脏数据）；
* **对账（reconcile）**：把引用计数/用量这类「缓存型计数器」与权威数据对齐，
  是所有后台系统的最后一道兜底。
"""

from __future__ import annotations

import os
from typing import Optional

from ..metrics import METRICS
from ..storage.cas import ChunkStore
from ..storage.metadata import MetadataStore
from ..util import dt_to_ts, utcnow


class MaintenanceService:
    def __init__(self, db, meta: MetadataStore, cas: ChunkStore, settings):
        self.db = db
        self.meta = meta
        self.cas = cas
        self.settings = settings

    # -------------------------------------------------------------------- GC
    def collect_garbage(self, *, grace_seconds: Optional[int] = None, limit: int = 500,
                        dry_run: bool = False) -> dict:
        grace = self.settings.gc_grace_seconds if grace_seconds is None else grace_seconds
        cutoff = dt_to_ts(_from_now(-abs(grace)))
        with self.db.read() as conn:
            candidates = self.meta.gc_candidates(conn, cutoff, limit)
        deleted, freed, skipped = 0, 0, 0
        for row in candidates:
            digest = row["hash"]
            if dry_run:
                deleted += 1
                freed += row["size"]
                continue
            with self.db.tx() as conn:
                fresh = self.meta.get_chunk(conn, digest)
                if not fresh or fresh["refcount"] > 0:
                    skipped += 1
                    continue
                # 二次复核：引用计数是缓存，chunk_refs 才是权威
                live = conn.execute(
                    "SELECT COUNT(*) FROM chunk_refs WHERE hash=?", (digest,)
                ).fetchone()[0]
                if live:
                    self.meta.fix_refcount(conn, digest, live)
                    skipped += 1
                    continue
                self.meta.drop_chunk_row(conn, digest)
            # 先删元数据再删文件：崩溃只会留下孤儿文件（可被扫描发现），
            # 反序则会留下「有元数据无文件」的坏对象。
            self.cas.delete(digest)
            deleted += 1
            freed += row["size"]
        METRICS.inc("stratum_gc_chunks_deleted_total", deleted)
        METRICS.inc("stratum_gc_bytes_freed_total", freed)
        return {"candidates": len(candidates), "deleted": deleted, "skipped": skipped,
                "freed_bytes": freed, "grace_seconds": grace, "dry_run": dry_run}

    # ----------------------------------------------------------------- scrub
    def scrub(self, *, limit: int = 2000, repair_refcount: bool = True,
              sample: Optional[int] = None) -> dict:
        started = utcnow()
        checked = bad = bytes_read = 0
        details: list[dict] = []
        offset = 0
        batch = 200
        while checked < limit:
            with self.db.read() as conn:
                rows = self.meta.all_chunks(conn, batch, offset)
            if not rows:
                break
            for row in rows:
                if checked >= limit:
                    break
                digest = row["hash"]
                checked += 1
                path = self.cas.path_for(digest)
                try:
                    with open(path, "rb") as fh:
                        import hashlib

                        h = hashlib.sha256()
                        size = 0
                        while True:
                            buf = fh.read(1 << 20)
                            if not buf:
                                break
                            size += len(buf)
                            h.update(buf)
                            bytes_read += len(buf)
                except OSError:
                    bad += 1
                    details.append({"hash": digest, "issue": "missing_file"})
                    with self.db.tx() as conn:
                        self.meta.mark_corrupt(conn, digest, True)
                    continue
                if h.hexdigest() != digest or size != row["size"]:
                    bad += 1
                    details.append({"hash": digest, "issue": "hash_mismatch",
                                    "expected_size": row["size"], "actual_size": size})
                    with self.db.tx() as conn:
                        self.meta.mark_corrupt(conn, digest, True)
                elif row["corrupt"]:
                    with self.db.tx() as conn:
                        self.meta.mark_corrupt(conn, digest, False)
            offset += len(rows)
        METRICS.inc("stratum_scrub_chunks_total", checked)
        METRICS.inc("stratum_scrub_bad_total", bad)
        with self.db.tx() as conn:
            run_id = self.meta.start_run(conn, "scrub", started)
            finished = utcnow()
            self.meta.finish_run(conn, run_id, "OK" if bad == 0 else "DEGRADED",
                                 {"checked": checked, "bad": bad}, finished)
            self.meta.add_scrub_report(conn, run_id, started, finished, checked, bad,
                                       bytes_read, details)
            if bad:
                self.meta.add_audit(conn, actor="scrub", action="integrity.failed", status=500,
                                    detail={"checked": checked, "bad": bad,
                                            "samples": details[:10]})
        return {"chunks_checked": checked, "chunks_bad": bad, "bytes_read": bytes_read,
                "details": details[:50], "status": "OK" if bad == 0 else "DEGRADED"}

    # ------------------------------------------------------------- reconcile
    def reconcile(self, *, fix: bool = True) -> dict:
        report = {"refcount_drift": [], "usage_drift": [], "missing_files": []}
        with self.db.read() as conn:
            drift = self.meta.refcount_drift(conn)
        for item in drift:
            report["refcount_drift"].append(item)
            if fix:
                with self.db.tx() as conn:
                    self.meta.fix_refcount(conn, item["hash"], item["real"])
        with self.db.read() as conn:
            rows = self.meta.all_chunks(conn, 5000, 0)
            for row in rows:
                if not self.cas.exists(row["hash"]):
                    report["missing_files"].append(row["hash"])
                    if fix:
                        with self.db.tx() as conn:
                            self.meta.mark_corrupt(conn, row["hash"], True)
        if fix:
            with self.db.tx() as conn:
                report["usage_drift"] = self.meta.recompute_usage(conn)
        else:
            with self.db.read() as conn:
                before = {b.name: (b.used_bytes, b.object_count)
                          for b in self.meta.list_buckets(conn)}
            report["usage_drift"] = [{"bucket": k, "before": v} for k, v in before.items()]
        summary = {
            "refcount_fixed": len(report["refcount_drift"]),
            "usage_buckets_fixed": len(report["usage_drift"]),
            "missing_files": len(report["missing_files"]),
            "applied": fix,
        }
        with self.db.tx() as conn:
            self.meta.add_audit(conn, actor="reconcile", action="maintenance.reconcile",
                                status=200, detail=summary)
        report["summary"] = summary
        return report

    # ------------------------------------------------------------ 杂项清理
    def sweep_temp_files(self) -> dict:
        removed = self.cas.sweep_tmp(3600)
        return {"removed": removed}

    def orphan_files(self, *, limit: int = 1000) -> dict:
        """CAS 中有文件但元数据无记录（GC 崩溃残留）。只报告，不自动删除。"""
        known = set()
        with self.db.read() as conn:
            offset = 0
            while True:
                rows = self.meta.all_chunks(conn, 5000, offset)
                if not rows:
                    break
                known.update(r["hash"] for r in rows)
                offset += len(rows)
        orphans = []
        for dirpath, _dirs, files in os.walk(self.cas.root):
            for name in files:
                if name.startswith(".tmp-") or name in known:
                    continue
                orphans.append(os.path.join(dirpath, name))
                if len(orphans) >= limit:
                    return {"orphans": len(orphans), "samples": orphans[:20], "truncated": True}
        return {"orphans": len(orphans), "samples": orphans[:20], "truncated": False}


def _from_now(delta_seconds: float):
    from datetime import datetime, timedelta, timezone

    return datetime.now(timezone.utc) + timedelta(seconds=delta_seconds)
