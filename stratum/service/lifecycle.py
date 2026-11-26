"""生命周期治理：过期、非当前版本裁剪、未完成上传清理、孤立删除标记回收。

设计取舍：不把清理逻辑写在 SQL 里做批量 DELETE。原因有两个：
1. 批量 DELETE 会绕过引用计数与审计，一旦有 bug 无法回溯是哪个规则删的；
2. 每删一个对象都要在同一事务里更新用量/引用/审计/事件，逐条走服务层更安全。
代价是吞吐受限（每轮 N 条），因此用 ``batch_limit`` 控制单轮规模，由 worker 反复调度。
"""

from __future__ import annotations

from typing import Optional

from ..storage.metadata import MetadataStore
from ..util import days_ago


class LifecycleService:
    def __init__(self, db, meta: MetadataStore, buckets, objects, multipart):
        self.db = db
        self.meta = meta
        self.buckets = buckets
        self.objects = objects
        self.multipart = multipart

    def run(self, *, dry_run: bool = False, batch_limit: int = 200,
            now: Optional[str] = None) -> dict:
        buckets = self.buckets.list()
        report = {"buckets_scanned": 0, "expired": 0, "versions_pruned": 0,
                  "markers_pruned": 0, "uploads_aborted": 0, "dry_run": dry_run,
                  "details": []}

        for item in buckets:
            policy = item.get("lifecycle") or {}
            if not policy:
                continue
            report["buckets_scanned"] += 1
            name = item["name"]
            bucket_report = {"bucket": name, "expired": [], "pruned": [],
                             "markers": [], "aborted": []}

            if policy.get("expire_after_days") is not None:
                cutoff = days_ago(float(policy["expire_after_days"]))
                with self.db.read() as conn:
                    candidates = self.meta.expired_candidates(conn, name, cutoff, batch_limit)
                for version in candidates:
                    if not dry_run:
                        try:
                            self.objects.delete_object(name, version.key, actor="lifecycle")
                        except Exception as exc:  # 单条失败不阻断整轮
                            bucket_report.setdefault("errors", []).append(
                                {"key": version.key, "error": str(exc)})
                            continue
                    bucket_report["expired"].append(version.key)
                    report["expired"] += 1

            if policy.get("noncurrent_expire_after_days") is not None:
                cutoff = days_ago(float(policy["noncurrent_expire_after_days"]))
                with self.db.read() as conn:
                    candidates = self.meta.noncurrent_candidates(conn, name, cutoff, batch_limit)
                for version in candidates:
                    if not dry_run:
                        try:
                            self.objects.delete_object(name, version.key,
                                                       version_id=version.version_id,
                                                       actor="lifecycle")
                        except Exception as exc:
                            bucket_report.setdefault("errors", []).append(
                                {"key": version.key, "error": str(exc)})
                            continue
                    bucket_report["pruned"].append(f"{version.key}@{version.version_id}")
                    report["versions_pruned"] += 1

            if policy.get("delete_marker_expire_after_days") is not None:
                cutoff = days_ago(float(policy["delete_marker_expire_after_days"]))
                with self.db.read() as conn:
                    candidates = self.meta.marker_candidates(conn, name, cutoff, batch_limit)
                for marker in candidates:
                    if not dry_run:
                        try:
                            self.objects.delete_object(name, marker.key,
                                                       version_id=marker.version_id,
                                                       actor="lifecycle")
                        except Exception:
                            continue
                    bucket_report["markers"].append(f"{marker.key}@{marker.version_id}")
                    report["markers_pruned"] += 1

            if policy.get("abort_incomplete_days") is not None:
                cutoff = days_ago(float(policy["abort_incomplete_days"]))
                with self.db.read() as conn:
                    stale = self.meta.stale_uploads(conn, cutoff, batch_limit)
                for upload in stale:
                    if upload.bucket != name:
                        continue
                    if not dry_run:
                        try:
                            self.multipart.abort(upload.upload_id, actor="lifecycle")
                        except Exception:
                            continue
                    bucket_report["aborted"].append(upload.upload_id)
                    report["uploads_aborted"] += 1

            report["details"].append(bucket_report)

        if not dry_run:
            with self.db.tx() as conn:
                self.meta.add_audit(conn, actor="lifecycle", action="lifecycle.run",
                                    status=200, detail={k: v for k, v in report.items()
                                                        if k != "details"})
        return report
