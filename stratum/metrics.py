"""极简指标注册表：只依赖标准库，直接输出 Prometheus 文本格式。

刻意不引入 prometheus_client：本项目要展示「可观测性怎么落」，而不是依赖堆叠。
计数器在进程内累加；容量类指标（对象数、分片数）由 /metrics 端点实时查库，
避免维护两份真相（内存 gauge 与数据库不一致是排障时最烦的坑）。
"""

from __future__ import annotations

import threading
from collections import defaultdict


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._help: dict[str, str] = {}

    def describe(self, name: str, help_text: str) -> None:
        self._help[name] = help_text

    def inc(self, name: str, value: float = 1.0, **labels) -> None:
        key = (name, tuple(sorted((str(k), str(v)) for k, v in labels.items())))
        with self._lock:
            self._counters[key] += value

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()

    def render(self) -> str:
        with self._lock:
            counters = dict(self._counters)
        lines: list[str] = []
        emitted: set[str] = set()
        for (name, labels), value in sorted(counters.items()):
            if name in self._help and name not in emitted:
                lines.append(f"# HELP {name} {self._help[name]}")
                emitted.add(name)
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name}{_fmt_labels(labels)} {value:g}")
        return "\n".join(lines) + ("\n" if lines else "")


def _fmt_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    body = ",".join(f'{k}="{_escape(v)}"' for k, v in labels)
    return "{" + body + "}"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


METRICS = Metrics()
METRICS.describe("stratum_objects_written_total", "累计写入对象数")
METRICS.describe("stratum_objects_deleted_total", "累计物理删除对象数")
METRICS.describe("stratum_bytes_written_total", "累计写入逻辑字节")
METRICS.describe("stratum_chunks_written_total", "累计写入 CAS 分片数")
METRICS.describe("stratum_chunks_deduped_total", "命中去重而未落盘的分片数")
METRICS.describe("stratum_gc_chunks_deleted_total", "GC 回收分片数")
METRICS.describe("stratum_gc_bytes_freed_total", "GC 释放磁盘字节")
METRICS.describe("stratum_scrub_chunks_total", "巡检分片数")
METRICS.describe("stratum_scrub_bad_total", "巡检发现坏分片数")
METRICS.describe("stratum_multipart_created_total", "创建的分片上传任务数")
METRICS.describe("stratum_multipart_parts_total", "上传的部件数")
METRICS.describe("stratum_multipart_completed_total", "完成的分片上传数")
METRICS.describe("stratum_multipart_aborted_total", "中止的分片上传数")
METRICS.describe("stratum_webhook_delivered_total", "Webhook 投递成功数")
METRICS.describe("stratum_webhook_failed_total", "Webhook 投递失败（进入重试）数")
METRICS.describe("stratum_webhook_dead_total", "Webhook 进入死信数")
METRICS.describe("stratum_bucket_operations_total", "bucket 操作计数")
METRICS.describe("stratum_http_requests_total", "HTTP 请求数")
METRICS.describe("stratum_http_bytes_in_total", "HTTP 入口字节")
METRICS.describe("stratum_http_bytes_out_total", "HTTP 出口字节")
