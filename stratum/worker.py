"""后台 worker 进程：与应用进程共享数据目录与元数据库，独立承担重活。

为什么不放在应用进程里跑一个 asyncio 任务：
* 巡检（scrub）会持续读盘，放在应用进程会与请求抢 page cache 与 GIL；
* 应用进程重启/滚动发布时不应该打断正在进行的 GC 与投递；
* 两者都通过 SQLite WAL + 租约（next_at）协作，不需要额外中间件。

用法：
    python -m stratum.worker                # 常驻循环
    python -m stratum.worker --once         # 跑一轮后退出（CI / 定时任务）
    python -m stratum.worker --jobs gc,webhook --interval 2
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time

from .config import load_settings
from .logging_setup import setup_logging
from .service.container import Container

logger = logging.getLogger("stratum.worker")

DEFAULT_JOBS = ("lifecycle", "gc", "webhook")


def run_cycle(container: Container, jobs: list[str], scrub_every: int = 0,
              cycles: int = 0) -> dict:
    report: dict = {}
    if "lifecycle" in jobs:
        report["lifecycle"] = container.lifecycle.run(batch_limit=100)
    if "gc" in jobs:
        report["gc"] = container.maintenance.collect_garbage(limit=300)
    if "webhook" in jobs:
        report["webhook"] = container.outbox.dispatch_due(limit=50)
    if "scrub" in jobs or (scrub_every and cycles % scrub_every == 0):
        report["scrub"] = container.maintenance.scrub(limit=500)
    if "reconcile" in jobs:
        report["reconcile"] = container.maintenance.reconcile(fix=True)
    if "sweep" in jobs:
        report["sweep"] = container.maintenance.sweep_temp_files()
    return report


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Stratum 后台维护 worker")
    parser.add_argument("--once", action="store_true", help="只跑一轮")
    parser.add_argument("--jobs", default=",".join(DEFAULT_JOBS),
                        help="逗号分隔：lifecycle,gc,webhook,scrub,reconcile,sweep")
    parser.add_argument("--interval", type=float, default=None, help="轮询间隔（秒）")
    parser.add_argument("--scrub-every", type=int, default=12, help="每 N 轮做一次巡检，0 关闭")
    args = parser.parse_args(argv)

    setup_logging()
    settings = load_settings()
    container = Container(settings)
    jobs = [j.strip() for j in args.jobs.split(",") if j.strip()]
    interval = args.interval if args.interval is not None else max(1, settings.worker_interval)

    stopping = {"flag": False}

    def _stop(signum, _frame):
        logger.info("收到信号 %s，准备退出", signum)
        stopping["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    cycles = 0
    while not stopping["flag"]:
        started = time.perf_counter()
        try:
            report = run_cycle(container, jobs, args.scrub_every, cycles)
            logger.info("worker cycle done in %.0fms: %s", (time.perf_counter() - started) * 1000,
                        {k: _brief(v) for k, v in report.items()})
        except Exception:
            logger.exception("worker cycle failed")
        cycles += 1
        if args.once:
            break
        for _ in range(int(max(1, interval) * 10)):
            if stopping["flag"]:
                break
            time.sleep(0.1)


def _brief(value: dict) -> dict:
    keep = ("deleted", "delivered", "expired", "versions_pruned", "uploads_aborted",
            "chunks_checked", "chunks_bad", "retry_scheduled", "dead")
    return {k: value[k] for k in keep if k in value}


if __name__ == "__main__":
    run(sys.argv[1:])
