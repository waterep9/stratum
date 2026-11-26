"""压测：并发 PUT / GET，输出吞吐与分位数延迟。

用法：
    python scripts/bench.py --base-url http://127.0.0.1:8080 --token change-me \
        --bucket bench --size 65536 --concurrency 8 --seconds 6

说明：脚本是单进程同步模型（线程 + httpx.Client）。python 的 GIL 会让压测端在更高并发下
先饱和，因此超过 16 并发时结果更多反映客户端瓶颈，而不是服务端上限。
"""

from __future__ import annotations

import argparse
import os
import statistics
import threading
import time

import httpx


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def summarize(latencies: list[float], errors: int, elapsed: float) -> dict:
    return {
        "requests": len(latencies),
        "rps": round(len(latencies) / elapsed, 1) if elapsed else 0.0,
        "errors": errors,
        "p50_ms": round(percentile(latencies, 50), 1),
        "p95_ms": round(percentile(latencies, 95), 1),
        "p99_ms": round(percentile(latencies, 99), 1),
        "mean_ms": round(statistics.fmean(latencies), 1) if latencies else 0.0,
    }


def run_phase(kind: str, client_kwargs: dict, concurrency: int, seconds: float,
              path_fn, body_fn=None) -> dict:
    latencies: list[float] = []
    error_count = 0
    lock = threading.Lock()
    stop = threading.Event()
    barrier = threading.Barrier(concurrency)

    def worker(idx: int) -> None:
        local: list[float] = []
        local_errors = 0
        counter = 0
        with httpx.Client(**client_kwargs) as client:
            barrier.wait()
            while not stop.is_set():
                path = path_fn(idx, counter)
                started = time.perf_counter()
                try:
                    if kind == "PUT":
                        res = client.put(path, content=body_fn(idx, counter))
                    else:
                        res = client.get(path)
                    if res.status_code >= 400:
                        local_errors += 1
                except Exception:
                    local_errors += 1
                local.append((time.perf_counter() - started) * 1000)
                counter += 1
        with lock:
            latencies.extend(local)
            nonlocal error_count
            error_count += local_errors

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrency)]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    time.sleep(seconds)
    stop.set()
    for thread in threads:
        thread.join()
    return summarize(latencies, error_count, time.perf_counter() - started)


def list_keys(client: httpx.Client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        params = {"prefix": prefix, "limit": 1000}
        if token:
            params["continuation"] = token
        page = client.get(f"/v1/buckets/{bucket}/objects", params=params).json()
        keys.extend(item["key"] for item in page["items"])
        if not page["is_truncated"]:
            return keys
        token = page["next_token"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--token", default=os.getenv("STRATUM_ADMIN_TOKEN", "stratum-admin-token"))
    parser.add_argument("--bucket", default="bench")
    parser.add_argument("--size", type=int, default=64 * 1024)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--seconds", type=float, default=6)
    args = parser.parse_args()

    headers = {"Authorization": f"Bearer {args.token}"}
    client_kwargs = {"base_url": args.base_url, "headers": headers, "timeout": 30}

    with httpx.Client(**client_kwargs) as client:
        if client.get(f"/v1/buckets/{args.bucket}").status_code == 404:
            client.post("/v1/buckets", json={"name": args.bucket, "versioning": False})
        for key in list_keys(client, args.bucket, "bench/"):
            client.delete(f"/v1/buckets/{args.bucket}/objects/{key}")

    print(f"bucket={args.bucket} size={args.size}B concurrency={args.concurrency}")
    bodies = [os.urandom(args.size) for _ in range(32)]

    put_stats = run_phase(
        "PUT", client_kwargs, args.concurrency, args.seconds,
        lambda idx, counter: f"/v1/buckets/{args.bucket}/objects/bench/{idx}-{counter}.bin",
        lambda idx, counter: bodies[counter % len(bodies)],
    )
    print("PUT ", put_stats)

    with httpx.Client(**client_kwargs) as client:
        keys = list_keys(client, args.bucket, "bench/")
    print(f"写入完成，共 {len(keys)} 个对象")
    if not keys:
        return

    get_stats = run_phase(
        "GET", client_kwargs, args.concurrency, args.seconds,
        lambda idx, counter: f"/v1/buckets/{args.bucket}/objects/{keys[(idx * 977 + counter) % len(keys)]}",
    )
    print("GET ", get_stats)

    with httpx.Client(**client_kwargs) as client:
        overview = client.get("/admin/overview").json()
    print("overview:", {k: overview[k] for k in ("objects", "chunks_total", "chunk_bytes_total")})


if __name__ == "__main__":
    main()
