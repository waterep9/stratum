"""演示数据装载：给控制台准备一个像样的场景。

用法：
    python scripts/seed_demo.py --base-url http://127.0.0.1:8080 --token change-me
"""

from __future__ import annotations

import argparse
import os
import random

import httpx

SAMPLES = {
    "docs/architecture.md": "# Stratum\n\n内容寻址对象存储。\n",
    "docs/runbook.md": "# 值班手册\n\n1. 看 /metrics\n2. 跑一次 reconcile\n",
    "logs/2026/11/26/app.log": "\n".join(f"2026-11-26T08:{m:02d}:00Z INFO request ok" for m in range(20)),
    "images/logo.svg": "<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16'/>",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--token", default=os.getenv("STRATUM_ADMIN_TOKEN", "stratum-admin-token"))
    args = parser.parse_args()

    headers = {"Authorization": f"Bearer {args.token}"}
    with httpx.Client(base_url=args.base_url, headers=headers, timeout=30) as client:
        health = client.get("/healthz")
        health.raise_for_status()

        for name, payload in {
            "媒体资源桶": {"name": "media-demo", "versioning": True,
                          "quota_bytes": 200 * 1024 * 1024,
                          "lifecycle": {"noncurrent_expire_after_days": 30,
                                        "abort_incomplete_days": 1}},
            "日志桶": {"name": "logs-archive", "versioning": False,
                       "lifecycle": {"expire_after_days": 90}},
        }.items():
            res = client.post("/v1/buckets", json=payload)
            print(f"[bucket] {payload['name']}: {res.status_code}")

        for key, text in SAMPLES.items():
            res = client.put(f"/v1/buckets/media-demo/objects/{key}",
                             content=text.encode("utf-8"),
                             headers={"Content-Type": "text/plain; charset=utf-8"})
            print(f"[put] {key}: {res.status_code} {res.json().get('size', '')}")

        # 制造一次版本覆盖
        client.put("/v1/buckets/media-demo/objects/docs/architecture.md",
                   content=b"# Stratum v2\n\nrevision two\n",
                   headers={"Content-Type": "text/markdown"})

        # 一起去重：同内容写两遍，第二遍应全部命中分片
        blob = bytes(random.getrandbits(8) for _ in range(120 * 1024))
        first = client.put("/v1/buckets/media-demo/objects/bulk/sample-a.bin", content=blob).json()
        second = client.put("/v1/buckets/media-demo/objects/bulk/sample-b.bin", content=blob).json()
        print(f"[dedup] 首写新分片={first['chunks_written']} 二写去重={second['chunks_deduped']}")

        # 分片上传
        big = os.urandom(5 * 1024 * 1024)
        upload = client.post("/v1/buckets/media-demo/uploads",
                             params={"key": "bulk/video-sample.bin"}).json()
        part = 2 * 1024 * 1024
        for idx, start in enumerate(range(0, len(big), part), start=1):
            piece = big[start:start + part]
            client.put(f"/v1/uploads/{upload['upload_id']}/parts/{idx}", content=piece)
        done = client.post(f"/v1/uploads/{upload['upload_id']}/complete").json()
        print(f"[multipart] {done['size']} 字节, etag={done['etag']}")

        # 签发一个只读密钥，供控制台演示预签名
        key = client.post("/admin/keys", json={
            "label": "demo-reader",
            "grants": [{"bucket": "media-demo", "perm": "r"}],
        }).json()
        print(f"[key] {key['access_key']} / {key['secret'][:10]}…")

        overview = client.get("/admin/overview").json()
        print("[overview]", {k: overview[k] for k in
                             ("objects", "versions", "logical_bytes", "chunk_bytes_total",
                              "chunks_total")})


if __name__ == "__main__":
    main()
