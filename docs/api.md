# Stratum API 文档

Base URL：`http://127.0.0.1:8080`
在线交互式文档：`/docs`（Swagger UI），机器可读：`/openapi.json`

---

## 1. 约定

### 1.1 认证（三选一）

| 方式 | 请求头 / 参数 | 适用 |
| --- | --- | --- |
| 管理令牌 | `Authorization: Bearer <STRATUM_ADMIN_TOKEN>` | 建桶、改策略、签发密钥、维护任务、全局审计 |
| 访问密钥 | `X-Stratum-Key: AK...` + `X-Stratum-Secret: SK...` | 业务进程读写，按 grants 限权 |
| 预签名 | 查询串 `X-Stratum-Key` / `X-Stratum-Expires` / `X-Stratum-Signature` | 浏览器直传/直下，无需暴露密钥 |

预签名只在**没有** `Authorization` 与 `X-Stratum-Key` 时才生效，避免两套凭证互相掩盖。

### 1.2 通用响应头

| 头 | 含义 |
| --- | --- |
| `X-Request-Id` | 服务端生成的请求 ID（也回显客户端传入值） |
| `X-Stratum-Elapsed-Ms` | 服务端处理耗时 |
| `ETag` | 对象内容 SHA-256（带引号） |
| `X-Stratum-Version-Id` | 本次写入/读取的版本号 |

### 1.3 错误体

```json
{
  "error": {
    "code": "PreconditionFailed",
    "message": "ETag 不匹配",
    "detail": {"expected": "ab12…", "provided": "cd34…"},
    "request_id": "9f2c1d7a4b8e4c33"
  }
}
```

| HTTP | code |
| --- | --- |
| 400 | `InvalidRequest` |
| 401 | `Unauthorized` |
| 403 | `AccessDenied` |
| 404 | `NoSuchBucket` / `NoSuchKey` / `NoSuchUpload` |
| 409 | `Conflict` |
| 412 | `PreconditionFailed` |
| 413 | `EntityTooLarge` |
| 416 | `InvalidRange` |
| 500 | `ChunkCorrupted` / `InternalError` |
| 507 | `QuotaExceeded` |

### 1.4 分页

列表接口使用**键集分页**（不是 offset）：

```json
{"items": [...], "is_truncated": true, "next_token": "eyJrZXkiOiJsb2dzL2EifQ", "count": 100}
```

把 `next_token` 原样回传为 `?continuation=`，直到 `is_truncated=false`。
键集分页的好处：翻页期间有写入也不会重复/漏项，且深翻页不退化。

---

## 2. 桶

### `GET /v1/buckets`

列出当前主体有权读的桶。

```bash
curl -s localhost:8080/v1/buckets -H "Authorization: Bearer change-me"
```

```json
{"items": [{"name": "media", "versioning": true, "quota_bytes": 104857600,
            "used_bytes": 5242880, "object_count": 7,
            "lifecycle": {"noncurrent_expire_after_days": 30},
            "webhook_url": null, "labels": {}}], "count": 1}
```

### `POST /v1/buckets`（管理员）

```json
{
  "name": "media",
  "versioning": true,
  "quota_bytes": 104857600,
  "max_object_bytes": 0,
  "default_content_type": "application/octet-stream",
  "lifecycle": {"expire_after_days": 90, "noncurrent_expire_after_days": 30,
                 "abort_incomplete_days": 1, "delete_marker_expire_after_days": 7},
  "webhook_url": "https://example.com/hooks/stratum",
  "labels": {"team": "data"}
}
```

返回值同 `GET /v1/buckets/{b}`。桶名规则：3–63 位小写字母/数字/`.`/`-`，首尾为字母或数字，不允许 `..`。

生命周期字段（单位：天）：

| 字段 | 作用 |
| --- | --- |
| `expire_after_days` | 当前版本到期后过期（版本化桶写删除标记，否则物理删除） |
| `noncurrent_expire_after_days` | 历史版本到期后物理删除 |
| `abort_incomplete_days` | 超过该天数未完成的分片上传被中止并清理部件 |
| `delete_marker_expire_after_days` | 孤立删除标记到期后清理 |

### `GET /v1/buckets/{bucket}` · `PATCH`（管理员） · `DELETE`（管理员）

```bash
# 扩容到 1 GiB 并调整策略
curl -s -X PATCH localhost:8080/v1/buckets/media -H "Authorization: Bearer change-me" \
  -H 'Content-Type: application/json' -d '{"quota_bytes": 1073741824}'

# 非空桶删除必须显式 force
curl -s -X DELETE "localhost:8080/v1/buckets/media?force=true" -H "Authorization: Bearer change-me"
```

---

## 3. 对象

### `PUT /v1/buckets/{bucket}/objects/{key}`

写入对象。请求体即对象内容（流式，服务端按 1 MiB 切片）。

| 请求头 | 说明 |
| --- | --- |
| `Content-Type` | 对象 MIME，缺省用桶默认值 |
| `X-Stratum-Metadata` | JSON 对象，自定义元数据（≤ 8 KB，最多 32 个键） |
| `If-None-Match: *` | 仅当对象不存在时写入，否则 412 |
| `If-Match: "<etag>"` | 仅当当前版本 ETag 匹配时写入，否则 412 |

```bash
curl -s -X PUT localhost:8080/v1/buckets/media/objects/docs/a.txt \
  -H "Authorization: Bearer change-me" \
  -H "Content-Type: text/plain" \
  -H 'X-Stratum-Metadata: {"owner":"data-team"}' \
  --data-binary @a.txt
```

```json
{"bucket": "media", "key": "docs/a.txt", "version_id": "6f1c…",
 "etag": "9f86d081884c7d65…", "size": 12, "chunks": 1,
 "chunks_written": 1, "chunks_deduped": 0, "created_at": "2026-11-26T01:15:42.123456Z"}
```

`chunks_written` / `chunks_deduped` 直接暴露这次写入命中多少去重，便于业务侧观察重复上传比例。

### `POST /v1/buckets/{bucket}/objects/{key}` — 服务端复制

```bash
curl -s -X POST localhost:8080/v1/buckets/media/objects/docs/b.txt \
  -H "Authorization: Bearer change-me" \
  -H "X-Stratum-Copy-Source: media/docs/a.txt"
```

只复制元数据与分片引用，不搬数据；返回 `201` + 新版本信息。

### `GET / HEAD /v1/buckets/{bucket}/objects/{key}`

| 查询/请求头 | 说明 |
| --- | --- |
| `?versionId=` | 读取指定版本（默认当前） |
| `Range: bytes=0-1023` | 区间读取，返回 `206` + `Content-Range` |

```bash
curl -s -D - -o /dev/null -H "Authorization: Bearer change-me" \
  -H "Range: bytes=0-15" localhost:8080/v1/buckets/media/objects/docs/a.txt
# HTTP/1.1 206 Partial Content
# content-range: bytes 0-15/27
```

越界返回 `416` 且带 `Content-Range: bytes */27`（客户端据此知道真实大小）。
`HEAD` 额外返回 `X-Stratum-Chunks`（分片数）与 `Last-Modified`。

### `DELETE /v1/buckets/{bucket}/objects/{key}`

* 不带 `versionId`：版本化桶写删除标记（随后 GET 返回 404）；非版本化桶直接删除并解引用。
* 带 `versionId`：物理删除该版本；若它是当前版本，**最近的历史版本自动成为当前版本**（即"删除 marker 即恢复对象"）。

```bash
curl -s -X DELETE "localhost:8080/v1/buckets/media/objects/docs/a.txt?versionId=6f1c…" \
  -H "Authorization: Bearer change-me"
# {"key":"docs/a.txt","version_id":"6f1c…","deleted":true,"promoted":"ab12…"}
```

### `GET /v1/buckets/{bucket}/objects?prefix=&limit=&continuation=`

列出当前对象（不含删除标记覆盖的键）。

```json
{"bucket": "media", "prefix": "docs/", "count": 2,
 "items": [{"key": "docs/a.txt", "size": 12, "etag": "9f86…",
            "content_type": "text/plain", "created_at": "…", "version_id": "6f1c…",
            "storage_class": "hot"}],
 "is_truncated": false, "next_token": null}
```

### `GET /v1/buckets/{bucket}/versions?prefix=&limit=&continuation=`

列出全部版本（含历史与删除标记），按 `key`、`created_at` 升序。

---

## 4. 分片上传

### `POST /v1/buckets/{bucket}/uploads?key=<对象键>`

```json
{"upload_id": "8d3f…", "bucket": "media", "key": "video/demo.bin",
 "created_at": "…", "part_size_hint": 1048576, "recommended_part_size": 8388608}
```

### `PUT /v1/uploads/{upload_id}/parts/{part_no}`

`part_no` 从 1 开始，可乱序、可重传（覆盖写）。单部件大小不限，但**非末片必须 ≥ 1 MiB**。

```json
{"upload_id": "8d3f…", "part_no": 1, "etag": "3b1f…", "size": 2097152}
```

### `GET /v1/uploads/{upload_id}/parts`

已上传部件清单，用于断点续传（客户端比对本地分片即可只补缺失部分）。

### `POST /v1/uploads/{upload_id}/complete`

校验部件连续性、最小片大小、对象大小上限与桶配额，然后流式组装入 CAS。

```json
{"bucket": "media", "key": "video/demo.bin", "version_id": "8d3f…",
 "etag": "ef07a5f41994cc1cb…-3", "size": 5242880, "upload_id": "8d3f…",
 "chunks": 5, "idempotent_replay": false}
```

**幂等**：重复调用返回同一版本，`idempotent_replay=true`；不会产生第二个对象。

### `DELETE /v1/uploads/{upload_id}`

中止并删除暂存部件。返回 `{"upload_id": "...", "aborted": true}`。

### `GET /v1/buckets/{bucket}/uploads?prefix=`

列出该桶未完成的上传（运维排查"为什么磁盘里有一堆 parts"）。

---

## 5. 预签名

### `POST /v1/presign`

需要**访问密钥**（管理令牌不能签发，因为它没有 secret 可供 HMAC）。

```bash
curl -s -X POST localhost:8080/v1/presign \
  -H "X-Stratum-Key: AK..." -H "X-Stratum-Secret: SK..." \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"media","key":"docs/a.txt","method":"GET","ttl":600}'
```

```json
{"url": "http://127.0.0.1:8080/v1/buckets/media/objects/docs/a.txt?X-Stratum-Key=AK...&X-Stratum-Expires=1793...&X-Stratum-Signature=...",
 "expires_at": 1793000000, "ttl": 600, "method": "GET", "algorithm": "HMAC-SHA256"}
```

签名串：`METHOD\nBUCKET\nKEY\nEXPIRES\nACCESS_KEY`，HMAC 密钥为 `sha256(secret)`。
TTL 上限 7 天，默认 900 s；`method` 支持 `GET` / `PUT` / `HEAD` / `DELETE`。
签名与 bucket/key 绑定，改路径、改过期时间、改方法都会校验失败。

---

## 6. 管理面

### `GET /admin/overview`

```json
{"buckets": 3, "objects": 7, "versions": 8, "delete_markers": 0,
 "logical_bytes": 5489564, "open_uploads": 0, "audit_events": 42,
 "chunks_total": 13, "chunks_live": 13, "chunk_bytes_total": 5497756,
 "chunk_bytes_live": 5497756, "chunks_corrupt": 0,
 "outbox": {"PENDING": 0, "DELIVERED": 128, "DEAD": 0},
 "buckets_detail": [...], "data_dir": "...", "chunk_size": 1048576}
```

### `GET /admin/audit?bucket=&action=&limit=&before_id=`

```json
{"items": [{"id": 42, "ts": "2026-11-26T01:15:42.123456Z", "actor": "admin",
            "action": "object.put", "bucket": "media", "key": "docs/a.txt",
            "version_id": "6f1c…", "status": 200, "request_id": "9f2c…",
            "detail": {"size": 12, "etag": "9f86…", "chunks": 1, "deduped": 0}}],
 "count": 1}
```

常用 `action`：`bucket.create` `bucket.update` `bucket.delete` `object.put` `object.delete`
`object.delete_marker.create` `object.version.delete` `object.copy` `multipart.*`
`access_key.*` `lifecycle.run` `maintenance.*` `integrity.failed` `webhook.replay`。

### 访问密钥

```bash
# 签发（secret 只在本次响应出现）
curl -s -X POST localhost:8080/admin/keys -H "Authorization: Bearer change-me" \
  -H 'Content-Type: application/json' \
  -d '{"label":"ci-bot","grants":[{"bucket":"media","perm":"rw"}]}'

# 列出（不含 secret）
curl -s localhost:8080/admin/keys -H "Authorization: Bearer change-me"

# 吊销
curl -s -X DELETE localhost:8080/admin/keys/AKXXXX -H "Authorization: Bearer change-me"
```

### `POST /admin/maintenance/{job}`

| job | 参数 | 作用 |
| --- | --- | --- |
| `gc` | `limit`、`dry_run`、`grace_seconds` | 回收无引用分片；`grace_seconds=0` 仅用于演示/应急 |
| `lifecycle` | `limit`、`dry_run` | 按桶策略过期、裁剪、中止上传 |
| `scrub` | `limit` | 逐分片复算哈希，坏片标记 + 审计 |
| `reconcile` | `dry_run` | 修正引用计数与桶用量 |
| `sweep` | —— | 清理崩溃遗留的 `.tmp-*` 文件 |
| `orphans` | `limit` | 扫描"有文件无元数据"的孤儿（只报告） |
| `webhook` | `limit` | 立刻投递一批待发送事件 |

```json
{"run_id": 12, "job": "gc",
 "result": {"candidates": 15, "deleted": 15, "skipped": 0,
            "freed_bytes": 983040, "grace_seconds": 0, "dry_run": false}}
```

### `GET /admin/maintenance`

返回最近的维护运行记录、巡检报告与 Outbox 状态，用于"昨天到底跑没跑"这类问题。

### `POST /admin/webhook/replay?limit=50`

把死信事件重新置为待投递（`attempts` 归零）。

### `GET /admin/buckets/{bucket}/stats`

单桶视角：版本数、对象数、逻辑字节、去重后的不同分片数与分片字节。

---

## 7. 其他端点

| 端点 | 用途 |
| --- | --- |
| `GET /healthz` | 存活探针（不查库） |
| `GET /readyz` | 就绪探针（执行 `SELECT 1`） |
| `GET /metrics` | Prometheus 文本指标 |
| `GET /` | 单页控制台 |
| `GET /docs` | Swagger UI |

---

## 8. 端到端示例：预签名直传

```bash
# 1) 管理员建桶
curl -s -X POST localhost:8080/v1/buckets -H "Authorization: Bearer change-me" \
  -H 'Content-Type: application/json' -d '{"name":"uploads","quota_bytes":104857600}'

# 2) 后端签发一个只允许写 uploads 桶的密钥（一次性操作，密钥存配置中心）
read -r AK SK <<< "$(curl -s -X POST localhost:8080/admin/keys \
  -H "Authorization: Bearer change-me" -H 'Content-Type: application/json' \
  -d '{"label":"browser-upload","grants":[{"bucket":"uploads","perm":"w"}]}' \
  | jq -r '.access_key + " " + .secret')"

# 3) 业务后端为某次上传签发 URL（60 秒内有效）
URL=$(curl -s -X POST localhost:8080/v1/presign \
  -H "X-Stratum-Key: $AK" -H "X-Stratum-Secret: $SK" \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"uploads","key":"avatar/u123.png","method":"PUT","ttl":60}' | jq -r .url)

# 4) 浏览器直接 PUT 到该 URL，不经过业务后端
curl -s -X PUT "$URL" -H 'Content-Type: image/png' --data-binary @avatar.png
```
