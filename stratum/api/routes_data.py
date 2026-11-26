"""数据面 API：bucket / 对象 / 分片上传 / 预签名。"""

from __future__ import annotations

import functools
import json
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.concurrency import run_in_threadpool

from ..errors import InvalidRequest
from ..service.auth import PERM_READ, PERM_WRITE
from .deps import get_container, resolve_principal
from .streams import BodyBridge

router = APIRouter()

MAX_METADATA_HEADER = 8 * 1024


def _etag_header(value: str) -> str:
    return f'"{value}"' if value else '"'


def _parse_metadata(raw: Optional[str]) -> dict:
    if not raw:
        return {}
    if len(raw.encode("utf-8")) > MAX_METADATA_HEADER:
        raise InvalidRequest("元数据头过大")
    try:
        data = json.loads(raw)
    except ValueError:
        raise InvalidRequest("X-Stratum-Metadata 必须是 JSON 对象")
    if not isinstance(data, dict):
        raise InvalidRequest("X-Stratum-Metadata 必须是 JSON 对象")
    return {str(k)[:64]: str(v)[:1024] for k, v in list(data.items())[:32]}


# --------------------------------------------------------------------- buckets
@router.get("/v1/buckets")
async def list_buckets(request: Request):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    buckets = container.buckets.list()
    if not principal.is_admin:
        buckets = [b for b in buckets if principal.allows(b["name"], PERM_READ)]
    return {"items": buckets, "count": len(buckets)}


@router.post("/v1/buckets", status_code=201)
async def create_bucket(request: Request):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    if not principal.is_admin:
        from ..errors import Forbidden
        raise Forbidden("只有管理员可以创建 bucket")
    body = await _json_body(request)
    return container.buckets.create(
        body.get("name", ""),
        versioning=bool(body.get("versioning", False)),
        quota_bytes=body.get("quota_bytes", 0) or 0,
        max_object_bytes=body.get("max_object_bytes", 0) or 0,
        default_content_type=body.get("default_content_type"),
        lifecycle=body.get("lifecycle") or {},
        webhook_url=body.get("webhook_url"),
        labels=body.get("labels") or {},
        actor=principal.name,
    )


@router.get("/v1/buckets/{bucket}")
async def get_bucket(request: Request, bucket: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    principal.require(bucket, PERM_READ)
    return container.buckets.get(bucket).to_dict()


@router.patch("/v1/buckets/{bucket}")
async def update_bucket(request: Request, bucket: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    if not principal.is_admin:
        from ..errors import Forbidden
        raise Forbidden("只有管理员可以修改 bucket 策略")
    body = await _json_body(request)
    return container.buckets.update(
        bucket,
        versioning=body.get("versioning"),
        quota_bytes=body.get("quota_bytes"),
        max_object_bytes=body.get("max_object_bytes"),
        default_content_type=body.get("default_content_type"),
        lifecycle=body.get("lifecycle"),
        webhook_url=body.get("webhook_url"),
        labels=body.get("labels"),
        actor=principal.name,
    )


@router.delete("/v1/buckets/{bucket}")
async def delete_bucket(request: Request, bucket: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    if not principal.is_admin:
        from ..errors import Forbidden
        raise Forbidden("只有管理员可以删除 bucket")
    force = request.query_params.get("force", "").lower() in ("1", "true", "yes")
    return container.buckets.delete(bucket, force=force, actor=principal.name)


# --------------------------------------------------------------------- objects
@router.get("/v1/buckets/{bucket}/objects")
async def list_objects(request: Request, bucket: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    principal.require(bucket, PERM_READ)
    params = request.query_params
    return container.objects.list_objects(
        bucket,
        prefix=params.get("prefix", ""),
        limit=_int_param(params.get("limit"), 100),
        continuation=params.get("continuation"),
    )


@router.get("/v1/buckets/{bucket}/versions")
async def list_versions(request: Request, bucket: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    principal.require(bucket, PERM_READ)
    params = request.query_params
    return container.objects.list_versions(
        bucket, prefix=params.get("prefix", ""),
        limit=_int_param(params.get("limit"), 100),
        continuation=params.get("continuation"),
    )


@router.put("/v1/buckets/{bucket}/objects/{key:path}")
async def put_object(request: Request, bucket: str, key: str):
    container = get_container(request)
    principal = resolve_principal(request, bucket, PERM_WRITE, presign_method="PUT")
    limit = container.settings.max_object_bytes
    bridge = await BodyBridge(request, max_bytes=limit).start()
    try:
        result = await run_in_threadpool(
            functools.partial(
                container.objects.put_object, bucket, key, bridge,
                content_type=request.headers.get("content-type"),
                metadata=_parse_metadata(request.headers.get("x-stratum-metadata")),
                if_match=request.headers.get("if-match"),
                if_none_match=request.headers.get("if-none-match"),
                actor=principal.name,
                request_id=getattr(request.state, "request_id", None),
            )
        )
    finally:
        await bridge.drain()
    return JSONResponse(result, status_code=200, headers={
        "ETag": _etag_header(result["etag"]),
        "X-Stratum-Version-Id": result["version_id"],
    })


@router.post("/v1/buckets/{bucket}/objects/{key:path}")
async def copy_object(request: Request, bucket: str, key: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    principal.require(bucket, PERM_WRITE)
    source = request.headers.get("x-stratum-copy-source", "")
    if "/" not in source:
        raise InvalidRequest("X-Stratum-Copy-Source 需形如 <bucket>/<key>")
    src_bucket, _, src_key = source.partition("/")
    principal.require(src_bucket, PERM_READ)
    result = container.objects.copy_object(
        src_bucket, src_key, bucket, key,
        if_match=request.headers.get("if-match"),
        actor=principal.name,
        request_id=getattr(request.state, "request_id", None),
    )
    return JSONResponse(result, status_code=201, headers={
        "ETag": _etag_header(result["etag"]),
        "X-Stratum-Version-Id": result["version_id"],
    })


@router.get("/v1/buckets/{bucket}/objects/{key:path}")
async def get_object(request: Request, bucket: str, key: str):
    container = get_container(request)
    resolve_principal(request, bucket, PERM_READ, presign_method="GET")
    version_id = request.query_params.get("versionId")
    handle = await run_in_threadpool(
        container.objects.open_object, bucket, key,
        request.headers.get("range"), version_id,
    )
    headers = {
        "ETag": _etag_header(handle.version.etag),
        "X-Stratum-Version-Id": handle.version.version_id,
        "X-Stratum-Storage-Class": handle.version.storage_class,
        "Accept-Ranges": "bytes",
        "Content-Length": str(handle.length),
        "Cache-Control": "private, max-age=0",
    }
    if handle.partial:
        headers["Content-Range"] = f"bytes {handle.start}-{handle.end}/{handle.version.size}"
    return StreamingResponse(
        container.objects.iter_chunks(handle),
        status_code=206 if handle.partial else 200,
        media_type=handle.version.content_type or "application/octet-stream",
        headers=headers,
    )


@router.head("/v1/buckets/{bucket}/objects/{key:path}")
async def head_object(request: Request, bucket: str, key: str):
    container = get_container(request)
    resolve_principal(request, bucket, PERM_READ, presign_method="HEAD")
    version = await run_in_threadpool(
        container.objects.head_object, bucket, key, request.query_params.get("versionId")
    )
    return Response(status_code=200, headers={
        "ETag": _etag_header(version.etag),
        "X-Stratum-Version-Id": version.version_id,
        "Content-Length": str(version.size),
        "Content-Type": version.content_type or "application/octet-stream",
        "Accept-Ranges": "bytes",
        "X-Stratum-Chunks": str(version.chunk_count),
        "Last-Modified": version.created_at,
    })


@router.delete("/v1/buckets/{bucket}/objects/{key:path}")
async def delete_object(request: Request, bucket: str, key: str):
    container = get_container(request)
    principal = resolve_principal(request, bucket, PERM_WRITE, presign_method="DELETE")
    result = container.objects.delete_object(
        bucket, key, request.query_params.get("versionId"),
        actor=principal.name, request_id=getattr(request.state, "request_id", None),
    )
    return result


# ------------------------------------------------------------------ multipart
@router.post("/v1/buckets/{bucket}/uploads", status_code=201)
async def create_upload(request: Request, bucket: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    principal.require(bucket, PERM_WRITE)
    key = request.query_params.get("key") or ""
    return container.multipart.create(
        bucket, key,
        content_type=request.headers.get("content-type"),
        metadata=_parse_metadata(request.headers.get("x-stratum-metadata")),
        actor=principal.name,
    )


@router.get("/v1/buckets/{bucket}/uploads")
async def list_uploads(request: Request, bucket: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    principal.require(bucket, PERM_READ)
    return container.multipart.list_uploads(bucket, request.query_params.get("prefix", ""))


@router.put("/v1/uploads/{upload_id}/parts/{part_no}")
async def upload_part(request: Request, upload_id: str, part_no: int):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    upload = container.multipart.get_upload(upload_id)
    principal.require(upload.bucket, PERM_WRITE)
    bridge = await BodyBridge(request, max_bytes=container.settings.max_object_bytes).start()
    try:
        result = await run_in_threadpool(
            container.multipart.upload_part, upload_id, part_no, bridge
        )
    finally:
        await bridge.drain()
    return JSONResponse(result, status_code=200,
                        headers={"ETag": _etag_header(result["etag"])})


@router.get("/v1/uploads/{upload_id}/parts")
async def list_parts(request: Request, upload_id: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    upload = container.multipart.get_upload(upload_id)
    principal.require(upload.bucket, PERM_READ)
    return container.multipart.list_parts(upload_id)


@router.post("/v1/uploads/{upload_id}/complete")
async def complete_upload(request: Request, upload_id: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    upload = container.multipart.get_upload(upload_id)
    principal.require(upload.bucket, PERM_WRITE)
    result = await run_in_threadpool(container.multipart.complete, upload_id,
                                     actor=principal.name)
    return JSONResponse(result, status_code=200, headers={
        "ETag": _etag_header(result["etag"]),
        "X-Stratum-Version-Id": result["version_id"],
    })


@router.delete("/v1/uploads/{upload_id}")
async def abort_upload(request: Request, upload_id: str):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    upload = container.multipart.get_upload(upload_id)
    principal.require(upload.bucket, PERM_WRITE)
    return container.multipart.abort(upload_id, actor=principal.name)


# ------------------------------------------------------------------- presign
@router.post("/v1/presign")
async def presign(request: Request):
    container = get_container(request)
    principal = container.auth.authenticate(request.headers)
    body = await _json_body(request)
    return container.presign.presign(
        principal, body.get("bucket", ""), body.get("key", ""),
        method=(body.get("method") or "GET").upper(),
        ttl=body.get("ttl"),
    )


# -------------------------------------------------------------------- helpers
async def _json_body(request: Request) -> dict:
    raw = await request.body()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except ValueError:
        raise InvalidRequest("请求体必须是合法 JSON")
    if not isinstance(data, dict):
        raise InvalidRequest("请求体必须是 JSON 对象")
    return data


def _int_param(value: Optional[str], default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default
