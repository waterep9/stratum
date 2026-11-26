"""管理面 API：巡检、维护任务、密钥、审计。

管理面与数据面刻意分开前缀（/admin vs /v1）：
生产部署时可以在网关层只把 /v1 暴露到公网，/admin 限制在内网或跳板机 ——
这条边界比「同一个路由靠角色判断」更容易在基础设施层落实。
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from ..errors import InvalidRequest
from ..storage.metadata import MetadataStore
from ..util import utcnow
from .deps import get_container, require_admin
from .routes_data import _int_param, _json_body

router = APIRouter(prefix="/admin")


@router.get("/overview")
async def overview(request: Request):
    container = get_container(request)
    require_admin(request)
    with container.db.read() as conn:
        stats = container.meta.overview(conn)
        buckets = [b.to_dict() for b in container.meta.list_buckets(conn)]
    stats["buckets_detail"] = buckets
    stats["data_dir"] = container.settings.data_dir
    stats["chunk_size"] = container.settings.chunk_size
    stats["gc_grace_seconds"] = container.settings.gc_grace_seconds
    return stats


@router.get("/audit")
async def audit(request: Request):
    container = get_container(request)
    require_admin(request)
    params = request.query_params
    with container.db.read() as conn:
        items = container.meta.list_audit(
            conn, bucket=params.get("bucket") or None, action=params.get("action") or None,
            limit=_int_param(params.get("limit"), 50),
            before_id=_int_param(params.get("before_id"), 0) or None,
        )
    return {"items": items, "count": len(items)}


@router.get("/keys")
async def list_keys(request: Request):
    container = get_container(request)
    require_admin(request)
    with container.db.read() as conn:
        return {"items": container.meta.list_access_keys(conn)}


@router.post("/keys", status_code=201)
async def create_key(request: Request):
    container = get_container(request)
    require_admin(request)
    body = await _json_body(request)
    return container.auth.issue_key(
        label=body.get("label") or "unnamed",
        grants=body.get("grants") or [],
        is_admin=bool(body.get("is_admin", False)),
    )


@router.delete("/keys/{access_key}")
async def revoke_key(request: Request, access_key: str):
    container = get_container(request)
    require_admin(request)
    with container.db.tx() as conn:
        changed = container.meta.revoke_access_key(conn, access_key, utcnow())
        container.meta.add_audit(conn, actor="admin", action="access_key.revoke", status=200,
                                 detail={"access_key": access_key, "changed": changed})
    return {"access_key": access_key, "revoked": bool(changed)}


@router.get("/maintenance")
async def maintenance_status(request: Request):
    container = get_container(request)
    require_admin(request)
    with container.db.read() as conn:
        runs = container.meta.recent_runs(conn, 20)
        scrubs = container.meta.recent_scrubs(conn, 10)
        outbox = container.meta.outbox_stats(conn)
    return {"runs": runs, "scrubs": scrubs, "outbox": outbox}


@router.post("/maintenance/{job}")
async def run_maintenance(request: Request, job: str):
    container = get_container(request)
    principal = require_admin(request)
    params = request.query_params
    dry_run = params.get("dry_run", "").lower() in ("1", "true", "yes")
    limit = _int_param(params.get("limit"), 200)

    if job == "gc":
        grace = params.get("grace_seconds")
        grace_seconds = int(grace) if grace not in (None, "") else None
        result = await _record(container, "gc",
                               lambda: container.maintenance.collect_garbage(
                                   limit=limit, dry_run=dry_run,
                                   grace_seconds=grace_seconds))
    elif job == "lifecycle":
        result = await _record(container, "lifecycle",
                               lambda: container.lifecycle.run(dry_run=dry_run,
                                                               batch_limit=limit))
    elif job == "scrub":
        result = await _record(container, "scrub",
                               lambda: container.maintenance.scrub(limit=limit))
    elif job == "reconcile":
        result = await _record(container, "reconcile",
                               lambda: container.maintenance.reconcile(fix=not dry_run))
    elif job == "sweep":
        result = await _record(container, "sweep",
                               lambda: container.maintenance.sweep_temp_files())
    elif job == "orphans":
        result = await _record(container, "orphans",
                               lambda: container.maintenance.orphan_files(limit=limit))
    elif job == "webhook":
        result = await _record(container, "webhook",
                               lambda: container.outbox.dispatch_due(limit=limit))
    else:
        raise InvalidRequest(f"未知的维护任务: {job}",
                             detail={"supported": ["gc", "lifecycle", "scrub", "reconcile",
                                                   "sweep", "orphans", "webhook"]})

    with container.db.tx() as conn:
        container.meta.add_audit(conn, actor=principal.name, action=f"maintenance.{job}",
                                 status=200, detail={"dry_run": dry_run})
    return result


@router.post("/webhook/replay")
async def replay_webhook(request: Request):
    container = get_container(request)
    require_admin(request)
    return container.outbox.replay_dead(limit=_int_param(request.query_params.get("limit"), 50))


@router.get("/buckets/{bucket}/stats")
async def bucket_stats(request: Request, bucket: str):
    container = get_container(request)
    require_admin(request)
    with container.db.read() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS versions, COALESCE(SUM(size),0) AS logical_bytes,"
            " SUM(CASE WHEN is_current=1 AND is_delete_marker=0 THEN 1 ELSE 0 END) AS objects"
            " FROM object_versions WHERE bucket=?", (bucket,)
        ).fetchone()
        chunks = conn.execute(
            "SELECT COUNT(DISTINCT r.hash) AS chunks, COALESCE(SUM(r.size),0) AS chunk_bytes"
            " FROM chunk_refs r WHERE r.bucket=?", (bucket,)
        ).fetchone()
    return {"bucket": bucket, "versions": row["versions"], "objects": row["objects"],
            "logical_bytes": row["logical_bytes"], "distinct_chunks": chunks["chunks"],
            "chunk_bytes": chunks["chunk_bytes"]}


async def _record(container, job: str, fn):
    from starlette.concurrency import run_in_threadpool

    started = utcnow()
    with container.db.tx() as conn:
        run_id = MetadataStore().start_run(conn, job, started)
    try:
        result = await run_in_threadpool(fn)
        status = "OK"
    except Exception as exc:
        with container.db.tx() as conn:
            MetadataStore().finish_run(conn, run_id, "FAILED", {"error": str(exc)}, utcnow())
        raise
    with container.db.tx() as conn:
        MetadataStore().finish_run(conn, run_id, status, result, utcnow())
    return {"run_id": run_id, "job": job, "result": result}
