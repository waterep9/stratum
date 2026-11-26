"""HTTP 入口：应用装配、中间件、错误映射、控制台。"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response

from .api import routes_admin, routes_data
from .config import Settings, load_settings
from .errors import InvalidRequest, StratumError
from .logging_setup import setup_logging
from .metrics import METRICS
from .service.container import Container
from .util import utcnow

logger = logging.getLogger("stratum.http")
WEB_DIR = os.path.join(os.path.dirname(__file__), "web")


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or load_settings()
    container = Container(settings)
    app = FastAPI(
        title="Stratum 对象存储",
        description="内容寻址 + 分片去重 + 版本化 + 生命周期治理的对象存储服务",
        version="0.1.0",
        docs_url="/docs",
        redoc_url=None,
        openapi_url="/openapi.json",
    )
    app.state.container = container

    @app.middleware("http")
    async def instrument(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        request.state.request_id = request_id
        started = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - started) * 1000
        route = request.scope.get("route")
        template = getattr(route, "path", request.url.path)
        METRICS.inc("stratum_http_requests_total", method=request.method,
                    route=template, status=str(response.status_code))
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Stratum-Elapsed-Ms"] = f"{elapsed_ms:.1f}"
        logger.info("request", extra={
            "request_id": request_id, "route": template, "status": response.status_code,
            "latency_ms": round(elapsed_ms, 1), "method": request.method,
        })
        return response

    @app.exception_handler(StratumError)
    async def handle_stratum_error(request: Request, exc: StratumError):
        payload = exc.to_payload()
        payload["error"]["request_id"] = getattr(request.state, "request_id", None)
        headers = {}
        if exc.status == 401:
            headers["WWW-Authenticate"] = 'Bearer realm="stratum"'
        if isinstance(exc, InvalidRequest) and getattr(exc, "total", None) is not None:
            headers["Content-Range"] = f"bytes */{exc.total}"
        return JSONResponse(payload, status_code=exc.status, headers=headers)

    @app.exception_handler(RequestValidationError)
    async def handle_validation(request: Request, exc: RequestValidationError):
        return JSONResponse(
            {"error": {"code": "InvalidRequest", "message": "请求参数校验失败",
                       "detail": {"errors": exc.errors()},
                       "request_id": getattr(request.state, "request_id", None)}},
            status_code=400,
        )

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception):
        logger.exception("unhandled error", extra={
            "request_id": getattr(request.state, "request_id", None)})
        return JSONResponse(
            {"error": {"code": "InternalError", "message": "服务内部错误",
                       "detail": {"type": type(exc).__name__},
                       "request_id": getattr(request.state, "request_id", None)}},
            status_code=500,
        )

    app.include_router(routes_data.router, tags=["数据面"])
    app.include_router(routes_admin.router, tags=["管理面"])

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def console():
        path = os.path.join(WEB_DIR, "index.html")
        with open(path, encoding="utf-8") as fh:
            return HTMLResponse(fh.read())

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon():
        return Response(status_code=204)

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        return {"status": "ok", "ts": utcnow()}

    @app.get("/readyz", include_in_schema=False)
    async def readyz():
        with container.db.read() as conn:
            conn.execute("SELECT 1").fetchone()
        return {"status": "ready", "ts": utcnow()}

    @app.get("/metrics", response_class=PlainTextResponse, include_in_schema=False)
    async def metrics():
        with container.db.read() as conn:
            stats = container.meta.overview(conn)
        body = METRICS.render()
        body += "# TYPE stratum_objects gauge\n"
        body += f"stratum_objects {stats['objects']}\n"
        body += "# TYPE stratum_object_versions gauge\n"
        body += f"stratum_object_versions {stats['versions']}\n"
        body += "# TYPE stratum_logical_bytes gauge\n"
        body += f"stratum_logical_bytes {stats['logical_bytes']}\n"
        body += "# TYPE stratum_chunks_physical gauge\n"
        body += f"stratum_chunks_physical {stats['chunks_total']}\n"
        body += "# TYPE stratum_chunk_bytes_physical gauge\n"
        body += f"stratum_chunk_bytes_physical {stats['chunk_bytes_total']}\n"
        body += "# TYPE stratum_chunks_corrupt gauge\n"
        body += f"stratum_chunks_corrupt {stats['chunks_corrupt']}\n"
        body += "# TYPE stratum_open_uploads gauge\n"
        body += f"stratum_open_uploads {stats['open_uploads']}\n"
        for state, count in stats["outbox"].items():
            body += f'stratum_outbox_events{{state="{state}"}} {count}\n'
        return PlainTextResponse(body)

    return app


def run() -> None:
    import uvicorn

    settings = load_settings()
    setup_logging(os.getenv("STRATUM_LOG_LEVEL", "INFO"))
    app = create_app(settings)
    uvicorn.run(app, host=os.getenv("STRATUM_HOST", "0.0.0.0"),
                port=int(os.getenv("STRATUM_PORT", "8080")), log_config=None)
