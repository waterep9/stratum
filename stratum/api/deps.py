"""HTTP 层公共依赖：容器访问、主体解析、预签名兜底。"""

from __future__ import annotations

from typing import Optional

from fastapi import Request

from ..errors import Forbidden
from ..service.container import Container
from ..service.objects import ObjectService
from ..service.auth import Principal


def get_container(request: Request) -> Container:
    return request.app.state.container


def get_objects(request: Request) -> ObjectService:
    return request.app.state.container.objects


def resolve_principal(request: Request, bucket: str, perm: str,
                      presign_method: Optional[str] = None) -> Principal:
    """优先走请求头凭证；没有凭证时尝试预签名 URL。

    预签名只在「无 Authorization / 无 X-Stratum-Key」时生效，避免两套凭证互相掩盖。
    """
    container = get_container(request)
    has_header_cred = bool(request.headers.get("authorization")
                           or request.headers.get("x-stratum-key"))
    if has_header_cred or not request.query_params.get("X-Stratum-Signature"):
        principal = container.auth.authenticate(request.headers)
        principal.require(bucket, perm)
        return principal
    container.presign.verify(bucket, request.path_params.get("key", ""),
                             presign_method or request.method, dict(request.query_params))
    return Principal(name="presigned", grants=[{"bucket": bucket, "perm": perm}],
                     via="presigned")


def require_admin(request: Request) -> Principal:
    principal = get_container(request).auth.authenticate(request.headers)
    if not principal.is_admin:
        raise Forbidden("该操作需要管理员令牌")
    return principal
