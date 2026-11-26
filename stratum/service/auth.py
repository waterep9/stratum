"""认证与授权。

三种主体：
1. ``admin``     —— 持有 ``STRATUM_ADMIN_TOKEN``，可管理 bucket / 签发密钥 / 触发维护；
2. ``access key`` —— ``X-Stratum-Key`` + ``X-Stratum-Secret``，权限由 grants 描述；
3. ``presigned``  —— 查询串里的 HMAC 签名，把读权限临时授给浏览器。

密钥只存 SHA-256 摘要：即使元数据库泄露也无法还原 secret；校验用 compare_digest 防时序侧信道。
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass, field
from typing import Optional

from ..errors import Forbidden, Unauthorized
from ..storage.metadata import MetadataStore
from ..util import new_id, utcnow

PERM_READ = "r"
PERM_WRITE = "w"


@dataclass
class Principal:
    name: str
    is_admin: bool = False
    grants: list = field(default_factory=list)
    access_key: Optional[str] = None
    via: str = "anonymous"

    def allows(self, bucket: str, perm: str) -> bool:
        if self.is_admin:
            return True
        for grant in self.grants:
            target = grant.get("bucket", "")
            if target not in ("*", bucket):
                continue
            perms = grant.get("perm", "")
            if perm == PERM_READ and "r" in perms:
                return True
            if perm == PERM_WRITE and "w" in perms:
                return True
        return False

    def require(self, bucket: str, perm: str) -> None:
        if not self.allows(bucket, perm):
            raise Forbidden(f"主体 {self.name} 无权对 bucket={bucket} 执行 {perm} 操作",
                            detail={"bucket": bucket, "perm": perm})


def hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


class AuthService:
    def __init__(self, db, meta: MetadataStore, admin_token: str):
        self.db = db
        self.meta = meta
        self.admin_token = admin_token

    def authenticate(self, headers) -> Principal:
        auth = (headers.get("authorization") or "").strip()
        if auth.lower().startswith("bearer "):
            token = auth[7:].strip()
            if not self.admin_token:
                raise Unauthorized("服务未配置管理令牌", status=503, code="AuthNotConfigured")
            if hmac.compare_digest(token, self.admin_token):
                return Principal(name="admin", is_admin=True, via="admin-token")
            raise Unauthorized("管理令牌无效")

        access_key = headers.get("x-stratum-key")
        secret = headers.get("x-stratum-secret")
        if access_key and secret:
            record = self._load_key(access_key)
            if not record:
                raise Unauthorized("访问密钥不存在或已吊销")
            if not hmac.compare_digest(hash_secret(secret), record["secret_hash"]):
                raise Unauthorized("访问密钥校验失败")
            return Principal(
                name=record["label"] or access_key,
                is_admin=record["is_admin"],
                grants=record["grants"],
                access_key=access_key,
                via="access-key",
            )
        raise Unauthorized("缺少凭证：请提供 Bearer 管理令牌或 X-Stratum-Key/Secret")

    def _load_key(self, access_key: str) -> Optional[dict]:
        with self.db.read() as conn:
            record = self.meta.get_access_key(conn, access_key)
        if not record or record["revoked_at"]:
            return None
        return record

    # ---------------------------------------------------------------- 管理操作
    def issue_key(self, label: str, grants: list, is_admin: bool = False) -> dict:
        access_key = "AK" + new_id().upper()[:20]
        secret = "SK" + secrets.token_urlsafe(24)
        normalized = []
        for grant in grants or []:
            bucket = str(grant.get("bucket", "")).strip()
            perm = str(grant.get("perm", "")).strip()
            if not bucket or any(ch not in "rw" for ch in perm) or not perm:
                raise Forbidden("grant 需形如 {'bucket': 'demo', 'perm': 'rw'}")
            normalized.append({"bucket": bucket, "perm": "".join(sorted(set(perm)))})
        with self.db.tx() as conn:
            self.meta.create_access_key(conn, access_key, hash_secret(secret), label,
                                        normalized, is_admin, utcnow())
            self.meta.add_audit(conn, actor="admin", action="access_key.create", status=201,
                                detail={"access_key": access_key, "label": label,
                                        "grants": normalized})
        return {"access_key": access_key, "secret": secret, "label": label,
                "grants": normalized, "is_admin": is_admin,
                "warning": "secret 仅在此次响应中返回，服务端只保存摘要"}
