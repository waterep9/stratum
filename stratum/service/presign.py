"""预签名 URL。

签名串设计：``METHOD\\nBUCKET\\nKEY\\nEXPIRES\\nACCESS_KEY``，HMAC-SHA256 后 hex。
选择把 bucket/key 纳入签名而不是只签路径，是为了让签名与路由解耦：即使以后
把对象路由换成 ``/o/{bucket}/{key}``，签名逻辑也不用改。
"""

from __future__ import annotations

import hashlib
import hmac
from urllib.parse import quote

from ..errors import Forbidden, InvalidRequest
from ..storage.metadata import MetadataStore


def _canonical(method: str, bucket: str, key: str, expires: int, access_key: str) -> str:
    return f"{method.upper()}\n{bucket}\n{key}\n{expires}\n{access_key}"


def sign(secret: str, method: str, bucket: str, key: str, expires: int, access_key: str) -> str:
    msg = _canonical(method, bucket, key, expires, access_key).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


class PresignService:
    def __init__(self, db, meta: MetadataStore, public_base_url: str, default_ttl: int):
        self.db = db
        self.meta = meta
        self.public_base_url = public_base_url
        self.default_ttl = default_ttl

    def presign(self, principal, bucket: str, key: str, method: str = "GET",
                ttl: int | None = None, now_epoch: int | None = None) -> dict:
        import time

        principal.require(bucket, "r" if method.upper() == "GET" else "w")
        if not principal.access_key:
            raise InvalidRequest("管理令牌不能直接签发预签名 URL，请使用访问密钥")
        with self.db.read() as conn:
            record = self.meta.get_access_key(conn, principal.access_key)
        if not record:
            raise Forbidden("访问密钥不可用")
        ttl = ttl or self.default_ttl
        now = now_epoch if now_epoch is not None else int(time.time())
        exp = now + ttl
        signature = sign(record["secret_hash"], method, bucket, key, exp, principal.access_key)
        url = (f"{self.public_base_url.rstrip('/')}/v1/buckets/{quote(bucket)}"
               f"/objects/{quote(key)}?X-Stratum-Key={quote(principal.access_key)}"
               f"&X-Stratum-Expires={exp}&X-Stratum-Signature={signature}")
        return {"url": url, "expires_at": exp, "ttl": ttl, "method": method.upper(),
                "algorithm": "HMAC-SHA256"}

    def verify(self, bucket: str, key: str, method: str, params: dict) -> None:
        """校验查询串签名。注意：这里直接用 secret_hash 当 HMAC 密钥。

        这是刻意的权衡：服务端不保存明文 secret，因此无法用明文签名；
        改用「摘要即密钥」（HMAC-SHA256(sha256(secret), msg)），泄露数据库仍不足以
        伪造签名以外的任何东西，但可以伪造签名本身——因此预签名 URL 限定短 TTL。
        """
        import time

        access_key = params.get("X-Stratum-Key")
        expires_raw = params.get("X-Stratum-Expires")
        signature = params.get("X-Stratum-Signature")
        if not (access_key and expires_raw and signature):
            raise Forbidden("预签名参数缺失")
        try:
            expires = int(expires_raw)
        except ValueError:
            raise Forbidden("预签名过期时间非法")
        if expires < int(time.time()):
            raise Forbidden("预签名 URL 已过期", detail={"expires_at": expires})
        with self.db.read() as conn:
            record = self.meta.get_access_key(conn, access_key)
        if not record or record["revoked_at"]:
            raise Forbidden("预签名访问密钥不可用")
        expected = sign(record["secret_hash"], method, bucket, key, expires, access_key)
        if not hmac.compare_digest(expected, signature):
            raise Forbidden("预签名签名不匹配")
