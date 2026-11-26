"""领域错误：统一映射为结构化 JSON 错误体，避免路由层逐个 try/except。"""

from __future__ import annotations

from typing import Any


class StratumError(Exception):
    status = 500
    code = "InternalError"

    def __init__(self, message: str, *, detail: Any = None, status: int | None = None,
                 code: str | None = None):
        super().__init__(message)
        self.message = message
        self.detail = detail or {}
        if status is not None:
            self.status = status
        if code is not None:
            self.code = code

    def to_payload(self) -> dict:
        return {"error": {"code": self.code, "message": self.message, "detail": self.detail}}


class InvalidRequest(StratumError):
    status = 400
    code = "InvalidRequest"


class Unauthorized(StratumError):
    status = 401
    code = "Unauthorized"


class Forbidden(StratumError):
    status = 403
    code = "AccessDenied"


class NotFound(StratumError):
    status = 404
    code = "NoSuchKey"


class BucketNotFound(NotFound):
    code = "NoSuchBucket"


class Conflict(StratumError):
    status = 409
    code = "Conflict"


class PreconditionFailed(StratumError):
    status = 412
    code = "PreconditionFailed"


class QuotaExceeded(StratumError):
    status = 507
    code = "QuotaExceeded"


class PayloadTooLarge(StratumError):
    status = 413
    code = "EntityTooLarge"


class ChunkCorrupted(StratumError):
    status = 500
    code = "ChunkCorrupted"
