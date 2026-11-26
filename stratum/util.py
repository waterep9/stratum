"""通用工具：时间、ID、哈希、键校验、JSON。"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

_CTRL = re.compile(r"[\x00-\x1f\x7f]")
MAX_KEY_LEN = 1024


def utcnow() -> str:
    """单调可排序的 UTC 时间戳字符串（精确到微秒，带 Z 后缀）。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def ts_to_dt(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def dt_to_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def days_ago(days: float) -> str:
    return dt_to_ts(datetime.now(timezone.utc) - timedelta(days=days))


def seconds_from_now(seconds: float) -> str:
    return dt_to_ts(datetime.now(timezone.utc) + timedelta(seconds=seconds))


def new_id() -> str:
    return uuid.uuid4().hex


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_file(path: str, block: int = 1 << 20) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            buf = fh.read(block)
            if not buf:
                break
            size += len(buf)
            h.update(buf)
    return h.hexdigest(), size


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=False)


def json_loads(raw: str | None, default: Any = None) -> Any:
    if not raw:
        return default if default is not None else {}
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return default if default is not None else {}


def validate_key(key: str) -> str:
    """对象键校验：拒绝路径穿越、控制字符、空键与超长键。

    对象键在 CAS 中不参与文件路径拼接（文件路径只由内容哈希决定），
    因此这里拒绝 ``..`` 主要是防止控制台/下游使用方把键误当路径使用。
    """
    if not key or not key.strip():
        raise ValueError("对象键不能为空")
    if len(key) > MAX_KEY_LEN:
        raise ValueError(f"对象键长度不能超过 {MAX_KEY_LEN} 字节")
    if _CTRL.search(key):
        raise ValueError("对象键不能包含控制字符")
    if key.startswith("/") or key.endswith("/"):
        raise ValueError("对象键不能以 / 开头或结尾")
    segments = key.split("/")
    if any(seg in ("", ".", "..") for seg in segments):
        raise ValueError("对象键不能包含空段、'.' 或 '..'")
    return key


def validate_bucket_name(name: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]", name or ""):
        raise ValueError("bucket 名称需为 3-63 位小写字母/数字/.-，且首尾为字母或数字")
    if ".." in name or ".-" in name or "-." in name:
        raise ValueError("bucket 名称不能包含 '..'、'.-'、'-.'")
    return name


def human_bytes(num: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(num) < 1024.0 or unit == "TiB":
            return f"{num:.1f} {unit}" if unit != "B" else f"{int(num)} B"
        num /= 1024.0
    return f"{num:.1f} TiB"


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path
