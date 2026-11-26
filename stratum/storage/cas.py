"""内容寻址分片仓库（CAS）。

目录布局：``<root>/chunks/<h[0:2]>/<h[2:4]>/<hash>``。
* 打散前缀是为了避免单目录下几十万文件导致 NTFS/ext4 目录项退化；
* 写入走「临时文件 + os.replace」，同分区内是原子操作，配合内容哈希天然幂等：
  两个客户端并发写同一分片，结果是同一个 inode 的覆盖，不会产生半截文件；
* 读取时不做缓存：分片最大 1 MiB，走 OS page cache 已足够，进程内再缓存会与 GC 抢内存。
"""

from __future__ import annotations

import os
import tempfile
from typing import Iterator

from ..util import ensure_dir, sha256_hex


class ChunkStore:
    def __init__(self, root: str, fsync: bool = True):
        self.root = root
        self.fsync = fsync
        ensure_dir(root)

    def path_for(self, digest: str) -> str:
        return os.path.join(self.root, digest[0:2], digest[2:4], digest)

    def exists(self, digest: str) -> bool:
        return os.path.exists(self.path_for(digest))

    def put(self, data: bytes) -> tuple[str, bool]:
        """写入分片，返回 (hash, 是否新写入)。已存在则直接复用（去重）。"""
        digest = sha256_hex(data)
        target = self.path_for(digest)
        if os.path.exists(target):
            return digest, False
        ensure_dir(os.path.dirname(target))
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(target), prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                if self.fsync:
                    os.fsync(fh.fileno())
            try:
                os.replace(tmp, target)
            except OSError:
                # Windows 上目标文件被并发读句柄占用时 replace 会失败（WinError 5/32）。
                # 因为路径即内容哈希，此时目标文件内容与我们写的必然一致，按命中去重处理。
                if os.path.exists(target):
                    _safe_unlink(tmp)
                    return digest, False
                raise
        except BaseException:
            if os.path.exists(tmp):
                _safe_unlink(tmp)
            raise
        return digest, True

    def put_file(self, path: str) -> tuple[str, int, bool]:
        """搬迁式写入：适用于已落盘的分片上传部件，避免再读进内存。"""
        digest, size = _hash_file(path)
        target = self.path_for(digest)
        if os.path.exists(target):
            os.unlink(path)
            return digest, size, False
        ensure_dir(os.path.dirname(target))
        os.replace(path, target)
        return digest, size, True

    def read(self, digest: str) -> bytes:
        with open(self.path_for(digest), "rb") as fh:
            return fh.read()

    def iter_range(self, digest: str, offset: int = 0, length: int | None = None) -> Iterator[bytes]:
        with open(self.path_for(digest), "rb") as fh:
            if offset:
                fh.seek(offset)
            remaining = length
            while True:
                size = 256 * 1024 if remaining is None else min(256 * 1024, remaining)
                if size <= 0:
                    return
                buf = fh.read(size)
                if not buf:
                    return
                if remaining is not None:
                    remaining -= len(buf)
                yield buf

    def delete(self, digest: str) -> bool:
        path = self.path_for(digest)
        try:
            os.unlink(path)
            return True
        except FileNotFoundError:
            return False

    def stat_size(self, digest: str) -> int:
        try:
            return os.path.getsize(self.path_for(digest))
        except OSError:
            return -1

    def total_bytes(self) -> int:
        total = 0
        for dirpath, _dirnames, filenames in os.walk(self.root):
            for name in filenames:
                if name.startswith(".tmp-"):
                    continue
                try:
                    total += os.path.getsize(os.path.join(dirpath, name))
                except OSError:
                    pass
        return total

    def sweep_tmp(self, older_than_seconds: float) -> int:
        """清理崩溃遗留的临时文件（os.replace 之前断电/被杀）。"""
        import time

        cutoff = time.time() - older_than_seconds
        removed = 0
        for dirpath, _dirnames, filenames in os.walk(self.root):
            for name in filenames:
                if not name.startswith(".tmp-"):
                    continue
                path = os.path.join(dirpath, name)
                try:
                    if os.path.getmtime(path) < cutoff:
                        os.unlink(path)
                        removed += 1
                except OSError:
                    pass
        return removed


def _hash_file(path: str) -> tuple[str, int]:
    import hashlib

    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            buf = fh.read(1 << 20)
            if not buf:
                break
            size += len(buf)
            h.update(buf)
    return h.hexdigest(), size


def _safe_unlink(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass
