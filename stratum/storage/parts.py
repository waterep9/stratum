"""分片上传的暂存区。

与 CAS 的区别：部件是「暂存态」，可被覆盖、可被中止清理，因此不能内容寻址；
按 upload_id 分目录，部件号直接做文件名，保证 part 覆盖写是幂等的。
Complete 时把部件流式读出来切块写入 CAS，然后整个 upload 目录被清空。
"""

from __future__ import annotations

import os
import shutil
from typing import Iterator

from ..util import ensure_dir


class PartStore:
    def __init__(self, root: str):
        self.root = root
        ensure_dir(root)

    def _dir(self, upload_id: str) -> str:
        return os.path.join(self.root, upload_id)

    def _path(self, upload_id: str, part_no: int) -> str:
        return os.path.join(self._dir(upload_id), f"{part_no:06d}.part")

    def write_part(self, upload_id: str, part_no: int, chunks: Iterator[bytes]) -> tuple[str, int]:
        ensure_dir(self._dir(upload_id))
        path = self._path(upload_id, part_no)
        tmp = path + ".tmp"
        import hashlib

        h = hashlib.sha256()
        size = 0
        with open(tmp, "wb") as fh:
            for buf in chunks:
                if not buf:
                    continue
                size += len(buf)
                h.update(buf)
                fh.write(buf)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        return h.hexdigest(), size

    def write_part_bytes(self, upload_id: str, part_no: int, data: bytes) -> tuple[str, int]:
        return self.write_part(upload_id, part_no, iter([data]))

    def open_part(self, upload_id: str, part_no: int):
        return open(self._path(upload_id, part_no), "rb")

    def part_size(self, upload_id: str, part_no: int) -> int:
        try:
            return os.path.getsize(self._path(upload_id, part_no))
        except OSError:
            return -1

    def delete_part(self, upload_id: str, part_no: int) -> None:
        try:
            os.unlink(self._path(upload_id, part_no))
        except FileNotFoundError:
            pass

    def drop_upload(self, upload_id: str) -> None:
        shutil.rmtree(self._dir(upload_id), ignore_errors=True)

    def upload_bytes(self, upload_id: str) -> int:
        total = 0
        directory = self._dir(upload_id)
        if not os.path.isdir(directory):
            return 0
        for name in os.listdir(directory):
            try:
                total += os.path.getsize(os.path.join(directory, name))
            except OSError:
                pass
        return total
