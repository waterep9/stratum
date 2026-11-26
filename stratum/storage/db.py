"""SQLite 访问层：连接生命周期、事务、迁移。

为什么用原生 sqlite3 而不是 ORM：
* 热路径是「一次 PUT 写 N 条 chunk_refs + 更新引用计数」，需要精确控制事务边界与
  BEGIN IMMEDIATE 语义；ORM 的自动 flush 顺序反而会放大写锁持有时间。
* 元数据表结构简单、查询固定，手写 SQL 比 ORM 更可读，也便于在 README 中直接呈现。
代价：需要自己维护迁移与类型转换（见 metadata.py 的 row -> dataclass 映射）。
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

from ..util import ensure_dir, utcnow

PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_PATH = os.path.join(PACKAGE_ROOT, "schema.sql")

MIGRATIONS = [
    (1, "initial_schema", "schema.sql"),
]


class Database:
    def __init__(self, path: str, cache_kb: int = 16384):
        self.path = path
        self.cache_kb = cache_kb
        self._local = threading.local()
        ensure_dir(os.path.dirname(path) or ".")

    def connect(self) -> sqlite3.Connection:
        """新建连接（迁移、后台任务等需要独立连接的场景）。"""
        conn = sqlite3.connect(
            self.path,
            timeout=15.0,
            isolation_level=None,      # 自己管事务，避免隐式 BEGIN
            check_same_thread=False,   # 连接可能被线程池或 worker 复用
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 15000")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute(f"PRAGMA cache_size = -{max(1024, self.cache_kb)}")
        return conn

    def _thread_conn(self) -> sqlite3.Connection:
        """线程内复用连接。

        一次 PUT 要读 bucket、读当前版本、再开写事务，共 3 次取连接；每建一次连接都要
        重放 4 条 PRAGMA，在 Windows 上单次约 1ms，直接吃掉可观的尾延迟。
        连接是 autocommit 模式（不持有隐式事务），复用不会把旧读快照带进后续写事务。
        """
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self.connect()
            self._local.conn = conn
        return conn

    def migrate(self) -> list[int]:
        """幂等迁移：只应用尚未记录的版本。"""
        ensure_dir(os.path.dirname(self.path) or ".")
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)"
            )
            applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
            done: list[int] = []
            for version, name, filename in MIGRATIONS:
                if version in applied:
                    continue
                with open(os.path.join(PACKAGE_ROOT, filename), encoding="utf-8") as fh:
                    conn.executescript(fh.read())
                conn.execute(
                    "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)",
                    (version, name, utcnow()),
                )
                done.append(version)
            return done
        finally:
            conn.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        yield self._thread_conn()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """写事务：BEGIN IMMEDIATE 立刻取写锁，避免「读后升级写」导致的死锁式重试。

        连接在线程内复用，因此不允许嵌套 tx（同一线程重入会报 "cannot start a transaction
        within a transaction"）。服务层每个用例只开一层，这条约束由测试兜底。
        """
        conn = self._thread_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
