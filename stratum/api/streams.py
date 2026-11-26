"""请求体桥接：把 ASGI 异步流变成同步迭代器，供 worker 线程里的写路径消费。

为什么需要这层
--------------
分片 + 落盘 + 事务提交是同步阻塞代码（放在线程池里跑，避免阻塞事件循环）；
而 ASGI 的 request.stream() 是 async。二者之间用有界队列 + 线程池 put 连接：

* 队列有界（maxsize=4）→ 上游读得快就会在 put 上阻塞，形成真实背压；
* put 放到线程池执行 → 阻塞的是线程池线程而不是事件循环；
* 消费端是同步迭代器 → 服务层保持纯粹的同步代码，方便单测与复用（分片上传也走同一条路径）。

代价：每个请求多一次线程切换；换来写路径逻辑只有一份。
"""

from __future__ import annotations

import asyncio
import queue
from typing import Iterator

from ..errors import PayloadTooLarge

_SENTINEL = object()


class BodyBridge:
    def __init__(self, request, max_bytes: int = 0, queue_size: int = 4):
        self.request = request
        self.max_bytes = max_bytes
        self.queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self.producer_task: asyncio.Task | None = None

    async def start(self) -> "BodyBridge":
        self.producer_task = asyncio.create_task(self._produce())
        return self

    async def _produce(self) -> None:
        loop = asyncio.get_running_loop()
        total = 0
        try:
            async for chunk in self.request.stream():
                if not chunk:
                    continue
                total += len(chunk)
                if self.max_bytes and total > self.max_bytes:
                    raise PayloadTooLarge(f"请求体超过上限 {self.max_bytes} 字节",
                                          detail={"limit": self.max_bytes})
                await loop.run_in_executor(None, self.queue.put, chunk)
        except BaseException as exc:  # noqa: BLE001 - 原样传给消费端
            await loop.run_in_executor(None, self.queue.put, exc)
        finally:
            await loop.run_in_executor(None, self.queue.put, _SENTINEL)

    def __iter__(self) -> Iterator[bytes]:
        while True:
            item = self.queue.get()
            if item is _SENTINEL:
                return
            if isinstance(item, BaseException):
                raise item
            yield item

    async def drain(self) -> None:
        """消费端提前返回（如条件写失败）时，确保生产者不再堆积。"""
        if self.producer_task and not self.producer_task.done():
            try:
                await asyncio.wait_for(self.producer_task, timeout=1.0)
            except (asyncio.TimeoutError, Exception):
                self.producer_task.cancel()
