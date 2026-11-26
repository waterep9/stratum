"""事务性 Outbox + Webhook 投递。

写入对象时把事件写进同库的 ``outbox_events``（与业务变更同一事务），投递由独立进程
或 worker 循环完成。好处：不会出现「对象写成功但事件丢了」或「事件发了但对象没写成」；
代价：至少一次投递语义，消费端需按 ``id`` 去重。

失败重试采用指数退避（2s, 4s, 8s ... 上限 300s），超过上限进 DEAD 表供人工重放。
抢占通过 ``next_at`` 租约实现：即使起多个 worker 也不会同一条事件并发投递。
"""

from __future__ import annotations

from typing import Callable, Optional

from ..metrics import METRICS
from ..storage.metadata import MetadataStore
from ..util import seconds_from_now, utcnow


class OutboxDispatcher:
    def __init__(self, db, meta: MetadataStore, *, sender: Optional[Callable[[dict], None]] = None,
                 max_attempts: int = 6, lease_seconds: int = 30, timeout: float = 5.0):
        self.db = db
        self.meta = meta
        self.sender = sender or self._http_sender
        self.max_attempts = max_attempts
        self.lease_seconds = lease_seconds
        self.timeout = timeout

    def _http_sender(self, event: dict) -> None:
        import httpx

        url = event.get("target_url")
        if not url:
            return
        response = httpx.post(url, json=event, timeout=self.timeout,
                              headers={"X-Stratum-Event": event["event_type"]})
        response.raise_for_status()

    def dispatch_due(self, limit: int = 20, now: Optional[str] = None) -> dict:
        now = now or utcnow()
        with self.db.read() as conn:
            events = self.meta.due_events(conn, now, limit)
        delivered = failed = dead = skipped = 0
        for event in events:
            lease_until = seconds_from_now(self.lease_seconds)
            with self.db.tx() as conn:
                if not self.meta.lease_event(conn, event["id"], event["next_at"], lease_until):
                    skipped += 1
                    continue
            attempts = event["attempts"] + 1
            try:
                self.sender(event)
            except Exception as exc:
                if attempts >= self.max_attempts:
                    with self.db.tx() as conn:
                        self.meta.mark_dead(conn, event["id"], str(exc))
                    dead += 1
                    METRICS.inc("stratum_webhook_dead_total")
                else:
                    backoff = min(2 ** attempts, 300)
                    with self.db.tx() as conn:
                        self.meta.mark_failed(conn, event["id"], seconds_from_now(backoff),
                                              str(exc))
                    failed += 1
                    METRICS.inc("stratum_webhook_failed_total")
                continue
            with self.db.tx() as conn:
                self.meta.mark_delivered(conn, event["id"], utcnow())
            delivered += 1
            METRICS.inc("stratum_webhook_delivered_total")
        return {"picked": len(events), "delivered": delivered, "retry_scheduled": failed,
                "dead": dead, "skipped": skipped}

    def replay_dead(self, limit: int = 50) -> dict:
        with self.db.tx() as conn:
            rows = conn.execute(
                "SELECT id FROM outbox_events WHERE state='DEAD' ORDER BY id LIMIT ?", (limit,)
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE outbox_events SET state='PENDING', attempts=0, next_at=? WHERE id=?",
                    (utcnow(), row["id"]),
                )
            self.meta.add_audit(conn, actor="admin", action="webhook.replay", status=200,
                                detail={"count": len(rows)})
        return {"replayed": len(rows)}
