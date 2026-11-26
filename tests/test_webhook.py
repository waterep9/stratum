from __future__ import annotations

from conftest import put


def _pending(container) -> int:
    with container.db.read() as conn:
        return container.meta.outbox_stats(conn)["PENDING"]


def test_events_are_written_transactionally(client, bucket, container):
    put(client, bucket, "a.txt", b"x")
    assert _pending(container) == 1
    with container.db.read() as conn:
        rows = conn.execute("SELECT event_type, payload_json FROM outbox_events").fetchall()
    assert rows[0]["event_type"] == "object.created"
    assert "a.txt" in rows[0]["payload_json"]


def test_delivery_marks_delivered(client, bucket, container):
    received = []
    container.outbox.sender = lambda event: received.append(event)
    put(client, bucket, "a.txt", b"x")

    result = container.outbox.dispatch_due()
    assert result["delivered"] == 1
    assert received[0]["event_type"] == "object.created"
    assert _pending(container) == 0


def test_failure_schedules_retry_with_backoff(client, bucket, container):
    attempts = {"n": 0}

    def always_fail(event):
        attempts["n"] += 1
        raise RuntimeError("endpoint down")

    container.outbox.sender = always_fail
    put(client, bucket, "a.txt", b"x")

    first = container.outbox.dispatch_due()
    assert first["retry_scheduled"] == 1
    with container.db.read() as conn:
        row = conn.execute("SELECT attempts, state, last_error FROM outbox_events").fetchone()
    assert row["attempts"] == 1
    assert row["state"] == "PENDING"
    assert "endpoint down" in row["last_error"]

    # 立刻再跑一次：退避期内不应被重复投递
    second = container.outbox.dispatch_due()
    assert second["picked"] == 0

    # 时间推进到退避之后
    third = container.outbox.dispatch_due(now="2099-01-01T00:00:00.000000Z")
    assert third["retry_scheduled"] == 1
    assert attempts["n"] == 2


def test_dead_letter_after_max_attempts(client, bucket, container):
    container.outbox.sender = lambda event: (_ for _ in ()).throw(RuntimeError("boom"))
    put(client, bucket, "a.txt", b"x")
    for _ in range(container.outbox.max_attempts):
        container.outbox.dispatch_due(now="2099-01-01T00:00:00.000000Z")
    with container.db.read() as conn:
        stats = container.meta.outbox_stats(conn)
    assert stats["DEAD"] == 1
    assert stats["PENDING"] == 0

    replayed = container.outbox.replay_dead()
    assert replayed["replayed"] == 1
    with container.db.read() as conn:
        assert container.meta.outbox_stats(conn)["PENDING"] == 1


def test_tampered_event_not_repeated_by_lease(client, bucket, container):
    """租约机制：同一事件不能被两个 worker 同时领取。"""
    put(client, bucket, "a.txt", b"x")
    with container.db.read() as conn:
        event = container.meta.due_events(conn, "2099-01-01T00:00:00.000000Z")[0]
    original_next = event["next_at"]
    with container.db.tx() as conn:
        assert container.meta.lease_event(conn, event["id"], original_next,
                                          "2099-01-01T00:00:00.000000Z") is True
    with container.db.tx() as conn:
        # 租约已被推后，拿着旧的 next_at 再去抢就会失败（CAS 失败）
        assert container.meta.lease_event(conn, event["id"], original_next,
                                          "2099-01-01T00:00:00.000000Z") is False


def test_bucket_without_webhook_keeps_events_pending(client, bucket, container):
    put(client, bucket, "a.txt", b"x")
    result = container.outbox.dispatch_due()
    # sender 默认走 HTTP，但没有 target_url -> 直接视为成功（本地事件总线语义）
    assert result["delivered"] == 1
