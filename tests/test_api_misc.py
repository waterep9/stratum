from __future__ import annotations

from conftest import put


def test_object_listing_pagination(client, bucket):
    for i in range(12):
        put(client, bucket, f"logs/2026/{i:02d}.log", b"line")
    put(client, bucket, "other.txt", b"x")

    seen, token = [], None
    while True:
        params = {"prefix": "logs/", "limit": 5}
        if token:
            params["continuation"] = token
        page = client.get("/v1/buckets/demo/objects", params=params).json()
        seen.extend(item["key"] for item in page["items"])
        if not page["is_truncated"]:
            break
        token = page["next_token"]
    assert len(seen) == 12
    assert len(set(seen)) == 12
    assert all(k.startswith("logs/") for k in seen)
    assert seen == sorted(seen)


def test_bad_continuation_token(client, bucket):
    res = client.get("/v1/buckets/demo/objects", params={"continuation": "not-a-token"})
    assert res.status_code == 400


def test_audit_log_records_writes(client, bucket):
    put(client, bucket, "audit.txt", b"x")
    client.delete("/v1/buckets/demo/objects/audit.txt")
    items = client.get("/admin/audit", params={"bucket": "demo"}).json()["items"]
    actions = [item["action"] for item in items]
    assert "object.put" in actions
    assert "object.delete_marker.create" in actions
    assert items[0]["actor"] == "admin"


def test_overview_and_metrics(client, bucket):
    put(client, bucket, "m.txt", b"metrics")
    overview = client.get("/admin/overview").json()
    assert overview["objects"] == 1
    assert overview["buckets"] == 1
    assert overview["chunk_bytes_total"] == 7

    metrics = client.get("/metrics").text
    assert "stratum_objects 1" in metrics
    assert "stratum_http_requests_total" in metrics
    assert "stratum_chunks_corrupt" in metrics


def test_request_id_echoed(client):
    res = client.get("/healthz", headers={"X-Request-Id": "trace-123"})
    assert res.headers["x-request-id"] == "trace-123"
    assert "x-stratum-elapsed-ms" in res.headers


def test_openapi_available(client):
    spec = client.get("/openapi.json").json()
    assert "/v1/buckets/{bucket}/objects/{key}" in spec["paths"]


def test_bucket_stats_endpoint(client, bucket, container):
    put(client, bucket, "s.bin", b"1234567890")
    stats = client.get("/admin/buckets/demo/stats").json()
    assert stats["objects"] == 1
    assert stats["logical_bytes"] == 10
    assert stats["distinct_chunks"] == 1


def test_delete_non_empty_bucket_requires_force(client, bucket):
    put(client, bucket, "x.txt", b"x")
    assert client.delete("/v1/buckets/demo").status_code == 409
    forced = client.delete("/v1/buckets/demo", params={"force": "true"}).json()
    assert forced["deleted"] is True
    assert client.get("/v1/buckets/demo").status_code == 404


def test_unknown_maintenance_job(client):
    assert client.post("/admin/maintenance/explode").status_code == 400


def test_versioning_toggle_after_objects(client):
    client.post("/v1/buckets", json={"name": "toggle"})
    put(client, "toggle", "a.txt", b"one")
    client.patch("/v1/buckets/toggle", json={"versioning": True})
    put(client, "toggle", "a.txt", b"two")
    items = client.get("/v1/buckets/toggle/versions").json()["items"]
    assert len(items) == 2


def test_head_unknown_version(client, bucket):
    put(client, bucket, "a.txt", b"x")
    assert client.head("/v1/buckets/demo/objects/a.txt?versionId=deadbeef").status_code == 404


def test_delete_object_missing(client, bucket):
    assert client.delete("/v1/buckets/demo/objects/none.txt").status_code == 404


def test_bucket_labels_roundtrip(client):
    created = client.post("/v1/buckets", json={"name": "tagged", "labels": {"team": "core"}}).json()
    assert created["labels"]["team"] == "core"
    patched = client.patch("/v1/buckets/tagged", json={"labels": {"team": "data"}}).json()
    assert patched["labels"]["team"] == "data"


def test_large_headers_rejected(client, bucket):
    res = client.put("/v1/buckets/demo/objects/big.txt", content=b"x",
                     headers={"X-Stratum-Metadata": "{" + "a" * 9000 + "}"})
    assert res.status_code == 400
