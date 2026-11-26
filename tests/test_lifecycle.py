from __future__ import annotations

import os


def test_expire_current_versions(client, container):
    client.post("/v1/buckets", json={"name": "logs", "lifecycle": {"expire_after_days": 0}})
    client.put("/v1/buckets/logs/objects/a.log", content=b"x")
    client.put("/v1/buckets/logs/objects/b.log", content=b"y")

    report = container.lifecycle.run()
    assert report["expired"] == 2
    assert client.get("/v1/buckets/logs/objects/a.log").status_code == 404


def test_dry_run_deletes_nothing(client, container):
    client.post("/v1/buckets", json={"name": "logs", "lifecycle": {"expire_after_days": 0}})
    client.put("/v1/buckets/logs/objects/a.log", content=b"x")
    report = container.lifecycle.run(dry_run=True)
    assert report["expired"] == 1
    assert client.get("/v1/buckets/logs/objects/a.log").status_code == 200


def test_noncurrent_versions_pruned(client, container):
    client.post("/v1/buckets", json={"name": "ver", "versioning": True,
                                     "lifecycle": {"noncurrent_expire_after_days": 0}})
    for i in range(3):
        client.put("/v1/buckets/ver/objects/doc.txt", content=f"v{i}".encode())
    report = container.lifecycle.run()
    assert report["versions_pruned"] == 2
    items = client.get("/v1/buckets/ver/versions").json()["items"]
    assert len(items) == 1
    assert client.get("/v1/buckets/ver/objects/doc.txt").content == b"v2"


def test_stale_upload_aborted(client, container):
    client.post("/v1/buckets", json={"name": "uploads", "lifecycle": {"abort_incomplete_days": 0}})
    upload = client.post("/v1/buckets/uploads/uploads", params={"key": "stale.bin"}).json()
    client.put(f"/v1/uploads/{upload['upload_id']}/parts/1", content=b"x" * 70000)
    report = container.lifecycle.run()
    assert report["uploads_aborted"] == 1
    assert not os.path.isdir(os.path.join(container.settings.part_root, upload["upload_id"]))


def test_lifecycle_policy_validation(client):
    assert client.post("/v1/buckets", json={"name": "bad", "lifecycle": {"oops": 1}}).status_code == 400
    assert client.post("/v1/buckets", json={"name": "bad2", "lifecycle": {"expire_after_days": -1}}).status_code == 400
    ok = client.post("/v1/buckets", json={"name": "okb", "lifecycle": {"expire_after_days": 30}})
    assert ok.status_code == 201
    assert ok.json()["lifecycle"]["expire_after_days"] == 30


def test_lifecycle_via_admin_endpoint(client):
    client.post("/v1/buckets", json={"name": "logs", "lifecycle": {"expire_after_days": 0}})
    client.put("/v1/buckets/logs/objects/a.log", content=b"x")
    res = client.post("/admin/maintenance/lifecycle")
    assert res.status_code == 200
    assert res.json()["result"]["expired"] == 1
    assert client.get("/v1/buckets/logs/objects/a.log").status_code == 404
