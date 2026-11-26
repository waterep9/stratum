from __future__ import annotations

from conftest import get, put


def test_versions_accumulate(client, bucket):
    for i in range(3):
        assert put(client, bucket, "doc.txt", f"v{i}".encode()).status_code == 200
    items = client.get("/v1/buckets/demo/versions").json()["items"]
    assert len(items) == 3
    assert sum(1 for i in items if i["is_current"]) == 1
    assert get(client, bucket, "doc.txt").content == b"v2"


def test_get_specific_version(client, bucket):
    first = put(client, bucket, "doc.txt", b"old").json()["version_id"]
    put(client, bucket, "doc.txt", b"new")
    old = client.get(f"/v1/buckets/demo/objects/doc.txt?versionId={first}")
    assert old.content == b"old"
    assert old.headers["x-stratum-version-id"] == first


def test_delete_marker_hides_object(client, bucket):
    put(client, bucket, "gone.txt", b"data")
    assert client.delete("/v1/buckets/demo/objects/gone.txt").status_code == 200
    assert get(client, bucket, "gone.txt").status_code == 404
    assert "gone.txt" not in [i["key"] for i in
                              client.get("/v1/buckets/demo/objects").json()["items"]]

    versions = client.get("/v1/buckets/demo/versions").json()["items"]
    assert any(v["is_delete_marker"] for v in versions)

    # 删掉删除标记即恢复对象
    marker = next(v for v in versions if v["is_delete_marker"])
    client.delete(f"/v1/buckets/demo/objects/gone.txt?versionId={marker['version_id']}")
    assert get(client, bucket, "gone.txt").content == b"data"


def test_unversioned_bucket_overwrites(client):
    client.post("/v1/buckets", json={"name": "plain"})
    v1 = put(client, "plain", "a.txt", b"one").json()
    v2 = put(client, "plain", "a.txt", b"two").json()
    assert v1["version_id"] == v2["version_id"] == "null"
    items = client.get("/v1/buckets/plain/versions").json()["items"]
    assert len(items) == 1
    assert get(client, "plain", "a.txt").content == b"two"


def test_unversioned_delete_removes_rows(client, container):
    client.post("/v1/buckets", json={"name": "plain"})
    put(client, "plain", "a.txt", b"one")
    client.delete("/v1/buckets/plain/objects/a.txt")
    assert client.get("/v1/buckets/plain/versions").json()["items"] == []
    with container.db.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM object_versions").fetchone()[0] == 0


def test_version_listing_pagination(client, bucket):
    for i in range(7):
        put(client, bucket, f"k{i:02d}.txt", b"x")
    seen = []
    token = None
    while True:
        params = {"limit": 3}
        if token:
            params["continuation"] = token
        page = client.get("/v1/buckets/demo/versions", params=params).json()
        seen.extend(v["key"] for v in page["items"])
        if not page["is_truncated"]:
            break
        token = page["next_token"]
    assert seen == sorted(seen)
    assert len(seen) == 7
