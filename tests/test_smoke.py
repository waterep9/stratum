def test_health_and_ready(client):
    assert client.get("/healthz").json()["status"] == "ok"
    assert client.get("/readyz").json()["status"] == "ready"


def test_console_page(client):
    res = client.get("/")
    assert res.status_code == 200
    assert "Stratum" in res.text


def test_bucket_crud(client):
    created = client.post("/v1/buckets", json={"name": "media", "versioning": True})
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["versioning"] is True

    assert client.post("/v1/buckets", json={"name": "media"}).status_code == 409
    assert client.post("/v1/buckets", json={"name": "AB"}).status_code == 400

    listed = client.get("/v1/buckets").json()
    assert [b["name"] for b in listed["items"]] == ["media"]

    patched = client.patch("/v1/buckets/media", json={"quota_bytes": 1024})
    assert patched.json()["quota_bytes"] == 1024

    assert client.delete("/v1/buckets/media").json()["deleted"] is True
    assert client.get("/v1/buckets/media").status_code == 404


def test_put_get_head_delete(client, bucket, container):
    res = client.put("/v1/buckets/demo/objects/docs/a.txt", content=b"hello stratum")
    assert res.status_code == 200, res.text
    payload = res.json()
    assert payload["size"] == 13
    assert res.headers["ETag"].startswith('"')

    got = client.get("/v1/buckets/demo/objects/docs/a.txt")
    assert got.status_code == 200
    assert got.content == b"hello stratum"
    assert got.headers["ETag"] == res.headers["ETag"]

    head = client.head("/v1/buckets/demo/objects/docs/a.txt")
    assert head.status_code == 200
    assert head.headers["content-length"] == "13"

    assert client.delete("/v1/buckets/demo/objects/docs/a.txt").status_code == 200
    assert client.get("/v1/buckets/demo/objects/docs/a.txt").status_code == 404


def test_etag_matches_content_hash(client, bucket):
    import hashlib

    data = b"x" * 5000
    res = client.put("/v1/buckets/demo/objects/hash.txt", content=data)
    assert res.json()["etag"] == hashlib.sha256(data).hexdigest()
