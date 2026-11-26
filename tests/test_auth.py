from __future__ import annotations

from conftest import put


def test_requires_credentials(anon, app):
    assert anon.get("/v1/buckets").status_code == 401
    assert anon.get("/v1/buckets").json()["error"]["code"] == "Unauthorized"


def test_invalid_admin_token(anon):
    res = anon.get("/v1/buckets", headers={"Authorization": "Bearer wrong"})
    assert res.status_code == 401


def test_scoped_key_read_only(client, anon):
    client.post("/v1/buckets", json={"name": "media", "versioning": False})
    put(client, "media", "a.txt", b"hello")
    issued = client.post("/admin/keys", json={
        "label": "reader", "grants": [{"bucket": "media", "perm": "r"}]
    }).json()

    headers = {"X-Stratum-Key": issued["access_key"], "X-Stratum-Secret": issued["secret"]}
    assert anon.get("/v1/buckets/media/objects/a.txt", headers=headers).content == b"hello"
    denied = anon.put("/v1/buckets/media/objects/b.txt", content=b"x", headers=headers)
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "AccessDenied"


def test_grant_scope_isolated(client, anon):
    client.post("/v1/buckets", json={"name": "one"})
    client.post("/v1/buckets", json={"name": "two"})
    issued = client.post("/admin/keys", json={
        "label": "one-only", "grants": [{"bucket": "one", "perm": "rw"}]
    }).json()
    headers = {"X-Stratum-Key": issued["access_key"], "X-Stratum-Secret": issued["secret"]}
    assert anon.put("/v1/buckets/one/objects/k", content=b"x", headers=headers).status_code == 200
    assert anon.put("/v1/buckets/two/objects/k", content=b"x", headers=headers).status_code == 403


def test_revoked_key_rejected(client, anon):
    issued = client.post("/admin/keys", json={"label": "tmp", "grants": [{"bucket": "*", "perm": "r"}]}).json()
    headers = {"X-Stratum-Key": issued["access_key"], "X-Stratum-Secret": issued["secret"]}
    assert anon.get("/v1/buckets", headers=headers).status_code == 200
    client.delete(f"/admin/keys/{issued['access_key']}")
    assert anon.get("/v1/buckets", headers=headers).status_code == 401


def test_secret_not_stored_in_plaintext(client, container):
    issued = client.post("/admin/keys", json={"label": "x", "grants": [{"bucket": "*", "perm": "r"}]}).json()
    with container.db.read() as conn:
        row = conn.execute("SELECT secret_hash FROM access_keys WHERE access_key=?",
                           (issued["access_key"],)).fetchone()
    assert row["secret_hash"] != issued["secret"]
    assert len(row["secret_hash"]) == 64


def test_presigned_get(client, anon, bucket):
    put(client, bucket, "share.txt", b"public-ish")
    issued = client.post("/admin/keys", json={"label": "signer", "grants": [{"bucket": "demo", "perm": "rw"}]}).json()
    headers = {"X-Stratum-Key": issued["access_key"], "X-Stratum-Secret": issued["secret"]}
    signed = anon.post("/v1/presign", json={"bucket": "demo", "key": "share.txt", "ttl": 60},
                       headers=headers).json()
    assert signed["algorithm"] == "HMAC-SHA256"

    res = anon.get(signed["url"].replace("http://testserver", ""))
    assert res.status_code == 200
    assert res.content == b"public-ish"


def test_presign_rejects_tampering(client, anon, bucket):
    put(client, bucket, "a.txt", b"a")
    put(client, bucket, "b.txt", b"b")
    issued = client.post("/admin/keys", json={"label": "s", "grants": [{"bucket": "demo", "perm": "rw"}]}).json()
    headers = {"X-Stratum-Key": issued["access_key"], "X-Stratum-Secret": issued["secret"]}
    signed = anon.post("/v1/presign", json={"bucket": "demo", "key": "a.txt"}, headers=headers).json()
    tampered = signed["url"].replace("/a.txt?", "/b.txt?").replace("http://testserver", "")
    assert anon.get(tampered).status_code == 403


def test_presign_expired(client, anon, bucket):
    put(client, bucket, "a.txt", b"a")
    issued = client.post("/admin/keys", json={"label": "s", "grants": [{"bucket": "demo", "perm": "r"}]}).json()
    headers = {"X-Stratum-Key": issued["access_key"], "X-Stratum-Secret": issued["secret"]}
    signed = anon.post("/v1/presign", json={"bucket": "demo", "key": "a.txt", "ttl": 1},
                       headers=headers).json()
    url = signed["url"].replace("http://testserver", "")
    assert anon.get(url).status_code == 200
    # 把过期时间手动改到过去（签名随之失配，两种拒绝都算通过）
    import time
    expired = url.replace(f"X-Stratum-Expires={signed['expires_at']}",
                          f"X-Stratum-Expires={int(time.time()) - 10}")
    assert anon.get(expired).status_code == 403


def test_admin_only_endpoints(client, anon):
    issued = client.post("/admin/keys", json={"label": "r", "grants": [{"bucket": "*", "perm": "rw"}]}).json()
    headers = {"X-Stratum-Key": issued["access_key"], "X-Stratum-Secret": issued["secret"]}
    assert anon.get("/admin/overview", headers=headers).status_code == 403
    assert anon.post("/v1/buckets", json={"name": "nope"}, headers=headers).status_code == 403


def test_grants_validation(client):
    assert client.post("/admin/keys", json={"label": "bad", "grants": [{"bucket": "x", "perm": "x"}]}).status_code == 403
