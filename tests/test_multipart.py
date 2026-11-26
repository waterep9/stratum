from __future__ import annotations

import os

from conftest import get


def _init(client, bucket, key, **kwargs):
    res = client.post(f"/v1/buckets/{bucket}/uploads", params={"key": key}, **kwargs)
    assert res.status_code == 201, res.text
    return res.json()["upload_id"]


def test_multipart_roundtrip(client, bucket):
    data = os.urandom(300 * 1024)
    upload_id = _init(client, bucket, "video/part.bin")
    parts = [data[:100 * 1024], data[100 * 1024:200 * 1024], data[200 * 1024:]]
    for idx, chunk in enumerate(parts, start=1):
        res = client.put(f"/v1/uploads/{upload_id}/parts/{idx}", content=chunk)
        assert res.status_code == 200, res.text
        assert res.headers["etag"].startswith('"')

    listed = client.get(f"/v1/uploads/{upload_id}/parts").json()
    assert [p["part_no"] for p in listed["parts"]] == [1, 2, 3]

    done = client.post(f"/v1/uploads/{upload_id}/complete")
    assert done.status_code == 200, done.text
    body = done.json()
    assert body["size"] == len(data)
    assert body["etag"].endswith("-3")
    assert get(client, bucket, "video/part.bin").content == data


def test_complete_is_idempotent(client, bucket):
    upload_id = _init(client, bucket, "idem.bin")
    client.put(f"/v1/uploads/{upload_id}/parts/1", content=b"a" * 70000)
    first = client.post(f"/v1/uploads/{upload_id}/complete").json()
    second = client.post(f"/v1/uploads/{upload_id}/complete")
    assert second.status_code == 200
    payload = second.json()
    assert payload["idempotent_replay"] is True
    assert payload["version_id"] == first["version_id"]
    versions = client.get("/v1/buckets/demo/versions").json()["items"]
    assert len(versions) == 1


def test_part_overwrite_keeps_last(client, bucket):
    upload_id = _init(client, bucket, "over.bin")
    first, second = b"F" * 70000, b"S" * 10
    client.put(f"/v1/uploads/{upload_id}/parts/1", content=first)
    client.put(f"/v1/uploads/{upload_id}/parts/2", content=second)
    client.put(f"/v1/uploads/{upload_id}/parts/1", content=b"X" * 70000)
    client.post(f"/v1/uploads/{upload_id}/complete")
    assert get(client, bucket, "over.bin").content == b"X" * 70000 + second


def test_abort_removes_parts(client, bucket, container):
    upload_id = _init(client, bucket, "abort.bin")
    client.put(f"/v1/uploads/{upload_id}/parts/1", content=b"x" * 70000)
    assert client.delete(f"/v1/uploads/{upload_id}").json()["aborted"] is True
    assert not os.path.isdir(os.path.join(container.settings.part_root, upload_id))
    assert client.post(f"/v1/uploads/{upload_id}/complete").status_code == 409


def test_part_numbers_must_be_contiguous(client, bucket):
    upload_id = _init(client, bucket, "gap.bin")
    client.put(f"/v1/uploads/{upload_id}/parts/1", content=b"a" * 70000)
    client.put(f"/v1/uploads/{upload_id}/parts/3", content=b"c" * 70000)
    res = client.post(f"/v1/uploads/{upload_id}/complete")
    assert res.status_code == 400
    assert "连续" in res.json()["error"]["message"]


def test_min_part_size_enforced(client, bucket):
    upload_id = _init(client, bucket, "tiny.bin")
    client.put(f"/v1/uploads/{upload_id}/parts/1", content=b"a" * 10)
    client.put(f"/v1/uploads/{upload_id}/parts/2", content=b"b" * 10)
    res = client.post(f"/v1/uploads/{upload_id}/complete")
    assert res.status_code == 400
    assert "最小部件" in res.json()["error"]["message"]


def test_unknown_upload(client):
    assert client.get("/v1/uploads/nope/parts").status_code == 404
    assert client.post("/v1/uploads/nope/complete").status_code == 404


def test_list_uploads(client, bucket):
    _init(client, bucket, "p1.bin")
    _init(client, bucket, "p2.bin")
    listing = client.get("/v1/buckets/demo/uploads").json()
    assert len(listing["uploads"]) == 2
