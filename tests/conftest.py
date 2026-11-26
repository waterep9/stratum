from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from stratum.config import Settings
from stratum.main import create_app

ADMIN = "test-admin-token"


@pytest.fixture()
def settings(tmp_path) -> Settings:
    return Settings(
        data_dir=str(tmp_path / "data"),
        admin_token=ADMIN,
        chunk_size=64 * 1024,
        gc_grace_seconds=0,
        max_object_bytes=8 * 1024 * 1024,
        presign_ttl=120,
        worker_interval=1,
        public_base_url="http://testserver",
    ).normalize()


@pytest.fixture()
def app(settings):
    return create_app(settings)


@pytest.fixture()
def client(app):
    with TestClient(app) as test_client:
        test_client.headers.update({"Authorization": f"Bearer {ADMIN}"})
        yield test_client


@pytest.fixture()
def anon(app):
    """不带任何凭证的客户端（用于鉴权与预签名测试）。"""
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def container(app):
    return app.state.container


@pytest.fixture()
def bucket(client):
    client.post("/v1/buckets", json={"name": "demo", "versioning": True})
    return "demo"


def put(client, bucket: str, key: str, data: bytes, **headers):
    return client.put(f"/v1/buckets/{bucket}/objects/{key}", content=data, headers=headers)


def get(client, bucket: str, key: str, **params):
    return client.get(f"/v1/buckets/{bucket}/objects/{key}", params=params)
