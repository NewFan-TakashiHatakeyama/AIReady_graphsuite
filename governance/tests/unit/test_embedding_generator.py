from __future__ import annotations

import io
import json

from services.embedding_generator import (
    generate_embedding,
    save_embedding_to_s3,
    split_text_into_chunks,
)


class _FakeBody:
    def __init__(self, payload: dict):
        self._buf = io.BytesIO(json.dumps(payload).encode("utf-8"))

    def read(self):
        return self._buf.read()


def test_split_text_into_chunks():
    chunks = split_text_into_chunks("abcdef", chunk_size=2)
    assert chunks == ["ab", "cd", "ef"]


def test_generate_embedding(monkeypatch):
    class _FakeBedrock:
        def invoke_model(self, **kwargs):
            return {"body": _FakeBody({"embedding": [0.1, 0.2, 0.3]})}

    monkeypatch.setattr("services.embedding_generator._bedrock_client", _FakeBedrock())
    result = generate_embedding("abcdef", chunk_size=3)
    assert len(result) == 2
    assert result[0]["dimension"] == 1024
    assert result[0]["vector"] == [0.1, 0.2, 0.3]


def test_save_embedding_to_s3(monkeypatch):
    captured = {}

    class _FakeS3:
        def put_object(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setenv("VECTORS_BUCKET", "vectors-bucket")
    monkeypatch.setattr("services.embedding_generator._s3_client", _FakeS3())

    key = save_embedding_to_s3(
        tenant_id="tenant-001",
        item_id="item-001",
        embedding=[{"chunk_index": 0, "vector": [0.1]}],
    )
    assert key == "vectors/tenant-001/item-001.jsonl"
    assert captured["Bucket"] == "vectors-bucket"
