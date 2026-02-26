from __future__ import annotations

import io
import json

from services.summarizer import generate_summary


class _FakeBody:
    def __init__(self, payload: dict):
        self._buf = io.BytesIO(json.dumps(payload).encode("utf-8"))

    def read(self):
        return self._buf.read()


def test_generate_summary_success(monkeypatch):
    class _FakeClient:
        def invoke_model(self, **kwargs):
            return {"body": _FakeBody({"content": [{"text": "要約結果です"}]})}

    monkeypatch.setattr("services.summarizer._bedrock_client", _FakeClient())
    summary = generate_summary("これはテスト文書です。")
    assert summary == "要約結果です"


def test_generate_summary_fallback_on_error(monkeypatch):
    class _FakeClient:
        def invoke_model(self, **kwargs):
            raise RuntimeError("bedrock failed")

    monkeypatch.setattr("services.summarizer._bedrock_client", _FakeClient())
    text = "a" * 300
    summary = generate_summary(text)
    assert len(summary) == 200
