"""remediateFinding Lambda の実 AWS スモーク（同期 invoke）。

Graph や DynamoDB の業務データを変更せず、デプロイ済みハンドラの到達性と
入力バリデーションを確認する。完全な execute の実結合は別途テナント上の
安全な drive_id / item_id を用いた手動または専用データで実施する。
"""

from __future__ import annotations

import json

import pytest

from tests.aws.conftest import REMEDIATE_FINDING_FN, TEST_TENANT_ID, invoke_lambda

pytestmark = pytest.mark.aws


def _parse_handler_response(invoke_result: dict) -> tuple[int, dict]:
    """Lambda の API Gateway 互換戻り値を (statusCode, body_dict) にする。"""
    assert invoke_result.get("error") is None, invoke_result
    outer = invoke_result["body"]
    if not isinstance(outer, dict):
        raise AssertionError(f"unexpected invoke body type: {type(outer)}")
    status = int(outer.get("statusCode", 500))
    raw = outer.get("body", "{}")
    if isinstance(raw, dict):
        return status, raw
    try:
        return status, json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise AssertionError(f"invalid JSON body: {raw!r}") from exc


class TestFTRemediateFindingSmoke:
    def test_ft_remediate_01_invoke_rejects_missing_ids(self, lambda_client):
        """tenant_id / finding_id 欠落時は 400。"""
        result = invoke_lambda(
            lambda_client,
            REMEDIATE_FINDING_FN,
            {"body": json.dumps({"action": "get"})},
        )
        status, body = _parse_handler_response(result)
        assert status == 400
        assert "required" in str(body.get("error", "")).lower()

    def test_ft_remediate_02_invoke_get_unknown_finding_400(self, lambda_client):
        """存在しない finding_id で get は ValueError → 400。"""
        result = invoke_lambda(
            lambda_client,
            REMEDIATE_FINDING_FN,
            {
                "body": json.dumps(
                    {
                        "tenant_id": TEST_TENANT_ID,
                        "finding_id": "ffffffffffffffffffffffffffffffff",
                        "action": "get",
                        "operator": "aws-smoke-test",
                    }
                )
            },
        )
        status, body = _parse_handler_response(result)
        assert status == 400
        assert "not found" in str(body.get("error", "")).lower()
