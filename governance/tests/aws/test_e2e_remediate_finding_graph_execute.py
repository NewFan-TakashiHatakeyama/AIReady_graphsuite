"""PoC / ステージング: remediateFinding が Graph 上の是正まで踏む E2E。

環境変数を明示的に設定したときだけ実行される（誤実行防止）。
Microsoft Graph による権限変更・DynamoDB の Finding 更新が発生する。

リスク再計算は Connect FileMetadata ストリーム経由（即時 process_item_batch は行わない）。

必須:
  GOVERNANCE_E2E_GRAPH_EXECUTE=1  （または true / yes）
  GOVERNANCE_E2E_EXECUTE_TENANT_ID
  GOVERNANCE_E2E_EXECUTE_FINDING_ID

任意:
  GOVERNANCE_E2E_APPROVE_BEFORE_EXECUTE=1
      ai_proposed から承認まで含めて試す。Lambda が approval_then_auto_execute の場合は
      承認応答内で execute まで完走するため、別途 execute を呼ばない。
  GOVERNANCE_E2E_EXPECT_ITEM_ID
      設定時、invoke 前に DynamoDB の Finding.item_id と一致することを検証するガード。
  GOVERNANCE_E2E_OPERATOR
      Lambda に渡す操作者（既定: graph-e2e-pytest）
"""

from __future__ import annotations

import json
import os

import pytest

from tests.aws.conftest import REMEDIATE_FINDING_FN, invoke_lambda

pytestmark = pytest.mark.aws


def _graph_e2e_env_enabled() -> bool:
    flag = os.environ.get("GOVERNANCE_E2E_GRAPH_EXECUTE", "").strip().lower()
    if flag not in ("1", "true", "yes"):
        return False
    if not os.environ.get("GOVERNANCE_E2E_EXECUTE_TENANT_ID", "").strip():
        return False
    if not os.environ.get("GOVERNANCE_E2E_EXECUTE_FINDING_ID", "").strip():
        return False
    return True


pytestmark = [
    pytest.mark.aws,
    pytest.mark.skipif(
        not _graph_e2e_env_enabled(),
        reason=(
            "Graph execute E2E: set GOVERNANCE_E2E_GRAPH_EXECUTE=1, "
            "GOVERNANCE_E2E_EXECUTE_TENANT_ID, GOVERNANCE_E2E_EXECUTE_FINDING_ID"
        ),
    ),
]


def _parse_handler_response(invoke_result: dict) -> tuple[int, dict]:
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


@pytest.fixture
def e2e_tenant_id() -> str:
    return os.environ["GOVERNANCE_E2E_EXECUTE_TENANT_ID"].strip()


@pytest.fixture
def e2e_finding_id() -> str:
    return os.environ["GOVERNANCE_E2E_EXECUTE_FINDING_ID"].strip()


@pytest.fixture
def e2e_operator() -> str:
    return os.environ.get("GOVERNANCE_E2E_OPERATOR", "graph-e2e-pytest").strip() or "graph-e2e-pytest"


class TestE2ERemediateFindingGraphExecute:
    def test_e2e_remediate_graph_execute_full_path(
        self,
        lambda_client,
        finding_table,
        e2e_tenant_id: str,
        e2e_finding_id: str,
        e2e_operator: str,
    ) -> None:
        expected_item = os.environ.get("GOVERNANCE_E2E_EXPECT_ITEM_ID", "").strip()
        if expected_item:
            row = finding_table.get_item(
                Key={"tenant_id": e2e_tenant_id, "finding_id": e2e_finding_id}
            ).get("Item")
            assert row is not None, "Finding が DynamoDB に存在しません"
            actual_item = str(row.get("item_id") or "").strip()
            assert actual_item == expected_item, (
                f"item_id ガード不一致: expected={expected_item!r} actual={actual_item!r}"
            )

        approve_first = (
            os.environ.get("GOVERNANCE_E2E_APPROVE_BEFORE_EXECUTE", "").strip().lower()
            in ("1", "true", "yes")
        )
        action = "approve" if approve_first else "execute"

        result = invoke_lambda(
            lambda_client,
            REMEDIATE_FINDING_FN,
            {
                "body": json.dumps(
                    {
                        "tenant_id": e2e_tenant_id,
                        "finding_id": e2e_finding_id,
                        "action": action,
                        "operator": e2e_operator,
                    }
                )
            },
        )
        status, body = _parse_handler_response(result)
        assert status == 200, f"Lambda returned {status}: {body}"

        result_obj = body.get("result") if isinstance(body.get("result"), dict) else {}
        pv = result_obj.get("post_verification") if isinstance(result_obj.get("post_verification"), dict) else {}
        assert pv.get("immediate_rescore") is False, f"immediate_rescore expected False: {pv!r}"
        assert pv.get("deferred_to") == "connect_filemetadata_stream", pv
        assert pv.get("success") is True, pv

        state = str(body.get("remediation_state") or "").strip().lower()
        assert state in ("executed", "manual_required"), f"unexpected remediation_state: {state!r}"

        updated = finding_table.get_item(
            Key={"tenant_id": e2e_tenant_id, "finding_id": e2e_finding_id}
        ).get("Item")
        assert updated is not None
        assert str(updated.get("status", "")).strip().lower() == "in_progress"
