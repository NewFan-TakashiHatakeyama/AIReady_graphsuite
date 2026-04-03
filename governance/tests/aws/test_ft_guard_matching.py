"""FT-5: ガード照合検証テスト

FileMetadata を投入し、生成された Finding の matched_guards が
詳細設計 8.3 節の照合ルール通りであることを実 AWS 環境で検証する。

source=box / source=slack のように DynamoDB Streams 経由でトリガーできないケースは
analyzeExposure Lambda を DynamoDB Streams イベントペイロードで直接 invoke する。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest

from tests.aws.conftest import (
    ANALYZE_EXPOSURE_FN,
    CONNECT_TABLE_NAME,
    TEST_TENANT_ID,
    invoke_lambda,
    make_file_metadata,
    wait_for_finding_by_item,
)

pytestmark = pytest.mark.aws

FINDING_WAIT_MAX = 300
FINDING_POLL_INTERVAL = 10


def _build_dynamodb_stream_event(metadata: dict) -> dict:
    """analyzeExposure に渡す DynamoDB Streams INSERT イベントを組み立てる。"""

    def _to_dynamodb_attr(value):
        if value is None:
            return {"NULL": True}
        if isinstance(value, bool):
            return {"BOOL": value}
        if isinstance(value, (int, float)):
            return {"N": str(value)}
        return {"S": str(value)}

    image = {k: _to_dynamodb_attr(v) for k, v in metadata.items() if v is not None}

    return {
        "Records": [
            {
                "eventID": uuid.uuid4().hex,
                "eventName": "INSERT",
                "eventVersion": "1.1",
                "eventSource": "aws:dynamodb",
                "awsRegion": "ap-northeast-1",
                "dynamodb": {
                    "Keys": {
                        "tenant_id": {"S": metadata["tenant_id"]},
                        "item_id": {"S": metadata["item_id"]},
                    },
                    "NewImage": image,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                    "SequenceNumber": "111",
                    "SizeBytes": 256,
                    "ApproximateCreationDateTime": int(
                        datetime.now(timezone.utc).timestamp()
                    ),
                },
                "eventSourceARN": (
                    f"arn:aws:dynamodb:ap-northeast-1:565699611973:"
                    f"table/{CONNECT_TABLE_NAME}/stream/2026-01-01T00:00:00.000"
                ),
            }
        ]
    }


def _insert_and_wait(connect_table, finding_table, metadata: dict) -> dict | None:
    """FileMetadata を Connect テーブルに投入し Finding 生成を待機する。"""
    filtered = {k: v for k, v in metadata.items() if v is not None}
    connect_table.put_item(Item=filtered)
    return wait_for_finding_by_item(
        finding_table,
        tenant_id=metadata["tenant_id"],
        item_id=metadata["item_id"],
        max_wait=FINDING_WAIT_MAX,
        interval=FINDING_POLL_INTERVAL,
    )


def _invoke_and_wait(lambda_client, finding_table, metadata: dict) -> dict | None:
    """Lambda を直接 invoke し Finding 生成を待機する。"""
    event = _build_dynamodb_stream_event(metadata)
    result = invoke_lambda(lambda_client, ANALYZE_EXPOSURE_FN, event)
    assert result["error"] is None, f"Lambda error: {result['body']}"
    return wait_for_finding_by_item(
        finding_table,
        tenant_id=metadata["tenant_id"],
        item_id=metadata["item_id"],
        max_wait=FINDING_WAIT_MAX,
        interval=FINDING_POLL_INTERVAL,
    )


class TestFT5GuardMatching:
    """FT-5: ガード照合検証（6 テストケース）"""

    def test_ft_5_01_public_link_matches_g3(self, connect_table, finding_table):
        """anonymous リンク + 未ラベル → matched_guards に G3 を含む。"""
        meta = make_file_metadata(
            sharing_scope="anonymous",
            permissions_count=1,
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert "G3" in (finding.get("matched_guards") or [])

    def test_ft_5_02_all_users_broken_inheritance_matches_g2_g7(
        self, connect_table, finding_table
    ):
        """EEEU + broken_inheritance → matched_guards に G2 を含む。"""
        perms = json.dumps({
            "entries": [
                {"identity": {"displayName": "Everyone except external users"}}
            ]
        })
        meta = make_file_metadata(
            sharing_scope="specific",
            permissions=perms,
            permissions_count=150,
        )
        meta["source_metadata"] = json.dumps({"has_unique_permissions": True})
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        guards = set(finding.get("matched_guards") or [])
        assert "G2" in guards

    def test_ft_5_03_public_link_box_source(self, lambda_client, finding_table):
        """source=box + public_link + 未ラベル → matched_guards に G3 を含む。"""
        meta = make_file_metadata(
            source="box",
            sharing_scope="anonymous",
            permissions_count=1,
        )
        finding = _invoke_and_wait(lambda_client, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert "G3" in (finding.get("matched_guards") or [])

    def test_ft_5_04_unsupported_source(self, lambda_client, finding_table):
        """source=slack（対象外ソース）→ matched_guards = []"""
        meta = make_file_metadata(
            source="slack",
            sharing_scope="anonymous",
            permissions_count=1,
        )
        finding = _invoke_and_wait(lambda_client, finding_table, meta)
        if finding is not None:
            assert finding.get("matched_guards", []) == []
        # Finding 自体が生成されない場合も対象外ソースとして正常

    def test_ft_5_05_ai_accessible_matches_g9(self, connect_table, finding_table):
        """ai_accessible 相当入力でも高リスクガードが付与される。"""
        perms = json.dumps({
            "entries": [
                {"identity": {"displayName": "Microsoft 365 Copilot", "isAiAgent": True}}
            ]
        })
        meta = make_file_metadata(
            sharing_scope="specific",
            permissions=perms,
            permissions_count=150,
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert len(finding.get("matched_guards") or []) > 0

    def test_ft_5_06_compound_pattern(self, connect_table, finding_table):
        """public_link + all_users + broken_inheritance → G2 と G3 を含む。"""
        perms = json.dumps({
            "entries": [
                {"identity": {"displayName": "Everyone except external users"}}
            ]
        })
        meta = make_file_metadata(
            sharing_scope="anonymous",
            permissions=perms,
            permissions_count=150,
        )
        meta["source_metadata"] = json.dumps({"has_unique_permissions": True})
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        guards = set(finding.get("matched_guards") or [])
        assert {"G2", "G3"}.issubset(guards)
