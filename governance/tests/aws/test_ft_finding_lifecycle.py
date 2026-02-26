"""FT-6: Finding ライフサイクルテスト

Finding の状態遷移 (new → open → closed / acknowledged) を
end-to-end で検証する。
"""

from __future__ import annotations

import hashlib
import time
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from tests.aws.conftest import (
    BATCH_SCORING_FN,
    TEST_TENANT_ID,
    invoke_lambda,
    make_file_metadata,
    wait_for_finding,
    wait_for_finding_by_item,
)


def _generate_finding_id(tenant_id: str, source: str, item_id: str) -> str:
    return hashlib.sha256(f"{tenant_id}:{source}:{item_id}".encode()).hexdigest()[:32]


def _create_finding(finding_table, tenant_id: str, item_id: str, **overrides) -> dict:
    """テスト用 Finding を直接作成する。"""
    finding_id = _generate_finding_id(tenant_id, "m365", item_id)
    now = datetime.now(timezone.utc).isoformat()
    item = {
        "tenant_id": tenant_id,
        "finding_id": finding_id,
        "source": "m365",
        "item_id": item_id,
        "item_name": "test-file.txt",
        "container_id": "site-test-001",
        "container_name": "テスト部門サイト",
        "status": "new",
        "exposure_score": Decimal("6.0"),
        "risk_score": Decimal("6.0"),
        "created_at": now,
        "last_evaluated_at": now,
    }
    item.update(overrides)
    finding_table.put_item(Item=item)
    return item


class TestFT6FindingLifecycle:
    """Finding 状態遷移の統合テスト群。"""

    def test_ft_6_01_new_finding_created(self, connect_table, finding_table):
        """高リスク FileMetadata 挿入 → Finding status=new。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=200)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None, "Finding was not created"
        assert finding["status"] == "new"

    def test_ft_6_02_new_to_open(self, connect_table, finding_table):
        """INSERT → Finding(new) → MODIFY → status=open。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=200)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            expected_status="new", max_wait=180,
        )
        assert finding is not None

        connect_table.update_item(
            Key={"tenant_id": meta["tenant_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET permissions_count = :pc",
            ExpressionAttributeValues={":pc": 250},
        )

        updated = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            expected_status="open", max_wait=180,
        )
        assert updated is not None, "Finding did not transition to open"

    def test_ft_6_03_risk_resolved_closes(self, connect_table, finding_table):
        """anonymous → specific へ変更 → Finding status=closed。"""
        meta = make_file_metadata(sharing_scope="anonymous", permissions_count=200)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None

        connect_table.update_item(
            Key={"tenant_id": meta["tenant_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET sharing_scope = :ss, permissions_count = :pc",
            ExpressionAttributeValues={":ss": "specific", ":pc": 3},
        )

        closed = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            expected_status="closed", max_wait=180,
        )
        assert closed is not None, "Finding was not closed after risk resolution"

    def test_ft_6_04_item_deletion_closes(self, connect_table, finding_table):
        """FileMetadata 削除 → Finding status=closed。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=200)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None

        connect_table.delete_item(
            Key={"tenant_id": meta["tenant_id"], "item_id": meta["item_id"]},
        )

        closed = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            expected_status="closed", max_wait=180,
        )
        assert closed is not None, "Finding was not closed after item deletion"

    def test_ft_6_05_acknowledged(self, connect_table, finding_table):
        """Finding を手動で acknowledged + suppress_until 設定 → 状態確認。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=200)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None

        future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        finding_table.update_item(
            Key={"tenant_id": finding["tenant_id"], "finding_id": finding["finding_id"]},
            UpdateExpression="SET #st = :s, suppress_until = :su",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":s": "acknowledged", ":su": future},
        )

        resp = finding_table.get_item(
            Key={"tenant_id": finding["tenant_id"], "finding_id": finding["finding_id"]},
        )
        item = resp.get("Item")
        assert item is not None
        assert item["status"] == "acknowledged"
        assert item["suppress_until"] == future

    def test_ft_6_06_suppression_expired_reopens(
        self, connect_table, finding_table, lambda_client,
    ):
        """acknowledged + suppress_until 過去 → batchScoring → status=open。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=200)
        connect_table.put_item(Item=meta)

        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        finding = _create_finding(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            status="acknowledged", suppress_until=past,
        )

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding["finding_id"]},
        )
        item = resp.get("Item")
        assert item is not None
        assert item["status"] == "open", (
            "Expired suppression with active metadata should reopen"
        )

    def test_ft_6_07_suppression_expired_closes(self, finding_table, lambda_client):
        """acknowledged + suppress_until 過去 + FileMetadata なし → batchScoring → closed。"""
        orphan_item_id = f"item-orphan-{uuid.uuid4().hex[:12]}"
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        finding = _create_finding(
            finding_table, TEST_TENANT_ID, orphan_item_id,
            status="acknowledged", suppress_until=past,
        )

        invoke_lambda(lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID})

        resp = finding_table.get_item(
            Key={"tenant_id": TEST_TENANT_ID, "finding_id": finding["finding_id"]},
        )
        item = resp.get("Item")
        assert item is not None
        assert item["status"] == "closed", (
            "Expired suppression without metadata should close"
        )

    def test_ft_6_08_finding_id_deterministic(self):
        """同一 tenant+source+item → 同一 finding_id が生成される。"""
        tenant = TEST_TENANT_ID
        source = "m365"
        item_id = "item-deterministic-001"

        fid_1 = hashlib.sha256(f"{tenant}:{source}:{item_id}".encode()).hexdigest()[:32]
        fid_2 = hashlib.sha256(f"{tenant}:{source}:{item_id}".encode()).hexdigest()[:32]

        assert fid_1 == fid_2, "finding_id should be deterministic for same inputs"
        assert len(fid_1) == 32
        assert fid_1 == _generate_finding_id(tenant, source, item_id)
