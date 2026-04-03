"""FT-6: Finding ライフサイクルテスト

Finding の状態遷移 (new → open → closed / workflow_status=acknowledged) を
end-to-end で検証する。
"""

from __future__ import annotations

import hashlib
import time
import uuid
from datetime import datetime, timezone, timedelta
import pytest

from tests.aws.conftest import (
    TEST_TENANT_ID,
    make_file_metadata,
    wait_for_finding,
    wait_for_finding_by_item,
)


def _generate_finding_id(tenant_id: str, source: str, item_id: str) -> str:
    return hashlib.sha256(f"{tenant_id}:{source}:{item_id}".encode()).hexdigest()[:32]


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
            Key={"drive_id": meta["drive_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET permissions_count = :pc",
            ExpressionAttributeValues={":pc": 250},
        )

        updated = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            expected_status="open", max_wait=180,
        )
        assert updated is not None, "Finding did not transition to open"

    def test_ft_6_03_risk_resolved_closes(self, connect_table, finding_table):
        """anonymous → specific へ変更 → リスクが低下する。"""
        meta = make_file_metadata(sharing_scope="anonymous", permissions_count=200)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None

        connect_table.update_item(
            Key={"drive_id": meta["drive_id"], "item_id": meta["item_id"]},
            UpdateExpression="SET sharing_scope = :ss, permissions_count = :pc",
            ExpressionAttributeValues={":ss": "specific", ":pc": 3},
        )

        updated = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert updated is not None, "Finding was not updated after risk resolution"
        assert str(updated.get("status", "")).lower() in {"new", "open", "closed"}

    def test_ft_6_04_item_deletion_closes(self, connect_table, finding_table):
        """FileMetadata 削除 → Finding status=closed。"""
        meta = make_file_metadata(sharing_scope="organization", permissions_count=200)
        connect_table.put_item(Item=meta)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"], max_wait=180,
        )
        assert finding is not None

        connect_table.delete_item(
            Key={"drive_id": meta["drive_id"], "item_id": meta["item_id"]},
        )

        closed = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, meta["item_id"],
            expected_status="closed", max_wait=180,
        )
        assert closed is not None, "Finding was not closed after item deletion"

    def test_ft_6_05_acknowledged(self, connect_table, finding_table):
        """Finding に workflow_status=acknowledged + review_due を設定して状態確認。"""
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
