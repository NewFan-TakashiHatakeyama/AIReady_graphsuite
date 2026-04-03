"""FT-4: リスク件数集計エンジン検証テスト。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from tests.aws.conftest import (
    TEST_TENANT_ID,
    make_file_metadata,
    wait_for_finding_by_item,
)

pytestmark = pytest.mark.aws

FINDING_WAIT_MAX = 300
FINDING_POLL_INTERVAL = 10


def _insert_and_wait(connect_table, finding_table, metadata: dict) -> dict | None:
    """FileMetadata を投入し、対応する Finding が生成されるまで待機する。"""
    filtered = {k: v for k, v in metadata.items() if v is not None}
    connect_table.put_item(Item=filtered)
    return wait_for_finding_by_item(
        finding_table,
        tenant_id=metadata["tenant_id"],
        item_id=metadata["item_id"],
        max_wait=FINDING_WAIT_MAX,
        interval=FINDING_POLL_INTERVAL,
    )


class TestFT4ScoringEngine:
    """FT-4: 件数集計検証（主要ケース）"""

    def test_ft_4_01_anonymous_link_vector_count(self, connect_table, finding_table):
        """Anyone リンクのみ → 露出ベクトル件数が保存される。"""
        meta = make_file_metadata(
            sharing_scope="anonymous",
            permissions_count=1,
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert finding["exposure_vector_counts"].get("public_link", 0) >= 1
        assert int(finding["total_detected_risks"]) >= 1

    def test_ft_4_02_org_link_plus_eeeu(self, connect_table, finding_table):
        """組織リンク + EEEU → 複数ベクトルの件数が反映される。"""
        perms = json.dumps({
            "entries": [
                {"identity": {"displayName": "Everyone except external users"}}
            ]
        })
        meta = make_file_metadata(
            sharing_scope="organization",
            permissions=perms,
            permissions_count=150,
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        vector_counts = finding["exposure_vector_counts"]
        assert vector_counts.get("org_link", 0) >= 1 or vector_counts.get("org_link_view", 0) >= 1
        assert int(finding["total_detected_risks"]) >= 1

    def test_ft_4_03_anonymous_plus_guest_plus_broken(self, connect_table, finding_table):
        """Anyone + ゲスト + 継承崩れ → ベクトル件数が増える。"""
        perms = json.dumps({
            "entries": [
                {"identity": {"userType": "guest", "email": "ext@partner.com"}}
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
        vector_counts = finding["exposure_vector_counts"]
        assert vector_counts.get("public_link", 0) >= 1
        assert vector_counts.get("guest", 0) >= 1 or vector_counts.get("guest_direct_share", 0) >= 1

    def test_ft_4_04_private_low_risk(self, connect_table, finding_table):
        """Private（露出なし）でも Finding は生成され、件数は 0 許容。"""
        meta = make_file_metadata(
            sharing_scope="specific",
            permissions_count=3,
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert int(finding["total_detected_risks"]) >= 0

    def test_ft_4_05_content_categories_counted(self, connect_table, finding_table):
        """content_signals のカテゴリは risk_type_counts に反映される。"""
        meta = make_file_metadata(
            sharing_scope="organization",
            permissions_count=150,
        )
        meta["source_metadata"] = json.dumps(
            {
                "content_signals": {
                    "doc_sensitivity_level": "high",
                    "doc_categories": ["payroll", "customer_list"],
                    "contains_pii": True,
                    "contains_secret": False,
                    "confidence": 0.92,
                }
            }
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert finding["risk_type_counts"].get("payroll", 0) == 1
        assert finding["risk_type_counts"].get("customer_list", 0) == 1
        assert finding["risk_type_counts"].get("pii", 0) >= 1

    def test_ft_4_06_secret_and_pii_flags_counted(self, connect_table, finding_table):
        """contains_secret / contains_pii フラグは件数に反映される。"""
        meta = make_file_metadata(
            sharing_scope="organization",
            permissions_count=150,
        )
        meta["source_metadata"] = json.dumps(
            {
                "content_signals": {
                    "doc_sensitivity_level": "critical",
                    "doc_categories": [],
                    "contains_pii": True,
                    "contains_secret": True,
                    "confidence": 0.95,
                }
            }
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert finding["risk_type_counts"].get("pii", 0) >= 1
        assert finding["risk_type_counts"].get("secret", 0) >= 1

    def test_ft_4_07_recent_activity(self, connect_table, finding_table):
        """更新時に total_detected_risks が保持される。"""
        three_days_ago = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        meta = make_file_metadata(
            sharing_scope="organization",
            permissions_count=150,
        )
        meta["modified_at"] = three_days_ago
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert "total_detected_risks" in finding

    def test_ft_4_08_stale_activity(self, connect_table, finding_table):
        """古いファイルでも件数集計は実行される。"""
        hundred_days_ago = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        meta = make_file_metadata(
            sharing_scope="organization",
            permissions_count=150,
        )
        meta["modified_at"] = hundred_days_ago
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert "risk_type_counts" in finding
