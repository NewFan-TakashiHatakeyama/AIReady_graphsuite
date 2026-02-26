"""FT-4: スコアリングエンジン検証テスト

FileMetadata を Connect テーブルに投入し、DynamoDB Streams → analyzeExposure
を経由して生成される Finding のスコアが詳細設計 6 章と一致することを検証する。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

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
    """FT-4: スコアリングエンジン検証（8 テストケース）"""

    def test_ft_4_01_anonymous_link_exposure_score(self, connect_table, finding_table):
        """Anyone リンクのみ → ExposureScore = 5.0"""
        meta = make_file_metadata(
            sharing_scope="anonymous",
            permissions_count=1,
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert Decimal(str(finding["exposure_score"])) == Decimal("5.0")

    def test_ft_4_02_org_link_plus_eeeu(self, connect_table, finding_table):
        """組織リンク + EEEU → ExposureScore = 4.1"""
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
        assert Decimal(str(finding["exposure_score"])) == Decimal("4.1")

    def test_ft_4_03_anonymous_plus_guest_plus_broken(self, connect_table, finding_table):
        """Anyone + ゲスト + 継承崩れ → ExposureScore = 6.2"""
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
        assert Decimal(str(finding["exposure_score"])) == Decimal("6.2")

    def test_ft_4_04_private_low_risk(self, connect_table, finding_table):
        """Private（露出なし）→ ExposureScore = 1.0, RiskScore < 2.0 → Finding 未生成"""
        meta = make_file_metadata(
            sharing_scope="specific",
            permissions_count=3,
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is None, "低リスクアイテムに対して Finding が生成された"

    def test_ft_4_05_label_confidential(self, connect_table, finding_table):
        """sensitivity_label=Confidential → SensitivityScore = 3.0"""
        meta = make_file_metadata(
            sharing_scope="organization",
            sensitivity_label="Confidential",
            permissions_count=150,
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert Decimal(str(finding["sensitivity_score"])) == Decimal("3.0")

    def test_ft_4_06_filename_salary(self, connect_table, finding_table):
        """item_name=給与一覧.xlsx → SensitivityScore >= 2.0"""
        meta = make_file_metadata(
            item_name="給与一覧.xlsx",
            sharing_scope="organization",
            permissions_count=150,
        )
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert Decimal(str(finding["sensitivity_score"])) >= Decimal("2.0")

    def test_ft_4_07_recent_activity(self, connect_table, finding_table):
        """modified_at = 3 日前 → ActivityScore = 2.0"""
        three_days_ago = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        meta = make_file_metadata(
            sharing_scope="organization",
            permissions_count=150,
        )
        meta["modified_at"] = three_days_ago
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert Decimal(str(finding["activity_score"])) == Decimal("2.0")

    def test_ft_4_08_stale_activity(self, connect_table, finding_table):
        """modified_at = 100 日前 → ActivityScore = 0.5"""
        hundred_days_ago = (datetime.now(timezone.utc) - timedelta(days=100)).isoformat()
        meta = make_file_metadata(
            sharing_scope="organization",
            permissions_count=150,
        )
        meta["modified_at"] = hundred_days_ago
        finding = _insert_and_wait(connect_table, finding_table, meta)
        assert finding is not None, "Finding が生成されなかった"
        assert Decimal(str(finding["activity_score"])) == Decimal("0.5")
