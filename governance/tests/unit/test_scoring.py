"""スコアリングエンジンの単体テスト

詳細設計 6 章の算出例テーブル全行をテストケース化。
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

from services.exposure_vectors import FileMetadata
from services.scoring import (
    ExposureResult,
    _parse_label_name,
    calculate_activity_score,
    calculate_exposure_score,
    calculate_preliminary_sensitivity,
    calculate_risk_score,
    calculate_sensitivity_score,
    classify_risk_level,
)


def _make_metadata(**kwargs) -> FileMetadata:
    defaults = {"tenant_id": "t-001", "item_id": "item-001"}
    defaults.update(kwargs)
    return FileMetadata(**defaults)


def _iso_days_ago(days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.isoformat()


# ─── ExposureScore (6.1) ───


class TestExposureScore:
    def test_anyone_link_only(self):
        """Anyone リンクのみ → score=5.0"""
        m = _make_metadata(sharing_scope="anonymous")
        result = calculate_exposure_score(m)
        assert result.score == 5.0
        assert result.vectors == ["public_link"]

    def test_org_link_plus_eeeu(self):
        """組織リンク + EEEU → max(3.0, 3.5) + 3.0*0.2 = 4.1"""
        perms = json.dumps({
            "entries": [
                {"identity": {"displayName": "Everyone except external users"}}
            ]
        })
        m = _make_metadata(sharing_scope="organization", permissions=perms)
        result = calculate_exposure_score(m)
        assert result.score == 4.1
        assert "org_link" in result.vectors
        assert "all_users" in result.vectors

    def test_anyone_plus_guest_plus_broken(self):
        """Anyone + ゲスト + 継承崩れ → 5.0 + (4.0+2.0)*0.2 = 6.2"""
        perms = json.dumps({
            "entries": [
                {"identity": {"userType": "guest", "email": "ext@partner.com"}}
            ]
        })
        sm = json.dumps({"has_unique_permissions": True})
        m = _make_metadata(
            sharing_scope="anonymous",
            permissions=perms,
            source_metadata=sm,
        )
        result = calculate_exposure_score(m)
        assert result.score == 6.2

    def test_private_only(self):
        """Private のみ → score=1.0"""
        m = _make_metadata(sharing_scope="specific")
        result = calculate_exposure_score(m)
        assert result.score == 1.0
        assert result.vectors == []

    def test_score_cap_at_max(self):
        """スコアが上限を超えない"""
        perms = json.dumps({
            "entries": [
                {"identity": {"displayName": "Everyone except external users"}},
                {"identity": {"userType": "guest"}},
                {"identity": {"isExternalUser": True}},
            ]
        })
        sm = json.dumps({"has_unique_permissions": True})
        m = _make_metadata(
            sharing_scope="anonymous",
            permissions=perms,
            permissions_count=100,
            source_metadata=sm,
        )
        result = calculate_exposure_score(m)
        assert result.score <= 10.0


# ─── SensitivityScore 暫定 (6.2) ───


class TestPreliminarySensitivity:
    def test_confidential_label(self):
        label = json.dumps({"name": "Confidential"})
        m = _make_metadata(sensitivity_label=label)
        result = calculate_preliminary_sensitivity(m)
        assert result.score == 3.0

    def test_highly_confidential_label(self):
        label = json.dumps({"name": "極秘"})
        m = _make_metadata(sensitivity_label=label)
        result = calculate_preliminary_sensitivity(m)
        assert result.score == 4.0

    def test_salary_filename(self):
        m = _make_metadata(item_name="給与一覧.xlsx")
        result = calculate_preliminary_sensitivity(m)
        assert result.score == 2.0

    def test_password_filename(self):
        m = _make_metadata(item_name="password_list.txt")
        result = calculate_preliminary_sensitivity(m)
        assert result.score == 2.5

    def test_general_file(self):
        m = _make_metadata(item_name="meeting_notes.txt")
        result = calculate_preliminary_sensitivity(m)
        assert result.score == 1.0

    def test_label_takes_precedence_over_filename(self):
        """ラベル (4.0) がファイル名 (2.0) より高い場合、ラベルが採用される"""
        label = json.dumps({"name": "Highly Confidential"})
        m = _make_metadata(sensitivity_label=label, item_name="給与.xlsx")
        result = calculate_preliminary_sensitivity(m)
        assert result.score == 4.0


# ─── SensitivityScore 正式 (6.3) ───


class TestSensitivityScore:
    def test_secret_detected(self):
        result = calculate_sensitivity_score(
            pii_results={"detected": False, "density": "none", "high_risk_detected": False},
            secret_results={"detected": True},
        )
        assert result == 5.0

    def test_high_risk_pii(self):
        """マイナンバー検出 → 4.0"""
        result = calculate_sensitivity_score(
            pii_results={"detected": True, "density": "low", "high_risk_detected": True},
            secret_results={"detected": False},
        )
        assert result == 4.0

    def test_pii_low_density(self):
        result = calculate_sensitivity_score(
            pii_results={"detected": True, "density": "low", "high_risk_detected": False},
            secret_results={"detected": False},
        )
        assert result == 2.5

    def test_pii_medium_density(self):
        result = calculate_sensitivity_score(
            pii_results={"detected": True, "density": "medium", "high_risk_detected": False},
            secret_results={"detected": False},
        )
        assert result == 3.5

    def test_pii_high_density(self):
        result = calculate_sensitivity_score(
            pii_results={"detected": True, "density": "high", "high_risk_detected": False},
            secret_results={"detected": False},
        )
        assert result == 4.0

    def test_no_pii_no_secret(self):
        result = calculate_sensitivity_score(
            pii_results={"detected": False, "density": "none", "high_risk_detected": False},
            secret_results={"detected": False},
        )
        assert result == 1.0

    def test_existing_label_score_preserved(self):
        """ラベルスコア (3.0) が PII 低密度 (2.5) より高い場合は維持"""
        result = calculate_sensitivity_score(
            pii_results={"detected": True, "density": "low", "high_risk_detected": False},
            secret_results={"detected": False},
            existing_label_score=3.0,
        )
        assert result == 3.0


# ─── ActivityScore (6.4) ───


class TestActivityScore:
    def test_recent_3_days(self):
        m = _make_metadata(modified_at=_iso_days_ago(3))
        assert calculate_activity_score(m) == 2.0

    def test_within_30_days(self):
        m = _make_metadata(modified_at=_iso_days_ago(15))
        assert calculate_activity_score(m) == 1.5

    def test_within_90_days(self):
        m = _make_metadata(modified_at=_iso_days_ago(60))
        assert calculate_activity_score(m) == 1.0

    def test_over_90_days(self):
        m = _make_metadata(modified_at=_iso_days_ago(100))
        assert calculate_activity_score(m) == 0.5

    def test_none_modified_at(self):
        m = _make_metadata(modified_at=None)
        assert calculate_activity_score(m) == 1.0

    def test_invalid_date_string(self):
        """不正な日付文字列 → 1.0 を返す（Lines 191-192）"""
        m = _make_metadata(modified_at="not-a-date")
        assert calculate_activity_score(m) == 1.0


# ─── RiskScore + 閾値 (6.5) ───


class TestRiskScore:
    def test_calculation(self):
        result = calculate_risk_score(5.0, 4.0, 2.0, 1.0)
        assert result == 40.0

    def test_low_score(self):
        result = calculate_risk_score(1.0, 1.0, 0.5, 1.0)
        assert result == 0.5


class TestClassifyRiskLevel:
    def test_critical(self):
        assert classify_risk_level(50.0) == "critical"
        assert classify_risk_level(120.0) == "critical"

    def test_high(self):
        assert classify_risk_level(20.0) == "high"
        assert classify_risk_level(49.9) == "high"

    def test_medium(self):
        assert classify_risk_level(5.0) == "medium"
        assert classify_risk_level(19.9) == "medium"

    def test_low(self):
        assert classify_risk_level(2.0) == "low"
        assert classify_risk_level(4.9) == "low"

    def test_none(self):
        assert classify_risk_level(1.9) == "none"
        assert classify_risk_level(0.0) == "none"

    def test_threshold_boundary(self):
        """閾値 2.0 の境界値テスト"""
        assert classify_risk_level(1.99) == "none"
        assert classify_risk_level(2.0) == "low"


# ─── 詳細設計 6.5 スコアリング具体例テーブル ───


class TestScoringExamples:
    def test_anyone_link_my_number_recent(self):
        """Anyone リンク + マイナンバー検出 + 最近更新 → 40.0 (High)"""
        score = calculate_risk_score(5.0, 4.0, 2.0, 1.0)
        assert score == 40.0
        assert classify_risk_level(score) == "high"

    def test_org_link_gokuhi_90d(self):
        """組織リンク + ラベル「極秘」+ 3ヶ月放置 → 6.0 (Medium)"""
        score = calculate_risk_score(3.0, 4.0, 0.5, 1.0)
        assert score == 6.0
        assert classify_risk_level(score) == "medium"

    def test_eeeu_secret_recent(self):
        """EEEU + Secret 検出 + 最近更新 → 35.0 (High)"""
        score = calculate_risk_score(3.5, 5.0, 2.0, 1.0)
        assert score == 35.0
        assert classify_risk_level(score) == "high"

    def test_specific_pii_low_30d(self):
        """Specific users + PII 低密度 + 30日以内 → 5.62 (Medium)
        1.5 * 2.5 * 1.5 * 1.0 = 5.625, round(5.625, 2) = 5.62 (banker's rounding)
        """
        score = calculate_risk_score(1.5, 2.5, 1.5, 1.0)
        assert score == 5.62
        assert classify_risk_level(score) == "medium"

    def test_private_no_label_90d(self):
        """Private + ラベルなし + 90日放置 → 0.5 (閾値未満)"""
        score = calculate_risk_score(1.0, 1.0, 0.5, 1.0)
        assert score == 0.5
        assert classify_risk_level(score) == "none"


# ─── _parse_label_name ヘルパーのエッジケース ───


class TestParseLabelName:
    def test_none_returns_none(self):
        """None → None（Line 228）"""
        assert _parse_label_name(None) is None

    def test_empty_string_returns_none(self):
        """空文字列 → None"""
        assert _parse_label_name("") is None

    def test_dict_json_returns_name(self):
        assert _parse_label_name('{"name": "Confidential"}') == "Confidential"

    def test_non_dict_json_returns_str(self):
        """JSON パース結果が dict でない場合 → str 変換（Line 236）"""
        assert _parse_label_name("42") == "42"
        assert _parse_label_name('"plain"') == "plain"

    def test_invalid_json_returns_raw(self):
        """JSON パース失敗 → 入力をそのまま返す（Lines 237-238）"""
        assert _parse_label_name("Not JSON {") == "Not JSON {"
        assert _parse_label_name("Highly Confidential") == "Highly Confidential"
