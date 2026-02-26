"""pii_detector 単体テスト

PII 検出ロジック（Presidio / GiNZA 非依存部分）を検証する。
Presidio / GiNZA がインストールされていない環境でも実行可能。
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from services.pii_detector import (
    PIIDetectionResult,
    PIIEntity,
    aggregate_pii_results,
    classify_density,
    deduplicate_by_position,
)


class TestClassifyDensity:
    def test_none(self):
        assert classify_density(0) == "none"

    def test_low(self):
        assert classify_density(1) == "low"
        assert classify_density(5) == "low"
        assert classify_density(9) == "low"

    def test_medium(self):
        assert classify_density(10) == "medium"
        assert classify_density(30) == "medium"
        assert classify_density(49) == "medium"

    def test_high(self):
        assert classify_density(50) == "high"
        assert classify_density(100) == "high"


class TestDeduplicateByPosition:
    def test_no_overlap(self):
        entities = [
            PIIEntity(type="EMAIL", start=0, end=10, score=0.9),
            PIIEntity(type="PHONE", start=20, end=30, score=0.8),
        ]
        result = deduplicate_by_position(entities)
        assert len(result) == 2

    def test_overlap_keeps_higher_score(self):
        entities = [
            PIIEntity(type="EMAIL", start=0, end=10, score=0.7),
            PIIEntity(type="PERSON", start=5, end=15, score=0.9),
        ]
        result = deduplicate_by_position(entities)
        assert len(result) == 1
        assert result[0].type == "PERSON"

    def test_empty_list(self):
        result = deduplicate_by_position([])
        assert result == []

    def test_single_entity(self):
        entities = [PIIEntity(type="EMAIL", start=0, end=10, score=0.9)]
        result = deduplicate_by_position(entities)
        assert len(result) == 1


class TestAggregatePiiResults:
    def test_no_entities(self):
        result = aggregate_pii_results([], [])
        assert result.detected is False
        assert result.count == 0
        assert result.density == "none"
        assert result.high_risk_detected is False

    def test_low_density(self):
        presidio = [
            PIIEntity(type="EMAIL_ADDRESS", start=0, end=20, score=0.95),
            PIIEntity(type="PERSON", start=30, end=40, score=0.85),
        ]
        result = aggregate_pii_results(presidio, [])
        assert result.detected is True
        assert result.count == 2
        assert result.density == "low"
        assert result.high_risk_detected is False
        assert "EMAIL_ADDRESS" in result.types
        assert "PERSON" in result.types

    def test_medium_density(self):
        entities = [
            PIIEntity(type="PERSON", start=i * 20, end=i * 20 + 10, score=0.8)
            for i in range(15)
        ]
        result = aggregate_pii_results(entities, [])
        assert result.density == "medium"

    def test_high_density(self):
        entities = [
            PIIEntity(type="PERSON", start=i * 20, end=i * 20 + 10, score=0.8)
            for i in range(55)
        ]
        result = aggregate_pii_results(entities, [])
        assert result.density == "high"

    def test_high_risk_my_number(self):
        entities = [
            PIIEntity(type="MY_NUMBER", start=0, end=12, score=0.95),
        ]
        result = aggregate_pii_results(entities, [])
        assert result.detected is True
        assert result.high_risk_detected is True

    def test_high_risk_bank_account(self):
        entities = [
            PIIEntity(type="BANK_ACCOUNT_JP", start=0, end=10, score=0.8),
        ]
        result = aggregate_pii_results(entities, [])
        assert result.high_risk_detected is True

    def test_high_risk_credit_card(self):
        entities = [
            PIIEntity(type="CREDIT_CARD", start=0, end=16, score=0.9),
        ]
        result = aggregate_pii_results(entities, [])
        assert result.high_risk_detected is True

    def test_merge_presidio_and_ginza(self):
        presidio = [
            PIIEntity(type="EMAIL_ADDRESS", start=0, end=20, score=0.95),
        ]
        ginza = [
            PIIEntity(type="PERSON_NAME_JA", start=30, end=35, score=0.85),
        ]
        result = aggregate_pii_results(presidio, ginza)
        assert result.detected is True
        assert result.count == 2
        assert "EMAIL_ADDRESS" in result.types
        assert "PERSON_NAME_JA" in result.types


class TestPIIDetectionResult:
    def test_default(self):
        result = PIIDetectionResult()
        assert result.detected is False
        assert result.types == []
        assert result.count == 0
        assert result.density == "none"
        assert result.high_risk_detected is False
        assert result.details == []


class TestGetGinzaNlp:
    """_get_ginza_nlp のモデルロード + フォールバックをテストする。"""

    def setup_method(self):
        import services.pii_detector as mod
        mod._nlp = None

    def teardown_method(self):
        import services.pii_detector as mod
        mod._nlp = None

    def test_default_loads_ginza_model(self, monkeypatch):
        """デフォルト（GINZA_MODEL 未設定）では ja_ginza を読み込む"""
        monkeypatch.delenv("GINZA_MODEL", raising=False)

        mock_nlp_obj = MagicMock(name="nlp_ginza_default")
        mock_spacy = MagicMock()
        mock_spacy.load.return_value = mock_nlp_obj

        import services.pii_detector as mod
        with patch.dict(sys.modules, {"spacy": mock_spacy}):
            result = mod._get_ginza_nlp()

        mock_spacy.load.assert_called_once_with("ja_ginza")
        assert result is mock_nlp_obj

    def test_env_var_overrides_model(self, monkeypatch):
        """GINZA_MODEL 環境変数でモデルを切り替えられる"""
        monkeypatch.setenv("GINZA_MODEL", "ja_ginza")

        mock_nlp_obj = MagicMock(name="nlp_ginza")
        mock_spacy = MagicMock()
        mock_spacy.load.return_value = mock_nlp_obj

        import services.pii_detector as mod
        with patch.dict(sys.modules, {"spacy": mock_spacy}):
            result = mod._get_ginza_nlp()

        mock_spacy.load.assert_called_once_with("ja_ginza")
        assert result is mock_nlp_obj

    def test_fallback_custom_model_to_ginza(self, monkeypatch):
        """カスタム指定モデルのロード失敗時に ja_ginza にフォールバックする"""
        monkeypatch.setenv("GINZA_MODEL", "ja_ginza_custom")

        mock_nlp_obj = MagicMock(name="nlp_fallback")
        mock_spacy = MagicMock()
        mock_spacy.load.side_effect = [OSError("model not found"), mock_nlp_obj]

        import services.pii_detector as mod
        with patch.dict(sys.modules, {"spacy": mock_spacy}):
            result = mod._get_ginza_nlp()

        assert mock_spacy.load.call_count == 2
        load_args = [call.args[0] for call in mock_spacy.load.call_args_list]
        assert load_args == ["ja_ginza_custom", "ja_ginza"]
        assert result is mock_nlp_obj

    def test_both_models_fail_returns_none(self, monkeypatch):
        """カスタム指定モデル / ja_ginza の両方失敗時は None を返す"""
        monkeypatch.setenv("GINZA_MODEL", "ja_ginza_custom")

        mock_spacy = MagicMock()
        mock_spacy.load.side_effect = OSError("model not found")

        import services.pii_detector as mod
        with patch.dict(sys.modules, {"spacy": mock_spacy}):
            result = mod._get_ginza_nlp()

        assert result is None
        assert mock_spacy.load.call_count == 2

    def test_no_fallback_when_env_is_ja_ginza(self, monkeypatch):
        """GINZA_MODEL=ja_ginza 指定で失敗時はフォールバック不要（1 回で終了）"""
        monkeypatch.setenv("GINZA_MODEL", "ja_ginza")

        mock_spacy = MagicMock()
        mock_spacy.load.side_effect = OSError("model not found")

        import services.pii_detector as mod
        with patch.dict(sys.modules, {"spacy": mock_spacy}):
            result = mod._get_ginza_nlp()

        mock_spacy.load.assert_called_once_with("ja_ginza")
        assert result is None
