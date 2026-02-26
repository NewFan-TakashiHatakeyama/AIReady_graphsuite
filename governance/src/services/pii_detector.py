"""PII 検出 — Presidio + GiNZA 統合による PII 検知

詳細設計 4.5 節準拠

Presidio Analyzer で英語 PII を、GiNZA NER で日本語 PII を検出し、
カスタム Recognizer でマイナンバー・口座番号・日本語電話番号を補完する。

Docker イメージ Lambda（ECR）で実行される前提。
import の遅延ロードにより、単体テスト時には Presidio/GiNZA を必須としない。
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from shared.logger import get_logger

logger = get_logger(__name__)

# 高リスク PII タイプ
HIGH_RISK_TYPES = frozenset({
    "MY_NUMBER",
    "BANK_ACCOUNT_JP",
    "CREDIT_CARD",
    "US_SSN",
    "PASSPORT_JP",
})


@dataclass
class PIIEntity:
    type: str
    start: int
    end: int
    score: float = 0.0


@dataclass
class PIIDetectionResult:
    detected: bool = False
    types: list[str] = field(default_factory=list)
    count: int = 0
    density: str = "none"
    high_risk_detected: bool = False
    details: list[PIIEntity] = field(default_factory=list)


# ─── Presidio Analyzer 初期化 ───

_analyzer = None
_nlp = None


def _get_presidio_analyzer():
    """Presidio Analyzer の遅延初期化（コールドスタート最適化）。"""
    global _analyzer
    if _analyzer is not None:
        return _analyzer

    try:
        from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern, RecognizerRegistry
        from presidio_analyzer.nlp_engine import NlpEngineProvider

        registry = RecognizerRegistry()
        try:
            registry.load_predefined_recognizers(languages=["en", "ja"])
        except TypeError:
            # 互換性確保: 古い API の場合は引数なしでロード
            registry.load_predefined_recognizers()

        my_number_recognizer = PatternRecognizer(
            supported_entity="MY_NUMBER",
            patterns=[
                Pattern(
                    name="my_number",
                    regex=r"(?<!\d)\d{4}[\s\-]?\d{4}[\s\-]?\d{4}(?!\d)",
                    score=0.7,
                ),
            ],
            context=["マイナンバー", "個人番号", "通知カード"],
            supported_language="ja",
        )
        registry.add_recognizer(my_number_recognizer)

        bank_account_recognizer = PatternRecognizer(
            supported_entity="BANK_ACCOUNT_JP",
            patterns=[
                Pattern(
                    name="bank_account",
                    regex=r"(?:普通|当座|貯蓄)\s*\d{7}",
                    score=0.6,
                ),
            ],
            context=["口座", "振込先", "銀行", "支店"],
            supported_language="ja",
        )
        registry.add_recognizer(bank_account_recognizer)

        phone_jp_recognizer = PatternRecognizer(
            supported_entity="PHONE_NUMBER_JP",
            patterns=[
                Pattern(
                    name="phone_jp",
                    regex=r"0\d{1,4}-?\d{1,4}-?\d{3,4}",
                    score=0.5,
                ),
            ],
            context=["電話", "TEL", "携帯", "連絡先"],
            supported_language="ja",
        )
        registry.add_recognizer(phone_jp_recognizer)

        model_candidates = ["en_core_web_trf", "en_core_web_sm", "en_core_web_lg"]
        for en_model in model_candidates:
            try:
                provider = NlpEngineProvider(
                    nlp_configuration={
                        "nlp_engine_name": "spacy",
                        "models": [{"lang_code": "en", "model_name": en_model}],
                    }
                )
                nlp_engine = provider.create_engine()
                _analyzer = AnalyzerEngine(
                    registry=registry,
                    nlp_engine=nlp_engine,
                    supported_languages=["en"],
                )
                logger.info(f"Presidio Analyzer initialized successfully (en={en_model})")
                return _analyzer
            except Exception as model_exc:
                logger.warning(f"Presidio NLP init failed for en={en_model}: {model_exc}")

        return None
    except ImportError:
        logger.warning("presidio_analyzer not available; Presidio PII detection disabled")
        return None


def _get_ginza_nlp():
    """GiNZA NLP モデルの遅延初期化。

    GINZA_MODEL 環境変数でモデルを切り替え可能。
    デフォルトは ja_ginza。
    フォールバック: カスタム指定モデル -> ja_ginza -> None
    """
    global _nlp
    if _nlp is not None:
        return _nlp

    import os
    model_name = os.environ.get("GINZA_MODEL", "ja_ginza")

    try:
        import spacy
        _nlp = spacy.load(model_name)
        logger.info(f"GiNZA NLP model loaded: {model_name}")
        return _nlp
    except (ImportError, OSError):
        logger.warning(f"GiNZA model '{model_name}' not available, trying fallback")

    if model_name != "ja_ginza":
        try:
            import spacy
            _nlp = spacy.load("ja_ginza")
            logger.info("GiNZA NLP model loaded (fallback): ja_ginza")
            return _nlp
        except (ImportError, OSError):
            pass

    logger.warning("GiNZA not available; Japanese NER disabled")
    return None


# ─── メイン API ───


def detect_pii(text: str) -> PIIDetectionResult:
    """テキストから PII を検出する（Presidio + GiNZA 統合）。"""
    if not text or not text.strip():
        return PIIDetectionResult()

    presidio_entities = _detect_presidio(text)
    ginza_entities = _detect_ginza(text)

    return aggregate_pii_results(presidio_entities, ginza_entities)


def _detect_presidio(text: str) -> list[PIIEntity]:
    """Presidio Analyzer で PII を検出する。"""
    analyzer = _get_presidio_analyzer()
    if analyzer is None:
        return []

    entities: list[PIIEntity] = []
    try:
        for lang in ["en", "ja"]:
            try:
                results = analyzer.analyze(text=text, language=lang)
                for result in results:
                    entities.append(
                        PIIEntity(
                            type=result.entity_type,
                            start=result.start,
                            end=result.end,
                            score=result.score,
                        )
                    )
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Presidio analysis failed: {e}")

    return entities


def _detect_ginza(text: str) -> list[PIIEntity]:
    """GiNZA NER で日本語 PII を検出する。"""
    nlp = _get_ginza_nlp()
    if nlp is None:
        return []

    entities: list[PIIEntity] = []
    try:
        max_chars = 100000
        analysis_text = text[:max_chars] if len(text) > max_chars else text

        doc = nlp(analysis_text)
        for ent in doc.ents:
            pii_type = _ginza_label_to_pii_type(ent.label_)
            if pii_type:
                entities.append(
                    PIIEntity(
                        type=pii_type,
                        start=ent.start_char,
                        end=ent.end_char,
                        score=0.85,
                    )
                )
    except Exception as e:
        logger.warning(f"GiNZA NER failed: {e}")

    return entities


def _ginza_label_to_pii_type(label: str) -> str | None:
    """GiNZA の固有表現ラベルを PII タイプにマッピングする。"""
    mapping = {
        "Person": "PERSON_NAME_JA",
        "Location": "ADDRESS_JA",
    }
    return mapping.get(label)


# ─── 結果集計 ───


def aggregate_pii_results(
    presidio_results: list[PIIEntity],
    ginza_results: list[PIIEntity],
) -> PIIDetectionResult:
    """Presidio と GiNZA の検出結果を統合して集計する。"""
    merged = deduplicate_by_position(presidio_results + ginza_results)

    type_counts = Counter(entity.type for entity in merged)
    unique_types = sorted(type_counts.keys())
    total_count = sum(type_counts.values())

    density = classify_density(total_count)
    high_risk_detected = bool(HIGH_RISK_TYPES & set(unique_types))

    return PIIDetectionResult(
        detected=total_count > 0,
        types=unique_types,
        count=total_count,
        density=density,
        high_risk_detected=high_risk_detected,
        details=merged,
    )


def classify_density(count: int) -> str:
    """PII 密度の分類。"""
    if count == 0:
        return "none"
    elif count <= 9:
        return "low"
    elif count <= 49:
        return "medium"
    else:
        return "high"


def deduplicate_by_position(entities: list[PIIEntity]) -> list[PIIEntity]:
    """位置ベースの重複排除（より高スコアの検出を優先）。"""
    if not entities:
        return []

    sorted_entities = sorted(entities, key=lambda e: (e.start, -e.score))
    result: list[PIIEntity] = [sorted_entities[0]]

    for entity in sorted_entities[1:]:
        last = result[-1]
        if entity.start < last.end:
            if entity.score > last.score:
                result[-1] = entity
        else:
            result.append(entity)

    return result
