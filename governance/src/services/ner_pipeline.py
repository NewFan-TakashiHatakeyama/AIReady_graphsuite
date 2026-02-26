"""NER + 名詞チャンク抽出サービス。"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from shared.logger import get_logger

logger = get_logger(__name__)

_nlp_ja = None
_nlp_en = None


@dataclass
class NEREntity:
    text: str
    label: str
    start: int
    end: int
    confidence: float = 0.85
    pii_flag: bool = False


@dataclass
class NERDetectionResult:
    entities: list[NEREntity] = field(default_factory=list)
    noun_chunks: list[str] = field(default_factory=list)
    language: str = "unknown"


def detect_language(text: str) -> str:
    """テキストの言語を推定する。"""
    if not text:
        return "unknown"
    if re.search(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]", text):
        return "ja"
    return "en"


def _get_nlp_ja():
    global _nlp_ja
    if _nlp_ja is None:
        import spacy

        _nlp_ja = spacy.load("ja_ginza")
        logger.info("Loaded Japanese NLP pipeline: ja_ginza")
    return _nlp_ja


def _get_nlp_en():
    global _nlp_en
    if _nlp_en is None:
        import spacy

        _nlp_en = spacy.load("en_core_web_trf")
        logger.info("Loaded English NLP pipeline: en_core_web_trf")
    return _nlp_en


def extract_ner_and_noun_chunks(text: str) -> NERDetectionResult:
    """テキストから NER と名詞チャンクを抽出する。"""
    if not text or not text.strip():
        return NERDetectionResult()

    language = detect_language(text)
    nlp = _get_nlp_ja() if language == "ja" else _get_nlp_en()
    doc = nlp(text)

    entities = [
        NEREntity(
            text=ent.text,
            label=ent.label_,
            start=ent.start_char,
            end=ent.end_char,
            confidence=0.85,
        )
        for ent in doc.ents
    ]

    try:
        raw_chunks = [chunk.text.strip() for chunk in doc.noun_chunks if chunk.text.strip()]
    except Exception:
        raw_chunks = []
    noun_chunks = sorted(set(raw_chunks))

    return NERDetectionResult(
        entities=entities,
        noun_chunks=noun_chunks,
        language=language,
    )
