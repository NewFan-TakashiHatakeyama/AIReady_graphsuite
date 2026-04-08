"""Ontology 取り込み対象判定の共通ポリシー。"""

from __future__ import annotations

import os
from typing import Iterable

from src.shared.inference import OntologyInferenceService

_DEFAULT_TARGET_EXTENSIONS = {
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".xlsm",
    ".ppt",
    ".pptx",
    ".pdf",
    ".txt",
    ".md",
    ".csv",
    ".rtf",
}


def get_ontology_catalog_ingest_mode() -> str:
    """UnifiedMetadata カタログ取り込みの最終判定モード。

    - ``llm_veto``（既定）: Bedrock ``infer_ontology_target`` の結果が最終決定（後方互換）。
    - ``rule_low_extension``: 対象拡張子かつ低リスクかつ Finding 状態が適格なら LLM を呼ばずカタログに含める。
      ``ai_eligible`` は取り込み判定に使わない（DocumentAnalysis 等の下流で別ゲート）。
    """
    raw = str(os.environ.get("ONTOLOGY_CATALOG_INGEST_MODE", "")).strip().lower()
    if raw == "rule_low_extension":
        return "rule_low_extension"
    return "llm_veto"


def get_target_extensions() -> set[str]:
    """対象拡張子セットを返す。環境変数で上書き可能。"""
    raw = str(os.environ.get("ONTOLOGY_TARGET_EXTENSIONS", "")).strip()
    if not raw:
        return set(_DEFAULT_TARGET_EXTENSIONS)
    parsed: set[str] = set()
    for token in raw.split(","):
        ext = str(token or "").strip().lower()
        if not ext:
            continue
        if not ext.startswith("."):
            ext = f".{ext}"
        parsed.add(ext)
    return parsed or set(_DEFAULT_TARGET_EXTENSIONS)


def extract_file_extension(file_name: str) -> str:
    """ファイル名から拡張子を抽出する。"""
    name = str(file_name or "").strip()
    if "." not in name:
        return ""
    dot_index = name.rfind(".")
    if dot_index <= 0 or dot_index == len(name) - 1:
        return ""
    return name[dot_index:].lower()


def is_supported_extension(file_name: str, *, target_extensions: Iterable[str] | None = None) -> bool:
    """対象拡張子かどうかを判定する。"""
    ext = extract_file_extension(file_name)
    if not ext:
        return False
    allowed = set(target_extensions) if target_extensions is not None else get_target_extensions()
    return ext in allowed


def is_target_finding_status(finding_status: str) -> bool:
    """Ontology 取り込み対象となる Finding status かを判定する。"""
    return str(finding_status or "").strip().lower() in {"new", "open"}


def _normalize_risk_level(risk_level: str) -> str:
    rl = str(risk_level or "").strip().lower()
    return "low" if rl == "none" else rl


def is_eligible_finding_status_for_ontology(finding_status: str, risk_level: str) -> bool:
    """Finding 状態がオントロジー取り込みの前提を満たすか。

    new/open は従来どおり対象。closed / completed は低リスク時のみカタログ用途で許可する
    （是正完了後は ``status=completed`` となり ``closed`` ではない）。
    in_progress かつ低リスクも許可する（FileMetadata ストリーム上で analyzeExposure より先に
    schemaTransform が走った場合の再スコア済み行を取りこぼさないため）。
    """
    st = str(finding_status or "").strip().lower()
    if st in {"new", "open"}:
        return True
    if st in {"closed", "completed", "in_progress"}:
        return _normalize_risk_level(risk_level) == "low"
    return False


def is_target_for_ontology(
    *,
    file_name: str,
    risk_level: str,
    ai_eligible: bool,
    finding_status: str,
) -> bool:
    """Ontology カタログ（UnifiedMetadata）取り込み対象かを判定する。

    拡張子・Finding 状態のチェック後、環境変数 ``ONTOLOGY_CATALOG_INGEST_MODE`` に応じて
    ルールのみで決めるか、Bedrock 推論に委ねるかを切り替える。
    """
    if not is_supported_extension(file_name):
        return False
    if not is_eligible_finding_status_for_ontology(finding_status, risk_level):
        return False
    if get_ontology_catalog_ingest_mode() == "rule_low_extension":
        return _normalize_risk_level(risk_level) == "low"
    service = OntologyInferenceService()
    return service.infer_target_decision(
        file_name=file_name,
        risk_level=risk_level,
        ai_eligible=ai_eligible,
        finding_status=finding_status,
    )
