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

    new/open は従来どおり対象。closed は低リスク時のみドキュメントカタログ用途で許可する
    （Finding 不在時のデフォルト closed でも risk が low なら後段の LLM/フォールバックへ進める）。
    """
    st = str(finding_status or "").strip().lower()
    if st in {"new", "open"}:
        return True
    if st == "closed":
        return _normalize_risk_level(risk_level) == "low"
    return False


def is_target_for_ontology(
    *,
    file_name: str,
    risk_level: str,
    ai_eligible: bool,
    finding_status: str,
) -> bool:
    """Ontology 取り込み対象を LLM 推論で判定する。"""
    if not is_supported_extension(file_name):
        return False
    if not is_eligible_finding_status_for_ontology(finding_status, risk_level):
        return False
    service = OntologyInferenceService()
    return service.infer_target_decision(
        file_name=file_name,
        risk_level=risk_level,
        ai_eligible=ai_eligible,
        finding_status=finding_status,
    )
