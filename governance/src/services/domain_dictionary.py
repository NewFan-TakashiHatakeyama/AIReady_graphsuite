"""ドメイン辞書の取得・補強ロジック。"""

from __future__ import annotations

import json

from shared.config import get_ssm_parameter
from shared.logger import get_logger

logger = get_logger(__name__)

SSM_DOMAIN_DICTIONARY = "/aiready/governance/domain_dictionary"
_cached_domain_dict: list[str] | None = None


def get_domain_dictionary() -> list[str]:
    """SSM からドメイン辞書を取得する。"""
    global _cached_domain_dict
    if _cached_domain_dict is not None:
        return _cached_domain_dict

    raw = get_ssm_parameter(SSM_DOMAIN_DICTIONARY, default="[]")
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            _cached_domain_dict = [str(v).strip() for v in parsed if str(v).strip()]
        else:
            _cached_domain_dict = []
    except json.JSONDecodeError:
        logger.warning("Domain dictionary is not valid JSON; ignored")
        _cached_domain_dict = []
    return _cached_domain_dict


def enrich_noun_chunks(chunks: list[str], domain_dict: list[str]) -> list[str]:
    """名詞チャンクをドメイン辞書で補強する。"""
    merged = set(c.strip() for c in chunks if c and c.strip())
    for term in domain_dict:
        normalized = term.strip()
        if normalized:
            merged.add(normalized)
    return sorted(merged)
