"""JSON 由来の型揺れを吸収する正規化ユーティリティ。"""

from __future__ import annotations

import json
from typing import Any


def parse_json_container(value: Any) -> dict[str, Any] | list[Any] | None:
    """dict/list/JSON文字列を dict/list へ寄せる。"""
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        if not value:
            return None
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, (dict, list)) else None
    return None


def parse_json_dict(value: Any, *, default: dict[str, Any] | None = None) -> dict[str, Any]:
    """値を dict に正規化し、失敗時は default を返す。"""
    container = parse_json_container(value)
    if isinstance(container, dict):
        return container
    return dict(default or {})


def parse_string_list(
    value: Any,
    *,
    parse_json_string: bool = True,
    fallback_single_string: bool = True,
) -> list[str]:
    """値を list[str] へ正規化する。"""
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        if not value:
            return []
        if parse_json_string:
            container = parse_json_container(value)
            if isinstance(container, list):
                return [str(v) for v in container]
        return [value] if fallback_single_string else []
    return []
