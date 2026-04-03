"""Canonical hash 計算ユーティリティ（エンティティ照合用）。"""

from __future__ import annotations

import hashlib


def compute_canonical_hash(value: str) -> str:
    """正規化済み値から canonical hash を生成する。

    Args:
        value: 変換対象値。

    Returns:
        str: 照合・重複判定で使う固定長キー（SHA-256 hex）。
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
