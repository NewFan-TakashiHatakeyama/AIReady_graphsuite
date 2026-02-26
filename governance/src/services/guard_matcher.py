"""ガード照合ロジック — ExposureVector からガードカテゴリを照合する

詳細設計 8.2 節準拠
"""

from __future__ import annotations

from services.guard_config import GUARD_CATEGORIES


def match_guards(
    exposure_vectors: list[str],
    source: str,
) -> list[str]:
    """ExposureVector からガードカテゴリを照合する。

    Args:
        exposure_vectors: アイテムの露出要因リスト（例: ["public_link", "eeeu"]）
        source: データソース識別子（例: "m365", "box"）

    Returns:
        マッチしたガード ID のソート済みリスト（例: ["G2", "G3"]）
    """
    vector_set = set(exposure_vectors)
    matched: list[str] = []

    for guard in GUARD_CATEGORIES.values():
        if source not in guard.applicable_sources:
            continue

        if vector_set & guard.vectors:
            matched.append(guard.guard_id)

    return sorted(matched)
