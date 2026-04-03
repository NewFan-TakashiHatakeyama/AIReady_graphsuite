"""コンテンツ品質スコア計算ユーティリティ。"""

from __future__ import annotations


def calculate_content_quality_score(
    freshness_score: float, uniqueness_score: float, relevance_score: float
) -> float:
    """鮮度・一意性・関連度を乗算して品質スコアを算出する。

    Args:
        freshness_score: 入力値。
        uniqueness_score: 入力値。
        relevance_score: 入力値。

    Returns:
        float: 処理結果。

    Notes:
        最小/最大値でクランプし、極端値での評価崩れを防ぐ。
    """
    score = float(freshness_score) * float(uniqueness_score) * float(relevance_score)
    return round(max(0.005, min(2.0, score)), 3)
