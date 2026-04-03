"""重複したドライブ接続を整理する判定ロジック。

同一ドライブに複数の connection が登録された場合に、
どの接続を正として残し、どの接続を退役させるかを決定する。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class CleanupDecision:
    """接続整理の判定結果。"""

    keep_connection_id: str
    retire_connection_ids: tuple[str, ...]
    reason: str


def is_placeholder_subscription_id(subscription_id: str) -> bool:
    """subscription_id が仮値かどうかを判定する。

    運用開始前や初期化直後に設定されるプレースホルダを
    実サブスクリプションと誤認しないために利用する。

    Args:
        subscription_id: 判定対象のサブスクリプション ID

    Returns:
        プレースホルダ形式であれば True
    """
    normalized = str(subscription_id or "").strip()
    if not normalized.startswith("sub-"):
        return False
    return normalized.endswith("-default") or "-conn-" in normalized


def has_real_subscription(connection: dict[str, Any]) -> bool:
    """接続に有効な実サブスクリプション ID が紐づいているか判定する。

    Args:
        connection: 接続情報辞書

    Returns:
        実サブスクリプションがある場合は True
    """
    subscription_id = str(connection.get("subscription_id") or "").strip()
    return bool(subscription_id and not is_placeholder_subscription_id(subscription_id))


def parse_iso_datetime(value: str) -> datetime:
    """ISO8601 文字列を `datetime` に変換する。

    変換できない場合は比較時に最も古い値となる `datetime.min` を返す。

    Args:
        value: ISO8601 形式の日時文字列

    Returns:
        変換後の datetime。変換不可時は datetime.min
    """
    normalized = str(value or "").strip()
    if not normalized:
        return datetime.min
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min


def choose_primary_connection(connections: list[dict[str, Any]]) -> CleanupDecision | None:
    """複数接続から主接続を 1 件選び、退役対象を返す。

    選定優先度:
    1. 実サブスクリプションを持つ接続
    2. ステータスが active/expiring の接続
    3. updated_at が新しい接続
    4. connection_id の辞書順（同率時の安定化）

    Args:
        connections: 同一 drive に紐づく接続一覧

    Returns:
        主接続と退役対象の判定結果。対象がなければ None
    """
    normalized = [row for row in connections if str(row.get("connection_id") or "").strip()]
    if not normalized:
        return None
    if len(normalized) == 1:
        connection_id = str(normalized[0]["connection_id"]).strip()
        return CleanupDecision(
            keep_connection_id=connection_id,
            retire_connection_ids=(),
            reason="single_connection_for_drive",
        )

    def _score(row: dict[str, Any]) -> tuple[int, int, int, str]:
        """並び替え用スコアを算出する。大きいほど優先。

        Args:
            row: 接続情報辞書

        Returns:
            優先度比較用タプル
        """
        status = str(row.get("status") or "").strip().lower()
        real_subscription = 1 if has_real_subscription(row) else 0
        active_status = 1 if status in {"active", "expiring"} else 0
        updated_at = int(parse_iso_datetime(str(row.get("updated_at") or "")).timestamp())
        connection_id = str(row.get("connection_id") or "").strip()
        return (real_subscription, active_status, updated_at, connection_id)

    sorted_rows = sorted(normalized, key=_score, reverse=True)
    keep_connection_id = str(sorted_rows[0]["connection_id"]).strip()
    retire_ids = tuple(
        str(row.get("connection_id") or "").strip()
        for row in sorted_rows[1:]
        if str(row.get("connection_id") or "").strip()
    )

    if has_real_subscription(sorted_rows[0]):
        reason = "keep_real_subscription_connection"
    else:
        reason = "keep_latest_connection_without_real_subscription"

    return CleanupDecision(
        keep_connection_id=keep_connection_id,
        retire_connection_ids=retire_ids,
        reason=reason,
    )
