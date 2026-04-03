"""LineageEventモデル定義。

OpenLineage互換イベントを DynamoDB 永続化するための
シリアライズ/デシリアライズ責務を持つ。
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class LineageEvent:
    """DynamoDB に保存する系譜イベントの表現。

    OpenLineage RunEvent を永続化しやすい形へ平坦化したモデルで、
    `inputs` / `outputs` / `metadata` は DynamoDB 保存時に JSON 文字列化される。
    """

    tenant_id: str
    lineage_id: str
    event_type: str
    event_time: str
    job_namespace: str
    job_name: str
    run_id: str
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0
    status: str = "success"
    error_message: str | None = None
    ttl: int = 0

    def to_dynamodb_item(self) -> dict[str, Any]:
        """LineageEvent を DynamoDB 保存形式へシリアライズする。

        Args:
            なし。

        Returns:
            dict[str, Any]: 処理結果の辞書。

        Notes:
            inputs/outputs/metadata は JSON 文字列へ変換して保存互換性を保つ。
        """
        item = asdict(self)
        item["inputs"] = json.dumps(self.inputs, ensure_ascii=False)
        item["outputs"] = json.dumps(self.outputs, ensure_ascii=False)
        item["metadata"] = json.dumps(self.metadata, ensure_ascii=False)
        return item

    @classmethod
    def from_dynamodb_item(cls, item: dict[str, Any]) -> "LineageEvent":
        """DynamoDB Item から LineageEvent を復元する。

        Args:
            item: 対象アイテム。

        Returns:
            'LineageEvent': 処理結果。

        Notes:
            欠損フィールドには既定値を補完し、JSON 文字列列は型復元する。
        """
        return cls(
            tenant_id=item["tenant_id"],
            lineage_id=item["lineage_id"],
            event_type=item["event_type"],
            event_time=item["event_time"],
            job_namespace=item.get("job_namespace", "ai-ready-ontology"),
            job_name=item["job_name"],
            run_id=item.get("run_id", item["lineage_id"]),
            inputs=_load_list(item.get("inputs")),
            outputs=_load_list(item.get("outputs")),
            metadata=_load_dict(item.get("metadata")),
            duration_ms=int(item.get("duration_ms", 0)),
            status=item.get("status", "success"),
            error_message=item.get("error_message"),
            ttl=int(item.get("ttl", 0)),
        )


def _load_list(value: Any) -> list[dict[str, Any]]:
    """値を list[dict] として安全に復元する。

    Args:
        value: 変換対象値。

    Returns:
        list[dict[str, Any]]: 処理結果の一覧。

    Notes:
        文字列は JSON parse し、不正値は空配列にフォールバックする。
    """
    if isinstance(value, str):
        return json.loads(value) if value else []
    if isinstance(value, list):
        return value
    return []


def _load_dict(value: Any) -> dict[str, Any]:
    """値を dict として安全に復元する。

    Args:
        value: 変換対象値。

    Returns:
        dict[str, Any]: 処理結果の辞書。

    Notes:
        文字列は JSON parse し、不正値は空辞書にフォールバックする。
    """
    if isinstance(value, str):
        return json.loads(value) if value else {}
    if isinstance(value, dict):
        return value
    return {}
