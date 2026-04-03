"""schemaTransform Lambda ハンドラ。

Connect の FileMetadata 変更を受け取り、UnifiedMetadata へ正規化して保存する。
削除イベントは論理削除（`is_deleted=true` + TTL）として扱う。
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from src.shared.analysis_enrichment import apply_document_analysis_fields
from src.shared.document_analysis_client import get_document_analysis
from src.shared.document_analysis_lookup import lookup_completed_document_analysis_fields
from src.shared.governance_client import (
    DEFAULT_GOVERNANCE_RESULT,
    lookup_governance_finding,
)
from src.shared.lineage_client import record_lineage_event
from src.shared.logger import log_structured
from src.shared.metrics import publish_metric  # test monkeypatch compatibility
from src.shared.ontology_target_policy import is_target_for_ontology
from src.shared.runtime_common import (
    get_dynamodb_resource,
    safe_publish_metric,
)
from src.shared.unified_soft_delete import execute_soft_delete_unified_item
from src.shared.unified_transform import build_unified_from_file_item

SOURCE = "microsoft365"
SCHEMA_VERSION = "1.0"
DELETE_TTL_DAYS = 30
TOKYO_TZ = ZoneInfo("Asia/Tokyo")

# テスト差し替え互換: 既存テストが module 変数を monkeypatch する。
_dynamodb_resource: Any | None = None

def handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """DynamoDB Streams から受けた FileMetadata 変更を一括反映する。

    Args:
        event: Streams イベント。`Records` に INSERT/MODIFY/REMOVE を含む。
        context: Lambda 実行コンテキスト（本関数では未使用）。

    Returns:
        dict[str, int]: 処理件数 `processed` と失敗件数 `errors`。

    Notes:
        各レコードは独立処理し、1件失敗しても他レコード処理を継続する。
        最後に処理件数・失敗件数をメトリクスへ送信する。
    """
    # Streams レコード群を取り出し、成功/失敗件数を初期化する。
    records = event.get("Records", [])
    processed = 0
    errors = 0

    # 各レコードは独立処理し、1件失敗しても全体処理は継続する。
    for record in records:
        try:
            _process_record(record)
            processed += 1
        except Exception as exc:
            errors += 1
            log_structured(
                "ERROR",
                "schemaTransform failed to process record",
                error=str(exc),
            )

    # バッチ結果をメトリクスへ反映する。
    _safe_publish_metric("SchemaTransformProcessed", processed)
    if errors > 0:
        _safe_publish_metric("SchemaTransformErrors", errors)

    return {"processed": processed, "errors": errors}


def _get_dynamodb_resource() -> Any:
    """DynamoDB Resource を遅延初期化で取得する。

    Args:
        なし。

    Returns:
        Any: boto3 DynamoDB Resource。

    Notes:
        同一コンテナ内で Resource を再利用し、クライアント生成コストを抑える。
    """
    global _dynamodb_resource
    if _dynamodb_resource is not None:
        return _dynamodb_resource
    return get_dynamodb_resource()


def _get_table(table_env_name: str) -> Any:
    """環境変数名から DynamoDB テーブルを解決する。

    Args:
        table_env_name: テーブル名を保持する環境変数キー。

    Returns:
        Any: DynamoDB Table オブジェクト。

    Notes:
        環境変数未設定時は例外を送出し、設定漏れを早期検知する。
    """
    # 環境変数からテーブル名を解決し、設定漏れは即時エラーにする。
    table_name = os.environ.get(table_env_name, "")
    if not table_name:
        raise ValueError(f"Environment variable '{table_env_name}' is required")
    return _get_dynamodb_resource().Table(table_name)


def _process_record(record: dict[str, Any]) -> None:
    """Streams 1レコードを UnifiedMetadata へ反映する。

    Args:
        record: DynamoDB Streams レコード。

    Returns:
        None: 戻り値なし。

    Notes:
        `REMOVE` または `is_deleted=true` は削除処理へ分岐する。
        フォルダ項目は対象外としてスキップし、通常項目はガバナンス/文書解析情報を
        付与して upsert する。
    """
    # Streams レコードからイベント種別と本体を取り出す。
    event_name = record.get("eventName", "")
    dynamodb_data = record.get("dynamodb", {})

    # REMOVE は Keys のみを使って論理削除処理へ委譲する。
    if event_name == "REMOVE":
        keys = _deserialize_dynamodb_image(dynamodb_data.get("Keys", {}))
        tenant_id = keys.get("tenant_id") or _extract_tenant_from_arn(record)
        item_id = keys.get("item_id", "")
        if not tenant_id or not item_id:
            return
        _handle_delete(tenant_id=tenant_id, item_id=item_id)
        return

    # INSERT/MODIFY は NewImage を復元して通常の upsert 処理へ進む。
    new_image = dynamodb_data.get("NewImage", {})
    if not new_image:
        return

    source_item = _deserialize_dynamodb_image(new_image)
    tenant_id = str(source_item.get("tenant_id", ""))
    item_id = str(source_item.get("item_id", ""))
    if not tenant_id or not item_id:
        return

    # ソース側で論理削除済みなら Unified も論理削除へ寄せる。
    if bool(source_item.get("is_deleted", False)):
        _handle_delete(tenant_id=tenant_id, item_id=item_id)
        return

    # フォルダは本文解析対象外のためスキップする。
    if bool(source_item.get("is_folder", False)):
        return

    # ガバナンス判定を参照し、失敗時はデフォルト判定で継続する。
    governance_result = _lookup_governance_defaulting(tenant_id=tenant_id, item_id=item_id)

    # 変換時刻と評価基準値を準備する。
    now_iso = datetime.now(TOKYO_TZ).isoformat()
    _gvr = str(governance_result.get("risk_level", "low") or "low").strip().lower()
    risk_level = "low" if _gvr == "none" else (_gvr or "low")
    file_name = str(source_item.get("name", ""))
    # Ontology 取り込み対象は「risk=low かつ 対象拡張子」のみ。
    if not is_target_for_ontology(
        file_name=file_name,
        risk_level=risk_level,
        ai_eligible=bool(governance_result.get("ai_eligible", False)),
        finding_status=str(governance_result.get("status", "closed") or "closed"),
    ):
        _delete_unified_if_exists(tenant_id=tenant_id, item_id=item_id)
        return

    # lineage 用の実行単位IDを採番する。
    lineage_id = str(uuid.uuid4())
    # FileMetadata を共通変換ロジックで UnifiedMetadata 辞書へ正規化する。
    unified_item = build_unified_from_file_item(
        item=source_item,
        tenant_id=tenant_id,
        governance_result=governance_result,
        source=SOURCE,
        schema_version=SCHEMA_VERSION,
        now_iso=now_iso,
        lineage_id=lineage_id,
    )
    # DocumentAnalysis completed のときのみ要約系フィールドを補完する。
    _apply_document_analysis_enrichment(unified_item)
    # DynamoDB は float 非対応のため、再帰的に Decimal へ変換してから upsert する。
    _get_table("UNIFIED_METADATA_TABLE").put_item(
        Item=_convert_floats_to_decimal(unified_item)
    )

    # 正常終了を lineage に記録し、追跡可能性を確保する。
    record_lineage_event(
        function_name=os.environ["LINEAGE_FUNCTION_NAME"],
        lineage_id=lineage_id,
        job_name="schemaTransform",
        input_dataset=f"FileMetadata/{tenant_id}/{item_id}",
        output_dataset=f"UnifiedMetadata/{tenant_id}/{item_id}",
        event_type="COMPLETE",
        metadata={"operation": "UPSERT"},
        tenant_id=tenant_id,
    )


def _handle_delete(tenant_id: str, item_id: str) -> None:
    """UnifiedMetadata 項目を論理削除状態へ更新する。

    Args:
        tenant_id: 対象テナントID。
        item_id: 対象アイテムID。

    Returns:
        None: 戻り値なし。

    Notes:
        既存項目をベースに `is_deleted` / `deleted_at` / `ttl` を更新する。
        削除操作は lineage へ `operation=DELETE` として記録する。
    """
    # 既存レコードを取得し、無い場合でも削除マーカーを作成できるようにする。
    table = _get_table("UNIFIED_METADATA_TABLE")
    existing = table.get_item(Key={"tenant_id": tenant_id, "item_id": item_id}).get("Item", {})
    now = datetime.now(TOKYO_TZ)
    execute_soft_delete_unified_item(
        table=table,
        tenant_id=tenant_id,
        item_id=item_id,
        existing=existing,
        now=now,
        ttl_days=DELETE_TTL_DAYS,
        record_lineage=True,
        lineage_callback=lambda: record_lineage_event(
            function_name=os.environ["LINEAGE_FUNCTION_NAME"],
            lineage_id=str(uuid.uuid4()),
            job_name="schemaTransform",
            input_dataset=f"FileMetadata/{tenant_id}/{item_id}",
            output_dataset=f"UnifiedMetadata/{tenant_id}/{item_id}",
            event_type="COMPLETE",
            metadata={"operation": "DELETE"},
            tenant_id=tenant_id,
        ),
    )
    # 削除件数メトリクスを加算する。
    _safe_publish_metric("SchemaTransformDeleted", 1)


def _delete_unified_if_exists(*, tenant_id: str, item_id: str) -> None:
    """既存 Unified がある場合のみ論理削除する。"""
    table = _get_table("UNIFIED_METADATA_TABLE")
    existing = table.get_item(Key={"tenant_id": tenant_id, "item_id": item_id}).get("Item")
    if not existing:
        return
    _handle_delete(tenant_id=tenant_id, item_id=item_id)


def _lookup_governance_defaulting(tenant_id: str, item_id: str) -> dict[str, Any]:
    """Governance Finding を取得し、失敗時は既定値へフォールバックする。

    Args:
        tenant_id: 対象テナントID。
        item_id: 対象ファイルID。

    Returns:
        dict[str, Any]: リスク/分類/PII 判定を含む辞書。

    Notes:
        参照失敗時は WARN を記録し、`DEFAULT_GOVERNANCE_RESULT` を返して継続する。
    """
    table_name = os.environ.get("GOVERNANCE_FINDING_TABLE", "").strip()
    if not table_name:
        log_structured(
            "WARN",
            "GOVERNANCE_FINDING_TABLE unset or empty in schemaTransform, defaulting",
            tenant_id=tenant_id,
            item_id=item_id,
        )
        return dict(DEFAULT_GOVERNANCE_RESULT)
    try:
        return lookup_governance_finding(
            tenant_id=tenant_id,
            file_id=item_id,
            finding_table_name=table_name,
        )
    except Exception as exc:
        log_structured(
            "WARN",
            "Governance finding lookup failed in schemaTransform, defaulting",
            tenant_id=tenant_id,
            item_id=item_id,
            error=str(exc),
        )
        return dict(DEFAULT_GOVERNANCE_RESULT)


def _apply_document_analysis_enrichment(unified_item: dict[str, Any]) -> None:
    """DocumentAnalysis の要約情報を UnifiedMetadata 辞書へ反映する。

    Args:
        unified_item: 更新対象の UnifiedMetadata 辞書。

    Returns:
        None: 戻り値なし。

    Notes:
        analysis_status が `completed` の場合のみ summary/keywords/embedding 等を設定する。
        未取得・未完了は miss メトリクスを記録してスキップする。
    """
    fields = lookup_completed_document_analysis_fields(
        tenant_id=str(unified_item.get("tenant_id") or ""),
        item_id=str(unified_item.get("item_id") or ""),
        lookup=get_document_analysis,
        on_lookup_error=lambda exc: log_structured(
            "WARN",
            "DocumentAnalysis lookup failed",
            tenant_id=unified_item.get("tenant_id"),
            item_id=unified_item.get("item_id"),
            error=str(exc),
        ),
        on_lookup_miss=lambda: _safe_publish_metric("DocumentAnalysisLookupMiss", 1),
    )
    apply_document_analysis_fields(unified_item, fields)


def _deserialize_dynamodb_image(image: dict[str, Any]) -> dict[str, Any]:
    """DynamoDB Streams の AttributeValue マップを通常辞書へ復元する。

    Args:
        image: Streams の `Keys` / `NewImage` 形式辞書。

    Returns:
        dict[str, Any]: Python 型へ変換済みの辞書。

    Notes:
        各属性の型変換は `_deserialize_attribute` に委譲する。
    """
    result: dict[str, Any] = {}
    # 各属性の型情報を見て Python 値へ変換する。
    for key, type_value in image.items():
        result[key] = _deserialize_attribute(type_value)
    return result


def _deserialize_attribute(type_value: dict[str, Any]) -> Any:
    """DynamoDB AttributeValue を Python 値へ変換する。

    Args:
        type_value: 1属性分の AttributeValue（S/N/BOOL/L/M など）。

    Returns:
        Any: 変換後の Python 値。

    Notes:
        未知フォーマットはそのまま返し、呼び出し側で扱えるようにする。
    """
    # 文字列型。
    if "S" in type_value:
        return type_value["S"]
    # 数値型（整数として解釈できる場合は int を優先）。
    if "N" in type_value:
        value = type_value["N"]
        return int(value) if value.isdigit() else float(value)
    # 真偽値型。
    if "BOOL" in type_value:
        return type_value["BOOL"]
    # Null 型。
    if "NULL" in type_value:
        return None
    # 文字列セット型。
    if "SS" in type_value:
        return type_value["SS"]
    # リスト型（再帰的に復元）。
    if "L" in type_value:
        return [_deserialize_attribute(v) for v in type_value["L"]]
    # マップ型（再帰的に復元）。
    if "M" in type_value:
        return {k: _deserialize_attribute(v) for k, v in type_value["M"].items()}
    # 未対応型はそのまま返し、呼び出し側で判断できるようにする。
    return type_value


def _extract_tenant_from_arn(record: dict[str, Any]) -> str:
    """Streams ARN から tenant_id を推定する。

    Args:
        record: Streams レコード。

    Returns:
        str: 推定した tenant_id。推定不能時は `TENANT_ID` 環境変数値。

    Notes:
        `FileMetadata-<tenant>` 命名規則を優先し、非準拠テーブルは環境変数へフォールバックする。
    """
    # ARN からテーブル名を取り出し、FileMetadata-<tenant> 規約を適用する。
    arn = record.get("eventSourceARN", "")
    if "/table/" in arn:
        table_name = arn.split("/table/")[1].split("/")[0]
    elif "/" in arn:
        table_name = arn.split("/")[1]
    else:
        table_name = ""
    extracted = table_name.replace("FileMetadata-", "")
    # 規約に合わないテーブル名は環境変数 TENANT_ID へフォールバックする。
    if extracted == table_name:
        return os.environ.get("TENANT_ID", "")
    return extracted


def _safe_publish_metric(metric_name: str, value: float) -> None:
    """CloudWatch メトリクスを安全に送信する。

    Args:
        metric_name: 送信メトリクス名。
        value: 送信値。

    Returns:
        None: 戻り値なし。

    Notes:
        送信失敗は WARN ログのみに留め、メイン処理を中断しない。
    """
    safe_publish_metric(metric_name, value)


def _convert_floats_to_decimal(value: Any) -> Any:
    """DynamoDB 保存前に float を Decimal へ再帰変換する。"""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_convert_floats_to_decimal(item) for item in value]
    if isinstance(value, dict):
        return {key: _convert_floats_to_decimal(item) for key, item in value.items()}
    return value
