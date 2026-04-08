"""analyzeExposure — DynamoDB Streams トリガーによるリアルタイム Oversharing 検知

詳細設計 3.1–3.8 節準拠

トリガー: AIReadyConnect-FileMetadata の DynamoDB Streams (NEW_AND_OLD_IMAGES)
出力:
  - DynamoDB ExposureFinding テーブルへの Finding upsert
  - CloudWatch Metrics / Logs
"""

from __future__ import annotations

from datetime import datetime
import json
from typing import Any
from zoneinfo import ZoneInfo

from services.exposure_vectors import FileMetadata, extract_exposure_vectors
from services.finding_manager import handle_item_deletion, upsert_finding
from services.content_signal_analyzer import analyze_content_signals
from services.guard_matcher import (
    match_guards,
    resolve_detection_reasons,
    resolve_guard_reason_codes,
)
from services.scoring import (
    calculate_exposure_score,
    summarize_detected_risks,
)
from services.policy_evaluator import evaluate_policy_snapshot, normalize_vectors
from services.policy_models import PolicyContext
from services.policy_repository import list_active_policies
from services.policy_resolver import resolve_effective_policy
from shared.dynamodb import deserialize_image
from shared.logger import get_logger
from shared.metrics import emit_count, emit_duration

logger = get_logger(__name__)
TOKYO_TZ = ZoneInfo("Asia/Tokyo")

SCORING_RELEVANT_FIELDS = frozenset({
    "sharing_scope",
    "permissions",
    "source_metadata",
    "permissions_count",
    "sensitivity_label",
    "is_deleted",
    "size",
    "item_name",
    "modified_at",
    "workflow_status",
    "exception_type",
    "exception_review_due_at",
    "first_opened_at",
    "asset_criticality",
    "scan_mode",
    "scan_confidence",
    "ai_reachability",
})

def handler(event: dict, context: Any) -> dict:
    """DynamoDB Streams バッチを処理するエントリーポイント。

    Args:
        event: Streams イベント（`Records` 配列を含む）。
        context: Lambda context（将来拡張用。現状は未使用）。

    Returns:
        処理件数サマリ（`processed`, `errors`）。

    Notes:
        1レコードでも例外が発生した場合は再送を期待して例外を再送出する。
    """
    # Lambda 1 回の実行で受け取った Streams レコード群を順次処理する。
    records = event.get("Records", [])
    processed = 0
    errors = 0

    for record in records:
        try:
            process_record(record)
            processed += 1
        except Exception:
            errors += 1
            logger.error(
                "Record processing failed: %s",
                record.get("eventID", ""),
                exc_info=True,
                extra={"extra_data": {
                    "event_id": record.get("eventID", ""),
                    "event_name": record.get("eventName", ""),
                }},
            )
            raise

    logger.info(f"Batch complete: {processed} processed, {errors} errors")
    return {"processed": processed, "errors": errors}


def process_record(record: dict) -> None:
    """Streams レコード1件を評価し、Finding 更新と後続処理投入を行う。

    Args:
        record: DynamoDB Streams レコード（INSERT/MODIFY/REMOVE）。

    Notes:
        分岐の考え方:
        - `REMOVE` / `is_deleted=true`: 対応 Finding を closed 化。
        - スコア影響のない MODIFY: 処理スキップ。
        - それ以外: リスク再計算して Finding を upsert。
    """
    event_name = record.get("eventName", "")
    ddb = record.get("dynamodb", {})

    if event_name == "REMOVE":
        old_image = deserialize_image(ddb.get("OldImage"))
        handle_item_deletion(old_image)
        emit_count("AIReadyGov.FindingsClosed", dimensions={"Trigger": "REMOVE"})
        return

    # INSERT/MODIFY 系は New/Old 両方を参照して差分判定する。
    new_image = deserialize_image(ddb.get("NewImage"))
    old_image = deserialize_image(ddb.get("OldImage"))

    if new_image.get("is_deleted", False):
        handle_item_deletion(new_image)
        emit_count("AIReadyGov.FindingsClosed", dimensions={"Trigger": "is_deleted"})
        return

    # スコアに寄与しない属性変更は無駄な再計算を避けてスキップする。
    force_re_evaluate = _should_force_re_evaluate(new_image)
    if old_image and not force_re_evaluate and not is_scoring_relevant_change(new_image, old_image):
        logger.debug("変更がスコアリングに影響しない: skip")
        return
    if old_image and force_re_evaluate:
        logger.info(
            "Force re-evaluate enabled; bypassing scoring-relevant diff check.",
            extra={"extra_data": {
                "tenant_id": new_image.get("tenant_id", ""),
                "item_id": new_image.get("item_id", ""),
                "last_change_type": new_image.get("last_change_type", ""),
            }},
        )

    upsert_finding_from_file_metadata_image(new_image)

    # detectSensitivity 連携は過剰共有限定スコープのため廃止。


def is_scoring_relevant_change(new_image: dict, old_image: dict) -> bool:
    """MODIFY で再スコアリングが必要かを判定する。

    Args:
        new_image: 更新後イメージ。
        old_image: 更新前イメージ。

    Returns:
        主要評価項目に差分があれば `True`。
    """
    for field in SCORING_RELEVANT_FIELDS:
        new_val = new_image.get(field)
        old_val = old_image.get(field)
        if new_val != old_val:
            return True
    return False


def _should_force_re_evaluate(new_image: dict[str, Any]) -> bool:
    """再接続/手動再同期/是正スナップショット更新で差分判定をバイパスすべきか判定する。

    是正実行後の ``update_item`` は Graph の ``lastModifiedDateTime`` 等の都合で
    ``SCORING_RELEVANT_FIELDS`` 上の差分が空に見えることがある。
    その場合に MODIFY をスキップすると ``in_progress`` のまま再スコアされず完了に進まない。
    """
    if bool(new_image.get("force_re_evaluate", False)):
        return True
    last_change_type = str(new_image.get("last_change_type", "")).strip().lower()
    if last_change_type == "manual-sync-check":
        return True
    return last_change_type in {"remediation-execute", "remediation-rollback"}


def extract_metadata(image: dict) -> FileMetadata:
    """Streams イメージから `FileMetadata` DTO を生成する。

    Args:
        image: Streams の `NewImage` を deserialize した dict。

    Returns:
        スコア計算関数群が扱う正規化済み DTO。
    """
    return FileMetadata(
        tenant_id=str(image.get("tenant_id", "")),
        item_id=str(image.get("item_id", "")),
        source=str(image.get("source", "m365")),
        container_id=str(image.get("container_id", "")),
        container_name=str(image.get("container_name", "")),
        container_type=str(image.get("container_type", "")),
        item_name=str(image.get("item_name", "")),
        web_url=str(image.get("web_url", "")),
        sharing_scope=str(image.get("sharing_scope", "specific")),
        permissions=str(image.get("permissions", "{}")),
        permissions_count=int(image.get("permissions_count", 0)),
        sensitivity_label=image.get("sensitivity_label"),
        sensitivity_label_name=image.get("sensitivity_label_name"),
        mime_type=str(image.get("mime_type", "")),
        size=int(image.get("size", 0)),
        modified_at=image.get("modified_at"),
        is_deleted=bool(image.get("is_deleted", False)),
        raw_s3_key=str(image.get("raw_s3_key", "")),
        permissions_summary=image.get("permissions_summary"),
        source_metadata=image.get("source_metadata"),
        path=str(image.get("path", "")),
        parent_item_id=str(image.get("parent_item_id", "")),
        created_by_user_id=str(image.get("created_by_user_id", "")),
        modified_by_user_id=str(image.get("modified_by_user_id", "")),
    )


def _source_metadata_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def build_policy_context(
    metadata: FileMetadata,
    *,
    content_signals: dict[str, Any] | None = None,
    content_analysis: dict[str, Any] | None = None,
) -> PolicyContext:
    source_meta = _source_metadata_dict(metadata.source_metadata)
    principal_ids = source_meta.get("principal_ids")
    if not isinstance(principal_ids, list):
        principal_ids = []
    return PolicyContext(
        tenant_id=metadata.tenant_id,
        department_id=str(source_meta.get("department_id", "")),
        site_id=str(source_meta.get("site_id", "")),
        container_path=str(source_meta.get("container_path", metadata.path or "")),
        owner_id=str(source_meta.get("owner_id", metadata.created_by_user_id or "")),
        principal_ids=[str(v) for v in principal_ids if str(v).strip()],
        use_case=str(source_meta.get("use_case", "")).strip().lower(),
        item_metadata={
            "sharing_scope": metadata.sharing_scope,
            "permissions_count": metadata.permissions_count,
            "container_type": metadata.container_type,
            "item_name": metadata.item_name,
            "doc_sensitivity_level": str((content_signals or {}).get("doc_sensitivity_level", "none")),
            "doc_categories": list((content_signals or {}).get("doc_categories", [])),
            "contains_pii": bool((content_signals or {}).get("contains_pii", False)),
            "contains_secret": bool((content_signals or {}).get("contains_secret", False)),
            "analysis_confidence": float((content_signals or {}).get("confidence", 0.0)),
            "expected_audience": str((content_signals or {}).get("expected_audience", "internal_need_to_know")),
            "expected_department": str((content_signals or {}).get("expected_department", "unknown")),
            "expected_department_confidence": _safe_float(
                (content_signals or {}).get("expected_department_confidence", 0.0)
            ),
        },
        content_signals=dict(content_signals or {}),
        content_analysis=dict(content_analysis or {}),
    )


def _resolve_content_analysis(metadata: FileMetadata) -> tuple[dict[str, Any], dict[str, Any]]:
    source_meta = _source_metadata_dict(metadata.source_metadata)
    cached = source_meta.get("content_signals")
    if isinstance(cached, dict):
        return (
            {
                "doc_sensitivity_level": str(cached.get("doc_sensitivity_level", "none")).strip().lower() or "none",
                "doc_categories": list(cached.get("doc_categories", [])),
                "contains_pii": bool(cached.get("contains_pii", False)),
                "contains_secret": bool(cached.get("contains_secret", False)),
                "confidence": _safe_float(cached.get("confidence", 0.0)),
                "expected_audience": str(cached.get("expected_audience", "internal_need_to_know")).strip().lower()
                or "internal_need_to_know",
                "expected_department": str(cached.get("expected_department", "unknown")).strip() or "unknown",
                "expected_department_confidence": _safe_float(cached.get("expected_department_confidence", 0.0)),
                "justification": str(cached.get("justification", "")).strip(),
            },
            {
                "analysis_status": "cached",
                "decision_source": "source_metadata",
                "model_id": str(source_meta.get("content_model_id", "")),
                "prompt_version": str(source_meta.get("content_prompt_version", "")),
                "confidence": _safe_float(cached.get("confidence", 0.0)),
            },
        )
    result = analyze_content_signals(
        item_name=metadata.item_name,
        mime_type=metadata.mime_type,
        source_metadata=source_meta,
        extracted_text=str(source_meta.get("text", "") or source_meta.get("content_preview", "")),
    )
    return (
        {
            "doc_sensitivity_level": result.doc_sensitivity_level,
            "doc_categories": result.doc_categories,
            "contains_pii": result.contains_pii,
            "contains_secret": result.contains_secret,
            "confidence": result.confidence,
            "expected_audience": result.expected_audience,
            "expected_department": result.expected_department,
            "expected_department_confidence": result.expected_department_confidence,
            "justification": result.justification,
        },
        {
            "analysis_status": result.analysis_status,
            "decision_source": result.decision_source,
            "model_id": result.model_id,
            "prompt_version": result.prompt_version,
            "confidence": result.confidence,
        },
    )


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def should_enqueue_sensitivity_scan(
    finding: dict[str, Any],
    old_image: dict | None,
) -> bool:
    """Hard-cut互換: 機微スキャンは常に無効。"""
    del finding, old_image
    return False


def enqueue_sensitivity_scan(finding: dict[str, Any], metadata: FileMetadata) -> bool:
    """Hard-cut互換: 機微スキャンは常に無効。"""
    del finding, metadata
    return False


def upsert_finding_from_file_metadata_image(new_image: dict[str, Any]) -> None:
    """FileMetadata 相当の dict から露出スコアを計算し Finding を upsert する。

    DynamoDB Streams 経由の ``process_record`` と、是正直後の同期再スコアの双方から呼ぶ。
    ``new_image`` は ``deserialize_image`` 済みのプレーン dict（streams と同形）を想定する。
    """
    # --- Step 1: 検知件数要素算出 ---
    metadata = extract_metadata(new_image)
    content_signals, content_analysis = _resolve_content_analysis(metadata)

    exposure_result = calculate_exposure_score(metadata)
    exposure_result.vectors = normalize_vectors(exposure_result.vectors)
    risk_summary = summarize_detected_risks(
        exposure_vectors=exposure_result.vectors,
        content_signals=content_signals,
    )

    # --- Step 2: 判定カテゴリ（Guard）マッチング ---
    matched_guards = match_guards(
        exposure_vectors=exposure_result.vectors,
        source=metadata.source,
    )
    guard_reason_codes = resolve_guard_reason_codes(
        exposure_vectors=exposure_result.vectors,
        matched_guards=matched_guards,
    )
    detection_reasons = resolve_detection_reasons(exposure_result.vectors)
    policy_context = build_policy_context(
        metadata,
        content_signals=content_signals,
        content_analysis=content_analysis,
    )
    effective_policy = resolve_effective_policy(
        context=policy_context,
        policies=list_active_policies(metadata.tenant_id),
        exposure_vectors=exposure_result.vectors,
    )
    policy_eval = evaluate_policy_snapshot(effective_policy)

    # --- Step 3: Finding の create/update ---
    upsert_finding(
        tenant_id=metadata.tenant_id,
        item=metadata,
        exposure_result=exposure_result,
        risk_level=risk_summary.risk_level,
        risk_type_counts=risk_summary.risk_type_counts,
        exposure_vector_counts=risk_summary.exposure_vector_counts,
        total_detected_risks=risk_summary.total_detected_risks,
        workflow_status=None,
        exception_type=None,
        exception_review_due_at=None,
        matched_guards=matched_guards,
        guard_reason_codes=guard_reason_codes,
        detection_reasons=detection_reasons,
        decision=str(policy_eval.get("decision", "review")),
        effective_policy_id=str(policy_eval.get("effective_policy_id", "")),
        effective_policy_version=int(policy_eval.get("effective_policy_version", 1)),
        matched_policy_ids=list(policy_eval.get("matched_policy_ids", [])),
        decision_trace=list(policy_eval.get("decision_trace", [])),
        reason_codes=list(policy_eval.get("reason_codes", [])),
        remediation_mode=str(policy_eval.get("remediation_mode", "manual")),
        remediation_action=str(policy_eval.get("remediation_action", "request_review")),
        policy_hash=str(policy_eval.get("policy_hash", "")),
        decision_source=str(policy_eval.get("decision_source", "fallback")),
        expected_audience=str(policy_eval.get("expected_audience", content_signals.get("expected_audience", "internal_need_to_know"))),
        expected_department=str(policy_eval.get("expected_department", content_signals.get("expected_department", "unknown"))),
        expectation_gap_reason=str(policy_eval.get("expectation_gap_reason", "")),
        expectation_gap_score=float(policy_eval.get("expectation_gap_score", 0.0)),
        content_signals=content_signals,
        content_analysis=content_analysis,
    )
    if bool(content_signals.get("contains_pii", False)):
        emit_count("AIReadyGov.PIIDetected", dimensions={"TenantId": metadata.tenant_id})

