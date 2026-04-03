"""Finding CRUD + ステータス遷移

詳細設計 7.1–7.4 節準拠
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from services.exposure_vectors import FileMetadata
from services.scoring import (
    ExposureResult,
    compute_ai_eligible,
)
from shared.config import get_env
from shared.dynamodb import float_to_decimal, get_table
from shared.logger import get_logger
from shared.metrics import emit_count

logger = get_logger(__name__)
TOKYO_TZ = ZoneInfo("Asia/Tokyo")

# テーブル参照（Lambda 初期化時に解決）
_finding_table = None


def _get_finding_table():
    """ExposureFinding テーブル参照を取得する。

    Returns:
        DynamoDB Table リソース。

    Notes:
        初回呼び出し時のみ環境変数からテーブル名を解決し、以降は再利用する。
    """
    global _finding_table
    if _finding_table is None:
        table_name = get_env("FINDING_TABLE_NAME")
        _finding_table = get_table(table_name)
    return _finding_table


def set_finding_table(table):
    """テスト用に Finding テーブル参照を差し替える。

    Args:
        table: 差し替えるテーブルオブジェクト。
    """
    global _finding_table
    _finding_table = table


# ─── 7.1 Finding の一意性 ───


def generate_finding_id(tenant_id: str, source: str, item_id: str) -> str:
    """Finding ID を生成する。

    Args:
        tenant_id: テナント ID。
        source: ソース識別子。
        item_id: アイテム ID。

    Returns:
        SHA-256 由来の短縮 Finding ID（32 文字）。

    Notes:
        同一入力から同一 ID を得る決定的生成方式。
        `tenant/source/item` の組み合わせで重複作成を防ぐ。
    """
    raw = f"{tenant_id}:{source}:{item_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


# ─── 7.2 Finding upsert ロジック ───


def upsert_finding(
    tenant_id: str,
    item: FileMetadata,
    exposure_result: ExposureResult,
    risk_level: str,
    risk_type_counts: dict[str, int],
    exposure_vector_counts: dict[str, int],
    total_detected_risks: int,
    matched_guards: list[str],
    workflow_status: str | None = None,
    exception_type: str | None = None,
    exception_review_due_at: str | None = None,
    guard_reason_codes: list[str] | None = None,
    detection_reasons: list[str] | None = None,
    finding_evidence: dict[str, Any] | None = None,
    decision: str | None = None,
    effective_policy_id: str | None = None,
    effective_policy_version: int | None = None,
    matched_policy_ids: list[str] | None = None,
    decision_trace: list[str] | None = None,
    reason_codes: list[str] | None = None,
    remediation_mode: str | None = None,
    remediation_action: str | None = None,
    policy_hash: str | None = None,
    content_signals: dict[str, Any] | None = None,
    content_analysis: dict[str, Any] | None = None,
    decision_source: str | None = None,
    expected_audience: str | None = None,
    expected_department: str | None = None,
    expectation_gap_reason: str | None = None,
    expectation_gap_score: float | None = None,
) -> dict[str, Any]:
    """Finding を作成または更新する（中核ロジック）。

    Args:
        tenant_id: テナント ID。
        item: ファイルメタデータ DTO。
        exposure_result: 露出スコア結果。
        sensitivity_result: 機微スコア結果。
        activity_score: 活動スコア。
        ai_amplification: AI 係数。
        risk_score: 総合リスクスコア。
        matched_guards: マッチしたガード ID 一覧。

    Returns:
        保存後の Finding 情報（`is_new` フラグ付き）。

    Notes:
        - 新規時は `status=new` で作成。
        - `workflow_status=acknowledged` でもスコア更新は継続する。
        - `sensitivity_scan_at` がある既存 Finding は、再計算時も既存 sensitivity を優先。
        - 更新時、上記 3 フィールドに `None` を渡した場合、既存 Finding が例外レジストリ相当
          （`workflow_status=acknowledged` または `exception_type` が `none` 以外）ならその値を保持する。
          それ以外は従来どおり FileMetadata 駆動の再評価として `workflow_status=open` / `exception_type=none` 等へ寄せる。
    """
    table = _get_finding_table()
    now = datetime.now(TOKYO_TZ).isoformat()
    finding_id = generate_finding_id(tenant_id, item.source, item.item_id)

    existing = get_finding(tenant_id, finding_id)
    initial_risk_level = str(risk_level or "none").strip().lower() or "none"
    stable_guard_reason_codes = sorted(set(guard_reason_codes or []))
    stable_detection_reasons = sorted(set(detection_reasons or []))
    stable_finding_evidence = _normalize_finding_evidence(
        finding_evidence or _build_finding_evidence(item)
    )
    stable_matched_policy_ids = sorted(set(str(v) for v in (matched_policy_ids or []) if str(v).strip()))
    stable_decision_trace = [str(v) for v in (decision_trace or []) if str(v).strip()]
    stable_reason_codes = sorted(set(str(v) for v in (reason_codes or []) if str(v).strip()))
    stable_decision = str(decision or "review").strip().lower()
    stable_policy_id = str(effective_policy_id or "")
    stable_policy_version = int(effective_policy_version or 1)
    stable_remediation_mode = str(remediation_mode or "manual").strip().lower()
    stable_remediation_action = str(remediation_action or "request_review").strip()
    stable_policy_hash = str(policy_hash or "")
    stable_decision_source = str(decision_source or "fallback").strip().lower() or "fallback"
    stable_expected_audience = str(
        expected_audience or (content_signals or {}).get("expected_audience") or "internal_need_to_know"
    ).strip().lower() or "internal_need_to_know"
    stable_expected_department = str(
        expected_department or (content_signals or {}).get("expected_department") or "unknown"
    ).strip() or "unknown"
    stable_expectation_gap_reason = str(expectation_gap_reason or "").strip()
    stable_expectation_gap_score = max(0.0, min(1.0, float(expectation_gap_score or 0.0)))
    stable_content_signals = _normalize_content_signals(content_signals)
    stable_content_analysis = _normalize_content_analysis(content_analysis, stable_content_signals)
    stable_content_signals_ddb = _to_dynamodb_compatible(stable_content_signals)
    stable_content_analysis_ddb = _to_dynamodb_compatible(stable_content_analysis)

    if existing is None:
        # 初回検知: new として登録する。
        finding = {
            "tenant_id": tenant_id,
            "finding_id": finding_id,
            "source": item.source,
            "container_id": item.container_id,
            "container_name": item.container_name,
            "container_type": item.container_type,
            "item_id": item.item_id,
            "item_name": item.item_name,
            "item_url": item.web_url,
            "mime_type": item.mime_type,
            "size": int(item.size),
            "raw_s3_key": item.raw_s3_key,
            "risk_level": initial_risk_level,
            "exposure_vectors": exposure_result.vectors,
            "risk_type_counts": _to_dynamodb_compatible(risk_type_counts),
            "exposure_vector_counts": _to_dynamodb_compatible(exposure_vector_counts),
            "total_detected_risks": int(total_detected_risks),
            "audience_scope": str(exposure_result.details.get("audience_scope", "individual")),
            "audience_scope_score": float_to_decimal(float(exposure_result.details.get("audience_scope_score", 0.05))),
            "privilege_strength_score": float_to_decimal(float(exposure_result.details.get("privilege_strength_score", 0.20))),
            "permission_weighted_level": str(exposure_result.details.get("permission_weighted_level", "view")),
            "permission_max_level": str(exposure_result.details.get("permission_max_level", "view")),
            "permission_max_level_score": float_to_decimal(float(exposure_result.details.get("permission_max_level_score", 0.20))),
            "discoverability": str(exposure_result.details.get("discoverability", "hidden")),
            "discoverability_score": float_to_decimal(float(exposure_result.details.get("discoverability_score", 0.10))),
            "externality": str(exposure_result.details.get("externality", "internal_only")),
            "externality_score": float_to_decimal(float(exposure_result.details.get("externality_score", 0.00))),
            "reshare_capability": str(exposure_result.details.get("reshare_capability", "none")),
            "reshare_capability_score": float_to_decimal(float(exposure_result.details.get("reshare_capability_score", 0.10))),
            "broken_inheritance_score": float_to_decimal(float(exposure_result.details.get("broken_inheritance_score", 0.00))),
            "permission_outlier_score": float_to_decimal(float(exposure_result.details.get("permission_outlier_score", 0.00))),
            "sharing_scope": item.sharing_scope,
            "permissions_summary": item.permissions_summary,
            "sensitivity_label": item.sensitivity_label_name,
            "pii_detected": False,
            "pii_types": None,
            "pii_count": 0,
            "pii_density": "none",
            "secrets_detected": False,
            "secret_types": None,
            "ai_eligible": compute_ai_eligible(
                initial_risk_level,
                total_detected_risks=int(total_detected_risks),
                pii_detected=False,
                secrets_detected=False,
            ),
            "sensitivity_scan_at": None,
            "status": "new",
            "workflow_status": "new",
            "exception_type": "none",
            "exception_review_due_at": None,
            "matched_guards": matched_guards,
            "guard_reason_codes": stable_guard_reason_codes,
            "detection_reasons": stable_detection_reasons,
            "finding_evidence": stable_finding_evidence,
            "decision": stable_decision,
            "effective_policy_id": stable_policy_id,
            "effective_policy_version": stable_policy_version,
            "matched_policy_ids": stable_matched_policy_ids,
            "decision_trace": stable_decision_trace,
            "reason_codes": stable_reason_codes,
            "remediation_mode": stable_remediation_mode,
            "remediation_action": stable_remediation_action,
            "policy_hash": stable_policy_hash,
            "decision_source": stable_decision_source,
            "expected_audience": stable_expected_audience,
            "expected_department": stable_expected_department,
            "expectation_gap_reason": stable_expectation_gap_reason,
            "expectation_gap_score": float_to_decimal(stable_expectation_gap_score),
            "content_signals": stable_content_signals_ddb,
            "content_analysis": stable_content_analysis_ddb,
            "detected_at": now,
            "last_evaluated_at": now,
            "remediated_at": None,
            "suppress_until": None,
            "acknowledged_reason": None,
            "acknowledged_by": None,
            "acknowledged_at": None,
            "evidence_s3_key": None,
            "source_metadata": item.source_metadata,
            "importance_score": None,
            "deep_analysis_eligible": None,
            "deep_analysis_skip_reason": None,
            "deep_analysis_rule": None,
            "deep_analysis_override_applied": False,
        }
        finding["is_new"] = True
        table.put_item(Item=finding)
        emit_count("AIReadyGov.FindingsCreated", dimensions={"TenantId": tenant_id})
        logger.info(f"Finding created: {finding_id}")
        return finding

    # 再接続・再検知時は closed/new を open へ戻し、再評価結果を反映する。
    current_status = str(existing.get("status") or "").strip().lower()
    new_status = "open" if current_status in {"new", "closed"} else existing.get("status", "open")
    updated_risk_level = initial_risk_level
    updated_ai_eligible = compute_ai_eligible(
        updated_risk_level,
        total_detected_risks=int(total_detected_risks),
        pii_detected=bool(existing.get("pii_detected", False)),
        secrets_detected=bool(existing.get("secrets_detected", False)),
    )
    _existing_ws = (
        str(existing.get("workflow_status") or existing.get("status") or "open")
        .strip()
        .lower()
        or "open"
    )
    _existing_et = str(existing.get("exception_type", "none")).strip().lower() or "none"
    _preserve_exception_registry = _existing_ws == "acknowledged" or _existing_et not in ("none", "")

    if workflow_status is None:
        next_workflow_status = _existing_ws if _preserve_exception_registry else "open"
    else:
        next_workflow_status = str(workflow_status or "").strip().lower() or "open"

    if exception_type is None:
        next_exception_type = _existing_et if _preserve_exception_registry else "none"
    else:
        next_exception_type = str(exception_type or "").strip().lower() or "none"

    if exception_review_due_at is None:
        merged_exception_review_due_at = (
            (existing.get("exception_review_due_at") or existing.get("suppress_until"))
            if _preserve_exception_registry
            else None
        )
    else:
        merged_exception_review_due_at = exception_review_due_at

    table.update_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id},
        UpdateExpression="""
            SET risk_level = :rl,
                ai_eligible = :ae,
                exposure_vectors = :ev,
                risk_type_counts = :risk_type_counts,
                exposure_vector_counts = :exposure_vector_counts,
                total_detected_risks = :total_detected_risks,
                sharing_scope = :scope,
                permissions_summary = :ps,
                sensitivity_label = :sl,
                item_name = :item_name,
                mime_type = :mime_type,
                #size_attr = :size,
                raw_s3_key = :raw_s3_key,
                matched_guards = :mg,
                guard_reason_codes = :grc,
                detection_reasons = :dr,
                finding_evidence = :fe,
                decision = :decision,
                effective_policy_id = :effective_policy_id,
                effective_policy_version = :effective_policy_version,
                matched_policy_ids = :matched_policy_ids,
                decision_trace = :decision_trace,
                reason_codes = :reason_codes,
                remediation_mode = :remediation_mode,
                remediation_action = :remediation_action,
                policy_hash = :policy_hash,
                decision_source = :decision_source,
                expected_audience = :expected_audience,
                expected_department = :expected_department,
                expectation_gap_reason = :expectation_gap_reason,
                expectation_gap_score = :expectation_gap_score,
                content_signals = :content_signals,
                content_analysis = :content_analysis,
                workflow_status = :workflow_status,
                exception_type = :exception_type,
                exception_review_due_at = :exception_review_due_at,
                audience_scope = :audience_scope,
                audience_scope_score = :audience_scope_score,
                privilege_strength_score = :privilege_strength_score,
                permission_weighted_level = :permission_weighted_level,
                permission_max_level = :permission_max_level,
                permission_max_level_score = :permission_max_level_score,
                discoverability = :discoverability,
                discoverability_score = :discoverability_score,
                externality = :externality,
                externality_score = :externality_score,
                reshare_capability = :reshare_capability,
                reshare_capability_score = :reshare_capability_score,
                broken_inheritance_score = :broken_inheritance_score,
                permission_outlier_score = :permission_outlier_score,
                last_evaluated_at = :now,
                #st = :status
        """,
        ExpressionAttributeNames={
            "#st": "status",
            "#size_attr": "size",
        },
        ExpressionAttributeValues={
            ":rl": updated_risk_level,
            ":ae": updated_ai_eligible,
            ":ev": exposure_result.vectors,
            ":risk_type_counts": _to_dynamodb_compatible(risk_type_counts),
            ":exposure_vector_counts": _to_dynamodb_compatible(exposure_vector_counts),
            ":total_detected_risks": int(total_detected_risks),
            ":scope": item.sharing_scope,
            ":ps": item.permissions_summary,
            ":sl": item.sensitivity_label_name,
            ":item_name": item.item_name,
            ":mime_type": item.mime_type,
            ":size": int(item.size),
            ":raw_s3_key": item.raw_s3_key,
            ":mg": matched_guards,
            ":grc": stable_guard_reason_codes,
            ":dr": stable_detection_reasons,
            ":fe": stable_finding_evidence,
            ":decision": stable_decision,
            ":effective_policy_id": stable_policy_id,
            ":effective_policy_version": stable_policy_version,
            ":matched_policy_ids": stable_matched_policy_ids,
            ":decision_trace": stable_decision_trace,
            ":reason_codes": stable_reason_codes,
            ":remediation_mode": stable_remediation_mode,
            ":remediation_action": stable_remediation_action,
            ":policy_hash": stable_policy_hash,
            ":decision_source": stable_decision_source,
            ":expected_audience": stable_expected_audience,
            ":expected_department": stable_expected_department,
            ":expectation_gap_reason": stable_expectation_gap_reason,
            ":expectation_gap_score": float_to_decimal(stable_expectation_gap_score),
            ":content_signals": stable_content_signals_ddb,
            ":content_analysis": stable_content_analysis_ddb,
            ":workflow_status": next_workflow_status,
            ":exception_type": next_exception_type,
            ":exception_review_due_at": merged_exception_review_due_at,
            ":audience_scope": str(exposure_result.details.get("audience_scope", "individual")),
            ":audience_scope_score": float_to_decimal(float(exposure_result.details.get("audience_scope_score", 0.05))),
            ":privilege_strength_score": float_to_decimal(float(exposure_result.details.get("privilege_strength_score", 0.20))),
            ":permission_weighted_level": str(exposure_result.details.get("permission_weighted_level", "view")),
            ":permission_max_level": str(exposure_result.details.get("permission_max_level", "view")),
            ":permission_max_level_score": float_to_decimal(float(exposure_result.details.get("permission_max_level_score", 0.20))),
            ":discoverability": str(exposure_result.details.get("discoverability", "hidden")),
            ":discoverability_score": float_to_decimal(float(exposure_result.details.get("discoverability_score", 0.10))),
            ":externality": str(exposure_result.details.get("externality", "internal_only")),
            ":externality_score": float_to_decimal(float(exposure_result.details.get("externality_score", 0.00))),
            ":reshare_capability": str(exposure_result.details.get("reshare_capability", "none")),
            ":reshare_capability_score": float_to_decimal(float(exposure_result.details.get("reshare_capability_score", 0.10))),
            ":broken_inheritance_score": float_to_decimal(float(exposure_result.details.get("broken_inheritance_score", 0.00))),
            ":permission_outlier_score": float_to_decimal(float(exposure_result.details.get("permission_outlier_score", 0.00))),
            ":now": now,
            ":status": new_status,
        },
    )
    existing["is_new"] = False
    emit_count("AIReadyGov.FindingsUpdated", dimensions={"TenantId": tenant_id})
    logger.info(f"Finding updated: {finding_id}")
    return existing


def _build_finding_evidence(item: FileMetadata) -> dict[str, Any]:
    source_metadata = _parse_json_dict(item.source_metadata)
    permissions = _parse_json_list(item.permissions)
    permission_targets = source_metadata.get("permission_targets", [])
    if not isinstance(permission_targets, list) or len(permission_targets) == 0:
        permission_targets = _build_permission_targets_from_permissions(permissions)

    evidence = {
        "sharing_scope": str(item.sharing_scope or ""),
        "permission_targets": permission_targets,
        "external_recipients": source_metadata.get("external_recipients", []),
        "anonymous_links": source_metadata.get("anonymous_links", []),
        "org_edit_links": source_metadata.get("org_edit_links", []),
        "acl_drift_diff": source_metadata.get("permission_delta", []),
    }
    return _normalize_finding_evidence(evidence)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _normalize_content_signals(raw: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {
            "doc_sensitivity_level": "none",
            "doc_categories": [],
            "contains_pii": False,
            "contains_secret": False,
            "confidence": 0.0,
            "expected_audience": "internal_need_to_know",
            "expected_department": "unknown",
            "expected_department_confidence": 0.0,
            "justification": "",
        }
    categories = raw.get("doc_categories")
    if not isinstance(categories, list):
        categories = []
    try:
        confidence = float(raw.get("confidence", 0.0))
    except Exception:
        confidence = 0.0
    return {
        "doc_sensitivity_level": str(raw.get("doc_sensitivity_level", "none")).strip().lower() or "none",
        "doc_categories": sorted(
            {
                str(v).strip().lower()
                for v in categories
                if str(v).strip()
            }
        ),
        "contains_pii": bool(raw.get("contains_pii", False)),
        "contains_secret": bool(raw.get("contains_secret", False)),
        "confidence": max(0.0, min(1.0, confidence)),
        "expected_audience": str(raw.get("expected_audience", "internal_need_to_know")).strip().lower()
        or "internal_need_to_know",
        "expected_department": str(raw.get("expected_department", "unknown")).strip() or "unknown",
        "expected_department_confidence": max(
            0.0,
            min(1.0, _safe_float(raw.get("expected_department_confidence", 0.0))),
        ),
        "justification": str(raw.get("justification", "")).strip(),
    }


def _normalize_content_analysis(
    raw: dict[str, Any] | None,
    content_signals: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(raw) if isinstance(raw, dict) else {}
    normalized.setdefault("analysis_status", "unknown")
    normalized.setdefault("decision_source", "fallback")
    normalized.setdefault("model_id", "")
    normalized.setdefault("prompt_version", "")
    normalized.setdefault("confidence", float(content_signals.get("confidence", 0.0)))
    return normalized


def _to_dynamodb_compatible(value: Any) -> Any:
    """Recursively convert Python values into DynamoDB-safe types."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_to_dynamodb_compatible(item) for item in value]
    if isinstance(value, tuple):
        return [_to_dynamodb_compatible(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _to_dynamodb_compatible(v) for k, v in value.items()}
    return value


def _parse_json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
        if isinstance(parsed, dict):
            return parsed
        return {}
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}


def _parse_json_list(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [entry for entry in raw if isinstance(entry, dict)]
    if not raw:
        return []
    try:
        parsed = json.loads(str(raw))
        if isinstance(parsed, list):
            return [entry for entry in parsed if isinstance(entry, dict)]
        return []
    except (json.JSONDecodeError, TypeError, ValueError):
        return []


def _build_permission_targets_from_permissions(
    permissions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for permission in permissions:
        if not isinstance(permission, dict):
            continue
        roles = permission.get("roles", [])
        role = "read"
        if isinstance(roles, list) and roles:
            role = str(roles[0]).strip().lower()
        link = permission.get("link", {})
        scope = str(link.get("scope") or "direct").strip().lower() if isinstance(link, dict) else "direct"
        principals = []
        for key in ("grantedToV2", "grantedTo"):
            principal = permission.get(key)
            if isinstance(principal, dict) and isinstance(principal.get("user"), dict):
                principals.append(principal["user"])
        for key in ("grantedToIdentitiesV2", "grantedToIdentities"):
            entries = permission.get(key)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if isinstance(entry, dict) and isinstance(entry.get("user"), dict):
                    principals.append(entry["user"])

        for principal in principals:
            email = str(principal.get("email") or "").strip().lower()
            user_type = str(principal.get("userType") or "").strip().lower()
            targets.append({
                "principal": email or str(principal.get("id") or "").strip(),
                "role": role,
                "is_external": user_type in {"guest", "external"} or "#ext#" in email,
                "scope": scope,
            })

    return sorted(
        targets,
        key=lambda target: (
            str(target.get("principal", "")),
            str(target.get("role", "")),
            str(target.get("scope", "")),
        ),
    )


def _normalize_finding_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "sharing_scope": str(evidence.get("sharing_scope", "")),
        "permission_targets": evidence.get("permission_targets", []),
        "external_recipients": evidence.get("external_recipients", []),
        "anonymous_links": evidence.get("anonymous_links", []),
        "org_edit_links": evidence.get("org_edit_links", []),
        "acl_drift_diff": evidence.get("acl_drift_diff", []),
    }

    normalized["external_recipients"] = sorted(
        {
            str(recipient).strip().lower()
            for recipient in normalized["external_recipients"]
            if str(recipient).strip()
        }
    )
    normalized["anonymous_links"] = sorted(
        {
            str(link).strip()
            for link in normalized["anonymous_links"]
            if str(link).strip()
        }
    )
    normalized["org_edit_links"] = sorted(
        {
            str(link).strip()
            for link in normalized["org_edit_links"]
            if str(link).strip()
        }
    )

    permission_targets = normalized["permission_targets"]
    if not isinstance(permission_targets, list):
        permission_targets = []
    normalized["permission_targets"] = sorted(
        [
            {
                "principal": str(target.get("principal", "")),
                "role": str(target.get("role", "")),
                "is_external": bool(target.get("is_external", False)),
                "scope": str(target.get("scope", "")),
            }
            for target in permission_targets
            if isinstance(target, dict) and str(target.get("principal", "")).strip()
        ],
        key=lambda target: (
            target["principal"],
            target["role"],
            target["scope"],
        ),
    )

    acl_drift_diff = normalized["acl_drift_diff"]
    if not isinstance(acl_drift_diff, list):
        acl_drift_diff = []
    normalized["acl_drift_diff"] = sorted(
        [
            {
                "principal": str(diff.get("principal", "")),
                "before": str(diff.get("before", "")),
                "after": str(diff.get("after", "")),
                "change": str(diff.get("change", "")),
            }
            for diff in acl_drift_diff
            if isinstance(diff, dict) and str(diff.get("principal", "")).strip()
        ],
        key=lambda diff: (
            diff["principal"],
            diff["change"],
            diff["before"],
            diff["after"],
        ),
    )

    return normalized


# ─── 7.4 Finding の Closed 処理 ───


def close_finding(tenant_id: str, finding_id: str) -> None:
    """Finding を `closed` 状態へ更新する。

    Args:
        tenant_id: テナント ID。
        finding_id: 対象 Finding ID。

    Returns:
        なし。

    Notes:
        対象が存在しない場合の ConditionalCheckFailed は無害として無視する。
    """
    table = _get_finding_table()
    now = datetime.now(TOKYO_TZ).isoformat()
    try:
        table.update_item(
            Key={"tenant_id": tenant_id, "finding_id": finding_id},
            UpdateExpression="SET #st = :status, last_evaluated_at = :now",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":status": "closed",
                ":now": now,
            },
            ConditionExpression="attribute_exists(finding_id)",
        )
        emit_count("AIReadyGov.FindingsClosed", dimensions={"TenantId": tenant_id})
        logger.info(f"Finding closed: {finding_id}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise


def handle_item_deletion(image: dict[str, Any]) -> None:
    """アイテム削除時に対応する Finding をクローズする。

    Args:
        image: Streams の image 由来データ。

    Returns:
        なし。

    Notes:
        `tenant_id` または `item_id` が欠落している場合は処理をスキップする。
    """
    tenant_id = image.get("tenant_id", "")
    item_id = image.get("item_id", "")
    source = image.get("source", "m365")
    if not tenant_id or not item_id:
        return
    finding_id = generate_finding_id(tenant_id, source, item_id)
    close_finding(tenant_id, finding_id)


# ─── 抑制（acknowledged）の登録 ───


def acknowledge_finding(
    tenant_id: str,
    finding_id: str,
    suppress_until: str,
    reason: str,
    acknowledged_by: str,
) -> dict[str, Any]:
    """Finding を `workflow_status=acknowledged` へ更新する。"""
    table = _get_finding_table()
    now = datetime.now(TOKYO_TZ).isoformat()

    table.update_item(
        Key={"tenant_id": tenant_id, "finding_id": finding_id},
        UpdateExpression="""
            SET #st = :status,
                workflow_status = :workflow_status,
                exception_type = :exception_type,
                exception_review_due_at = :review_due,
                suppress_until = :suppress,
                acknowledged_reason = :reason,
                acknowledged_by = :by,
                acknowledged_at = :at,
                last_evaluated_at = :now
        """,
        ExpressionAttributeNames={"#st": "status"},
        ExpressionAttributeValues={
            ":status": "acknowledged",
            ":workflow_status": "acknowledged",
            ":exception_type": "temporary_accept",
            ":review_due": suppress_until,
            ":suppress": suppress_until,
            ":reason": reason,
            ":by": acknowledged_by,
            ":at": now,
            ":now": now,
        },
    )
    logger.info(f"Finding acknowledged: {finding_id}")
    return {
        "finding_id": finding_id,
        "workflow_status": "acknowledged",
        "suppress_until": suppress_until,
    }


# ─── クエリ ───


def get_finding(tenant_id: str, finding_id: str) -> dict[str, Any] | None:
    """PK/SK で Finding を取得する。

    Args:
        tenant_id: テナント ID。
        finding_id: Finding ID。

    Returns:
        Finding 辞書。未存在時は `None`。

    Notes:
        一意キー直接参照のため最小コストで取得できる。
    """
    table = _get_finding_table()
    response = table.get_item(Key={"tenant_id": tenant_id, "finding_id": finding_id})
    return response.get("Item")


def get_finding_by_item(tenant_id: str, item_id: str) -> dict[str, Any] | None:
    """item_id から Finding を逆引きする。

    Args:
        tenant_id: テナント ID。
        item_id: アイテム ID。

    Returns:
        先頭 1 件の Finding。未存在時は `None`。

    Notes:
        `GSI-ItemFinding` を利用し `Limit=1` で取得する。
        item_id に対して 1件前提の運用モデル。
    """
    table = _get_finding_table()
    response = table.query(
        IndexName="GSI-ItemFinding",
        KeyConditionExpression=Key("item_id").eq(item_id) & Key("tenant_id").eq(tenant_id),
        Limit=1,
    )
    items = response.get("Items", [])
    return items[0] if items else None


def query_findings_by_status(tenant_id: str, status: str) -> list[dict[str, Any]]:
    """status 別に Finding 一覧を取得する。

    Args:
        tenant_id: テナント ID。
        status: 取得対象ステータス。

    Returns:
        該当する Finding 辞書の配列。

    Notes:
        `GSI-StatusFinding` を利用して同一テナント内を走査する。
    """
    table = _get_finding_table()
    response = table.query(
        IndexName="GSI-StatusFinding",
        KeyConditionExpression=Key("tenant_id").eq(tenant_id) & Key("status").eq(status),
    )
    return response.get("Items", [])


def query_findings_by_workflow_status(tenant_id: str, workflow_status: str) -> list[dict[str, Any]]:
    """workflow_status 別に Finding 一覧を取得する。"""
    table = _get_finding_table()
    normalized = str(workflow_status or "").strip().lower()
    if not normalized:
        return []
    response = table.query(
        KeyConditionExpression=Key("tenant_id").eq(tenant_id),
    )
    rows = response.get("Items", [])
    def _matches_workflow(row: dict[str, Any]) -> bool:
        workflow = str(row.get("workflow_status", "")).strip().lower()
        if workflow:
            return workflow == normalized
        # Backward compatibility for legacy rows/tests that only persisted `status`.
        return str(row.get("status", "")).strip().lower() == normalized

    return [row for row in rows if _matches_workflow(row)]


def close_finding_if_exists(tenant_id: str, item_id: str, source: str = "m365") -> None:
    """item_id に対応する Finding をクローズする。

    Args:
        tenant_id: テナント ID。
        item_id: アイテム ID。
        source: ソース識別子。

    Returns:
        なし。

    Notes:
        Finding の存在確認は `close_finding()` 側の条件更新に委譲する。
    """
    finding_id = generate_finding_id(tenant_id, source, item_id)
    close_finding(tenant_id, finding_id)
