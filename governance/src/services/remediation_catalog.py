"""M365 remediation action catalog for Governance findings."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any

import boto3


@dataclass(frozen=True)
class RemediationAction:
    """Action proposal derived from guard/vector signals."""

    action_type: str
    title: str
    reason: str
    permission_ids: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
    executable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_bedrock_client = None
LLM_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"


def _iter_permission_users(permission: dict[str, Any]) -> list[dict[str, Any]]:
    users: list[dict[str, Any]] = []
    for key in ("grantedToV2", "grantedTo"):
        granted = permission.get(key)
        if isinstance(granted, dict):
            user = granted.get("user")
            if isinstance(user, dict):
                users.append(user)
    for key in ("grantedToIdentitiesV2", "grantedToIdentities"):
        identities = permission.get(key)
        if isinstance(identities, list):
            for identity in identities:
                if not isinstance(identity, dict):
                    continue
                user = identity.get("user")
                if isinstance(user, dict):
                    users.append(user)
    return users


def _permission_link_scope(permission: dict[str, Any]) -> str:
    link = permission.get("link") or {}
    return str(link.get("scope") or "").strip().lower()


def _permission_id(permission: dict[str, Any]) -> str:
    return str(permission.get("id") or "").strip()


def _permission_roles(permission: dict[str, Any]) -> set[str]:
    roles = permission.get("roles")
    if not isinstance(roles, list):
        return set()
    return {str(role).strip().lower() for role in roles if str(role).strip()}


def _is_owner_permission(permission: dict[str, Any], owner_user_id: str) -> bool:
    if "owner" in _permission_roles(permission):
        return True
    if not owner_user_id:
        return False
    return any(
        str(user.get("id") or "").strip() == owner_user_id
        for user in _iter_permission_users(permission)
    )


def _is_inherited_permission(permission: dict[str, Any]) -> bool:
    inherited = permission.get("inheritedFrom")
    if isinstance(inherited, dict):
        return len(inherited) > 0
    if isinstance(inherited, list):
        return len(inherited) > 0
    if isinstance(inherited, str):
        return bool(inherited.strip())
    return False


def _is_site_default_permission(permission: dict[str, Any]) -> bool:
    permission_id = _permission_id(permission).lower()
    if permission_id.startswith("c:0-.f|rolemanager|"):
        return True

    for user in _iter_permission_users(permission):
        display_name = str(user.get("displayName") or "").strip().lower()
        if "all users" in display_name or "すべてのユーザー" in display_name:
            return True
    return False


def _is_removable_permission(permission: dict[str, Any], owner_user_id: str) -> bool:
    permission_id = _permission_id(permission)
    if not permission_id:
        return False
    if _is_owner_permission(permission, owner_user_id):
        return False
    if _is_inherited_permission(permission):
        return False
    if _is_site_default_permission(permission):
        return False
    return True


def _is_sharing_link_org_or_anonymous(permission: dict[str, Any]) -> bool:
    """True when permission represents an anyone or organization sharing link facet."""
    return _permission_link_scope(permission) in {"anonymous", "organization"}


def _is_removable_sharing_link_for_graph(permission: dict[str, Any], owner_user_id: str) -> bool:
    """Whether this permission id should be proposed for Graph DELETE.

    Item-level sharing links are revoked via DELETE /permissions/{{id}} even when Graph
    populates `inheritedFrom` on nested items; excluding all inherited rows made
    org-wide link remediation a no-op. Site-default and owner rows stay non-removable.
    """
    if not _permission_id(permission):
        return False
    if _is_owner_permission(permission, owner_user_id):
        return False
    if _is_site_default_permission(permission):
        return False
    if _is_sharing_link_org_or_anonymous(permission):
        return True
    return _is_removable_permission(permission, owner_user_id)


def _is_external_permission(permission: dict[str, Any]) -> bool:
    for user in _iter_permission_users(permission):
        user_type = str(user.get("userType") or "").strip().lower()
        if user_type in {"guest", "external"}:
            return True
        email = str(user.get("email") or "").strip().lower()
        if "#ext#" in email:
            return True
        principal_id = str(user.get("id") or "").strip().lower()
        if "#ext#" in principal_id:
            return True
    return False


def _unique_permission_ids(permissions: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for permission in permissions:
        permission_id = _permission_id(permission)
        if not permission_id or permission_id in seen:
            continue
        seen.add(permission_id)
        result.append(permission_id)
    return result


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _infer_vectors_from_permissions(
    permissions: list[dict[str, Any]],
    *,
    owner_user_id: str,
) -> set[str]:
    vectors: set[str] = set()
    direct_non_owner_count = 0
    for permission in permissions:
        if _permission_link_scope(permission) == "anonymous":
            vectors.add("public_link")
        if _permission_link_scope(permission) == "organization":
            vectors.add("org_link")
            link_obj = permission.get("link") if isinstance(permission.get("link"), dict) else {}
            if str(link_obj.get("type") or "").strip().lower() == "edit":
                vectors.add("org_link_editable")
            elif _permission_roles(permission) & {"write", "edit"}:
                vectors.add("org_link_editable")
        if _is_external_permission(permission):
            vectors.add("guest")
        if _is_removable_permission(permission, owner_user_id):
            direct_non_owner_count += 1
    if direct_non_owner_count >= 3:
        vectors.add("excessive_permissions")
    return vectors


def _get_bedrock_client():
    global _bedrock_client
    if _bedrock_client is None:
        _bedrock_client = boto3.client("bedrock-runtime")
    return _bedrock_client


def _llm_reason_for_inferred_action(
    *,
    vectors: set[str],
    permission_count: int,
) -> str:
    enabled = str(
        os.getenv("GOVERNANCE_ENABLE_LLM_REMEDIATION_PROPOSAL", "true")
    ).strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return "権限構造を解析し、最小権限化の適用を推奨します。"

    prompt = (
        "あなたは Microsoft 365 の権限是正支援アシスタントです。"
        "次の条件で、運用者向けに1文（120文字以内）の是正理由を日本語で作成してください。"
        "不要な装飾や箇条書きは不要です。\n"
        f"- 検出ベクトル: {', '.join(sorted(vectors)) or 'none'}\n"
        f"- 権限件数: {permission_count}\n"
    )
    try:
        client = _get_bedrock_client()
        response = client.invoke_model(
            modelId=LLM_MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(
                {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 180,
                    "messages": [{"role": "user", "content": prompt}],
                }
            ),
        )
        payload = json.loads(response["body"].read())
        content = str(((payload.get("content") or [{}])[0]).get("text") or "").strip()
        if content:
            return content[:120]
    except Exception:
        pass
    return "権限構造を解析し、最小権限化の適用を推奨します。"


def build_m365_action_plan(
    *,
    matched_guards: list[str] | None,
    exposure_vectors: list[str] | None,
    permissions: list[dict[str, Any]] | None,
    owner_user_id: str = "",
    pii_detected: bool = False,
    secrets_detected: bool = False,
    remediation_mode: str = "",
    remediation_action: str = "",
    content_signals: dict[str, Any] | None = None,
) -> list[RemediationAction]:
    """Build remediation actions from guards, vectors and effective permissions."""
    provided_vectors = {
        str(value).strip().lower() for value in (exposure_vectors or []) if str(value).strip()
    }
    vectors = set(provided_vectors)
    guards = {
        str(value).strip().upper() for value in (matched_guards or []) if str(value).strip()
    }
    permission_rows = permissions or []
    content = content_signals if isinstance(content_signals, dict) else {}
    categories = {
        str(value).strip().lower()
        for value in (content.get("doc_categories") or [])
        if str(value).strip()
    }
    content_contains_secret = bool(content.get("contains_secret", False))
    expected_audience = str(content.get("expected_audience", "internal_need_to_know")).strip().lower()
    analysis_confidence = _safe_float(content.get("confidence", 0.0))
    expected_department_confidence = _safe_float(content.get("expected_department_confidence", 0.0))
    confidence_threshold = _safe_float(os.getenv("GOVERNANCE_CONTENT_CONFIDENCE_THRESHOLD", "0.7"), 0.7)

    if not vectors and not guards and permission_rows:
        vectors |= _infer_vectors_from_permissions(
            permission_rows,
            owner_user_id=owner_user_id,
        )

    link_permission_ids = [
        permission_id
        for permission_id, permission in [
            (_permission_id(row), row) for row in permission_rows
        ]
        if permission_id
        and _permission_link_scope(permission) in {"anonymous", "organization"}
        and _is_removable_sharing_link_for_graph(permission, owner_user_id)
    ]
    external_permission_ids = [
        permission_id
        for permission_id, permission in [
            (_permission_id(row), row) for row in permission_rows
        ]
        if permission_id and _is_external_permission(permission) and _is_removable_permission(permission, owner_user_id)
    ]
    removable_direct_permission_ids = [
        permission_id
        for permission_id, permission in [
            (_permission_id(row), row) for row in permission_rows
        ]
        if permission_id
        and _permission_link_scope(permission) not in {"anonymous", "organization"}
        and _is_removable_permission(permission, owner_user_id)
    ]
    writable_non_owner_ids = [
        permission_id
        for permission_id, permission in [
            (_permission_id(row), row) for row in permission_rows
        ]
        if permission_id
        and "write" in _permission_roles(permission)
        and _is_removable_permission(permission, owner_user_id)
    ]
    non_owner_ids = [
        permission_id
        for permission_id, permission in [
            (_permission_id(row), row) for row in permission_rows
        ]
        if permission_id and _is_removable_permission(permission, owner_user_id)
    ]

    inherited_or_site_default_ids = [
        permission_id
        for permission_id, permission in [(_permission_id(row), row) for row in permission_rows]
        if permission_id and not _is_removable_permission(permission, owner_user_id)
    ]

    _wide_link_vectors = {
        "public_link",
        "org_link",
        "org_link_view",
        "org_link_edit",
        "org_link_editable",
        "all_users",
    }
    actions: list[RemediationAction] = []
    if vectors & _wide_link_vectors or guards & {"G2", "G3"}:
        if link_permission_ids:
            actions.append(
                RemediationAction(
                    action_type="remove_permissions",
                    title="共有リンク権限の遮断",
                    reason="公開・組織リンク由来の露出を遮断する",
                    permission_ids=link_permission_ids,
                )
            )

    if vectors & {
        "guest",
        "external_domain",
        "guest_direct_share",
        "external_email_direct_share",
        "external_domain_share",
    }:
        if not external_permission_ids and vectors & {
            "guest_direct_share",
            "external_email_direct_share",
            "external_domain_share",
        }:
            # direct_share 系ベクトルが立っているのに外部判定ができない場合は、
            # 共有リンク以外の可削除権限を縮退対象にする（誤判定時の manual 固定化を回避）。
            external_permission_ids = removable_direct_permission_ids
    if vectors & {
        "guest",
        "external_domain",
        "guest_direct_share",
        "external_email_direct_share",
        "external_domain_share",
    } and external_permission_ids:
        actions.append(
            RemediationAction(
                action_type="remove_permissions",
                title="外部共有権限の遮断",
                reason="外部ゲスト・外部ドメイン共有を解除する",
                permission_ids=external_permission_ids,
            )
        )

    if "excessive_permissions" in vectors:
        target_ids = writable_non_owner_ids or non_owner_ids
        if target_ids:
            actions.append(
                RemediationAction(
                    action_type="remove_permissions",
                    title="過剰権限の縮退",
                    reason="非所有者権限を整理して最小権限化する",
                    permission_ids=target_ids,
                )
            )

    del pii_detected, secrets_detected

    if content_contains_secret and "public_link" in vectors and link_permission_ids:
        actions.insert(
            0,
            RemediationAction(
                action_type="remove_permissions",
                title="機密情報を含む公開リンクの即時遮断",
                reason="contains_secret=true かつ public_link のため公開リンク削除を優先",
                permission_ids=link_permission_ids,
            ),
        )

    if expected_audience in {"owner_only", "department_only"}:
        broad_vectors = vectors.intersection(
            {"public_link", "all_users", "org_link", "org_link_view", "org_link_edit", "org_link_editable"}
        )
        if broad_vectors and link_permission_ids:
            actions.insert(
                0,
                RemediationAction(
                    action_type="remove_permissions",
                    title="想定閲覧範囲との差分是正",
                    reason=f"expected_audience={expected_audience} に対して広域共有が検知されたため優先遮断",
                    permission_ids=link_permission_ids,
                ),
            )
        if expected_audience == "department_only" and external_permission_ids:
            actions.insert(
                0,
                RemediationAction(
                    action_type="remove_permissions",
                    title="部署限定想定の外部共有解除",
                    reason="department_only 文書で外部共有が検出されたため解除",
                    permission_ids=external_permission_ids,
                ),
            )

    # When vectors/guards were absent but permissions were actionable,
    # add an AI reason to explain permission-structure based proposal.
    if actions and not provided_vectors and not guards:
        reason = _llm_reason_for_inferred_action(
            vectors=vectors,
            permission_count=len(_unique_permission_ids(permission_rows)),
        )
        actions = [
            RemediationAction(
                action_type=action.action_type,
                title=action.title,
                reason=f"{action.reason} / {reason}" if action.action_type == "remove_permissions" else action.reason,
                permission_ids=action.permission_ids,
                payload=action.payload,
                executable=action.executable,
            )
            for action in actions
        ]

    if "inherited_oversharing" in vectors:
        actions.append(
            RemediationAction(
                action_type="owner_review",
                title="継承権限の所有者レビュー",
                reason="継承由来のため自動削除できず、親スコープでの見直しが必要",
                payload={"non_removable_permission_ids": inherited_or_site_default_ids},
                executable=False,
            )
        )

    if "all_users" in vectors:
        actions.append(
            RemediationAction(
                action_type="suggest_restricted_access",
                title="Restricted Access の適用提案",
                reason="広域共有が検知されたためサイト単位の制限を推奨",
                payload={"suggestion": "restricted_access"},
                executable=False,
            )
        )

    if remediation_mode.strip().lower() in {"owner_review", "manual", "recommend_only"}:
        # Ensure policy-driven non-auto modes never attempt destructive auto execution.
        actions = [
            RemediationAction(
                action_type=("owner_review" if remediation_mode.strip().lower() == "owner_review" else "manual_review"),
                title="ポリシー準拠レビュー",
                reason=f"policy remediation_mode={remediation_mode} のためレビュー運用へ分岐",
                payload={"requested_action": remediation_action, "existing_actions": [a.to_dict() for a in actions]},
                executable=False,
            )
        ]

    if analysis_confidence < confidence_threshold or expected_department_confidence < (confidence_threshold - 0.15):
        actions = [
            RemediationAction(
                action_type="owner_review",
                title="低信頼判定の所有者レビュー",
                reason="LLM 推論の信頼度が閾値未満のため自動是正を保留",
                payload={
                    "analysis_confidence": analysis_confidence,
                    "expected_department_confidence": expected_department_confidence,
                    "requested_action": remediation_action,
                    "existing_actions": [a.to_dict() for a in actions],
                },
                executable=False,
            )
        ]

    # Low-confidence gate replaces the plan with owner_review; restore executable Graph
    # removals when policy explicitly allows approval/auto + remove_permissions.
    restored_executable: list[RemediationAction] = []
    _policy_mode = remediation_mode.strip().lower()
    _policy_action = remediation_action.strip().lower()
    if _policy_mode in {"approval", "auto"} and _policy_action == "remove_permissions":
        _ext_vec = {
            "external_domain_share",
            "external_email_direct_share",
            "guest_direct_share",
            "external_domain",
            "guest",
        }
        if (
            vectors
            & {
                "public_link",
                "org_link",
                "org_link_view",
                "org_link_edit",
                "org_link_editable",
                "all_users",
            }
            or guards & {"G2", "G3"}
        ) and link_permission_ids:
            restored_executable.append(
                RemediationAction(
                    action_type="remove_permissions",
                    title="共有リンク権限の遮断",
                    reason="公開・組織リンク由来の露出を遮断する（低信頼度ゲート後の Graph 実行可能プラン復元）",
                    permission_ids=list(link_permission_ids),
                    executable=True,
                )
            )
        if vectors.intersection(_ext_vec) and external_permission_ids:
            restored_executable.append(
                RemediationAction(
                    action_type="remove_permissions",
                    title="外部共有権限の遮断",
                    reason="外部ゲスト・外部ドメイン共有を解除する（低信頼度ゲート後の Graph 実行可能プラン復元）",
                    permission_ids=list(external_permission_ids),
                    executable=True,
                )
            )
        if "excessive_permissions" in vectors:
            _excess_ids = writable_non_owner_ids or non_owner_ids
            if _excess_ids:
                restored_executable.append(
                    RemediationAction(
                        action_type="remove_permissions",
                        title="過剰権限の縮退",
                        reason="非所有者権限を整理して最小権限化する（低信頼度ゲート後の Graph 実行可能プラン復元）",
                        permission_ids=_excess_ids,
                        executable=True,
                    )
                )
    if restored_executable:
        actions = restored_executable

    if categories & {"executive_confidential", "security_incident"}:
        requested_mode = remediation_mode.strip().lower()
        review_mode = "owner_review" if requested_mode in {"", "auto"} else requested_mode
        actions = [
            RemediationAction(
                action_type=review_mode,
                title="高機密カテゴリの承認レビュー",
                reason="executive_confidential/security_incident は自動是正を禁止",
                payload={"doc_categories": sorted(categories), "existing_actions": [a.to_dict() for a in actions]},
                executable=False,
            )
        ]

    # Policy-driven external-share remediation should remain executable even when
    # high-sensitivity categories exist; otherwise actions are downgraded to
    # non-executable review and Graph deletion never runs.
    requested_mode = remediation_mode.strip().lower()
    requested_action = remediation_action.strip().lower()
    external_share_vectors = {
        "external_domain_share",
        "external_email_direct_share",
        "guest_direct_share",
        "external_domain",
        "guest",
    }
    if (
        requested_mode == "approval"
        and requested_action == "remove_permissions"
        and vectors.intersection(external_share_vectors)
    ):
        priority_ids = external_permission_ids or removable_direct_permission_ids
        if priority_ids:
            actions = [
                RemediationAction(
                    action_type="remove_permissions",
                    title="外部共有権限の遮断",
                    reason="approval/remove_permissions 方針に従い外部共有権限を解除",
                    permission_ids=_unique_permission_ids(
                        [{"id": permission_id} for permission_id in priority_ids]
                    ),
                    executable=True,
                )
            ]

    if not actions:
        actions.append(
            RemediationAction(
                action_type="manual_review",
                title="手動レビュー",
                reason="自動実行可能な差分がないため手動判断を要求",
                payload={
                    "permission_count": len(_unique_permission_ids(permission_rows)),
                    "non_removable_permission_ids": inherited_or_site_default_ids,
                },
                executable=False,
            )
        )
    return actions
