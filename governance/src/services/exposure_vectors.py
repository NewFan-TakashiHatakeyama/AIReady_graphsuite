"""ExposureVector 抽出 — メタデータから露出要因を識別子リストとして抽出する

詳細設計 3.5 節準拠
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from shared.config import (
    SSM_PERMISSIONS_COUNT_THRESHOLD,
    get_ssm_int,
)
from shared.logger import get_logger

logger = get_logger(__name__)

# EEEU (Everyone Except External Users) を示す識別子
EEEU_IDENTIFIERS = frozenset({
    "everyone except external users",
    "eeeu",
    "all users",
    "everyone",
    "全社員",
})

GUEST_ROLE_TYPES = frozenset({"guest", "external"})
EXTERNAL_DOMAIN_INDICATORS = frozenset({"#ext#"})
EDITABLE_ROLES = frozenset({"write", "edit", "owner", "manage", "fullcontrol"})


@dataclass
class FileMetadata:
    """Exposure 判定に必要な最小メタデータ DTO。

    Notes:
        Connect の FileMetadata 保存形式から、露出判定に使う属性のみを保持する。
    """

    tenant_id: str
    item_id: str
    source: str = "m365"
    container_id: str = ""
    container_name: str = ""
    container_type: str = ""
    item_name: str = ""
    web_url: str = ""
    sharing_scope: str = "specific"
    permissions: str = "{}"
    permissions_count: int = 0
    sensitivity_label: str | None = None
    sensitivity_label_name: str | None = None
    mime_type: str = ""
    size: int = 0
    modified_at: str | None = None
    is_deleted: bool = False
    raw_s3_key: str = ""
    permissions_summary: str | None = None
    source_metadata: str | None = None
    path: str = ""
    parent_item_id: str = ""
    created_by_user_id: str = ""
    modified_by_user_id: str = ""


def extract_exposure_vectors(metadata: FileMetadata) -> list[str]:
    """メタデータから露出要因を識別子リストとして抽出する。

    Args:
        metadata: 評価対象のファイルメタデータ DTO。

    Returns:
        露出要因ベクトル一覧（例: `public_link`, `guest`）。

    Notes:
        判定の入口。`sharing_scope` と `permissions` を組み合わせて、
        複数ベクトル（例: guest + broken_inheritance）を同時に返す。
    """
    vectors: list[str] = []

    # 1) 共有スコープ由来ベクトル
    scope = (metadata.sharing_scope or "").lower()
    if scope == "anonymous":
        vectors.append("public_link")
    elif scope == "organization":
        vectors.append("org_link")

    source_metadata = parse_source_metadata(metadata.source_metadata)
    tenant_domains = _normalize_tenant_domains(source_metadata.get("tenant_domains", []))

    # 2) 権限詳細由来ベクトル（過剰共有のみ）
    permissions = parse_permissions(metadata.permissions)
    external_recipients = _extract_external_recipients(
        source_metadata=source_metadata,
        permissions=permissions,
        tenant_domains=tenant_domains,
    )

    # A) organization + editable
    if scope == "organization" and _has_org_link_editable(
        permissions=permissions,
        source_metadata=source_metadata,
    ):
        vectors.append("org_link_editable")

    # B) external direct share
    if external_recipients:
        vectors.append("guest_direct_share")
        if any("@" in recipient for recipient in external_recipients):
            vectors.append("external_email_direct_share")
        if any(_is_external_email(recipient, tenant_domains) for recipient in external_recipients):
            vectors.append("external_domain_share")

    if has_eeeu_access(permissions):
        vectors.append("all_users")

    if has_external_guests(permissions):
        vectors.append("guest")

    if has_external_domain_users(permissions):
        vectors.append("external_domain")

    # 3) 権限数しきい値（過剰権限）
    threshold = _get_permissions_threshold()
    if metadata.permissions_count > threshold:
        vectors.append("excessive_permissions")

    return sorted(set(vectors))


def parse_permissions(permissions_json: str | None) -> list[dict[str, Any]]:
    """permissions JSON 文字列をパースしてエントリリストを返す。

    Args:
        permissions_json: Connect 保存形式の permissions JSON 文字列。

    Returns:
        権限エントリの辞書配列。不正形式時は空配列。

    Notes:
        Connect 側の差異（list 形式 / {"entries": [...]} 形式）を吸収する。
    """
    if not permissions_json:
        return []
    try:
        parsed = json.loads(permissions_json)
        if isinstance(parsed, dict):
            return parsed.get("entries", [])
        if isinstance(parsed, list):
            return parsed
        return []
    except (json.JSONDecodeError, TypeError):
        return []


def parse_source_metadata(source_metadata_json: str | None) -> dict[str, Any]:
    if not source_metadata_json:
        return {}
    try:
        parsed = json.loads(source_metadata_json)
        if isinstance(parsed, dict):
            return parsed
        return {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _extract_label_name(raw_label: str | None) -> str | None:
    """ラベル文字列（JSON/平文）からラベル名を抽出する。"""
    if raw_label is None:
        return None
    raw_text = str(raw_label).strip()
    if not raw_text:
        return None

    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, dict):
            name = str(parsed.get("name") or "").strip()
            return name or None
        if parsed is None:
            return None
        normalized = str(parsed).strip()
        return normalized or None
    except (json.JSONDecodeError, TypeError):
        return raw_text


def _has_effective_sensitivity_label(metadata: FileMetadata) -> bool:
    """sensitivity_label / sensitivity_label_name から有効ラベル有無を判定する。"""
    from_payload = _extract_label_name(metadata.sensitivity_label)
    from_name = str(metadata.sensitivity_label_name or "").strip()
    return bool(from_payload or from_name)


def has_eeeu_access(permissions: list[dict[str, Any]]) -> bool:
    """EEEU（Everyone Except External Users）グループが含まれているか判定する。

    Args:
        permissions: 権限エントリ配列。

    Returns:
        EEEU 相当の権限が見つかれば `True`。

    Notes:
        表示名だけでなくメール表記の `everyone` も補助判定に利用する。
        誤検知を避けるため `external` を含むケースは除外する。
    """
    for identity in _iter_permission_identities(permissions):
        display_name = (identity.get("displayName") or "").lower()
        if display_name in EEEU_IDENTIFIERS:
            return True

        email = (identity.get("email") or "").lower()
        if "everyone" in email and "external" not in email:
            return True
    return False


def has_external_guests(permissions: list[dict[str, Any]]) -> bool:
    """外部ゲストユーザーが含まれているか判定する。

    Args:
        permissions: 権限エントリ配列。

    Returns:
        外部ゲストが含まれていれば `True`。

    Notes:
        `userType` とメール内インジケータ（`#ext#`）の双方を参照する。
    """
    for identity in _iter_permission_identities(permissions):
        user_type = (identity.get("userType") or "").lower()
        if user_type in GUEST_ROLE_TYPES:
            return True

        email = (identity.get("email") or "").lower()
        if any(ind in email for ind in EXTERNAL_DOMAIN_INDICATORS):
            return True
    return False


def has_external_domain_users(permissions: list[dict[str, Any]]) -> bool:
    """外部ドメインユーザーが含まれているか判定する。

    Args:
        permissions: 権限エントリ配列。

    Returns:
        外部ドメインユーザーが含まれていれば `True`。

    Notes:
        `isExternalUser` フラグ、または `domain != orgDomain` の差分で判定する。
    """
    for identity in _iter_permission_identities(permissions):
        is_external = identity.get("isExternalUser", False)
        if is_external:
            return True

        domain = identity.get("domain", "")
        org_domain = identity.get("orgDomain", "")
        if domain and org_domain and domain.lower() != org_domain.lower():
            return True
    return False


def is_broken_inheritance(metadata: FileMetadata) -> bool:
    """権限の継承崩れを判定する。

    Args:
        metadata: 評価対象ファイルのメタデータ DTO。

    Returns:
        継承崩れが検出された場合は `True`。

    Notes:
        Connect が `source_metadata.has_unique_permissions` を保持している場合に判定可能。
        未保持/不正形式では安全側に倒して `False` とする。
    """
    if not metadata.source_metadata:
        return False
    try:
        sm = json.loads(metadata.source_metadata)
        if not isinstance(sm, dict):
            return False
        return bool(sm.get("has_unique_permissions", False))
    except (json.JSONDecodeError, TypeError):
        return False


def evaluate_acl_drift(source_metadata: dict[str, Any]) -> dict[str, bool]:
    permission_delta = source_metadata.get("permission_delta", [])
    effective_hash = str(source_metadata.get("effective_permissions_hash") or "").strip()
    baseline_hash = str(source_metadata.get("baseline_permissions_hash") or "").strip()

    detected = bool(permission_delta) or (
        bool(effective_hash) and bool(baseline_hash) and effective_hash != baseline_hash
    )
    added_principal = False
    privilege_escalation = False
    if isinstance(permission_delta, list):
        for diff in permission_delta:
            if not isinstance(diff, dict):
                continue
            change = str(diff.get("change") or "").strip().lower()
            if change == "added":
                added_principal = True
            if change in {"escalation", "privilege_escalation"}:
                privilege_escalation = True

    return {
        "detected": detected,
        "added_principal": added_principal,
        "privilege_escalation": privilege_escalation,
    }


def _normalize_tenant_domains(raw_domains: Any) -> set[str]:
    if not isinstance(raw_domains, list):
        return set()
    return {
        str(domain).strip().lower()
        for domain in raw_domains
        if str(domain).strip()
    }


def _extract_domain(email: str) -> str:
    if "@" not in email:
        return ""
    return email.rsplit("@", 1)[-1].strip().lower()


def _is_external_email(email: str, tenant_domains: set[str]) -> bool:
    normalized = str(email or "").strip().lower()
    if not normalized:
        return False
    if "#ext#" in normalized:
        return True
    domain = _extract_domain(normalized)
    if not domain:
        return False
    if not tenant_domains:
        return not domain.endswith(".onmicrosoft.com")
    return domain not in tenant_domains


def _extract_external_recipients(
    source_metadata: dict[str, Any],
    permissions: list[dict[str, Any]],
    tenant_domains: set[str],
) -> list[str]:
    recipients = set()
    for recipient in source_metadata.get("external_recipients", []):
        normalized = str(recipient or "").strip().lower()
        if normalized:
            recipients.add(normalized)

    if recipients:
        return sorted(recipients)

    for identity in _iter_permission_identities(permissions):
        email = str(identity.get("email") or "").strip().lower()
        if not email:
            continue
        user_type = str(identity.get("userType") or "").strip().lower()
        if user_type in GUEST_ROLE_TYPES or _is_external_email(email, tenant_domains):
            recipients.add(email)
    return sorted(recipients)


def _has_org_link_editable(
    permissions: list[dict[str, Any]],
    source_metadata: dict[str, Any],
) -> bool:
    org_edit_links = source_metadata.get("org_edit_links", [])
    if isinstance(org_edit_links, list) and len(org_edit_links) > 0:
        return True

    for entry in permissions:
        if not isinstance(entry, dict):
            continue
        link = entry.get("link", {})
        if not isinstance(link, dict):
            continue
        scope = str(link.get("scope") or "").strip().lower()
        if scope != "organization":
            continue
        link_type = str(link.get("type") or "").strip().lower()
        if link_type == "edit":
            return True
        roles = {
            str(role).strip().lower()
            for role in (entry.get("roles") or [])
            if str(role).strip()
        }
        if EDITABLE_ROLES.intersection(roles):
            return True
    return False


def _iter_permission_identities(permissions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    identities: list[dict[str, Any]] = []

    for entry in permissions:
        if not isinstance(entry, dict):
            continue

        identity = entry.get("identity")
        if isinstance(identity, dict):
            identities.append(identity)

        for key in ("grantedToV2", "grantedTo"):
            principal = entry.get(key)
            if isinstance(principal, dict) and isinstance(principal.get("user"), dict):
                identities.append(principal["user"])

        for key in ("grantedToIdentitiesV2", "grantedToIdentities"):
            principals = entry.get(key)
            if not isinstance(principals, list):
                continue
            for principal in principals:
                if isinstance(principal, dict) and isinstance(principal.get("user"), dict):
                    identities.append(principal["user"])

    return identities


def _get_permissions_threshold() -> int:
    """`excessive_permissions` 判定閾値を SSM から取得する。

    Args:
        なし。

    Returns:
        閾値（件数）。

    Notes:
        取得失敗時はデフォルト値 50 を返す。
    """
    try:
        return get_ssm_int(SSM_PERMISSIONS_COUNT_THRESHOLD, default=50)
    except Exception:
        return 50
