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


@dataclass
class FileMetadata:
    """FileMetadata テーブルのレコードを表す DTO"""

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


def extract_exposure_vectors(metadata: FileMetadata) -> list[str]:
    """メタデータから露出要因を識別子リストとして抽出する。"""
    vectors: list[str] = []

    scope = (metadata.sharing_scope or "").lower()
    if scope == "anonymous":
        vectors.append("public_link")
    elif scope == "organization":
        vectors.append("org_link")

    permissions = parse_permissions(metadata.permissions)

    if has_eeeu_access(permissions):
        vectors.append("all_users")

    if has_external_guests(permissions):
        vectors.append("guest")

    if has_external_domain_users(permissions):
        vectors.append("external_domain")

    if is_broken_inheritance(metadata):
        vectors.append("broken_inheritance")

    threshold = _get_permissions_threshold()
    if metadata.permissions_count > threshold:
        vectors.append("excessive_permissions")

    return vectors


def parse_permissions(permissions_json: str | None) -> list[dict[str, Any]]:
    """permissions JSON 文字列をパースしてエントリリストを返す。"""
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


def has_eeeu_access(permissions: list[dict[str, Any]]) -> bool:
    """EEEU（Everyone Except External Users）グループが含まれているか判定する。"""
    for entry in permissions:
        identity = entry.get("identity", {})
        display_name = (identity.get("displayName") or "").lower()
        if display_name in EEEU_IDENTIFIERS:
            return True

        email = (identity.get("email") or "").lower()
        if "everyone" in email and "external" not in email:
            return True
    return False


def has_external_guests(permissions: list[dict[str, Any]]) -> bool:
    """外部ゲストユーザーが含まれているか判定する。"""
    for entry in permissions:
        identity = entry.get("identity", {})

        user_type = (identity.get("userType") or "").lower()
        if user_type in GUEST_ROLE_TYPES:
            return True

        email = (identity.get("email") or "").lower()
        if any(ind in email for ind in EXTERNAL_DOMAIN_INDICATORS):
            return True
    return False


def has_external_domain_users(permissions: list[dict[str, Any]]) -> bool:
    """外部ドメインユーザー（ゲストではないが外部組織の正規メンバー）が含まれているか判定する。"""
    for entry in permissions:
        identity = entry.get("identity", {})
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

    Connect が source_metadata 内に ``has_unique_permissions`` フラグを含めている場合、
    それを使って判定する。
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


def _get_permissions_threshold() -> int:
    """excessive_permissions 判定の閾値を SSM から取得する。"""
    try:
        return get_ssm_int(SSM_PERMISSIONS_COUNT_THRESHOLD, default=50)
    except Exception:
        return 50
