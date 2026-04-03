"""共有スコープ判定（Connect M365 normalizer と同じ優先度）。"""

from __future__ import annotations

from typing import Any


def determine_sharing_scope_from_permissions(permissions: list[dict[str, Any]]) -> str:
    """権限リストから sharing_scope を返す。

    優先順位: anonymous > organization > specific_users > private
    （`connect/src/connectors/m365/normalizer.py` の `determine_sharing_scope` と整合）
    """
    scope = "private"
    for perm in permissions:
        link = perm.get("link", {})
        if not isinstance(link, dict):
            continue
        link_scope = str(link.get("scope", "") or "").strip().lower()
        if link_scope == "anonymous":
            return "anonymous"
        if link_scope == "organization":
            scope = "organization"
        elif perm.get("grantedToV2") or perm.get("grantedTo"):
            if scope == "private":
                scope = "specific_users"
    return scope
