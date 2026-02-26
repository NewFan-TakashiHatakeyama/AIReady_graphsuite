"""T-022: DriveItem 正規化 — Graph API の全フィールドを抽出・保存

PoC 段階では取得可能な全フィールドを保存し、
本番環境で必要な情報を精査する方針。

DriveItem の主要プロパティ:
  - 基本情報: id, name, description, size, webUrl, webDavUrl
  - 日時: createdDateTime, lastModifiedDateTime
  - ユーザー: createdBy, lastModifiedBy (IdentitySet)
  - 階層: parentReference (driveId, driveType, id, name, path, siteId)
  - ファイル: file (mimeType, hashes), folder (childCount)
  - メディア: image, video, audio, photo
  - 位置: location (GeoCoordinates)
  - 状態: deleted, shared
  - SharePoint: sharepointIds, listItem
  - バージョン: eTag, cTag
  - セキュリティ: sensitivityLabel, malware
  - 権限: permissions (別途取得)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _safe_get_nested(obj: dict, *keys: str, default: Any = "") -> Any:
    """ネストされた dict から安全に値を取得する"""
    current = obj
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current


def _extract_identity(identity_set: dict[str, Any]) -> dict[str, str]:
    """IdentitySet から user/application/device 情報を抽出する"""
    result = {}
    for key in ("user", "application", "device", "group"):
        identity = identity_set.get(key)
        if identity and isinstance(identity, dict):
            result[key] = {
                "id": identity.get("id", ""),
                "displayName": identity.get("displayName", ""),
                "email": identity.get("email", ""),
            }
    return result


def determine_sharing_scope(permissions: list[dict[str, Any]]) -> str:
    """権限情報から共有スコープを判定する

    優先順位: anonymous > organization > specific_users > private

    Args:
        permissions: Permission オブジェクトのリスト

    Returns:
        "anonymous" | "organization" | "specific_users" | "private"
    """
    scope = "private"

    for perm in permissions:
        link = perm.get("link", {})
        link_scope = link.get("scope", "")

        if link_scope == "anonymous":
            return "anonymous"  # 最も広い共有 → 即確定
        elif link_scope == "organization":
            scope = "organization"
        elif perm.get("grantedToV2") or perm.get("grantedTo"):
            if scope == "private":
                scope = "specific_users"

    return scope


def normalize_item(
    item: dict[str, Any],
    permissions: list[dict[str, Any]],
    drive_id: str,
    tenant_id: str,
) -> dict[str, Any]:
    """DriveItem + Permissions を正規化し、DynamoDB 保存用の dict を返す

    PoC: 取得可能な全フィールドを保存する。

    Args:
        item: Graph API から取得した DriveItem JSON
        permissions: Permission オブジェクトのリスト
        drive_id: ドライブ ID
        tenant_id: テナント識別子

    Returns:
        DynamoDB に保存する正規化済み dict
    """
    now = datetime.now(timezone.utc).isoformat()
    item_id = item.get("id", "")
    is_deleted = item.get("deleted") is not None
    is_folder = item.get("folder") is not None
    is_file = item.get("file") is not None

    # 共有スコープ判定
    sharing_scope = determine_sharing_scope(permissions) if not is_deleted else "unknown"

    # 親参照
    parent_ref = item.get("parentReference", {})

    # ファイル固有情報
    file_info = item.get("file", {})
    folder_info = item.get("folder", {})

    # メディア情報
    image_info = item.get("image", {})
    video_info = item.get("video", {})
    audio_info = item.get("audio", {})
    photo_info = item.get("photo", {})

    # 作成者/更新者
    created_by = item.get("createdBy", {})
    modified_by = item.get("lastModifiedBy", {})

    # SharePoint 固有 ID
    sharepoint_ids = item.get("sharepointIds", {})

    # shared 情報
    shared_info = item.get("shared", {})

    # sensitivity label
    sensitivity_label = item.get("sensitivityLabel", {})

    # malware 情報
    malware_info = item.get("malware", {})

    # ── 正規化結果 ──
    metadata: dict[str, Any] = {
        # ── Primary Key ──
        "drive_id": drive_id,
        "item_id": item_id,

        # ── テナント ──
        "tenant_id": tenant_id,

        # ── 基本情報 ──
        "name": item.get("name", ""),
        "description": item.get("description", ""),
        "size": item.get("size", 0),
        "web_url": item.get("webUrl", ""),
        "web_dav_url": item.get("webDavUrl", ""),
        "etag": item.get("eTag", ""),
        "ctag": item.get("cTag", ""),

        # ── 型フラグ ──
        "is_file": is_file,
        "is_folder": is_folder,
        "is_deleted": is_deleted,

        # ── パス / 階層 ──
        "path": _safe_get_nested(parent_ref, "path", default=""),
        "parent_item_id": _safe_get_nested(parent_ref, "id", default=""),
        "parent_drive_id": _safe_get_nested(parent_ref, "driveId", default=""),
        "parent_drive_type": _safe_get_nested(parent_ref, "driveType", default=""),
        "parent_site_id": _safe_get_nested(parent_ref, "siteId", default=""),

        # ── 日時 ──
        "created_at": item.get("createdDateTime", ""),
        "modified_at": item.get("lastModifiedDateTime", ""),

        # ── ユーザー情報 (展開) ──
        "created_by_user_name": _safe_get_nested(created_by, "user", "displayName", default=""),
        "created_by_user_id": _safe_get_nested(created_by, "user", "id", default=""),
        "created_by_user_email": _safe_get_nested(created_by, "user", "email", default=""),
        "created_by_app_name": _safe_get_nested(created_by, "application", "displayName", default=""),
        "created_by_app_id": _safe_get_nested(created_by, "application", "id", default=""),
        "modified_by_user_name": _safe_get_nested(modified_by, "user", "displayName", default=""),
        "modified_by_user_id": _safe_get_nested(modified_by, "user", "id", default=""),
        "modified_by_user_email": _safe_get_nested(modified_by, "user", "email", default=""),
        "modified_by_app_name": _safe_get_nested(modified_by, "application", "displayName", default=""),
        "modified_by_app_id": _safe_get_nested(modified_by, "application", "id", default=""),

        # ── ファイル情報 ──
        "mime_type": file_info.get("mimeType", ""),
        "file_hashes": json.dumps(file_info.get("hashes", {})),

        # ── フォルダ情報 ──
        "child_count": folder_info.get("childCount", 0) if is_folder else 0,

        # ── メディア情報 (JSON 文字列で保存) ──
        "image_metadata": json.dumps(image_info) if image_info else "",
        "video_metadata": json.dumps(video_info) if video_info else "",
        "audio_metadata": json.dumps(audio_info) if audio_info else "",
        "photo_metadata": json.dumps(photo_info) if photo_info else "",

        # ── 位置情報 ──
        "location": json.dumps(item.get("location", {})) if item.get("location") else "",

        # ── 共有・権限 ──
        "sharing_scope": sharing_scope,
        "shared_info": json.dumps(shared_info) if shared_info else "",
        "permissions": json.dumps(permissions),
        "permissions_count": len(permissions),

        # ── SharePoint 固有 ──
        "sharepoint_ids": json.dumps(sharepoint_ids) if sharepoint_ids else "",
        "list_item": json.dumps(item.get("listItem", {})) if item.get("listItem") else "",

        # ── セキュリティ ──
        "sensitivity_label": json.dumps(sensitivity_label) if sensitivity_label else "",
        "malware_detected": bool(malware_info),
        "malware_info": json.dumps(malware_info) if malware_info else "",

        # ── 特殊フラグ ──
        "is_root": item.get("root") is not None,
        "is_remote": item.get("remoteItem") is not None,
        "remote_item": json.dumps(item.get("remoteItem", {})) if item.get("remoteItem") else "",
        "special_folder": json.dumps(item.get("specialFolder", {})) if item.get("specialFolder") else "",
        "package_type": _safe_get_nested(item.get("package", {}), "type", default=""),

        # ── 削除情報 ──
        "deleted_state": _safe_get_nested(item.get("deleted", {}), "state", default=""),

        # ── Raw JSON (全フィールド保持) ──
        "raw_item": json.dumps(item),
        "raw_created_by": json.dumps(created_by),
        "raw_modified_by": json.dumps(modified_by),
        "raw_parent_reference": json.dumps(parent_ref),

        # ── 同期メタデータ ──
        "synced_at": now,
        "sync_source": "delta_query",
    }

    # 空文字列を除去（DynamoDB のストレージ最適化）
    # ただし Primary Key / Sort Key / GSI キー は除外
    protected_keys = {
        "drive_id", "item_id", "tenant_id", "sharing_scope",
        "modified_at", "synced_at", "name",
    }
    metadata = {
        k: v for k, v in metadata.items()
        if k in protected_keys or (v is not None and v != "" and v != 0 and v != "{}" and v != "[]")
    }

    # 数値 0 やブール False は保持する
    for key in ("size", "child_count", "permissions_count"):
        if key not in metadata:
            original = item.get(key)
            if original is not None:
                metadata[key] = original

    # ブール値は必ず保持
    metadata["is_file"] = is_file
    metadata["is_folder"] = is_folder
    metadata["is_deleted"] = is_deleted
    metadata["is_root"] = item.get("root") is not None
    metadata["malware_detected"] = bool(malware_info)

    return metadata


def normalize_deleted_item(
    item: dict[str, Any],
    drive_id: str,
    tenant_id: str,
) -> dict[str, Any]:
    """削除された DriveItem を正規化する

    削除アイテムは最小限の情報しか持たないため、
    is_deleted=True と基本情報のみ保存する。

    Args:
        item: Delta Query から返された削除アイテム
        drive_id: ドライブ ID
        tenant_id: テナント識別子

    Returns:
        DynamoDB に保存する正規化済み dict
    """
    now = datetime.now(timezone.utc).isoformat()
    return {
        "drive_id": drive_id,
        "item_id": item.get("id", ""),
        "tenant_id": tenant_id,
        "name": item.get("name", ""),
        "is_file": item.get("file") is not None,
        "is_folder": item.get("folder") is not None,
        "is_deleted": True,
        "deleted_state": _safe_get_nested(item.get("deleted", {}), "state", default=""),
        "sharing_scope": "unknown",
        "modified_at": item.get("lastModifiedDateTime", now),
        "synced_at": now,
        "sync_source": "delta_query",
        "raw_item": json.dumps(item),
    }
