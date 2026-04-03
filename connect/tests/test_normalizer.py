"""T-038: 正規化テスト

テスト対象: src/connectors/m365/normalizer.py
- DriveItem (ファイル) の正規化
- 削除アイテムの正規化
- 共有スコープ判定 (anonymous / organization / specific_users / private)
"""

import json

import pytest

from src.connectors.m365.normalizer import (
    determine_sharing_scope,
    normalize_deleted_item,
    normalize_item,
)

DRIVE_ID = "b!test-drive-id-12345"
TENANT_ID = "test-tenant"


# ── normalize_item (ファイル) ──


class TestNormalizeItem:
    """DriveItem 正規化テスト"""

    def test_file_item_basic_fields(self, load_fixture):
        """ファイルアイテムの基本フィールドが正しく変換される"""
        item = load_fixture("drive_item_file.json")
        permissions = [
            {"grantedTo": {"user": {"id": "user-001"}}, "roles": ["read"]}
        ]

        result = normalize_item(item, permissions, DRIVE_ID, TENANT_ID)

        assert result["item_id"] == "ITEM001_FILE"
        assert result["name"] == "test-report-2026.docx"
        assert result["drive_id"] == DRIVE_ID
        assert result["tenant_id"] == TENANT_ID
        assert result["size"] == 15360
        assert result["is_file"] is True
        assert result["is_folder"] is False
        assert result["is_deleted"] is False
        assert result["mime_type"] == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

    def test_file_item_datetime_fields(self, load_fixture):
        """日時フィールドが正しく保存される"""
        item = load_fixture("drive_item_file.json")
        result = normalize_item(item, [], DRIVE_ID, TENANT_ID)

        assert result["created_at"] == "2026-01-15T10:00:00Z"
        assert result["modified_at"] == "2026-02-08T14:30:00Z"

    def test_file_item_user_info(self, load_fixture):
        """作成者・更新者情報が展開される"""
        item = load_fixture("drive_item_file.json")
        result = normalize_item(item, [], DRIVE_ID, TENANT_ID)

        assert result["created_by_user_name"] == "Taro Yamada"
        assert result["created_by_user_id"] == "user-001"
        assert result["modified_by_user_name"] == "Hanako Suzuki"
        assert result["modified_by_app_name"] == "GraphSuite-FileSync"

    def test_file_item_parent_reference(self, load_fixture):
        """親参照が正しく変換される"""
        item = load_fixture("drive_item_file.json")
        result = normalize_item(item, [], DRIVE_ID, TENANT_ID)

        assert result["parent_item_id"] == "PARENT001"
        assert result["parent_drive_type"] == "documentLibrary"
        assert result["parent_site_id"] == "site-001"

    def test_file_item_has_raw_json(self, load_fixture):
        """raw_item に元の JSON が完全に保存される"""
        item = load_fixture("drive_item_file.json")
        result = normalize_item(item, [], DRIVE_ID, TENANT_ID)

        assert "raw_item" in result
        raw = json.loads(result["raw_item"])
        assert raw["id"] == "ITEM001_FILE"
        assert raw["name"] == "test-report-2026.docx"

    def test_file_item_has_synced_at(self, load_fixture):
        """synced_at が設定される"""
        item = load_fixture("drive_item_file.json")
        result = normalize_item(item, [], DRIVE_ID, TENANT_ID)

        assert "synced_at" in result
        assert result["sync_source"] == "delta_query"

    def test_source_metadata_includes_link_and_external_recipients(self, load_fixture):
        """source_metadata に A/B 判定用の最小証跡が入る"""
        item = load_fixture("drive_item_file.json")
        permissions = [
            {
                "id": "perm-org-edit",
                "roles": ["write"],
                "link": {
                    "scope": "organization",
                    "type": "edit",
                    "webUrl": "https://contoso.sharepoint.com/:w:/r/org-edit",
                },
            },
            {
                "id": "perm-anon",
                "roles": ["read"],
                "link": {
                    "scope": "anonymous",
                    "type": "view",
                    "webUrl": "https://contoso.sharepoint.com/:w:/g/anon-link",
                },
            },
            {
                "id": "perm-external",
                "roles": ["write"],
                "grantedToV2": {
                    "user": {
                        "id": "user-ext-1",
                        "displayName": "External User",
                        "email": "stayhungry.stayfoolish.1990@gmail.com",
                        "userType": "guest",
                    }
                },
            },
        ]

        result = normalize_item(
            item,
            permissions,
            DRIVE_ID,
            TENANT_ID,
            tenant_domains=["contoso.com"],
        )
        source_metadata = json.loads(result["source_metadata"])

        assert "stayhungry.stayfoolish.1990@gmail.com" in source_metadata["external_recipients"]
        assert "https://contoso.sharepoint.com/:w:/r/org-edit" in source_metadata["org_edit_links"]
        assert "https://contoso.sharepoint.com/:w:/g/anon-link" in source_metadata["anonymous_links"]
        assert isinstance(source_metadata.get("permission_targets"), list)
        assert source_metadata.get("tenant_domains") == ["contoso.com"]

    def test_folder_item(self):
        """フォルダアイテムの正規化"""
        item = {
            "id": "FOLDER001",
            "name": "Documents",
            "folder": {"childCount": 5},
            "createdDateTime": "2026-01-01T00:00:00Z",
            "lastModifiedDateTime": "2026-02-01T00:00:00Z",
            "createdBy": {},
            "lastModifiedBy": {},
        }
        result = normalize_item(item, [], DRIVE_ID, TENANT_ID)

        assert result["is_folder"] is True
        assert result["is_file"] is False
        assert result["child_count"] == 5


# ── normalize_deleted_item ──


class TestNormalizeDeletedItem:
    """削除アイテム正規化テスト"""

    def test_deleted_item(self, load_fixture):
        """削除アイテムが最小情報で正規化される"""
        item = load_fixture("drive_item_deleted.json")
        result = normalize_deleted_item(item, DRIVE_ID, TENANT_ID)

        assert result["item_id"] == "ITEM002_DELETED"
        assert result["name"] == "old-document.pdf"
        assert result["is_deleted"] is True
        assert result["deleted_state"] == "deleted"
        assert result["sharing_scope"] == "unknown"
        assert result["drive_id"] == DRIVE_ID
        assert result["tenant_id"] == TENANT_ID

    def test_deleted_item_has_raw_json(self, load_fixture):
        """削除アイテムも raw_item を保持する"""
        item = load_fixture("drive_item_deleted.json")
        result = normalize_deleted_item(item, DRIVE_ID, TENANT_ID)

        raw = json.loads(result["raw_item"])
        assert raw["deleted"]["state"] == "deleted"


# ── determine_sharing_scope ──


class TestDetermineSharingScope:
    """共有スコープ判定テスト"""

    def test_no_permissions(self):
        """権限なし → private"""
        assert determine_sharing_scope([]) == "private"

    def test_specific_users(self):
        """特定ユーザー共有 → specific_users"""
        perms = [
            {"grantedTo": {"user": {"id": "user-001"}}, "roles": ["read"]},
        ]
        assert determine_sharing_scope(perms) == "specific_users"

    def test_organization_link(self):
        """組織共有リンク → organization"""
        perms = [
            {"link": {"scope": "organization", "type": "view"}},
        ]
        assert determine_sharing_scope(perms) == "organization"

    def test_anonymous_link(self):
        """匿名リンク → anonymous"""
        perms = [
            {"link": {"scope": "anonymous", "type": "view"}},
        ]
        assert determine_sharing_scope(perms) == "anonymous"

    def test_anonymous_overrides_all(self):
        """anonymous は他の権限より優先される"""
        perms = [
            {"grantedTo": {"user": {"id": "user-001"}}, "roles": ["read"]},
            {"link": {"scope": "organization", "type": "view"}},
            {"link": {"scope": "anonymous", "type": "view"}},
        ]
        assert determine_sharing_scope(perms) == "anonymous"

    def test_organization_overrides_specific(self):
        """organization は specific_users より優先される"""
        perms = [
            {"grantedTo": {"user": {"id": "user-001"}}, "roles": ["read"]},
            {"link": {"scope": "organization", "type": "view"}},
        ]
        assert determine_sharing_scope(perms) == "organization"

    def test_granted_to_v2(self):
        """grantedToV2 でも specific_users と判定"""
        perms = [
            {"grantedToV2": {"user": {"id": "user-001"}}, "roles": ["read"]},
        ]
        assert determine_sharing_scope(perms) == "specific_users"
