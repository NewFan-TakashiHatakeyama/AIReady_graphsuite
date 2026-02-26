"""T-039: Delta Query テスト

テスト対象: src/connectors/m365/delta.py + src/shared/dynamodb.py
- ページングの正しいハンドリング
- deltaLink の保存と取得
- DynamoDB ヘルパー (冪等チェック, FileMetadata CRUD)
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.shared.dynamodb import (
    get_delta_token,
    get_file_metadata,
    is_already_processed,
    mark_as_processed,
    put_file_metadata,
    save_delta_token,
)
from src.connectors.m365.delta import fetch_delta


# ── DeltaTokens CRUD ──


class TestDeltaTokens:
    """DeltaTokens テーブルのテスト"""

    def test_save_and_get_delta_token(self, aws_mock):
        """deltaLink の保存と取得"""
        drive_id = "b!test-drive"
        delta_link = "https://graph.microsoft.com/v1.0/drives/xxx/root/delta?token=abc123"

        save_delta_token(drive_id, delta_link)
        result = get_delta_token(drive_id)

        assert result == delta_link

    def test_get_delta_token_not_found(self, aws_mock):
        """未保存の drive_id → None"""
        result = get_delta_token("nonexistent-drive")
        assert result is None

    def test_overwrite_delta_token(self, aws_mock):
        """deltaLink の上書き"""
        drive_id = "b!test-drive"
        save_delta_token(drive_id, "old-link")
        save_delta_token(drive_id, "new-link")

        result = get_delta_token(drive_id)
        assert result == "new-link"


# ── IdempotencyKeys ──


class TestIdempotencyKeys:
    """冪等チェックテスト"""

    def test_not_processed(self, aws_mock):
        """未処理イベント → False"""
        assert is_already_processed("event-001") is False

    def test_mark_and_check(self, aws_mock):
        """処理済みマーク → True"""
        mark_as_processed("event-001", "test-tenant")
        assert is_already_processed("event-001") is True

    def test_different_events(self, aws_mock):
        """別のイベント ID → False"""
        mark_as_processed("event-001")
        assert is_already_processed("event-002") is False


# ── FileMetadata CRUD ──


class TestFileMetadata:
    """FileMetadata テーブルのテスト"""

    def test_put_and_get(self, aws_mock):
        """メタデータの保存と取得"""
        item = {
            "drive_id": "b!test-drive",
            "item_id": "ITEM001",
            "name": "test.docx",
            "size": 1024,
            "is_file": True,
            "is_deleted": False,
        }
        put_file_metadata(item)
        result = get_file_metadata("b!test-drive", "ITEM001")

        assert result is not None
        assert result["name"] == "test.docx"
        assert result["size"] == 1024

    def test_get_not_found(self, aws_mock):
        """存在しないアイテム → None"""
        result = get_file_metadata("b!test-drive", "NONEXISTENT")
        assert result is None

    def test_upsert_overwrites(self, aws_mock):
        """同一キーの上書き (upsert)"""
        item1 = {
            "drive_id": "b!test-drive",
            "item_id": "ITEM001",
            "name": "v1.docx",
            "size": 100,
        }
        item2 = {
            "drive_id": "b!test-drive",
            "item_id": "ITEM001",
            "name": "v2.docx",
            "size": 200,
        }
        put_file_metadata(item1)
        put_file_metadata(item2)

        result = get_file_metadata("b!test-drive", "ITEM001")
        assert result["name"] == "v2.docx"
        assert result["size"] == 200


# ── fetch_delta (モックテスト) ──


class TestFetchDelta:
    """Delta Query ページングテスト"""

    @patch("src.connectors.m365.delta.get_config")
    @patch("src.connectors.m365.delta.get_delta_token")
    @patch("src.connectors.m365.delta.save_delta_token")
    def test_single_page_delta(self, mock_save, mock_get_token, mock_config):
        """単一ページの Delta Query"""
        mock_config.return_value = MagicMock(
            graph_base_url="https://graph.microsoft.com/v1.0"
        )
        mock_get_token.return_value = None  # 初回スキャン

        client = MagicMock()
        # 初回 URL も http:// 始まりなので client.get() が呼ばれる
        client.get.return_value = MagicMock(
            json=MagicMock(return_value={
                "value": [
                    {"id": "item1", "name": "file1.txt"},
                    {"id": "item2", "name": "file2.txt"},
                ],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta?token=new",
            })
        )

        items = fetch_delta(client, "b!test-drive")

        assert len(items) == 2
        assert items[0]["id"] == "item1"
        assert items[1]["id"] == "item2"
        mock_save.assert_called_once_with(
            "b!test-drive",
            "https://graph.microsoft.com/v1.0/delta?token=new",
        )

    @patch("src.connectors.m365.delta.get_config")
    @patch("src.connectors.m365.delta.get_delta_token")
    @patch("src.connectors.m365.delta.save_delta_token")
    def test_multi_page_delta(self, mock_save, mock_get_token, mock_config):
        """複数ページの Delta Query (nextLink → deltaLink)"""
        mock_config.return_value = MagicMock(
            graph_base_url="https://graph.microsoft.com/v1.0"
        )
        mock_get_token.return_value = None

        client = MagicMock()
        # ページ 1: nextLink あり → ページ 2: deltaLink あり (最終ページ)
        page1 = {
            "value": [{"id": "item1"}],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/delta?skip=1",
        }
        page2 = {
            "value": [{"id": "item2"}],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta?token=final",
        }

        # 全 URL が http 始まりなので client.get() が連続呼び出しされる
        resp1 = MagicMock(json=MagicMock(return_value=page1))
        resp2 = MagicMock(json=MagicMock(return_value=page2))
        client.get.side_effect = [resp1, resp2]

        items = fetch_delta(client, "b!test-drive")

        assert len(items) == 2
        assert client.get.call_count == 2
        mock_save.assert_called_once_with(
            "b!test-drive",
            "https://graph.microsoft.com/v1.0/delta?token=final",
        )

    @patch("src.connectors.m365.delta.get_config")
    @patch("src.connectors.m365.delta.get_delta_token")
    @patch("src.connectors.m365.delta.save_delta_token")
    def test_uses_saved_delta_link(self, mock_save, mock_get_token, mock_config):
        """保存済み deltaLink がある場合それを使用する"""
        mock_config.return_value = MagicMock(
            graph_base_url="https://graph.microsoft.com/v1.0"
        )
        saved_link = "https://graph.microsoft.com/v1.0/delta?token=saved"
        mock_get_token.return_value = saved_link

        client = MagicMock()
        client.get.return_value = MagicMock(
            json=MagicMock(return_value={
                "value": [{"id": "item_changed"}],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/delta?token=updated",
            })
        )

        items = fetch_delta(client, "b!test-drive")

        assert len(items) == 1
        assert items[0]["id"] == "item_changed"
        client.get.assert_called_once()
