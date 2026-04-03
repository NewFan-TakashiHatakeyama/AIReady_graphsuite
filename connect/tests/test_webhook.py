"""T-037: Webhook テスト

テスト対象: src/connectors/m365/webhook.py
- validation リクエスト判定 (POST + GET)
- URL デコード付きトークン取得
- clientState 不一致検出
- 正常通知のパース
- ヘルスチェック判定
"""

import base64
import json

import pytest

from src.connectors.m365.webhook import (
    extract_resource_info,
    get_validation_token,
    is_health_check,
    is_validation_request,
    parse_notification_body,
    verify_client_state,
)


# ── is_validation_request ──


class TestIsValidationRequest:
    """バリデーションリクエスト判定テスト"""

    def test_post_with_validation_token(self):
        """POST + validationToken → True (Graph API の実際の動作)"""
        event = {
            "httpMethod": "POST",
            "path": "/webhook",
            "queryStringParameters": {"validationToken": "test-token"},
        }
        assert is_validation_request(event) is True

    def test_get_with_validation_token(self):
        """GET + validationToken → True (互換性)"""
        event = {
            "httpMethod": "GET",
            "path": "/webhook",
            "queryStringParameters": {"validationToken": "test-token"},
        }
        assert is_validation_request(event) is True

    def test_post_without_validation_token(self):
        """POST (通知) → False"""
        event = {
            "httpMethod": "POST",
            "path": "/webhook",
            "queryStringParameters": {},
        }
        assert is_validation_request(event) is False

    def test_none_query_params(self):
        """queryStringParameters が None → False"""
        event = {
            "httpMethod": "POST",
            "path": "/webhook",
            "queryStringParameters": None,
        }
        assert is_validation_request(event) is False

    def test_post_validation_token_in_multi_value_only(self):
        """multiValueQueryStringParameters のみに validationToken（ALB 互換）"""
        event = {
            "httpMethod": "POST",
            "path": "/",
            "queryStringParameters": None,
            "multiValueQueryStringParameters": {"validationToken": ["from-multi"]},
        }
        assert is_validation_request(event) is True


# ── get_validation_token ──


class TestGetValidationToken:
    """バリデーショントークン取得テスト"""

    def test_plain_token(self):
        """プレーントークン取得"""
        event = {"queryStringParameters": {"validationToken": "simple-token"}}
        assert get_validation_token(event) == "simple-token"

    def test_url_encoded_token(self):
        """URL エンコード済みトークンのデコード (ALB の挙動再現)"""
        encoded = "Validation%3a+Testing+client+application+reachability"
        event = {"queryStringParameters": {"validationToken": encoded}}
        assert get_validation_token(event) == "Validation: Testing client application reachability"

    def test_empty_query(self):
        """空のクエリ → 空文字列"""
        event = {"queryStringParameters": {}}
        assert get_validation_token(event) == ""

    def test_none_query(self):
        """None → 空文字列"""
        event = {"queryStringParameters": None}
        assert get_validation_token(event) == ""

    def test_token_from_multi_value_only(self):
        event = {
            "queryStringParameters": None,
            "multiValueQueryStringParameters": {"validationToken": ["abc%2Bdef"]},
        }
        assert get_validation_token(event) == "abc+def"


# ── is_health_check ──


class TestIsHealthCheck:
    """ヘルスチェック判定テスト"""

    def test_get_root(self):
        """GET / → True"""
        event = {
            "httpMethod": "GET",
            "path": "/",
            "queryStringParameters": {},
        }
        assert is_health_check(event) is True

    def test_get_empty_path_is_health(self):
        """ALB が path を空文字で渡す GET をヘルスとみなす（Graph 検証以外のプローブで 405 にしない）"""
        event = {
            "httpMethod": "GET",
            "path": "",
            "queryStringParameters": {},
        }
        assert is_health_check(event) is True

    def test_get_root_with_validation_token(self):
        """GET / + validationToken → False (バリデーションを優先)"""
        event = {
            "httpMethod": "GET",
            "path": "/",
            "queryStringParameters": {"validationToken": "xxx"},
        }
        assert is_health_check(event) is False

    def test_post_root(self):
        """POST / → False"""
        event = {
            "httpMethod": "POST",
            "path": "/",
            "queryStringParameters": {},
        }
        assert is_health_check(event) is False

    def test_get_webhook_path(self):
        """GET /webhook → False (パス不一致)"""
        event = {
            "httpMethod": "GET",
            "path": "/webhook",
            "queryStringParameters": {},
        }
        assert is_health_check(event) is False


# ── verify_client_state ──


class TestVerifyClientState:
    """clientState 検証テスト"""

    def test_valid_client_state(self):
        """一致 → True"""
        notification = {"clientState": "my-secret-state"}
        assert verify_client_state(notification, "my-secret-state") is True

    def test_invalid_client_state(self):
        """不一致 → False"""
        notification = {"clientState": "wrong-state"}
        assert verify_client_state(notification, "my-secret-state") is False

    def test_missing_client_state(self):
        """clientState 無し → False"""
        notification = {}
        assert verify_client_state(notification, "my-secret-state") is False


# ── parse_notification_body ──


class TestParseNotificationBody:
    """通知ボディパーステスト"""

    def test_valid_json_body(self, load_fixture):
        """正常な通知ペイロードのパース"""
        payload = load_fixture("notification_payload.json")
        event = {
            "body": json.dumps(payload),
            "isBase64Encoded": False,
        }
        notifications = parse_notification_body(event)
        assert len(notifications) == 1
        assert notifications[0]["subscriptionId"] == "sub-001"
        assert notifications[0]["changeType"] == "updated"

    def test_base64_encoded_body(self, load_fixture):
        """Base64 エンコードボディのデコード"""
        payload = load_fixture("notification_payload.json")
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        event = {
            "body": encoded,
            "isBase64Encoded": True,
        }
        notifications = parse_notification_body(event)
        assert len(notifications) == 1
        assert notifications[0]["subscriptionId"] == "sub-001"

    def test_empty_body(self):
        """空のボディ → 空リスト"""
        event = {"body": ""}
        assert parse_notification_body(event) == []

    def test_invalid_json(self):
        """不正な JSON → 空リスト"""
        event = {"body": "not-json", "isBase64Encoded": False}
        assert parse_notification_body(event) == []

    def test_no_value_key(self):
        """value キーなし → 空リスト"""
        event = {"body": json.dumps({"data": "no value"}), "isBase64Encoded": False}
        assert parse_notification_body(event) == []


# ── extract_resource_info ──


class TestExtractResourceInfo:
    """リソース情報抽出テスト"""

    def test_normal_notification(self, load_fixture):
        """正常な通知からリソース情報を抽出"""
        payload = load_fixture("notification_payload.json")
        notification = payload["value"][0]
        info = extract_resource_info(notification)

        assert info["subscription_id"] == "sub-001"
        assert info["change_type"] == "updated"
        assert info["resource_type"] == "drive"
        assert info["drive_id"] == "b!test-drive-id-12345"
        assert info["item_id"] == "ITEM001_FILE"
        assert info["resource"] == "drives/b!test-drive-id-12345/root"

    def test_empty_notification(self):
        """空の通知 → デフォルト値"""
        info = extract_resource_info({})
        assert info["subscription_id"] == ""
        assert info["change_type"] == ""
        assert info["resource_type"] == "unknown"
        assert info["drive_id"] == ""
        assert info["item_id"] == ""

    def test_teams_channel_message_notification(self):
        notification = {
            "subscriptionId": "sub-msg-1",
            "changeType": "created",
            "resource": "teams/team-1/channels/channel-1/messages/170000",
            "resourceData": {"id": "170000"},
        }
        info = extract_resource_info(notification)
        assert info["resource_type"] == "message"
        assert info["team_id"] == "team-1"
        assert info["channel_id"] == "channel-1"
        assert info["message_id"] == "170000"
        assert info["item_id"] == "170000"

    def test_chat_message_notification(self):
        notification = {
            "subscriptionId": "sub-msg-2",
            "changeType": "updated",
            "resource": "chats/19:chat-id@thread.v2/messages/180000",
            "resourceData": {"id": "180000"},
        }
        info = extract_resource_info(notification)
        assert info["resource_type"] == "message"
        assert info["chat_id"] == "19:chat-id@thread.v2"
        assert info["message_id"] == "180000"
