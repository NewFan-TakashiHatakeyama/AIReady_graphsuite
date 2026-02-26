"""secret_detector 単体テスト

Secret/Credential 検出の各パターンを検証する。
"""

import pytest

from services.secret_detector import SecretDetectionResult, detect_secrets


class TestDetectSecrets:
    def test_aws_access_key(self):
        text = "config: AKIAIOSFODNN7EXAMPLE"
        result = detect_secrets(text)
        assert result.detected is True
        assert "aws_access_key" in result.types
        assert result.count >= 1

    def test_aws_secret_key(self):
        text = "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY01"
        result = detect_secrets(text)
        assert result.detected is True
        assert "aws_secret_key" in result.types

    def test_github_token(self):
        text = "token = ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn"
        result = detect_secrets(text)
        assert result.detected is True
        assert "github_token" in result.types

    def test_slack_token(self):
        text = "SLACK_TOKEN=xoxb-123456789012-1234567890123-abcdefghij"
        result = detect_secrets(text)
        assert result.detected is True
        assert "slack_token" in result.types

    def test_generic_password(self):
        text = 'password = "MySecretP@ssw0rd!"'
        result = detect_secrets(text)
        assert result.detected is True
        assert "generic_password" in result.types

    def test_jwt_token(self):
        text = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0.Gfx6VO9tcxwk6xqx9yYzSfebfeakZp5JYIgP_edcw_A"
        result = detect_secrets(text)
        assert result.detected is True
        assert "jwt_token" in result.types

    def test_private_key(self):
        text = "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA..."
        result = detect_secrets(text)
        assert result.detected is True
        assert "private_key" in result.types

    def test_generic_api_key(self):
        text = 'api_key = "example_non_secret_key_for_test_only"'
        result = detect_secrets(text)
        assert result.detected is True
        assert "generic_api_key" in result.types

    def test_connection_string(self):
        text = "Server=myserver.database.windows.net;User ID=admin;Password=secretpass123"
        result = detect_secrets(text)
        assert result.detected is True
        assert "connection_string" in result.types

    def test_no_secrets(self):
        text = "This is a normal document about project planning. No secrets here."
        result = detect_secrets(text)
        assert result.detected is False
        assert result.count == 0
        assert result.types == []

    def test_empty_text(self):
        result = detect_secrets("")
        assert result.detected is False
        assert result.count == 0

    def test_multiple_secrets(self):
        text = (
            "AWS_KEY: AKIAIOSFODNN7EXAMPLE\n"
            "password = SuperSecret123!\n"
            "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn\n"
        )
        result = detect_secrets(text)
        assert result.detected is True
        assert result.count >= 3
        assert "aws_access_key" in result.types
        assert "generic_password" in result.types
        assert "github_token" in result.types

    def test_deduplication(self):
        text = "password=MySecretPassword123"
        result = detect_secrets(text)
        assert result.detected is True
        assert result.count >= 1


class TestSecretDetectionResult:
    def test_default_result(self):
        result = SecretDetectionResult()
        assert result.detected is False
        assert result.types == []
        assert result.count == 0
        assert result.details == []
