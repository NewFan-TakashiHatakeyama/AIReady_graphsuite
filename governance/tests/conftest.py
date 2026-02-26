"""共通テスト設定"""

import os
from pathlib import Path

import pytest


def pytest_addoption(parser):
    """--run-aws フラグを追加する。"""
    parser.addoption(
        "--run-aws",
        action="store_true",
        default=False,
        help="AWS 実環境テスト（tests/aws/）を実行する",
    )


def pytest_collection_modifyitems(config, items):
    """--run-aws が指定されていない場合、aws マーカー付きテストをスキップする。"""
    if config.getoption("--run-aws"):
        return
    skip_aws = pytest.mark.skip(reason="--run-aws フラグが必要です")
    for item in items:
        if "aws" in item.keywords or "tests/aws" in str(item.fspath) or "tests\\aws" in str(item.fspath):
            item.add_marker(skip_aws)


@pytest.fixture(autouse=True)
def aws_env(request, monkeypatch):
    """テスト用の AWS 環境変数を設定（tests/aws/ 配下では適用しない）"""
    test_path = str(request.fspath)
    if "tests/aws" in test_path or "tests\\aws" in test_path:
        return
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("FINDING_TABLE_NAME", "AIReadyGov-ExposureFinding")
    monkeypatch.setenv("SENSITIVITY_QUEUE_URL", "https://sqs.ap-northeast-1.amazonaws.com/123456789012/AIReadyGov-SensitivityDetectionQueue")
    monkeypatch.setenv("RAW_PAYLOAD_BUCKET", "aireadyconnect-raw-payload-123456789012")
    monkeypatch.setenv("REPORT_BUCKET", "aireadygov-reports-123456789012")
    monkeypatch.setenv("CONNECT_TABLE_NAME", "AIReadyConnect-FileMetadata")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
