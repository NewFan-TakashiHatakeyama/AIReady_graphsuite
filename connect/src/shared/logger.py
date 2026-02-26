"""T-017: 構造化ログモジュール

JSON 形式のログを出力し、tenant_id / event_id / request_id を必須フィールドとして含む。
CloudWatch Logs Insights での検索を容易にする。
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any


class StructuredFormatter(logging.Formatter):
    """JSON 形式のログフォーマッター"""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "function_name": os.getenv("AWS_LAMBDA_FUNCTION_NAME", "local"),
            "request_id": getattr(record, "request_id", ""),
            "tenant_id": getattr(record, "tenant_id", ""),
            "event_id": getattr(record, "event_id", ""),
        }

        # 追加のコンテキスト情報
        extra = getattr(record, "extra_data", None)
        if extra and isinstance(extra, dict):
            log_entry["data"] = extra

        # 例外情報
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False, default=str)


def get_logger(
    name: str,
    tenant_id: str = "",
    request_id: str = "",
) -> logging.Logger:
    """構造化ログ出力用の Logger を取得する

    Args:
        name: ロガー名（通常は __name__）
        tenant_id: テナント ID
        request_id: Lambda リクエスト ID

    Returns:
        設定済みの Logger インスタンス
    """
    logger = logging.getLogger(name)

    # ハンドラの重複防止
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)

    logger.setLevel(logging.DEBUG if os.getenv("LOG_LEVEL") == "DEBUG" else logging.INFO)

    # デフォルトのコンテキスト情報を LogRecord に付与するフィルター
    class ContextFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            if not hasattr(record, "tenant_id") or not record.tenant_id:
                record.tenant_id = tenant_id  # type: ignore[attr-defined]
            if not hasattr(record, "request_id") or not record.request_id:
                record.request_id = request_id  # type: ignore[attr-defined]
            if not hasattr(record, "event_id"):
                record.event_id = ""  # type: ignore[attr-defined]
            return True

    # 既存フィルタをクリアして再設定
    logger.filters.clear()
    logger.addFilter(ContextFilter())

    return logger


def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    *,
    event_id: str = "",
    extra_data: dict[str, Any] | None = None,
    exc_info: bool = False,
) -> None:
    """コンテキスト情報を付与してログ出力する

    Args:
        logger: Logger インスタンス
        level: ログレベル (logging.INFO など)
        message: メッセージ
        event_id: イベント ID
        extra_data: 追加データ
        exc_info: 例外情報を含めるか
    """
    logger.log(
        level,
        message,
        extra={"event_id": event_id, "extra_data": extra_data},
        exc_info=exc_info,
    )
