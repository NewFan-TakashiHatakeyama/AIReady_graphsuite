"""構造化ログユーティリティ。

CloudWatch Logs で扱いやすい JSON フォーマットへ統一し、
全ハンドラ/サービスで同一形式のログを出力できるようにする。
"""

import json
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

TOKYO_TZ = ZoneInfo("Asia/Tokyo")


class JsonFormatter(logging.Formatter):
    """CloudWatch Logs 向けの JSON 構造化ログフォーマッタ"""

    def format(self, record: logging.LogRecord) -> str:
        """LogRecord を JSON 文字列へ整形する。"""
        log_entry = {
            "timestamp": datetime.now(TOKYO_TZ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra_data"):
            log_entry["data"] = record.extra_data

        return json.dumps(log_entry, ensure_ascii=False, default=str)


def get_logger(name: str) -> logging.Logger:
    """構造化ログ対応のロガーを取得する。
    
    Args:
        name: 引数。
    
    Returns:
        戻り値。
    
    Notes:
        なし。
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    return logger
