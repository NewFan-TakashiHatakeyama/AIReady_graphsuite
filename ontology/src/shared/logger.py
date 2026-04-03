"""構造化ログ出力ユーティリティ。"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

TOKYO_TZ = ZoneInfo("Asia/Tokyo")


def log_structured(level: str, message: str, **kwargs: Any) -> None:
    """共通メタ情報付きの JSON ログを出力する。

    Args:
        level: 入力値。
        message: 入力メッセージ。
        **kwargs: 可変長キーワード引数。

    Returns:
        None: 戻り値なし。

    Notes:
        tenant_id/request_id を共通キーへ寄せ、追加属性は kwargs 展開で保持する。
    """
    log_entry = {
        "timestamp": datetime.now(TOKYO_TZ).isoformat(),
        "level": level.upper(),
        "function_name": os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown"),
        "tenant_id": kwargs.pop("tenant_id", "unknown"),
        "request_id": kwargs.pop("request_id", ""),
        "message": message,
        **kwargs,
    }
    print(json.dumps(log_entry, ensure_ascii=False))
