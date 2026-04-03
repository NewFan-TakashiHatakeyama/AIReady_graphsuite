"""CloudWatch Logs Insights repository for Connect jobs/audit read APIs."""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timedelta, timezone

from services.aws_clients import get_logs_client
from services.connect_settings import load_connect_settings
from services.runtime_config import load_aws_runtime_config

_runtime_config = load_aws_runtime_config()
_connect_settings = load_connect_settings()
_MAX_QUERY_POLLS = max(1, min(int(os.getenv("CONNECT_LOGS_QUERY_MAX_POLLS", "12")), 40))
_QUERY_POLL_INTERVAL_SEC = max(0.05, min(float(os.getenv("CONNECT_LOGS_QUERY_POLL_INTERVAL_SEC", "0.15")), 1.0))

_CORRELATION_ID_PATTERN = re.compile(
    r"(?:correlation_id|correlationId)[\"'=:\s]+([A-Za-z0-9\-_.:/]+)"
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ConnectLogsRepository:
    """Repository that queries Connect Lambda logs via Logs Insights."""

    def __init__(self, *, logs_client=None, log_groups: list[str] | tuple[str, ...] | None = None) -> None:
        self._logs_client = logs_client
        self._explicit_log_groups = log_groups

    def _client(self):
        if self._logs_client is None:
            self._logs_client = get_logs_client(_runtime_config)
        return self._logs_client

    def _connect_log_groups(self) -> list[str]:
        if self._explicit_log_groups:
            return list(self._explicit_log_groups)
        return list(_connect_settings.log_groups)

    def query_recent_rows(
        self,
        *,
        tenant_id: str,
        query_string: str,
        limit: int,
        lookback_days: int = 2,
    ) -> list[dict[str, str]]:
        log_groups = self._connect_log_groups()
        end_time = int(_now().timestamp())
        start_time = int((_now() - timedelta(days=max(1, lookback_days))).timestamp())

        response = self._client().start_query(
            logGroupNames=log_groups,
            startTime=start_time,
            endTime=end_time,
            queryString=query_string,
            limit=max(1, min(limit, 500)),
        )
        query_id = str(response.get("queryId", ""))
        if not query_id:
            return []

        for _ in range(_MAX_QUERY_POLLS):
            query_result = self._client().get_query_results(queryId=query_id)
            status = str(query_result.get("status", "")).lower()
            if status in {"complete", "failed", "cancelled", "timeout", "unknown"}:
                if status != "complete":
                    return []
                rows: list[dict[str, str]] = []
                for raw_row in query_result.get("results", []):
                    parsed_row: dict[str, str] = {}
                    for field in raw_row:
                        key = str(field.get("field", "")).lstrip("@")
                        parsed_row[key] = str(field.get("value", ""))
                    message = parsed_row.get("message", "")
                    # Temporary logs-based data source must enforce tenant isolation.
                    if tenant_id and tenant_id not in message:
                        correlation = _CORRELATION_ID_PATTERN.search(message)
                        if not correlation:
                            continue
                    rows.append(parsed_row)
                return rows
            time.sleep(_QUERY_POLL_INTERVAL_SEC)
        return []
