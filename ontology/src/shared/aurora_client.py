"""Aurora connection management through RDS Proxy."""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
from botocore.client import BaseClient

try:
    import psycopg2  # type: ignore
except Exception:  # pragma: no cover - optional in local unit tests
    psycopg2 = None


_conn: Any | None = None


def get_aurora_connection(
    *,
    connect_func: Any | None = None,
    secrets_client: BaseClient | None = None,
) -> Any:
    """Get reusable Aurora connection and re-connect on unhealthy sessions."""
    global _conn
    if _conn and not _conn.closed:
        try:
            _conn.cursor().execute("SELECT 1")
            return _conn
        except Exception:
            _conn = None

    connector = connect_func or _connect
    _conn = connector(secrets_client=secrets_client)
    _conn.autocommit = False
    _apply_session_settings(_conn)
    return _conn


def _connect(*, secrets_client: BaseClient | None = None) -> Any:
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required for Aurora connections")

    return psycopg2.connect(
        host=os.environ["AURORA_PROXY_ENDPOINT"],
        port=int(os.environ.get("AURORA_PORT", "5432")),
        dbname=os.environ["AURORA_DB_NAME"],
        user=os.environ["AURORA_USERNAME"],
        password=_get_aurora_password(secrets_client=secrets_client),
        sslmode="require",
        connect_timeout=5,
    )


def _get_aurora_password(*, secrets_client: BaseClient | None = None) -> str:
    """Read Aurora DB password from Secrets Manager."""
    client = secrets_client or boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=os.environ["AURORA_SECRET_ARN"])
    return json.loads(response["SecretString"])["password"]


def _apply_session_settings(conn: Any) -> None:
    """Apply session-level DB settings after the proxy connection is established."""
    cur = conn.cursor()
    try:
        cur.execute("SET statement_timeout = 30000")
    finally:
        try:
            cur.close()
        except Exception:
            pass
