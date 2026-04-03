"""Connect backend service backed by AWS resources."""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import time
import unicodedata
import uuid
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Lock
from typing import Any

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from services.aws_clients import (
    get_dynamodb_resource,
    get_lambda_client,
    get_secretsmanager_client,
    get_ssm_client,
)
from services.connect_settings import load_connect_settings
from services.connect_settings import validate_connect_tenant_access
from services.repositories.connect_logs_repository import ConnectLogsRepository
from services.repositories.connect_connections_repository import (
    ConnectConnectionsRepository,
)
from services.runtime_config import load_aws_runtime_config
from domain.connect.value_objects import ConnectTenantConfig

logger = logging.getLogger(__name__)

_runtime_config = load_aws_runtime_config()
_connect_settings = load_connect_settings()
_dynamodb = None
_ssm = None
_secretsmanager = None
_lambda_client = None
_connect_logs_repository = ConnectLogsRepository()
_connect_connections_repository = ConnectConnectionsRepository(
    _connect_settings.connections_table_name
)
_connect_idempotency_table_name = (
    os.getenv("CONNECT_IDEMPOTENCY_KEYS_TABLE_NAME") or "AIReadyConnect-IdempotencyKeys"
).strip()
_CONNECT_READ_CACHE_TTL_SEC = max(
    0.0,
    min(float(os.getenv("CONNECT_READ_CACHE_TTL_SEC", "5")), 30.0),
)
_CONNECT_READ_CACHE_MAX_ITEMS = max(
    10,
    min(int(os.getenv("CONNECT_READ_CACHE_MAX_ITEMS", "256")), 2000),
)
_CACHE_HIT_FIELD = "_cache_hit"
_connect_read_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_connect_read_cache_lock = Lock()

_CORRELATION_ID_PATTERN = re.compile(
    r"(?:correlation_id|correlationId)[\"'=:\s]+([A-Za-z0-9\-_.:/]+)"
)
_EVENT_ID_PATTERN = re.compile(r"(evt-[A-Za-z0-9\-_.:/]+)")
_TEAM_DISCOVERY_PHASE1_PERMISSIONS = [
    "Sites.Read.All",
    "Files.Read.All",
    "Group.Read.All",
    "Team.ReadBasic.All",
]
_TEAM_DISCOVERY_PHASE2_PERMISSIONS = [
    "ChannelMessage.Read.All",
    "Chat.Read.All",
]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _build_read_cache_key(*parts: Any) -> tuple[Any, ...]:
    return tuple(parts)


def _get_cached_read_response(cache_key: tuple[Any, ...]) -> dict[str, Any] | None:
    if _CONNECT_READ_CACHE_TTL_SEC <= 0:
        return None
    now_monotonic = time.monotonic()
    with _connect_read_cache_lock:
        cached = _connect_read_cache.get(cache_key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now_monotonic:
            _connect_read_cache.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _set_cached_read_response(cache_key: tuple[Any, ...], payload: dict[str, Any]) -> None:
    if _CONNECT_READ_CACHE_TTL_SEC <= 0:
        return
    now_monotonic = time.monotonic()
    expires_at = now_monotonic + _CONNECT_READ_CACHE_TTL_SEC
    with _connect_read_cache_lock:
        _connect_read_cache[cache_key] = (expires_at, copy.deepcopy(payload))
        if len(_connect_read_cache) <= _CONNECT_READ_CACHE_MAX_ITEMS:
            return
        # Evict earliest expiry first to keep cache bounded.
        oldest_key = min(_connect_read_cache.items(), key=lambda item: item[1][0])[0]
        _connect_read_cache.pop(oldest_key, None)


def _invalidate_cached_reads(*, tenant_id: str, namespaces: tuple[str, ...]) -> None:
    with _connect_read_cache_lock:
        keys_to_delete = [
            key for key in _connect_read_cache.keys()
            if len(key) >= 2 and key[1] == tenant_id and str(key[0]) in namespaces
        ]
        for key in keys_to_delete:
            _connect_read_cache.pop(key, None)


def _with_cache_meta(payload: dict[str, Any], *, cache_hit: bool) -> dict[str, Any]:
    response = copy.deepcopy(payload)
    response[_CACHE_HIT_FIELD] = "HIT" if cache_hit else "MISS"
    return response


def _require_connect_tenant(tenant_id: str) -> None:
    # T-063: enforce tenant registry boundary where configured.
    validate_connect_tenant_access(_connect_settings, tenant_id)


def _to_plain_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [_to_plain_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_plain_value(v) for k, v in value.items()}
    return value


def _connect_table(table_name: str):
    return _dynamodb_resource().Table(table_name)


def _dynamodb_resource():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = get_dynamodb_resource(_runtime_config)
    return _dynamodb


def _ssm_client_resource():
    global _ssm
    if _ssm is None:
        _ssm = get_ssm_client(_runtime_config)
    return _ssm


def _secretsmanager_client_resource():
    global _secretsmanager
    if _secretsmanager is None:
        _secretsmanager = get_secretsmanager_client(_runtime_config)
    return _secretsmanager


def _lambda_client_resource():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = get_lambda_client(_runtime_config)
    return _lambda_client


def _get_ssm_value(name: str, with_decryption: bool = False) -> str:
    try:
        response = _ssm_client_resource().get_parameter(Name=name, WithDecryption=with_decryption)
    except ClientError:
        return ""
    parameter = response.get("Parameter", {})
    return str(parameter.get("Value", "")).strip()


def _first_env_value(*names: str) -> str:
    for name in names:
        value = str(os.getenv(name, "")).strip()
        if value:
            return value
    return ""


def _is_uuid_like(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            str(value or "").strip(),
        )
    )


def _tenant_drive_id(tenant_id: str) -> str:
    # Current Connect pipeline is single-drive. Keep deterministic mapping.
    tenant_specific = _get_ssm_value(f"/aiready/connect/{tenant_id}/drive_id")
    if tenant_specific:
        return tenant_specific
    explicit = _get_ssm_value("MSGraphDriveId")
    if explicit:
        return explicit
    return f"{tenant_id}-default-drive"


def _latest_drive_for_tenant(tenant_id: str) -> str:
    """Return the latest drive_id observed for the tenant in FileMetadata."""
    try:
        file_table = _connect_table(_connect_settings.file_metadata_table_name)
        response = file_table.query(
            IndexName="GSI-ModifiedAt",
            KeyConditionExpression=Key("tenant_id").eq(tenant_id),
            ScanIndexForward=False,
            Limit=1,
        )
    except ClientError:
        return ""
    items = response.get("Items", [])
    if not items:
        return ""
    drive_id = str(_to_plain_value(items[0]).get("drive_id") or "").strip()
    return drive_id


def _tenant_for_drive_id(drive_id: str) -> str:
    """Resolve tenant_id from a drive partition in FileMetadata."""
    if not drive_id:
        return ""
    try:
        file_table = _connect_table(_connect_settings.file_metadata_table_name)
        response = file_table.query(
            KeyConditionExpression=Key("drive_id").eq(drive_id),
            Limit=1,
        )
    except ClientError:
        return ""
    items = response.get("Items", [])
    if not items:
        return ""
    tenant = str(_to_plain_value(items[0]).get("tenant_id") or "").strip()
    return tenant


def _tenant_ids_for_drive_id(drive_id: str, *, max_items: int = 200) -> list[str]:
    """Resolve all observed tenant_id values for the given drive_id partition."""
    if not drive_id:
        return []
    tenants: list[str] = []
    try:
        file_table = _connect_table(_connect_settings.file_metadata_table_name)
        last_evaluated_key: dict[str, Any] | None = None
        scanned = 0
        while True:
            query_kwargs: dict[str, Any] = {
                "KeyConditionExpression": Key("drive_id").eq(drive_id),
                "Limit": min(100, max_items),
            }
            if last_evaluated_key:
                query_kwargs["ExclusiveStartKey"] = last_evaluated_key
            response = file_table.query(**query_kwargs)
            items = response.get("Items", [])
            scanned += len(items)
            for item in items:
                tenant = str(_to_plain_value(item).get("tenant_id") or "").strip()
                if tenant and tenant not in tenants:
                    tenants.append(tenant)
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key or scanned >= max_items:
                break
    except ClientError:
        return []
    return tenants


def _tenant_aliases(tenant_id: str) -> list[str]:
    normalized = str(tenant_id or "").strip()
    if not normalized:
        return []
    aliases = [normalized]
    # Compatibility aliases observed in this codebase/runtime:
    # UI/API tenant_id can be "tenant-default", while ingestion may persist "default".
    if normalized == "tenant-default":
        aliases.append("default")
    elif normalized == "default":
        aliases.append("tenant-default")
    return aliases


def _ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        token = str(value or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return ordered


def _resolve_event_tenant_partitions(tenant_id: str, *, scope_drive_id: str) -> list[str]:
    candidates: list[str] = []
    candidates.extend(_tenant_aliases(tenant_id))
    target_drive_id = scope_drive_id or _tenant_drive_id(tenant_id)
    drive_tenants = _tenant_ids_for_drive_id(target_drive_id)
    for drive_tenant in drive_tenants:
        candidates.extend(_tenant_aliases(drive_tenant))
    return _ordered_unique(candidates)


def _drive_id_from_scope_id(scope_id: str | None) -> str:
    raw_scope_id = str(scope_id or "").strip()
    if raw_scope_id.startswith("scope-msg-"):
        return ""
    if not raw_scope_id.startswith("scope-"):
        return ""
    return raw_scope_id[len("scope-") :]


def _conversation_key_from_scope_id(scope_id: str | None) -> str:
    raw = str(scope_id or "").strip()
    prefix = "scope-msg-"
    if not raw.startswith(prefix):
        return ""
    return raw[len(prefix) :].strip()


def _is_placeholder_subscription(subscription_id: str) -> bool:
    normalized = str(subscription_id or "").strip()
    if not normalized.startswith("sub-"):
        return False
    return normalized.endswith("-default") or "-conn-" in normalized


def tenant_has_active_scoring_connection(tenant_id: str) -> bool:
    """True when the tenant has at least one non-retired Connect row with a real Graph subscription.

    Used by dashboard readiness to avoid showing governance/ontology scores when no data path exists.
    Does not enforce tenant registry (returns False on lookup errors).
    """
    try:
        connections = _connect_connections_repository.list_connections_for_tenant(
            str(tenant_id or "").strip(), limit=500
        )
    except Exception:
        return False
    for connection in connections:
        status = str(connection.get("status", "")).strip().lower()
        if status in {"deprecated", "retired", "deleted"}:
            continue
        subscription_id = str(connection.get("subscription_id", "")).strip()
        if not subscription_id or _is_placeholder_subscription(subscription_id):
            continue
        return True
    return False


def _row_pending_subscription_reflection(row: dict[str, Any]) -> bool:
    """True when UI would still treat the row as not fully reflected (matches WebUI hasPlaceholderSubscription)."""
    if bool(row.get("is_placeholder", False)):
        return True
    if str(row.get("reflection_status") or "").strip().lower() == "pending":
        return True
    if str(row.get("status") or "").strip().lower() == "initializing":
        return True
    return False


def _subscription_rows_reflected(
    rows: list[dict[str, Any]],
    *,
    connection_id: str = "",
) -> bool:
    """When connection_id is set, only that connection's row(s) must be ready (Teams multi-connection)."""
    if not rows:
        return False
    cid = str(connection_id or "").strip()
    if cid:
        targets = [r for r in rows if str(r.get("connection_id") or "").strip() == cid]
        if not targets:
            return False
        return not any(_row_pending_subscription_reflection(t) for t in targets)
    return not any(_row_pending_subscription_reflection(r) for r in rows)


def _wait_for_subscription_reflection(
    tenant_id: str,
    *,
    connection_id: str = "",
    attempts: int = 5,
    interval_sec: float = 0.8,
) -> tuple[dict[str, Any], bool, int]:
    started = time.perf_counter()
    last = list_connect_subscriptions(tenant_id=tenant_id)
    rows = last.get("rows", [])
    if _subscription_rows_reflected(rows, connection_id=connection_id):
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return last, True, elapsed_ms

    for _ in range(max(0, attempts - 1)):
        time.sleep(max(0.0, interval_sec))
        last = list_connect_subscriptions(tenant_id=tenant_id)
        rows = last.get("rows", [])
        if _subscription_rows_reflected(rows, connection_id=connection_id):
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return last, True, elapsed_ms

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    return last, False, elapsed_ms


def _delivery_status(queue_backlog: int, failed_jobs_24h: int) -> str:
    if failed_jobs_24h > 5:
        return "failed"
    if queue_backlog > 100:
        return "degraded"
    return "healthy"


def _infer_job_status(message: str) -> str:
    normalized = message.lower()
    if "dead-letter" in normalized or "dlq" in normalized:
        return "dead-lettered"
    if "retry" in normalized:
        return "retrying"
    if "error" in normalized or "exception" in normalized or "failed" in normalized:
        return "failed"
    if "start" in normalized or "running" in normalized or "processing" in normalized:
        return "running"
    return "success"


def _infer_operator(message: str) -> str:
    for key in ("operator", "requested_by", "executed_by", "username"):
        pattern = re.compile(rf"{key}[\"'=:\s]+([A-Za-z0-9@._:-]+)")
        found = pattern.search(message)
        if found:
            return found.group(1)
    return "system"


def _tenant_param_name(tenant_id: str, key: str) -> str:
    return f"/aiready/connect/{tenant_id}/{key}"


def _tenant_connection_param_name(tenant_id: str, connection_id: str, key: str) -> str:
    return f"/aiready/connect/{tenant_id}/{connection_id}/{key}"


def _normalize_ssm_parameter_value(value: str | None) -> str:
    """Strip whitespace and invisible / format characters so PutParameter never gets an empty Value."""
    s = str(value if value is not None else "").strip()
    for ch in ("\ufeff", "\u200b", "\u200c", "\u200d", "\u2060"):
        s = s.replace(ch, "")
    s = "".join(c for c in s if unicodedata.category(c) != "Cf")
    return s.strip()


def _put_ssm_parameter(name: str, value: str, *, secure: bool = False) -> None:
    # SSM PutParameter rejects empty Value (ValidationException: length >= 1).
    # OneDrive / partial discovery flows may legitimately omit site_id.
    # Onboarding path: POST /connect/onboarding -> graph_routes.post_connect_onboarding_route
    # -> create_connect_onboarding (all tenant SSM writes use this helper).
    normalized = _normalize_ssm_parameter_value(value)
    if not normalized:
        if name.rstrip("/").endswith("site_id"):
            logger.info("Connect SSM PutParameter skipped (optional empty site_id): %s", name)
        else:
            logger.warning(
                "Connect SSM PutParameter skipped (empty after normalize; check caller): %s",
                name,
            )
        return
    try:
        _ssm_client_resource().put_parameter(
            Name=name,
            Value=normalized,
            Type="SecureString" if secure else "String",
            Overwrite=True,
        )
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", "") or "").strip()
        aws_msg = str(exc.response.get("Error", {}).get("Message", "") or "").strip()
        logger.error(
            "Connect SSM PutParameter failed (parameter name only, value not logged): %s code=%s msg=%s",
            name,
            error_code,
            aws_msg,
        )
        raise RuntimeError(
            f"SSM PutParameter failed for parameter {name!r} ({error_code}): {aws_msg}"
        ) from exc


def _upsert_secret(secret_name: str, secret_value: str) -> None:
    payload = json.dumps({"client_secret": secret_value}, ensure_ascii=True)
    secrets_client = _secretsmanager_client_resource()
    try:
        secrets_client.describe_secret(SecretId=secret_name)
        secrets_client.put_secret_value(SecretId=secret_name, SecretString=payload)
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", ""))
        if error_code not in {"ResourceNotFoundException", "ValidationException"}:
            raise
        secrets_client.create_secret(Name=secret_name, SecretString=payload)


def _validate_notification_url(notification_url: str) -> None:
    parsed = urlparse(notification_url)
    host = str(parsed.netloc or "").strip().lower()
    if not host:
        raise ValueError("notification_url host is required.")
    allowed_raw = os.getenv("CONNECT_ALLOWED_NOTIFICATION_DOMAINS", "").strip()
    if not allowed_raw:
        return
    allowed = {token.strip().lower() for token in allowed_raw.split(",") if token.strip()}
    if host not in allowed:
        raise ValueError("notification_url host is not in CONNECT_ALLOWED_NOTIFICATION_DOMAINS.")


def _graph_access_token(*, azure_tenant_id: str, client_id: str, client_secret: str) -> str:
    token_url = f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/v2.0/token"
    body = urllib_parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }
    ).encode("utf-8")
    request = urllib_request.Request(
        token_url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        raise RuntimeError(f"Graph token request failed with status={exc.code}.") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Graph token request failed: {exc.reason}.") from exc
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError("Graph token response did not include access_token.")
    return access_token


def _graph_get_json(url: str, *, access_token: str) -> dict[str, Any]:
    request = urllib_request.Request(
        url,
        method="GET",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
    )
    try:
        with urllib_request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        raise RuntimeError(f"Graph API request failed with status={exc.code}: {url}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Graph API request failed: {exc.reason}: {url}") from exc


def _graph_get_json_optional(url: str, *, access_token: str) -> dict[str, Any] | None:
    """Return Graph JSON payload or None when request fails."""
    try:
        return _graph_get_json(url, access_token=access_token)
    except RuntimeError:
        return None


def team_channel_discovery_permission_guide() -> dict[str, list[str]]:
    """Permission guide used by Teams discovery UI and docs."""
    return {
        "phase1": list(_TEAM_DISCOVERY_PHASE1_PERMISSIONS),
        "phase2": list(_TEAM_DISCOVERY_PHASE2_PERMISSIONS),
    }


def _graph_delete_subscription(*, subscription_id: str, access_token: str) -> str:
    normalized_subscription_id = str(subscription_id or "").strip()
    if not normalized_subscription_id:
        return "skipped"
    request = urllib_request.Request(
        f"https://graph.microsoft.com/v1.0/subscriptions/{urllib_parse.quote(normalized_subscription_id)}",
        method="DELETE",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
    )
    try:
        with urllib_request.urlopen(request, timeout=30):
            return "deleted"
    except urllib_error.HTTPError as exc:
        if exc.code == 404:
            return "not_found"
        return f"failed:{exc.code}"
    except urllib_error.URLError:
        return "failed:network"


def _site_relative_path_from_url(site_url: str) -> tuple[str, str]:
    parsed = urlparse(str(site_url or "").strip())
    hostname = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if not hostname or not path:
        raise ValueError("site_url must include hostname and path.")
    segments = [segment for segment in path.split("/") if segment]
    if len(segments) >= 2 and segments[0] in {"sites", "teams"}:
        # Normalize document/library URLs to canonical site path:
        # /sites/{siteName}/... -> /sites/{siteName}
        relative_path = f"/{segments[0]}/{segments[1]}"
    elif segments:
        relative_path = "/" + segments[0]
    else:
        relative_path = "/"
    return hostname, relative_path


def _slugify_connection_name(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip().lower())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or "sharepoint-site"


def _source_type_for_site_web_url(site_web_url: str) -> str:
    lower_url = str(site_web_url or "").strip().lower()
    if not lower_url:
        return "sharepoint"
    if "-my.sharepoint.com" in lower_url or "/personal/" in lower_url:
        return "onedrive"
    if "/teams/" in lower_url:
        return "teams"
    return "sharepoint"


def _canonical_site_base(site_web_url: str) -> str:
    normalized = str(site_web_url or "").strip()
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    host = str(parsed.netloc or "").strip().lower()
    path = str(parsed.path or "").strip()
    if not host:
        return ""
    segments = [segment for segment in path.split("/") if segment]
    if len(segments) >= 2 and segments[0].lower() in {"sites", "teams"}:
        return f"{host}/{segments[0].lower()}/{segments[1].lower()}"
    if len(segments) >= 2 and segments[0].lower() == "personal":
        return f"{host}/{segments[0].lower()}/{segments[1].lower()}"
    return f"{host}{path.lower().rstrip('/')}"


def _build_site_search_tokens(source_type: str, query: str) -> list[str]:
    normalized_source_type = str(source_type or "").strip().lower()
    normalized_query = str(query or "").strip()
    tokens: list[str] = []
    if normalized_query:
        tokens.append(normalized_query)
    if normalized_source_type == "teams":
        tokens.extend(["team", "teams", "site"])
    elif normalized_source_type == "onedrive":
        tokens.extend(["onedrive", "personal", "my"])
    else:
        tokens.extend(["site", "team"])
    deduped: list[str] = []
    for token in tokens:
        candidate = str(token or "").strip()
        if not candidate or candidate in deduped:
            continue
        deduped.append(candidate)
    return deduped or ["site"]


def _resolve_graph_credentials(
    tenant_id: str,
    *,
    azure_tenant_id: str = "",
    client_id: str = "",
    client_secret: str = "",
) -> tuple[str, str, str]:
    candidate_azure_tenant = str(azure_tenant_id or "").strip()
    tenant_scoped_azure = _get_ssm_value(_tenant_param_name(tenant_id, "tenant_id"))
    if not _is_uuid_like(candidate_azure_tenant):
        if _is_uuid_like(tenant_scoped_azure):
            candidate_azure_tenant = tenant_scoped_azure
        else:
            candidate_azure_tenant = (
                _get_ssm_value("MSGraphTenantId")
                or _first_env_value("MS_GRAPH_TENANT_ID", "MSGraphTenantId")
            )

    resolved_client_id = str(client_id or "").strip() or _get_ssm_value(
        _tenant_param_name(tenant_id, "client_id")
    ) or _get_ssm_value("MSGraphClientId") or _first_env_value(
        "MS_GRAPH_CLIENT_ID", "MSGraphClientId"
    )
    resolved_client_secret = str(client_secret or "").strip() or _get_ssm_value(
        _tenant_param_name(tenant_id, "client_secret"), with_decryption=True
    ) or _get_ssm_value(
        "MSGraphClientSecret", with_decryption=True
    ) or _first_env_value(
        "MS_GRAPH_CLIENT_SECRET", "MSGraphClientSecret"
    )

    if not _is_uuid_like(candidate_azure_tenant):
        raise ValueError("Azure Tenant GUID is not configured.")
    if not resolved_client_id:
        raise ValueError("Graph client_id is not configured.")
    if not resolved_client_secret:
        raise ValueError("Graph client_secret is not configured.")
    return candidate_azure_tenant, resolved_client_id, resolved_client_secret


def list_connect_site_options(
    tenant_id: str,
    *,
    azure_tenant_id: str,
    client_id: str,
    client_secret: str,
    source_type: str = "sharepoint",
    query: str = "",
) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    normalized_azure_tenant_id, normalized_client_id, normalized_client_secret = (
        _resolve_graph_credentials(
            tenant_id,
            azure_tenant_id=azure_tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
    )
    normalized_source_type = str(source_type or "sharepoint").strip().lower()
    normalized_query = str(query or "").strip()
    if normalized_source_type not in {"sharepoint", "teams", "onedrive"}:
        raise ValueError("source_type must be one of sharepoint, teams, onedrive.")

    access_token = _graph_access_token(
        azure_tenant_id=normalized_azure_tenant_id,
        client_id=normalized_client_id,
        client_secret=normalized_client_secret,
    )
    team_site_ids: set[str] = set()
    team_site_bases: set[str] = set()
    if normalized_source_type == "teams":
        # Teams 実体が /sites/... で返るケースを補正するため、
        # Team/Channel の filesFolder 由来 site 情報を照合キーとして保持する。
        team_options = list_connect_team_channel_options(
            tenant_id=tenant_id,
            azure_tenant_id=normalized_azure_tenant_id,
            client_id=normalized_client_id,
            client_secret=normalized_client_secret,
            max_teams=20,
            max_channels_per_team=20,
        )
        for option in team_options.get("rows", []):
            site_id = str(option.get("site_id") or "").strip()
            if site_id:
                team_site_ids.add(site_id)
            site_base = _canonical_site_base(str(option.get("files_folder_web_url") or ""))
            if site_base:
                team_site_bases.add(site_base)

    search_tokens = _build_site_search_tokens(normalized_source_type, normalized_query)
    rows: list[dict[str, Any]] = []
    seen_site_ids: set[str] = set()
    for search_token in search_tokens:
        candidates_payload = _graph_get_json(
            f"https://graph.microsoft.com/v1.0/sites?search={urllib_parse.quote(search_token)}",
            access_token=access_token,
        )
        for item in candidates_payload.get("value", []):
            site_id = str(item.get("id") or "").strip()
            if not site_id or site_id in seen_site_ids:
                continue
            site_web_url = str(item.get("webUrl") or "").strip()
            canonical_site_base = _canonical_site_base(site_web_url)
            resolved_source_type = _source_type_for_site_web_url(site_web_url)
            if normalized_source_type == "teams":
                if site_id in team_site_ids or canonical_site_base in team_site_bases:
                    resolved_source_type = "teams"
            if normalized_source_type != resolved_source_type:
                continue
            site_name = str(item.get("displayName") or item.get("name") or site_web_url).strip()
            rows.append(
                {
                    "site_id": site_id,
                    "site_name": site_name,
                    "site_web_url": site_web_url,
                    "source_type": resolved_source_type,
                }
            )
            seen_site_ids.add(site_id)
            if len(rows) >= 50:
                break
        if len(rows) >= 50:
            break
    return {"rows": rows}


def list_connect_team_channel_options(
    tenant_id: str,
    *,
    azure_tenant_id: str,
    client_id: str,
    client_secret: str,
    team_query: str = "",
    channel_query: str = "",
    site_id: str = "",
    max_teams: int = 20,
    max_channels_per_team: int = 30,
) -> dict[str, Any]:
    """Discover Team/Channel to filesFolder drive mappings.

    This endpoint is read-only and used to help users pick the right
    Teams channel-backed SharePoint drive before onboarding.
    """
    _require_connect_tenant(tenant_id)
    normalized_azure_tenant_id, normalized_client_id, normalized_client_secret = (
        _resolve_graph_credentials(
            tenant_id,
            azure_tenant_id=azure_tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
    )
    normalized_team_query = str(team_query or "").strip().lower()
    normalized_channel_query = str(channel_query or "").strip().lower()
    normalized_site_id = str(site_id or "").strip()
    bounded_teams = max(1, min(int(max_teams), 100))
    bounded_channels = max(1, min(int(max_channels_per_team), 200))

    access_token = _graph_access_token(
        azure_tenant_id=normalized_azure_tenant_id,
        client_id=normalized_client_id,
        client_secret=normalized_client_secret,
    )

    guide = team_channel_discovery_permission_guide()
    warnings: list[str] = []
    groups_url = (
        "https://graph.microsoft.com/v1.0/groups"
        "?%24filter=resourceProvisioningOptions/Any(x:x%20eq%20%27Team%27)"
        "&%24select=id,displayName,mail"
    )
    groups_payload = _graph_get_json_optional(groups_url, access_token=access_token)
    if groups_payload is None:
        warnings.append(
            "Could not list Teams groups. Verify Graph application permissions "
            "(Group.Read.All and Team.ReadBasic.All) and admin consent status."
        )
        return {
            "rows": [],
            "warnings": warnings,
            "required_application_permissions_phase1": guide["phase1"],
            "required_application_permissions_phase2": guide["phase2"],
        }

    rows: list[dict[str, Any]] = []
    teams = groups_payload.get("value", [])
    for group in teams:
        team_id = str(group.get("id") or "").strip()
        team_name = str(group.get("displayName") or "").strip()
        team_mail = str(group.get("mail") or "").strip()
        if not team_id or not team_name:
            continue
        if normalized_team_query and normalized_team_query not in team_name.lower():
            continue
        if len(rows) >= bounded_teams * bounded_channels:
            break

        channels_payload = _graph_get_json_optional(
            f"https://graph.microsoft.com/v1.0/teams/{urllib_parse.quote(team_id)}/channels"
            "?%24select=id,displayName,membershipType,webUrl",
            access_token=access_token,
        )
        if channels_payload is None:
            warnings.append(f"Could not list channels for team '{team_name}'.")
            continue

        channels = channels_payload.get("value", [])
        channel_seen = 0
        for channel in channels:
            if channel_seen >= bounded_channels:
                break
            channel_id = str(channel.get("id") or "").strip()
            channel_name = str(channel.get("displayName") or "").strip()
            membership_type = str(channel.get("membershipType") or "").strip()
            if not channel_id or not channel_name:
                continue
            if normalized_channel_query and normalized_channel_query not in channel_name.lower():
                continue

            files_folder_payload = _graph_get_json_optional(
                f"https://graph.microsoft.com/v1.0/teams/{urllib_parse.quote(team_id)}"
                f"/channels/{urllib_parse.quote(channel_id)}/filesFolder"
                "?%24select=id,name,webUrl,parentReference",
                access_token=access_token,
            )
            if files_folder_payload is None:
                rows.append(
                    {
                        "team_id": team_id,
                        "team_name": team_name,
                        "team_mail": team_mail,
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "membership_type": membership_type,
                        "files_drive_id": "",
                        "files_folder_web_url": "",
                        "files_folder_name": "",
                        "site_id": "",
                        "site_web_url": "",
                        "source_type": "teams",
                        "discovery_status": "files_folder_unavailable",
                        "error_message": "filesFolder endpoint is not accessible for this channel.",
                    }
                )
                channel_seen += 1
                continue

            parent_ref = files_folder_payload.get("parentReference", {})
            files_drive_id = (
                str(parent_ref.get("driveId") or "").strip() if isinstance(parent_ref, dict) else ""
            )
            resolved_site_id = (
                str(parent_ref.get("siteId") or "").strip() if isinstance(parent_ref, dict) else ""
            )
            if normalized_site_id and normalized_site_id != resolved_site_id:
                continue
            rows.append(
                {
                    "team_id": team_id,
                    "team_name": team_name,
                    "team_mail": team_mail,
                    "channel_id": channel_id,
                    "channel_name": channel_name,
                    "membership_type": membership_type,
                    "files_drive_id": files_drive_id,
                    "files_folder_web_url": str(files_folder_payload.get("webUrl") or "").strip(),
                    "files_folder_name": str(files_folder_payload.get("name") or "").strip(),
                    "site_id": resolved_site_id,
                    "site_web_url": "",
                    "source_type": "teams",
                    "discovery_status": "ready" if files_drive_id else "files_folder_unavailable",
                    "error_message": "",
                }
            )
            channel_seen += 1
    return {
        "rows": rows,
        "warnings": warnings,
        "required_application_permissions_phase1": guide["phase1"],
        "required_application_permissions_phase2": guide["phase2"],
    }


def get_connect_onboarding_defaults(tenant_id: str) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    azure_tenant_id, client_id, _ = _resolve_graph_credentials(tenant_id)
    fallback = (os.getenv("WEBHOOK_URL") or "https://webhook.graphsuite.jp").strip()
    raw = (_get_ssm_value(_tenant_param_name(tenant_id, "notification_url")) or "").strip()
    if not raw:
        notification_url = fallback
    else:
        host = (urlparse(raw).netloc or "").lower()
        notification_url = fallback if "example.com" in host else raw
    return {
        "tenant_id": azure_tenant_id,
        "client_id": client_id,
        "auth_method": "client_secret",
        "permission_profile": "sites_selected",
        "notification_url": notification_url,
        "client_secret_parameter": "MSGraphClientSecret",
        "client_state_parameter": "MSGraphClientState",
    }


def resolve_connect_site_discovery(
    tenant_id: str,
    *,
    azure_tenant_id: str,
    client_id: str,
    client_secret: str,
    site_url: str = "",
    site_id: str = "",
) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    normalized_azure_tenant_id, normalized_client_id, normalized_client_secret = (
        _resolve_graph_credentials(
            tenant_id,
            azure_tenant_id=azure_tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
    )
    normalized_site_url = str(site_url or "").strip()
    normalized_site_id = str(site_id or "").strip()
    if not normalized_site_url and not normalized_site_id:
        raise ValueError("site_url or site_id is required.")

    access_token = _graph_access_token(
        azure_tenant_id=normalized_azure_tenant_id,
        client_id=normalized_client_id,
        client_secret=normalized_client_secret,
    )
    if normalized_site_id:
        site_payload = _graph_get_json(
            f"https://graph.microsoft.com/v1.0/sites/{urllib_parse.quote(normalized_site_id, safe=',')}",
            access_token=access_token,
        )
    else:
        hostname, relative_path = _site_relative_path_from_url(normalized_site_url)
        encoded_relative_path = urllib_parse.quote(relative_path, safe="/-_.~")
        site_payload = _graph_get_json(
            f"https://graph.microsoft.com/v1.0/sites/{hostname}:{encoded_relative_path}",
            access_token=access_token,
        )
    resolved_site_id = str(site_payload.get("id") or "").strip()
    if not resolved_site_id:
        raise RuntimeError("Graph site API did not return site id.")
    drive_payload = _graph_get_json(
        f"https://graph.microsoft.com/v1.0/sites/{urllib_parse.quote(resolved_site_id, safe=',')}/drive",
        access_token=access_token,
    )
    drive_id = str(drive_payload.get("id") or "").strip()
    if not drive_id:
        raise RuntimeError("Graph drive API did not return drive id.")
    site_name = str(site_payload.get("displayName") or site_payload.get("name") or "").strip()
    site_web_url = str(site_payload.get("webUrl") or normalized_site_url).strip()
    suggested_connection_name = _slugify_connection_name(site_name or site_web_url or resolved_site_id)
    return {
        "site_id": resolved_site_id,
        "drive_id": drive_id,
        "site_name": site_name,
        "site_web_url": site_web_url,
        "suggested_connection_name": suggested_connection_name,
    }


def _bootstrap_subscription(
    tenant_id: str,
    *,
    connection_id: str,
    site_id: str,
    drive_id: str,
    notification_url: str,
    client_state: str,
    correlation_id: str,
    resource_type: str = "drive",
    resource_path: str = "",
    change_type: str = "updated",
    team_id: str = "",
    channel_id: str = "",
    chat_id: str = "",
) -> tuple[bool, str, str, str]:
    function_name = (
        _connect_settings.init_subscription_lambda_name or "AIReadyConnect-initSubscription"
    )
    payload = {
        "tenant_id": tenant_id,
        "connection_id": connection_id,
        "site_id": site_id,
        "drive_id": drive_id,
        "notification_url": notification_url,
        "client_state": client_state,
        "resource_type": resource_type,
        "resource_path": resource_path,
        "change_type": change_type,
        "team_id": team_id,
        "channel_id": channel_id,
        "chat_id": chat_id,
        "correlation_id": correlation_id,
        "trigger": "onboarding",
    }
    try:
        response = _lambda_client_resource().invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except ClientError as exc:
        error_code = str(exc.response.get("Error", {}).get("Code", "Unknown"))
        return False, "failed", "", f"invoke failed ({error_code})"
    status_code = int(response.get("StatusCode", 500))
    if status_code < 200 or status_code >= 300:
        return False, "failed", "", f"invoke status={status_code}"
    payload_stream = response.get("Payload")
    raw_payload = payload_stream.read().decode("utf-8") if payload_stream else "{}"
    try:
        lambda_payload = json.loads(raw_payload or "{}")
    except json.JSONDecodeError:
        return False, "failed", "", f"invalid lambda payload: {raw_payload[:200]}"
    body = lambda_payload.get("body", {})
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            body = {"message": body}
    if not isinstance(body, dict):
        body = {}
    bootstrap_status = str(body.get("bootstrap_status") or "").strip() or (
        "succeeded" if int(lambda_payload.get("statusCode", 500)) < 400 else "failed"
    )
    subscription_id = _normalize_ssm_parameter_value(str(body.get("subscription_id") or ""))
    error_message = str(body.get("bootstrap_error") or "").strip()
    initialized = bootstrap_status == "succeeded" and bool(subscription_id)
    if bootstrap_status == "succeeded" and not subscription_id:
        logger.warning(
            "Connect init_subscription reported succeeded but subscription_id empty after normalize "
            "(check Lambda response / Graph): tenant_id=%s connection_id=%s correlation_id=%s",
            tenant_id,
            connection_id,
            correlation_id,
        )
    return initialized, bootstrap_status, subscription_id, error_message


def get_connect_overview(tenant_id: str) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    cache_key = _build_read_cache_key("overview", tenant_id)
    cached_response = _get_cached_read_response(cache_key)
    if cached_response is not None:
        return _with_cache_meta(cached_response, cache_hit=True)
    failed_jobs_24h = 0
    queue_backlog = 0

    now = _now()
    response = {
        "tenant_id": tenant_id,
        "delivery_status": _delivery_status(queue_backlog, failed_jobs_24h),
        "queue_backlog": queue_backlog,
        "failed_jobs_24h": failed_jobs_24h,
        "next_subscription_renewal_at": (now + timedelta(hours=12)).isoformat(),
        "next_token_renewal_at": (now + timedelta(minutes=30)).isoformat(),
        "next_delta_sync_at": (now + timedelta(hours=24)).isoformat(),
    }
    _set_cached_read_response(cache_key, response)
    return _with_cache_meta(response, cache_hit=False)


def list_connect_subscriptions(tenant_id: str) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    cache_key = _build_read_cache_key("subscriptions", tenant_id)
    cached_response = _get_cached_read_response(cache_key)
    if cached_response is not None:
        return _with_cache_meta(cached_response, cache_hit=True)
    connections: list[dict[str, Any]] = []
    try:
        connections = _connect_connections_repository.list_connections_for_tenant(tenant_id, limit=300)
    except Exception:
        connections = []

    client_state = _get_ssm_value(_tenant_param_name(tenant_id, "client_state"), with_decryption=True) or _get_ssm_value(
        "MSGraphClientState", with_decryption=True
    )
    tenant_hint = _get_ssm_value(_tenant_param_name(tenant_id, "tenant_id")) or _get_ssm_value("MSGraphTenantId")
    rows: list[dict[str, Any]] = []

    for connection in connections:
        connection_id = str(connection.get("connection_id") or "").strip()
        connection_name = str(connection.get("connection_name") or "").strip()
        drive_id = str(connection.get("drive_id") or "").strip()
        resource_type = str(connection.get("resource_type") or "drive").strip().lower() or "drive"
        resource_path = str(connection.get("resource_path") or "").strip()
        target_type = str(connection.get("target_type") or "").strip() or None
        subscription_id = str(connection.get("subscription_id") or "").strip()
        if not subscription_id and connection_id:
            ssm_subscription_id = _get_ssm_value(
                _tenant_connection_param_name(tenant_id, connection_id, "subscription_id")
            )
            subscription_id = str(ssm_subscription_id or "").strip()
        connection_status = str(connection.get("status") or "").strip().lower()
        if connection_status in {"deprecated", "retired", "deleted"}:
            continue
        resolved_subscription_id = subscription_id or f"sub-{tenant_id}-{connection_id or 'default'}"
        placeholder = _is_placeholder_subscription(resolved_subscription_id)
        status = connection_status
        if not subscription_id or placeholder:
            status = "initializing"
        elif status not in {"active", "expiring", "failed", "initializing"}:
            status = "initializing" if placeholder else "active"
        row = {
            "id": resolved_subscription_id,
            "resource": f"/{resource_path}" if resource_path else f"/drives/{drive_id or _tenant_drive_id(tenant_id)}/root",
            "expiration_at": (_now() + timedelta(days=1)).isoformat(),
            "client_state_verified": bool(client_state),
            "status": status,
            "resource_type": resource_type,
            "target_type": target_type,
            "is_placeholder": placeholder,
            "reflection_status": "pending" if placeholder else "ready",
            "tenant_hint": tenant_hint or tenant_id,
        }
        if connection_id:
            row["connection_id"] = connection_id
        if connection_name:
            row["connection_name"] = connection_name
        rows.append(row)

    if not rows:
        # Do not synthesize subscription rows from legacy SSM values.
        # Only real connection records should be shown in subscriptions.
        response = {
            "rows": [],
            "pagination": {"limit": 100, "offset": 0, "total_count": 0},
        }
        _set_cached_read_response(cache_key, response)
        return _with_cache_meta(response, cache_hit=False)

    response = {
        "rows": rows,
        "pagination": {"limit": 100, "offset": 0, "total_count": len(rows)},
    }
    _set_cached_read_response(cache_key, response)
    return _with_cache_meta(response, cache_hit=False)


def _find_connection_by_subscription(
    tenant_id: str,
    *,
    subscription_id: str = "",
    connection_id: str = "",
) -> dict[str, Any] | None:
    normalized_subscription_id = str(subscription_id or "").strip()
    normalized_connection_id = str(connection_id or "").strip()
    try:
        connections = _connect_connections_repository.list_connections_for_tenant(tenant_id, limit=500)
    except Exception:
        return None
    for connection in connections:
        current_connection_id = str(connection.get("connection_id") or "").strip()
        current_subscription_id = str(connection.get("subscription_id") or "").strip()
        resolved_subscription_id = current_subscription_id or f"sub-{tenant_id}-{current_connection_id or 'default'}"
        if normalized_connection_id and current_connection_id == normalized_connection_id:
            return connection
        if normalized_subscription_id and resolved_subscription_id == normalized_subscription_id:
            return connection
    return None


def _active_drive_ids_for_tenant(tenant_id: str) -> set[str]:
    try:
        connections = _connect_connections_repository.list_connections_for_tenant(tenant_id, limit=500)
    except Exception:
        return set()
    active_drive_ids: set[str] = set()
    for connection in connections:
        connection_status = str(connection.get("status") or "").strip().lower()
        if connection_status in {"deprecated", "retired", "deleted"}:
            continue
        drive_id = str(connection.get("drive_id") or "").strip()
        if drive_id:
            active_drive_ids.add(drive_id)
    return active_drive_ids


def _latest_active_connection_for_tenant(tenant_id: str) -> dict[str, Any] | None:
    try:
        connections = _connect_connections_repository.list_connections_for_tenant(tenant_id, limit=500)
    except Exception:
        return None
    for connection in connections:
        status = str(connection.get("status") or "").strip().lower()
        if status in {"deprecated", "retired", "deleted"}:
            continue
        return connection
    return None


def _delete_file_metadata_rows_for_drive(drive_id: str) -> int:
    normalized_drive_id = str(drive_id or "").strip()
    if not normalized_drive_id:
        return 0
    file_table = _connect_table(_connect_settings.file_metadata_table_name)
    deleted_count = 0
    last_evaluated_key: dict[str, Any] | None = None
    while True:
        query_kwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("drive_id").eq(normalized_drive_id),
            "ProjectionExpression": "drive_id, item_id",
            "Limit": 200,
        }
        if last_evaluated_key:
            query_kwargs["ExclusiveStartKey"] = last_evaluated_key
        response = file_table.query(**query_kwargs)
        items = response.get("Items", [])
        if items:
            with file_table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(
                        Key={
                            "drive_id": item["drive_id"],
                            "item_id": item["item_id"],
                        }
                    )
                    deleted_count += 1
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    return deleted_count


def _delete_delta_token_for_drive(drive_id: str) -> int:
    normalized_drive_id = str(drive_id or "").strip()
    if not normalized_drive_id:
        return 0
    delta_table = _connect_table(_connect_settings.delta_tokens_table_name)
    existing = delta_table.get_item(Key={"drive_id": normalized_drive_id}).get("Item")
    if not existing:
        return 0
    delta_table.delete_item(Key={"drive_id": normalized_drive_id})
    return 1


def _delete_idempotency_keys_for_tenant(tenant_id: str) -> int:
    tenant_aliases = _ordered_unique(_tenant_aliases(tenant_id))
    if not tenant_aliases:
        return 0
    idempotency_table = _connect_table(_connect_idempotency_table_name)
    deleted_count = 0
    last_evaluated_key: dict[str, Any] | None = None
    while True:
        scan_kwargs: dict[str, Any] = {
            "ProjectionExpression": "event_id, tenant_id",
            "Limit": 200,
        }
        if len(tenant_aliases) == 1:
            scan_kwargs["FilterExpression"] = Attr("tenant_id").eq(tenant_aliases[0])
        else:
            scan_kwargs["FilterExpression"] = Attr("tenant_id").is_in(tenant_aliases)
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
        response = idempotency_table.scan(**scan_kwargs)
        items = response.get("Items", [])
        if items:
            with idempotency_table.batch_writer() as batch:
                for item in items:
                    event_id = str(item.get("event_id") or "").strip()
                    if not event_id:
                        continue
                    batch.delete_item(Key={"event_id": event_id})
                    deleted_count += 1
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    return deleted_count


def _cascade_delete_connect_artifacts(
    *,
    tenant_id: str,
    drive_id: str,
) -> dict[str, int]:
    function_name = (
        os.getenv("CONNECT_CLEANUP_ARTIFACTS_LAMBDA_NAME")
        or "AIReadyConnect-cleanupConnectionArtifacts"
    ).strip()
    response = _lambda_client_resource().invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {"tenant_id": tenant_id, "drive_id": drive_id},
            ensure_ascii=True,
        ).encode("utf-8"),
    )
    status_code = int(response.get("StatusCode", 500))
    if status_code < 200 or status_code >= 300:
        raise ValueError(f"Connect cleanup lambda invoke failed: status={status_code}")
    payload_stream = response.get("Payload")
    raw_payload = payload_stream.read().decode("utf-8") if payload_stream else "{}"
    parsed = json.loads(raw_payload or "{}")
    handler_status = int(parsed.get("statusCode", 500))
    body = parsed.get("body", {})
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            body = {"message": body}
    if handler_status >= 400:
        raise ValueError(str(body.get("error") or body.get("message") or "cleanup failed"))
    return {
        "file_metadata_deleted": int(body.get("file_metadata_deleted", 0) or 0),
        "delta_tokens_deleted": int(body.get("delta_tokens_deleted", 0) or 0),
        "idempotency_keys_deleted": int(body.get("idempotency_keys_deleted", 0) or 0),
    }


def _trigger_initial_sync_check_after_onboarding(
    *,
    tenant_id: str,
    bootstrap_status: str,
    subscription_initialized: bool,
    correlation_id: str,
) -> None:
    normalized_status = str(bootstrap_status or "").strip().lower()
    if normalized_status != "succeeded" or not subscription_initialized:
        return
    try:
        trigger_connect_sync_check(
            tenant_id=tenant_id,
            requested_by="system-onboarding",
            correlation_id=f"{correlation_id}-initial-sync",
        )
    except Exception:
        # Initial sync trigger is best-effort.
        pass


def _invoke_chat_message_backfill_async(
    tenant_id: str,
    *,
    connection_id: str,
    chat_id: str,
    correlation_id: str,
) -> None:
    fn_name = (_connect_settings.backfill_chat_messages_lambda_name or "").strip()
    if not fn_name:
        return
    payload = {
        "tenant_id": tenant_id,
        "connection_id": connection_id,
        "chat_id": chat_id,
        "correlation_id": correlation_id,
    }
    try:
        _lambda_client_resource().invoke(
            FunctionName=fn_name,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except Exception:
        logger.warning(
            "Connect chat message backfill invoke failed (non-fatal): tenant_id=%s connection_id=%s",
            tenant_id,
            connection_id,
            exc_info=True,
        )


def create_connect_onboarding(
    tenant_id: str,
    *,
    client_id: str,
    site_id: str,
    drive_id: str,
    notification_url: str,
    client_secret: str,
    client_state: str,
    connection_name: str,
    initialize_subscription: bool,
    resource_type: str = "drive",
    resource_path: str = "",
    change_type: str = "updated",
    target_type: str = "drive",
    team_id: str = "",
    channel_id: str = "",
    chat_id: str = "",
) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    normalized_resource_type = str(resource_type or "drive").strip().lower()
    normalized_target_type = str(target_type or normalized_resource_type).strip().lower()
    normalized_team_id = str(team_id or "").strip()
    normalized_channel_id = str(channel_id or "").strip()
    normalized_chat_id = str(chat_id or "").strip()
    effective_drive_id = str(drive_id or "").strip()
    if normalized_resource_type == "message" and not effective_drive_id:
        if normalized_target_type == "chat" and normalized_chat_id:
            effective_drive_id = f"msg-chat-{normalized_chat_id}"
        elif normalized_target_type == "channel" and normalized_team_id and normalized_channel_id:
            effective_drive_id = f"msg-channel-{normalized_team_id}-{normalized_channel_id}"
        else:
            effective_drive_id = "msg-generic"
    resolved_azure_tenant_id, resolved_client_id, resolved_client_secret = _resolve_graph_credentials(
        tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    config = ConnectTenantConfig.create(
        tenant_id=tenant_id,
        client_id=resolved_client_id,
        client_secret=resolved_client_secret,
        site_id=site_id,
        drive_id=effective_drive_id,
        notification_url=notification_url,
        client_state=client_state,
        connection_name=connection_name,
    )
    normalized_resource_path = str(resource_path or "").strip()
    normalized_change_type = str(change_type or "updated").strip() or "updated"
    if normalized_resource_type not in {"drive", "message"}:
        raise ValueError("resource_type must be either 'drive' or 'message'.")
    if normalized_resource_type == "drive":
        normalized_resource_path = normalized_resource_path or f"drives/{config.drive_id}/root"
    else:
        if not normalized_resource_path:
            if normalized_target_type == "channel":
                if not normalized_team_id or not normalized_channel_id:
                    raise ValueError("team_id and channel_id are required for channel message subscriptions.")
                normalized_resource_path = f"teams/{normalized_team_id}/channels/{normalized_channel_id}/messages"
            elif normalized_target_type == "chat":
                if not normalized_chat_id:
                    raise ValueError("chat_id is required for chat message subscriptions.")
                normalized_resource_path = f"chats/{normalized_chat_id}/messages"
            else:
                raise ValueError("target_type must be 'channel' or 'chat' for message subscriptions.")
    _validate_notification_url(config.notification_url)

    # Prevent duplicate resource registration within the same tenant.
    try:
        existing_connections = _connect_connections_repository.list_connections_for_tenant(
            config.tenant_id, limit=500
        )
    except Exception:
        existing_connections = []
    for existing in existing_connections:
        existing_status = str(existing.get("status") or "").strip().lower()
        if existing_status in {"deprecated", "retired", "deleted"}:
            continue
        existing_drive_id = str(existing.get("drive_id") or "").strip()
        if existing_drive_id and existing_drive_id == config.drive_id:
            existing_connection_id = str(existing.get("connection_id") or "").strip()
            raise ValueError(
                "Resource already registered for this tenant. "
                f"drive_id={config.drive_id} "
                f"(existing_connection_id={existing_connection_id or '-'})"
            )

    connection_id = f"conn-{uuid.uuid4().hex[:12]}"

    _invalidate_cached_reads(
        tenant_id=tenant_id,
        namespaces=("overview", "subscriptions", "scopes", "events", "jobs", "audit"),
    )

    correlation_id = f"connect-onboarding-{uuid.uuid4().hex[:12]}"
    _put_ssm_parameter(_tenant_param_name(config.tenant_id, "tenant_id"), resolved_azure_tenant_id)
    _put_ssm_parameter(_tenant_param_name(config.tenant_id, "client_id"), config.client_id)
    _put_ssm_parameter(_tenant_param_name(config.tenant_id, "site_id"), config.site_id)
    _put_ssm_parameter(_tenant_param_name(config.tenant_id, "drive_id"), config.drive_id)
    _put_ssm_parameter(
        _tenant_param_name(config.tenant_id, "notification_url"), config.notification_url
    )
    if config.client_state:
        _put_ssm_parameter(
            _tenant_param_name(config.tenant_id, "client_state"),
            config.client_state,
            secure=True,
        )
    _put_ssm_parameter(
        _tenant_param_name(config.tenant_id, "active_connection_id"), connection_id
    )

    # Connection-scoped parameters (new path)
    _put_ssm_parameter(
        _tenant_connection_param_name(config.tenant_id, connection_id, "tenant_id"),
        resolved_azure_tenant_id,
    )
    _put_ssm_parameter(
        _tenant_connection_param_name(config.tenant_id, connection_id, "client_id"),
        config.client_id,
    )
    _put_ssm_parameter(
        _tenant_connection_param_name(config.tenant_id, connection_id, "site_id"),
        config.site_id,
    )
    _put_ssm_parameter(
        _tenant_connection_param_name(config.tenant_id, connection_id, "drive_id"),
        config.drive_id,
    )
    _put_ssm_parameter(
        _tenant_connection_param_name(config.tenant_id, connection_id, "notification_url"),
        config.notification_url,
    )
    _put_ssm_parameter(
        _tenant_connection_param_name(config.tenant_id, connection_id, "resource_type"),
        normalized_resource_type,
    )
    _put_ssm_parameter(
        _tenant_connection_param_name(config.tenant_id, connection_id, "resource_path"),
        normalized_resource_path,
    )
    if config.client_state:
        _put_ssm_parameter(
            _tenant_connection_param_name(config.tenant_id, connection_id, "client_state"),
            config.client_state,
            secure=True,
        )

    secret_name = f"/aiready/connect/{config.tenant_id}/{connection_id}/client_secret"
    try:
        _upsert_secret(secret_name, config.client_secret)
    except ClientError:
        pass
    # Keep SSM secure param as compatibility source for all paths.
    _put_ssm_parameter(
        _tenant_connection_param_name(config.tenant_id, connection_id, "client_secret"),
        config.client_secret,
        secure=True,
    )
    _put_ssm_parameter(
        _tenant_param_name(config.tenant_id, "client_secret"),
        config.client_secret,
        secure=True,
    )

    try:
        _connect_connections_repository.upsert_connection(
            tenant_id=config.tenant_id,
            connection_id=connection_id,
            connection_name=config.connection_name or connection_id,
            site_id=config.site_id,
            drive_id=config.drive_id,
            status="pending",
            resource_type=normalized_resource_type,
            resource_path=normalized_resource_path,
            target_type=normalized_target_type,
            team_id=normalized_team_id,
            channel_id=normalized_channel_id,
            chat_id=normalized_chat_id,
        )
    except Exception:
        logger.error(
            "Connect upsert_connection failed (phase=pending) tenant_id=%s connection_id=%s correlation_id=%s",
            config.tenant_id,
            connection_id,
            correlation_id,
            exc_info=True,
        )

    initialized = False
    init_status = "skipped"
    subscription_id = ""
    bootstrap_error = ""
    if initialize_subscription:
        initialized, init_status, subscription_id, bootstrap_error = _bootstrap_subscription(
            config.tenant_id,
            connection_id=connection_id,
            site_id=config.site_id,
            drive_id=config.drive_id,
            notification_url=config.notification_url,
            client_state=config.client_state,
            correlation_id=correlation_id,
            resource_type=normalized_resource_type,
            resource_path=normalized_resource_path,
            change_type=normalized_change_type,
            team_id=normalized_team_id,
            channel_id=normalized_channel_id,
            chat_id=normalized_chat_id,
        )
    if init_status == "succeeded" and not subscription_id:
        init_status = "failed"
        bootstrap_error = bootstrap_error or "subscription_id is missing from init subscription response."
    bootstrap_status_for_initial_sync = init_status
    if subscription_id:
        _put_ssm_parameter(
            _tenant_connection_param_name(config.tenant_id, connection_id, "subscription_id"),
            subscription_id,
        )
        _put_ssm_parameter(_tenant_param_name(config.tenant_id, "subscription_id"), subscription_id)
    try:
        _connect_connections_repository.upsert_connection(
            tenant_id=config.tenant_id,
            connection_id=connection_id,
            connection_name=config.connection_name or connection_id,
            site_id=config.site_id,
            drive_id=config.drive_id,
            status="active" if init_status == "succeeded" else "error" if init_status == "failed" else "pending",
            subscription_id=subscription_id,
            resource_type=normalized_resource_type,
            resource_path=normalized_resource_path,
            target_type=normalized_target_type,
            team_id=normalized_team_id,
            channel_id=normalized_channel_id,
            chat_id=normalized_chat_id,
        )
    except Exception:
        logger.error(
            "Connect upsert_connection failed (phase=post_bootstrap) tenant_id=%s connection_id=%s "
            "correlation_id=%s init_status=%s",
            config.tenant_id,
            connection_id,
            correlation_id,
            init_status,
            exc_info=True,
        )

    subscriptions = list_connect_subscriptions(tenant_id=tenant_id)
    reflection_ready = True
    reflection_wait_ms = 0
    if initialize_subscription and initialized:
        attempts_raw = os.getenv("CONNECT_ONBOARDING_REFLECTION_POLL_ATTEMPTS", "5")
        interval_raw = os.getenv("CONNECT_ONBOARDING_REFLECTION_POLL_INTERVAL_SEC", "0.8")
        try:
            attempts = max(1, min(int(attempts_raw), 15))
        except ValueError:
            attempts = 5
        try:
            interval_sec = max(0.1, min(float(interval_raw), 5.0))
        except ValueError:
            interval_sec = 0.8
        subscriptions, reflection_ready, reflection_wait_ms = _wait_for_subscription_reflection(
            tenant_id=tenant_id,
            connection_id=connection_id,
            attempts=attempts,
            interval_sec=interval_sec,
        )
        if not reflection_ready and init_status in {"accepted", "succeeded", "started"}:
            init_status = "initializing"

    _trigger_initial_sync_check_after_onboarding(
        tenant_id=config.tenant_id,
        bootstrap_status=bootstrap_status_for_initial_sync,
        subscription_initialized=initialized,
        correlation_id=correlation_id,
    )
    if (
        initialize_subscription
        and str(init_status).strip().lower() == "succeeded"
        and normalized_target_type == "chat"
        and normalized_chat_id
    ):
        _invoke_chat_message_backfill_async(
            config.tenant_id,
            connection_id=connection_id,
            chat_id=normalized_chat_id,
            correlation_id=f"{correlation_id}-chat-backfill",
        )
    return {
        "tenant_id": config.tenant_id,
        "status": "accepted",
        "connection_id": connection_id,
        "bootstrap_status": init_status,
        "subscription_id": subscription_id or None,
        "subscription_status": init_status,
        "bootstrap_error": bootstrap_error or None,
        "subscription_initialized": initialized,
        "subscription_init_status": init_status,
        "subscription_reflection_ready": reflection_ready,
        "subscription_reflection_wait_ms": reflection_wait_ms,
        "subscriptions": subscriptions,
    }


def list_connect_scopes(
    tenant_id: str,
    *,
    subscription_id: str | None = None,
) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    normalized_subscription_id = subscription_id or ""
    cache_key = _build_read_cache_key("scopes", tenant_id, normalized_subscription_id)
    cached_response = _get_cached_read_response(cache_key)
    if cached_response is not None:
        return _with_cache_meta(cached_response, cache_hit=True)
    delta_table = _connect_table(_connect_settings.delta_tokens_table_name)
    rows: list[dict[str, Any]] = []
    seen_scope_ids: set[str] = set()

    try:
        connections = _connect_connections_repository.list_connections_for_tenant(tenant_id, limit=500)
    except Exception:
        connections = []

    for connection in connections:
        connection_status = str(connection.get("status") or "").strip().lower()
        if connection_status in {"deprecated", "retired", "deleted"}:
            continue

        current_connection_id = str(connection.get("connection_id") or "").strip()
        current_subscription_id = str(connection.get("subscription_id") or "").strip()
        resolved_subscription_id = current_subscription_id or f"sub-{tenant_id}-{current_connection_id or 'default'}"
        if normalized_subscription_id and resolved_subscription_id != normalized_subscription_id:
            continue

        drive_id = str(connection.get("drive_id") or "").strip()
        if not drive_id:
            continue

        scope_id = f"scope-{drive_id}"
        if scope_id in seen_scope_ids:
            continue
        seen_scope_ids.add(scope_id)

        item = delta_table.get_item(Key={"drive_id": drive_id}).get("Item", {})
        rows.append(
            {
                "id": scope_id,
                "subscription_id": resolved_subscription_id,
                "tenant_id": tenant_id,
                "site": str(connection.get("site_id") or "").strip() or _get_ssm_value("MSGraphTenantId") or "m365-site",
                "drive": drive_id,
                "excluded_path_count": 0,
                "last_delta_sync_at": str(item.get("updated_at") or item.get("synced_at") or ""),
            }
        )

    # Do not synthesize scope rows from legacy settings.
    # Scope rows must originate from active connection records only.

    response = {
        "rows": rows,
        "pagination": {"limit": 100, "offset": 0, "total_count": len(rows)},
    }
    _set_cached_read_response(cache_key, response)
    return _with_cache_meta(response, cache_hit=False)


def delete_connect_subscription(
    tenant_id: str,
    *,
    subscription_id: str,
    connection_id: str = "",
    delete_mode: str = "safe",
) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    normalized_subscription_id = str(subscription_id or "").strip()
    normalized_connection_id = str(connection_id or "").strip()
    normalized_delete_mode = str(delete_mode or "safe").strip().lower()
    if not normalized_subscription_id and not normalized_connection_id:
        raise ValueError("subscription_id or connection_id is required.")
    if normalized_delete_mode not in {"safe", "force"}:
        raise ValueError("delete_mode must be either 'safe' or 'force'.")

    target_connection = _find_connection_by_subscription(
        tenant_id,
        subscription_id=normalized_subscription_id,
        connection_id=normalized_connection_id,
    )
    if not target_connection:
        raise ValueError("Target subscription was not found.")

    target_connection_id = str(target_connection.get("connection_id") or "").strip()
    target_subscription_id = str(target_connection.get("subscription_id") or "").strip()
    if not target_subscription_id:
        target_subscription_id = normalized_subscription_id

    graph_unsubscribe_status = "skipped"
    graph_unsubscribe_failed = False
    if target_subscription_id and not _is_placeholder_subscription(target_subscription_id):
        try:
            resolved_azure_tenant_id, resolved_client_id, resolved_client_secret = _resolve_graph_credentials(tenant_id)
            access_token = _graph_access_token(
                azure_tenant_id=resolved_azure_tenant_id,
                client_id=resolved_client_id,
                client_secret=resolved_client_secret,
            )
            graph_unsubscribe_status = _graph_delete_subscription(
                subscription_id=target_subscription_id,
                access_token=access_token,
            )
        except Exception:
            graph_unsubscribe_status = "failed"
        graph_unsubscribe_failed = graph_unsubscribe_status.startswith("failed")

    if normalized_delete_mode == "safe" and graph_unsubscribe_failed:
        raise ValueError(
            "Graph unsubscribe failed. Local connection was kept. "
            "Retry with delete_mode=force if you want to delete locally anyway."
        )

    target_drive_id = str(target_connection.get("drive_id") or "").strip()
    deleted_at = _now().isoformat()
    status = "deleted"
    if normalized_delete_mode == "force":
        _cascade_delete_connect_artifacts(
            tenant_id=tenant_id,
            drive_id=target_drive_id,
        )
        deleted = _connect_connections_repository.delete_connection(
            tenant_id=tenant_id,
            connection_id=target_connection_id,
        )
        if not deleted:
            raise ValueError("Target connection was not found during hard delete.")
    else:
        updated = _connect_connections_repository.update_connection_status(
            tenant_id=tenant_id,
            connection_id=target_connection_id,
            status="deleted",
            cleanup_reason="user_deleted_from_connect_subscriptions",
        )
        status = str(updated.get("status") or "deleted")
        deleted_at = str(updated.get("updated_at") or deleted_at)

    _invalidate_cached_reads(
        tenant_id=tenant_id,
        namespaces=("overview", "subscriptions", "scopes", "events", "jobs", "audit"),
    )

    return {
        "tenant_id": tenant_id,
        "connection_id": target_connection_id,
        "subscription_id": target_subscription_id,
        "delete_mode": normalized_delete_mode,
        "status": status,
        "graph_unsubscribe_status": graph_unsubscribe_status,
        "deleted_at": deleted_at,
    }


def _list_connect_message_events(
    tenant_id: str,
    *,
    conversation_key: str,
    normalized_scope_id: str,
    status: str | None,
    normalized_limit: int,
    normalized_offset: int,
) -> dict[str, Any]:
    effective_tenant_ids = _ordered_unique(_tenant_aliases(tenant_id))
    if not effective_tenant_ids:
        effective_tenant_ids = [tenant_id]
    cache_key = _build_read_cache_key(
        "events",
        "|".join(effective_tenant_ids),
        normalized_scope_id,
        str(status or ""),
        normalized_limit,
        normalized_offset,
    )
    cached_response = _get_cached_read_response(cache_key)
    if cached_response is not None:
        return _with_cache_meta(cached_response, cache_hit=True)
    msg_table = _connect_table(_connect_settings.message_metadata_table_name)
    rows_by_event_id: dict[str, dict[str, Any]] = {}
    max_scan_items = max(500, min(int(os.getenv("CONNECT_EVENTS_MAX_SCAN_ITEMS", "5000")), 20000))
    for partition_tenant_id in effective_tenant_ids:
        last_evaluated_key: dict[str, Any] | None = None
        scanned_items = 0
        while True:
            query_limit = 500
            query_kwargs: dict[str, Any] = {
                "IndexName": "GSI-TenantModifiedAt",
                "KeyConditionExpression": Key("tenant_id").eq(partition_tenant_id),
                "ScanIndexForward": False,
                "Limit": query_limit,
            }
            if last_evaluated_key:
                query_kwargs["ExclusiveStartKey"] = last_evaluated_key
            try:
                response = msg_table.query(**query_kwargs)
            except ClientError as exc:
                error_code = str(exc.response.get("Error", {}).get("Code", ""))
                if error_code in {"ValidationException", "ResourceNotFoundException"}:
                    raise RuntimeError(
                        "Connect message events query requires GSI-TenantModifiedAt (tenant_id, modified_at)."
                    ) from exc
                raise
            items = response.get("Items", [])
            scanned_items += len(items)
            for item in items:
                plain = _to_plain_value(item)
                if str(plain.get("conversation_key") or "") != conversation_key:
                    continue
                row_status = "processed" if not plain.get("is_deleted", False) else "deleted"
                if status and row_status != status:
                    continue
                msg_mid = str(plain.get("message_id") or "").strip()
                if not msg_mid:
                    continue
                event_key = msg_mid
                received_at = str(
                    plain.get("synced_at")
                    or plain.get("modified_at")
                    or plain.get("created_at")
                    or ""
                )
                resource_label = (
                    str(plain.get("subject") or "").strip()
                    or str(plain.get("summary") or "").strip()
                    or str(plain.get("resource") or "").strip()
                    or event_key
                )
                row = {
                    "id": f"evt-msg-{event_key}",
                    "scope_id": normalized_scope_id,
                    "received_at": received_at,
                    "change_type": "deleted" if plain.get("is_deleted") else "updated",
                    "resource": resource_label,
                    "idempotency_key": f"{partition_tenant_id}:msg:{event_key}",
                    "status": row_status,
                }
                existing = rows_by_event_id.get(row["id"])
                if existing is None or str(existing.get("received_at") or "") < received_at:
                    rows_by_event_id[row["id"]] = row
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key or scanned_items >= max_scan_items:
                break
    rows = list(rows_by_event_id.values())
    rows.sort(key=lambda row: str(row.get("received_at") or ""), reverse=True)
    paged = rows[normalized_offset : normalized_offset + normalized_limit]
    out = {
        "rows": paged,
        "pagination": {
            "limit": normalized_limit,
            "offset": normalized_offset,
            "total_count": len(rows),
        },
        "resolved_tenant_id": ",".join(effective_tenant_ids),
    }
    _set_cached_read_response(cache_key, out)
    return _with_cache_meta(out, cache_hit=False)


def list_connect_events(
    tenant_id: str,
    *,
    scope_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    normalized_limit = max(1, min(int(limit), 500))
    normalized_offset = max(0, int(offset))
    normalized_scope_id = str(scope_id or "")
    message_conversation_key = _conversation_key_from_scope_id(normalized_scope_id)
    if message_conversation_key:
        return _list_connect_message_events(
            tenant_id,
            conversation_key=message_conversation_key,
            normalized_scope_id=normalized_scope_id,
            status=status,
            normalized_limit=normalized_limit,
            normalized_offset=normalized_offset,
        )
    file_table = _connect_table(_connect_settings.file_metadata_table_name)
    scope_drive_id = _drive_id_from_scope_id(normalized_scope_id)
    active_drive_ids = _active_drive_ids_for_tenant(tenant_id)
    if scope_drive_id:
        if active_drive_ids and scope_drive_id not in active_drive_ids:
            response = {
                "rows": [],
                "pagination": {
                    "limit": normalized_limit,
                    "offset": normalized_offset,
                    "total_count": 0,
                },
                "resolved_tenant_id": tenant_id,
            }
            _set_cached_read_response(
                _build_read_cache_key(
                    "events",
                    tenant_id,
                    normalized_scope_id,
                    str(status or ""),
                    normalized_limit,
                    normalized_offset,
                ),
                response,
            )
            return _with_cache_meta(response, cache_hit=False)
    elif not active_drive_ids:
        response = {
            "rows": [],
            "pagination": {
                "limit": normalized_limit,
                "offset": normalized_offset,
                "total_count": 0,
            },
            "resolved_tenant_id": tenant_id,
        }
        _set_cached_read_response(
            _build_read_cache_key(
                "events",
                tenant_id,
                normalized_scope_id,
                str(status or ""),
                normalized_limit,
                normalized_offset,
            ),
            response,
        )
        return _with_cache_meta(response, cache_hit=False)
    effective_tenant_ids = _resolve_event_tenant_partitions(
        tenant_id,
        scope_drive_id=scope_drive_id,
    )
    if not effective_tenant_ids:
        effective_tenant_ids = _ordered_unique(_tenant_aliases(tenant_id))
    if not effective_tenant_ids:
        effective_tenant_ids = [tenant_id]
    cache_key = _build_read_cache_key(
        "events",
        "|".join(effective_tenant_ids),
        normalized_scope_id,
        str(status or ""),
        normalized_limit,
        normalized_offset,
    )
    cached_response = _get_cached_read_response(cache_key)
    if cached_response is not None:
        return _with_cache_meta(cached_response, cache_hit=True)
    needed_count = normalized_offset + normalized_limit
    rows: list[dict[str, Any]] = []
    rows_by_event_id: dict[str, dict[str, Any]] = {}
    max_scan_items = max(500, min(int(os.getenv("CONNECT_EVENTS_MAX_SCAN_ITEMS", "5000")), 20000))
    for partition_tenant_id in effective_tenant_ids:
        last_evaluated_key: dict[str, Any] | None = None
        scanned_items = 0
        while True:
            # received_at を synced_at 優先で扱うため、modified_at の並び順に
            # 依存し過ぎないよう、十分な候補を収集してからメモリ上で再ソートする。
            query_limit = 500
            query_kwargs: dict[str, Any] = {
                "IndexName": "GSI-ModifiedAt",
                "KeyConditionExpression": Key("tenant_id").eq(partition_tenant_id),
                "ScanIndexForward": False,
                "Limit": query_limit,
            }
            if last_evaluated_key:
                query_kwargs["ExclusiveStartKey"] = last_evaluated_key

            try:
                response = file_table.query(**query_kwargs)
            except ClientError as exc:
                error_code = str(exc.response.get("Error", {}).get("Code", ""))
                if error_code in {"ValidationException", "ResourceNotFoundException"}:
                    raise RuntimeError(
                        "Connect events query requires GSI-ModifiedAt (tenant_id, modified_at)."
                    ) from exc
                raise

            items = response.get("Items", [])
            scanned_items += len(items)
            for item in items:
                plain = _to_plain_value(item)
                item_drive_id = str(plain.get("drive_id") or "")
                if active_drive_ids and item_drive_id and item_drive_id not in active_drive_ids:
                    continue
                if scope_drive_id and item_drive_id and item_drive_id != scope_drive_id:
                    continue
                row_status = "processed" if not plain.get("is_deleted", False) else "deleted"
                if status and row_status != status:
                    continue
                event_key = str(plain.get("item_id") or f"idx-{len(rows_by_event_id)}")
                received_at = str(
                    plain.get("synced_at")
                    or plain.get("modified_at")
                    or plain.get("created_at")
                    or ""
                )
                row = {
                    "id": f"evt-{event_key}",
                    "scope_id": normalized_scope_id or f"scope-{item_drive_id or 'default'}",
                    "received_at": received_at,
                    "change_type": "deleted" if plain.get("is_deleted") else "updated",
                    "resource": str(plain.get("web_url") or plain.get("name") or plain.get("item_id")),
                    "idempotency_key": f"{partition_tenant_id}:{event_key}",
                    "status": row_status,
                }
                existing = rows_by_event_id.get(row["id"])
                if existing is None or str(existing.get("received_at") or "") < received_at:
                    rows_by_event_id[row["id"]] = row

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key or scanned_items >= max_scan_items:
                break

    rows = list(rows_by_event_id.values())

    rows.sort(key=lambda row: str(row.get("received_at") or ""), reverse=True)
    paged = rows[normalized_offset : normalized_offset + normalized_limit]
    total_count = len(rows)
    response = {
        "rows": paged,
        "pagination": {
            "limit": normalized_limit,
            "offset": normalized_offset,
            "total_count": total_count,
        },
        "resolved_tenant_id": ",".join(effective_tenant_ids),
    }
    _set_cached_read_response(cache_key, response)
    return _with_cache_meta(response, cache_hit=False)


def list_connect_jobs(
    tenant_id: str,
    *,
    event_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    normalized_limit = max(1, min(int(limit), 500))
    normalized_offset = max(0, int(offset))
    cache_key = _build_read_cache_key(
        "jobs",
        tenant_id,
        str(event_id or ""),
        str(status or ""),
        normalized_limit,
        normalized_offset,
    )
    cached_response = _get_cached_read_response(cache_key)
    if cached_response is not None:
        return _with_cache_meta(cached_response, cache_hit=True)
    fetch_limit = min(500, normalized_limit + normalized_offset + 30)
    try:
        rows = _connect_logs_repository.query_recent_rows(
            tenant_id=tenant_id,
            query_string=(
                "fields @timestamp as timestamp, @message as message, @log as log_group, "
                "@logStream as log_stream | sort @timestamp desc"
            ),
            limit=fetch_limit,
            lookback_days=1,
        )
    except Exception:
        # CloudWatch Logs access can fail transiently (permission, throttling, query timeout).
        # Keep Connect UI responsive by returning an empty page instead of HTTP 500.
        rows = []
    mapped_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        message = row.get("message", "")
        derived_event_id = None
        event_match = _EVENT_ID_PATTERN.search(message)
        if event_match:
            derived_event_id = event_match.group(1)
        row_status = _infer_job_status(message)
        if status and row_status != status:
            continue
        if event_id and derived_event_id != event_id:
            continue
        correlation_match = _CORRELATION_ID_PATTERN.search(message)
        log_group = row.get("log_group", "")
        mapped_rows.append(
            {
                "id": f"job-log-{index + 1:05d}",
                "event_id": derived_event_id,
                "job_type": "ingestion"
                if "pullFileMetadata" in log_group
                else "governance_trigger",
                "started_at": row.get("timestamp", ""),
                "status": row_status,
                "last_message": message[:500],
                "correlation_id": correlation_match.group(1) if correlation_match else None,
                "source": log_group,
            }
        )

    paged_rows = mapped_rows[normalized_offset : normalized_offset + normalized_limit]
    response = {
        "rows": paged_rows,
        "pagination": {
            "limit": normalized_limit,
            "offset": normalized_offset,
            "total_count": len(mapped_rows),
        },
    }
    _set_cached_read_response(cache_key, response)
    return _with_cache_meta(response, cache_hit=False)


def list_connect_audit(
    tenant_id: str,
    *,
    query: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    normalized_limit = max(1, min(int(limit), 500))
    normalized_offset = max(0, int(offset))
    cache_key = _build_read_cache_key(
        "audit",
        tenant_id,
        str(query or ""),
        normalized_limit,
        normalized_offset,
    )
    cached_response = _get_cached_read_response(cache_key)
    if cached_response is not None:
        return _with_cache_meta(cached_response, cache_hit=True)
    fetch_limit = min(500, normalized_limit + normalized_offset + 30)
    try:
        rows = _connect_logs_repository.query_recent_rows(
            tenant_id=tenant_id,
            query_string=(
                "fields @timestamp as timestamp, @message as message, @log as log_group, "
                "@logStream as log_stream | sort @timestamp desc"
            ),
            limit=fetch_limit,
            lookback_days=1,
        )
    except Exception:
        # Same fallback policy as jobs endpoint.
        rows = []
    search_token = (query or "").strip().lower()
    mapped_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        message = row.get("message", "")
        if search_token and search_token not in message.lower():
            continue
        correlation_match = _CORRELATION_ID_PATTERN.search(message)
        log_group = row.get("log_group", "")
        action = (
            "connect.sync.job"
            if "pullFileMetadata" in log_group
            else "connect.notification"
            if "receiveNotification" in log_group
            else "connect.scheduled.maintenance"
        )
        mapped_rows.append(
            {
                "id": f"audit-log-{index + 1:05d}",
                "operated_at": row.get("timestamp", ""),
                "operator": _infer_operator(message),
                "action": action,
                "target_type": "job",
                "target_id": row.get("log_stream", "")[:120],
                "correlation_id": correlation_match.group(1) if correlation_match else None,
                "source": log_group,
            }
        )

    paged_rows = mapped_rows[normalized_offset : normalized_offset + normalized_limit]
    response = {
        "rows": paged_rows,
        "pagination": {
            "limit": normalized_limit,
            "offset": normalized_offset,
            "total_count": len(mapped_rows),
        },
    }
    _set_cached_read_response(cache_key, response)
    return _with_cache_meta(response, cache_hit=False)


def trigger_connect_sync_check(
    tenant_id: str,
    *,
    requested_by: str,
    correlation_id: str,
) -> dict[str, Any]:
    _require_connect_tenant(tenant_id)
    function_name = _connect_settings.pull_file_metadata_lambda_name
    drive_id = _tenant_drive_id(tenant_id)
    payload = {
        "tenant_id": tenant_id,
        "drive_id": drive_id,
        "trigger": "manual-sync-check",
        "requested_by": requested_by,
        "correlation_id": correlation_id,
        "requested_at": _now().isoformat(),
    }
    response = _lambda_client_resource().invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    # Ensure UI can fetch fresh rows immediately after manual trigger.
    _invalidate_cached_reads(
        tenant_id=tenant_id,
        namespaces=("overview", "subscriptions", "scopes", "events", "jobs", "audit"),
    )
    status_code = int(response.get("StatusCode", 202))
    return {
        "tenant_id": tenant_id,
        "status": "accepted" if status_code in {200, 202} else "failed",
        "lambda_function_name": function_name,
        "status_code": status_code,
        "requested_by": requested_by,
        "correlation_id": correlation_id,
        "requested_at": payload["requested_at"],
    }
