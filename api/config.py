"""
Configuration for the GraphSuite API server (no LightRAG).
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

from dotenv import load_dotenv

load_dotenv(dotenv_path=".env", override=False)

API_VERSION = "0.1.0"

DEFAULT_WORKERS = 1
DEFAULT_TIMEOUT = 300


def get_env_value(
    env_key: str,
    default: Any,
    value_type: type | None = None,
    special_none: bool = False,
) -> Any:
    """Read an environment variable with optional typing."""
    raw = os.getenv(env_key)
    if raw is None or raw == "":
        return default
    if special_none and str(raw).strip().lower() in ("none", "null", ""):
        return None
    if value_type is None:
        return raw
    if value_type is bool:
        return str(raw).strip().lower() in ("true", "1", "yes", "on", "t")
    if value_type is int:
        return int(str(raw).strip())
    if value_type is float:
        return float(str(raw).strip())
    if value_type is list:
        return [part.strip() for part in str(raw).split(",") if part.strip()]
    return raw


def parse_args() -> argparse.Namespace:
    """Parse command line arguments with environment variable fallback."""
    parser = argparse.ArgumentParser(description="GraphSuite API Server")

    parser.add_argument(
        "--host",
        default=get_env_value("HOST", "0.0.0.0"),
        help="Server host (default: from env or 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=get_env_value("PORT", 9621, int),
        help="Server port (default: from env or 9621)",
    )
    parser.add_argument(
        "--working-dir",
        default=get_env_value("WORKING_DIR", "./rag_storage"),
        help="Legacy working directory (unused without RAG; default from env)",
    )
    parser.add_argument(
        "--input-dir",
        default=get_env_value("INPUT_DIR", "./inputs"),
        help="Legacy input directory (unused without RAG; default from env)",
    )
    parser.add_argument(
        "--timeout",
        default=get_env_value("TIMEOUT", DEFAULT_TIMEOUT, int, special_none=True),
        type=int,
        help="Request timeout hint for reverse proxies / docs (seconds)",
    )
    parser.add_argument(
        "--log-level",
        default=get_env_value("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: from env or INFO)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=get_env_value("VERBOSE", False, bool),
        help="Verbose debug output",
    )
    parser.add_argument(
        "--key",
        type=str,
        default=get_env_value("LIGHTRAG_API_KEY", None),
        help="API key (env LIGHTRAG_API_KEY). Legacy name kept for compatibility.",
    )
    parser.add_argument(
        "--ssl",
        action="store_true",
        default=get_env_value("SSL", False, bool),
        help="Enable HTTPS (default: from env or False)",
    )
    parser.add_argument(
        "--ssl-certfile",
        default=get_env_value("SSL_CERTFILE", None),
        help="Path to SSL certificate file (required if --ssl is enabled)",
    )
    parser.add_argument(
        "--ssl-keyfile",
        default=get_env_value("SSL_KEYFILE", None),
        help="Path to SSL private key file (required if --ssl is enabled)",
    )
    parser.add_argument(
        "--workspace",
        type=str,
        default=get_env_value("WORKSPACE", ""),
        help="Legacy workspace label (unused without RAG)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=get_env_value("WORKERS", DEFAULT_WORKERS, int),
        help="Number of worker processes (Gunicorn; uvicorn forces 1)",
    )

    args, _ = parser.parse_known_args()

    args.working_dir = os.path.abspath(args.working_dir)
    args.input_dir = os.path.abspath(args.input_dir)

    if args.ssl:
        if not args.ssl_certfile or not args.ssl_keyfile:
            raise SystemExit("SSL certificate and key files must be provided when SSL is enabled")
        if not os.path.exists(args.ssl_certfile):
            raise SystemExit(f"SSL certificate file not found: {args.ssl_certfile}")
        if not os.path.exists(args.ssl_keyfile):
            raise SystemExit(f"SSL key file not found: {args.ssl_keyfile}")

    args.cors_origins = get_env_value("CORS_ORIGINS", "*")
    args.whitelist_paths = get_env_value("WHITELIST_PATHS", "/health,/api/*")

    args.auth_accounts = get_env_value("AUTH_ACCOUNTS", "")
    args.auth_account_tenants = get_env_value("AUTH_ACCOUNT_TENANTS", "")
    args.token_secret = get_env_value("TOKEN_SECRET", "lightrag-jwt-default-secret")
    args.token_expire_hours = get_env_value("TOKEN_EXPIRE_HOURS", 48, int)
    args.guest_token_expire_hours = get_env_value("GUEST_TOKEN_EXPIRE_HOURS", 24, int)
    args.jwt_algorithm = get_env_value("JWT_ALGORITHM", "HS256")
    args.cognito_jwks_url = get_env_value("COGNITO_JWKS_URL", "")
    args.cognito_issuer = get_env_value("COGNITO_ISSUER", "")
    args.cognito_audience = get_env_value("COGNITO_AUDIENCE", "")
    args.tenant_claim_keys = get_env_value(
        "TENANT_CLAIM_KEYS", "custom:tenant_id,tenant_id,metadata.tenant_id"
    )
    args.admin_roles = get_env_value("ADMIN_ROLES", "admin,platform_admin")
    args.default_tenant_id = get_env_value("DEFAULT_TENANT_ID", "")

    args.aws_region = get_env_value("AWS_REGION", "")
    args.governance_aws_region = get_env_value("GOVERNANCE_AWS_REGION", args.aws_region)
    args.governance_finding_table_name = get_env_value(
        "GOVERNANCE_FINDING_TABLE_NAME", "AIReadyGov-ExposureFinding"
    )
    # 接続削除時に ExposureFinding を closed へ更新するため、API タスクロールに
    # 当テーブルへの dynamodb:UpdateItem（および FileMetadata 読取）が必要。
    governance_document_analysis_default = get_env_value(
        "DOCUMENT_ANALYSIS_TABLE", "AIReadyGov-DocumentAnalysis"
    )
    args.governance_document_analysis_table_name = get_env_value(
        "GOVERNANCE_DOCUMENT_ANALYSIS_TABLE_NAME", governance_document_analysis_default
    )
    args.governance_api_strict_mode = get_env_value(
        "GOVERNANCE_API_STRICT_MODE", False, bool
    )
    args.governance_allow_cloudwatch_fallback = get_env_value(
        "GOVERNANCE_ALLOW_CLOUDWATCH_FALLBACK", not args.governance_api_strict_mode, bool
    )
    args.governance_allow_missing_policy_scope_fallback = get_env_value(
        "GOVERNANCE_ALLOW_MISSING_POLICY_SCOPE_FALLBACK",
        not args.governance_api_strict_mode,
        bool,
    )
    args.ontology_graph_db_root = get_env_value("ONTOLOGY_GRAPH_DB_ROOT", "./tenant_storage")
    args.tenant_db_registry_json = get_env_value("TENANT_DB_REGISTRY_JSON", "")
    args.startup_fail_fast = get_env_value("STARTUP_FAIL_FAST", True, bool)
    args.aws_healthcheck_on_startup = get_env_value(
        "AWS_HEALTHCHECK_ON_STARTUP", True, bool
    )
    args.connect_aws_region = get_env_value("CONNECT_AWS_REGION", args.aws_region)
    args.connect_file_metadata_table_name = get_env_value(
        "CONNECT_FILE_METADATA_TABLE_NAME", "AIReadyConnect-FileMetadata"
    )
    args.connect_delta_tokens_table_name = get_env_value(
        "CONNECT_DELTA_TOKENS_TABLE_NAME", "AIReadyConnect-DeltaTokens"
    )
    args.connect_log_groups = get_env_value("CONNECT_LOG_GROUPS", "")
    args.connect_pull_file_metadata_lambda_name = get_env_value(
        "CONNECT_PULL_FILE_METADATA_LAMBDA_NAME", "AIReadyConnect-pullFileMetadata"
    )
    args.connect_init_subscription_lambda_name = get_env_value(
        "CONNECT_INIT_SUBSCRIPTION_LAMBDA_NAME", "AIReadyConnect-initSubscription"
    )
    args.connect_startup_validate = get_env_value("CONNECT_STARTUP_VALIDATE", True, bool)
    args.connect_startup_validate_resources = get_env_value(
        "CONNECT_STARTUP_VALIDATE_RESOURCES", True, bool
    )
    args.connect_require_tenant_registry = get_env_value(
        "CONNECT_REQUIRE_TENANT_REGISTRY", False, bool
    )

    return args


def update_uvicorn_mode_config() -> None:
    if global_args.workers > 1:
        original_workers = global_args.workers
        global_args.workers = 1
        logging.warning(
            ">> Forcing workers=1 in uvicorn mode (ignoring workers=%s)",
            original_workers,
        )


global_args = parse_args()
