"""
Utility functions for the GraphSuite API.
"""

import argparse
import os
import sys
from typing import List, Optional, Tuple

from ascii_colors import ASCIIColors
from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader, OAuth2PasswordBearer
from starlette.status import HTTP_403_FORBIDDEN
from auth import auth_handler
from config import API_VERSION, global_args
from tenant_context import TenantContext


def check_env_file():
    """
    Check if .env file exists and handle user confirmation if needed.
    Returns True if should continue, False if should exit.
    """
    if not os.path.exists(".env"):
        warning_msg = "Warning: Startup directory must contain .env file for multi-instance support."
        ASCIIColors.yellow(warning_msg)

        # Check if running in interactive terminal
        if sys.stdin.isatty():
            response = input("Do you want to continue? (yes/no): ")
            if response.lower() != "yes":
                ASCIIColors.red("Server startup cancelled")
                return False
    return True


# Get whitelist paths from global_args, only once during initialization
whitelist_paths = global_args.whitelist_paths.split(",")

# Pre-compile path matching patterns
whitelist_patterns: List[Tuple[str, bool]] = []
for path in whitelist_paths:
    path = path.strip()
    if path:
        # If path ends with /*, match all paths with that prefix
        if path.endswith("/*"):
            prefix = path[:-2]
            whitelist_patterns.append((prefix, True))  # (prefix, is_prefix_match)
        else:
            whitelist_patterns.append((path, False))  # (exact_path, is_prefix_match)

# Global authentication configuration
auth_configured = bool(auth_handler.accounts)


def get_combined_auth_dependency(api_key: Optional[str] = None):
    """
    Create a combined authentication dependency that implements authentication logic
    based on API key, OAuth2 token, and whitelist paths.

    Args:
        api_key (Optional[str]): API key for validation

    Returns:
        Callable: A dependency function that implements the authentication logic
    """
    # Use global whitelist_patterns and auth_configured variables
    # whitelist_patterns and auth_configured are already initialized at module level

    # Only calculate api_key_configured as it depends on the function parameter
    api_key_configured = bool(api_key)

    # Create security dependencies with proper descriptions for Swagger UI
    oauth2_scheme = OAuth2PasswordBearer(
        tokenUrl="login", auto_error=False, description="OAuth2 Password Authentication"
    )

    # If API key is configured, create an API key header security
    api_key_header = None
    if api_key_configured:
        api_key_header = APIKeyHeader(
            name="X-API-Key", auto_error=False, description="API Key Authentication"
        )

    async def combined_dependency(
        request: Request,
        token: str = Security(oauth2_scheme),
        api_key_header_value: Optional[str] = None
        if api_key_header is None
        else Security(api_key_header),
    ):
        # 1. Check if path is in whitelist
        path = request.url.path
        for pattern, is_prefix in whitelist_patterns:
            if (is_prefix and path.startswith(pattern)) or (
                not is_prefix and path == pattern
            ):
                return  # Whitelist path, allow access

        # 2. Validate token first if provided in the request (Ensure 401 error if token is invalid)
        if token:
            try:
                token_info = auth_handler.validate_token(token)
                request.state.token_info = token_info
                # Accept guest token if no auth is configured
                if not auth_configured and token_info.get("role") == "guest":
                    return token_info
                # Accept non-guest token if auth is configured
                if auth_configured and token_info.get("role") != "guest":
                    return token_info

                # Token validation failed, immediately return 401 error
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid token. Please login again.",
                )
            except HTTPException as e:
                # If already a 401 error, re-raise it
                if e.status_code == status.HTTP_401_UNAUTHORIZED:
                    raise
                # For other exceptions, continue processing

        # 3. Acept all request if no API protection needed
        if not auth_configured and not api_key_configured:
            return None

        # 4. Validate API key if provided and API-Key authentication is configured
        if (
            api_key_configured
            and api_key_header_value
            and api_key_header_value == api_key
        ):
            request.state.token_info = None
            return  # API key validation successful

        ### Authentication failed ####

        # if password authentication is configured but not provided, ensure 401 error if auth_configured
        if auth_configured and not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No credentials provided. Please login.",
            )

        # if api key is provided but validation failed
        if api_key_header_value:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail="Invalid API Key",
            )

        # if api_key_configured but not provided
        if api_key_configured and not api_key_header_value:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail="API Key required",
            )

        # Otherwise: refuse access and return 403 error
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="API Key required or login authentication required.",
        )

    return combined_dependency


def get_tenant_context_dependency(api_key: Optional[str] = None):
    combined_auth = get_combined_auth_dependency(api_key)

    async def tenant_context_dependency(
        request: Request,
        _auth=Depends(combined_auth),
    ) -> TenantContext:
        token_info = getattr(request.state, "token_info", None)
        if not token_info:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="JWT token is required to resolve tenant context.",
            )

        tenant_id = str(token_info.get("tenant_id") or "").strip()
        if not tenant_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="tenant_id claim is missing in token.",
            )

        roles = [str(role) for role in token_info.get("roles", [])]
        return TenantContext(
            tenant_id=tenant_id,
            username=str(token_info.get("username", "")),
            roles=roles,
            is_admin=bool(token_info.get("is_admin", False)),
            claims=token_info.get("claims", {}),
        )

    return tenant_context_dependency


def display_splash_screen(args: argparse.Namespace) -> None:
    """Display startup banner (Connect / Governance / Ontology API; no embedded RAG)."""
    try:
        "╔📡".encode(sys.stdout.encoding or "utf-8")
    except UnicodeEncodeError:
        print(f"GraphSuite API v{API_VERSION}")
        print(f"Host: {args.host}  Port: {args.port}  Workers: {args.workers}")
        return

    top_border = "╔══════════════════════════════════════════════════════════════╗"
    bottom_border = "╚══════════════════════════════════════════════════════════════╝"
    width = len(top_border) - 4

    line1_text = f"GraphSuite API v{API_VERSION}"
    line2_text = "Connect · Governance · Ontology gateway"

    line1 = f"║ {line1_text.center(width)} ║"
    line2 = f"║ {line2_text.center(width)} ║"

    banner = f"""
    {top_border}
    {line1}
    {line2}
    {bottom_border}
    """
    ASCIIColors.cyan(banner)

    ASCIIColors.magenta("\n📡 Server:")
    ASCIIColors.white("    ├─ Host: ", end="")
    ASCIIColors.yellow(f"{args.host}")
    ASCIIColors.white("    ├─ Port: ", end="")
    ASCIIColors.yellow(f"{args.port}")
    ASCIIColors.white("    ├─ Workers: ", end="")
    ASCIIColors.yellow(f"{args.workers}")
    ASCIIColors.white("    ├─ Timeout: ", end="")
    ASCIIColors.yellow(f"{args.timeout}")
    ASCIIColors.white("    ├─ CORS Origins: ", end="")
    ASCIIColors.yellow(f"{args.cors_origins}")
    ASCIIColors.white("    ├─ SSL: ", end="")
    ASCIIColors.yellow(f"{args.ssl}")
    if args.ssl:
        ASCIIColors.white("    ├─ SSL Cert: ", end="")
        ASCIIColors.yellow(f"{args.ssl_certfile}")
        ASCIIColors.white("    ├─ SSL Key: ", end="")
        ASCIIColors.yellow(f"{args.ssl_keyfile}")
    ASCIIColors.white("    ├─ Log Level: ", end="")
    ASCIIColors.yellow(f"{args.log_level}")
    ASCIIColors.white("    ├─ API Key: ", end="")
    ASCIIColors.yellow("Set" if args.key else "Not Set")
    ASCIIColors.white("    └─ JWT Auth: ", end="")
    ASCIIColors.yellow("Enabled" if args.auth_accounts else "Disabled")

    ASCIIColors.magenta("\n📂 Legacy paths (unused without RAG):")
    ASCIIColors.white("    ├─ Working: ", end="")
    ASCIIColors.yellow(f"{args.working_dir}")
    ASCIIColors.white("    └─ Input: ", end="")
    ASCIIColors.yellow(f"{args.input_dir}")

    ASCIIColors.green("\n✨ Server starting up...\n")

    protocol = "https" if args.ssl else "http"
    if args.host == "0.0.0.0":
        ASCIIColors.magenta("\n🌐 Access:")
        ASCIIColors.white("    ├─ Local: ", end="")
        ASCIIColors.yellow(f"{protocol}://localhost:{args.port}")
        ASCIIColors.white("    ├─ Docs: ", end="")
        ASCIIColors.yellow(f"{protocol}://localhost:{args.port}/docs")
        ASCIIColors.white("    └─ ReDoc: ", end="")
        ASCIIColors.yellow(f"{protocol}://localhost:{args.port}/redoc")
    else:
        base_url = f"{protocol}://{args.host}:{args.port}"
        ASCIIColors.magenta("\n🌐 Access:")
        ASCIIColors.white("    ├─ Base: ", end="")
        ASCIIColors.yellow(base_url)
        ASCIIColors.white("    ├─ Docs: ", end="")
        ASCIIColors.yellow(f"{base_url}/docs")
        ASCIIColors.white("    └─ ReDoc: ", end="")
        ASCIIColors.yellow(f"{base_url}/redoc")

    if args.key:
        ASCIIColors.yellow("\n⚠️  API Key authentication is enabled (X-API-Key).")
    if args.auth_accounts:
        ASCIIColors.yellow("\n⚠️  JWT authentication is enabled (Authorization: Bearer …).")

    sys.stdout.flush()
