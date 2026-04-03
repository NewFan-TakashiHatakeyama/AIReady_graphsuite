"""
GraphSuite FastAPI Server — Connect, Governance, and Ontology API (no LightRAG).
"""

from __future__ import annotations

import configparser
import logging
import logging.config
import os
import signal
import sys
import uuid
from contextlib import asynccontextmanager
import uvicorn
from ascii_colors import ASCIIColors
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm

from auth import auth_handler
from config import API_VERSION, global_args, update_uvicorn_mode_config
from routers.graph_routes import create_graph_routes
from services.aws_clients import check_aws_connectivity
from services.connect_settings import (
    check_connect_resources,
    load_connect_settings,
    validate_connect_settings,
)
from services.log_sanitizer import RedactingLogFilter
from services.ontology_ops_validator import (
    run_ontology_ops_checks,
    run_production_gate_checks,
)
from services.runtime_config import load_aws_runtime_config, validate_runtime_config
from utils_api import (
    check_env_file,
    display_splash_screen,
    get_combined_auth_dependency,
)

load_dotenv(dotenv_path=".env", override=False)

logger = logging.getLogger("graphsuite")

webui_title = os.getenv("WEBUI_TITLE")
webui_description = os.getenv("WEBUI_DESCRIPTION")

config = configparser.ConfigParser()
config.read("config.ini")

auth_configured = bool(auth_handler.accounts)

DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5
DEFAULT_LOG_FILENAME = os.getenv("GRAPHSUITE_LOG_FILENAME", "graphsuite.log")


def setup_signal_handlers() -> None:
    def signal_handler(sig, _frame):
        print(f"\n\nReceived signal {sig}, shutting down gracefully...")
        print(f"Process ID: {os.getpid()}")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def create_app(args):
    api_key = os.getenv("LIGHTRAG_API_KEY") or args.key
    runtime_config = load_aws_runtime_config()
    connect_settings = load_connect_settings()
    startup_validation = {
        "config": "skipped",
        "aws_connectivity": "skipped",
        "connect_config": "skipped",
        "connect_resources": "skipped",
    }

    if runtime_config.startup_fail_fast:
        validate_runtime_config(runtime_config)
        startup_validation["config"] = "ok"
        if connect_settings.startup_validate:
            validate_connect_settings(connect_settings)
            startup_validation["connect_config"] = "ok"
        if runtime_config.aws_healthcheck_on_startup:
            check_aws_connectivity(runtime_config)
            startup_validation["aws_connectivity"] = "ok"
            if (
                connect_settings.startup_validate
                and connect_settings.startup_validate_resources
            ):
                check_connect_resources(connect_settings)
                startup_validation["connect_resources"] = "ok"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            try:
                ASCIIColors.green("\nServer is ready to accept connections! 🚀\n")
            except UnicodeEncodeError:
                print("\nServer is ready to accept connections!\n")
            yield
        finally:
            pass

    app = FastAPI(
        title="GraphSuite Server API",
        description="Connect, Governance, and Ontology HTTP API.",
        version=API_VERSION,
        openapi_url="/openapi.json",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
        swagger_ui_parameters={
            "persistAuthorization": True,
            "tryItOutEnabled": True,
        },
    )
    app.state.startup_validation = startup_validation

    @app.middleware("http")
    async def correlation_id_middleware(request, call_next):
        incoming_correlation_id = request.headers.get("X-Correlation-Id", "").strip()
        correlation_id = incoming_correlation_id or str(uuid.uuid4())
        request.state.correlation_id = correlation_id
        response = await call_next(request)
        response.headers["X-Correlation-Id"] = correlation_id
        return response

    def get_cors_origins():
        origins_str = global_args.cors_origins
        if origins_str == "*":
            return ["*"]
        return [origin.strip() for origin in origins_str.split(",")]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=get_cors_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Correlation-Id"],
    )

    combined_auth = get_combined_auth_dependency(api_key)
    app.include_router(create_graph_routes(api_key))

    @app.get("/")
    async def redirect_to_webui():
        return RedirectResponse(url="/webui")

    @app.get("/auth-status")
    async def get_auth_status():
        if not auth_handler.accounts:
            guest_token = auth_handler.create_token(
                username="guest",
                role="guest",
                metadata={"auth_mode": "disabled"},
                tenant_id=auth_handler.default_tenant_id or None,
            )
            return {
                "auth_configured": False,
                "access_token": guest_token,
                "token_type": "bearer",
                "auth_mode": "disabled",
                "message": "Authentication is disabled. Using guest access.",
                "core_version": API_VERSION,
                "api_version": API_VERSION,
                "webui_title": webui_title,
                "webui_description": webui_description,
            }

        return {
            "auth_configured": True,
            "auth_mode": "enabled",
            "core_version": API_VERSION,
            "api_version": API_VERSION,
            "webui_title": webui_title,
            "webui_description": webui_description,
        }

    @app.post("/login")
    async def login(form_data: OAuth2PasswordRequestForm = Depends()):
        if not auth_handler.accounts:
            guest_token = auth_handler.create_token(
                username="guest",
                role="guest",
                metadata={"auth_mode": "disabled"},
                tenant_id=auth_handler.default_tenant_id or None,
            )
            return {
                "access_token": guest_token,
                "token_type": "bearer",
                "auth_mode": "disabled",
                "message": "Authentication is disabled. Using guest access.",
                "core_version": API_VERSION,
                "api_version": API_VERSION,
                "webui_title": webui_title,
                "webui_description": webui_description,
            }
        username = form_data.username
        if auth_handler.accounts.get(username) != form_data.password:
            raise HTTPException(status_code=401, detail="Incorrect credentials")

        login_tenant_id = auth_handler.resolve_login_tenant_id(username)
        user_token = auth_handler.create_token(
            username=username,
            role="user",
            metadata={"auth_mode": "enabled"},
            tenant_id=login_tenant_id,
        )
        return {
            "access_token": user_token,
            "token_type": "bearer",
            "auth_mode": "enabled",
            "core_version": API_VERSION,
            "api_version": API_VERSION,
            "webui_title": webui_title,
            "webui_description": webui_description,
        }

    @app.get("/healthz")
    async def health_check():
        return {"status": "healthy", "service": "GraphSuite API"}

    @app.get("/health", dependencies=[Depends(combined_auth)])
    async def get_status():
        try:
            auth_mode = "disabled" if not auth_configured else "enabled"
            ontology_ops_validation = run_ontology_ops_checks()
            return {
                "status": "healthy",
                "service": "graphsuite-api",
                "startup_validation": app.state.startup_validation,
                "ontology_ops_validation": ontology_ops_validation,
                "production_gate": run_production_gate_checks(ontology_ops_validation),
                "auth_mode": auth_mode,
                "core_version": API_VERSION,
                "api_version": API_VERSION,
                "webui_title": webui_title,
                "webui_description": webui_description,
            }
        except Exception as e:
            logger.error("Error getting health status: %s", str(e))
            raise HTTPException(status_code=500, detail=str(e)) from e

    return app


def get_application(args=None):
    if args is None:
        args = global_args
    return create_app(args)


def configure_logging() -> None:
    for logger_name in ("uvicorn", "uvicorn.access", "uvicorn.error", "graphsuite"):
        lg = logging.getLogger(logger_name)
        lg.handlers = []
        lg.filters = []

    log_dir = os.getenv("LOG_DIR", os.getcwd())
    log_file_path = os.path.abspath(os.path.join(log_dir, DEFAULT_LOG_FILENAME))

    print(f"\nGraphSuite log file: {log_file_path}\n")
    os.makedirs(log_dir, exist_ok=True)

    from config import get_env_value

    log_max_bytes = get_env_value("LOG_MAX_BYTES", DEFAULT_LOG_MAX_BYTES, int)
    log_backup_count = get_env_value("LOG_BACKUP_COUNT", DEFAULT_LOG_BACKUP_COUNT, int)

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {"format": "%(levelname)s: %(message)s"},
                "detailed": {
                    "format": (
                        "%(asctime)s - %(name)s - %(levelname)s "
                        "[%(filename)s:%(lineno)d] - %(message)s"
                    ),
                },
            },
            "handlers": {
                "console": {
                    "formatter": "detailed",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stderr",
                    "filters": ["redaction_filter"],
                },
                "file": {
                    "formatter": "detailed",
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": log_file_path,
                    "maxBytes": log_max_bytes,
                    "backupCount": log_backup_count,
                    "encoding": "utf-8",
                    "filters": ["redaction_filter"],
                },
            },
            "loggers": {
                "uvicorn": {
                    "handlers": ["console", "file"],
                    "level": "INFO",
                    "propagate": False,
                },
                "uvicorn.access": {
                    "handlers": ["console", "file"],
                    "level": "INFO",
                    "propagate": False,
                },
                "uvicorn.error": {
                    "handlers": ["console", "file"],
                    "level": "INFO",
                    "propagate": False,
                },
                "graphsuite": {
                    "handlers": ["console", "file"],
                    "level": "INFO",
                    "propagate": False,
                },
            },
            "filters": {
                "redaction_filter": {
                    "()": RedactingLogFilter,
                },
            },
        }
    )


def check_and_install_dependencies() -> None:
    """Ensure critical runtime packages are importable."""
    for module_name, pip_name in (
        ("uvicorn", "uvicorn"),
        ("fastapi", "fastapi"),
    ):
        try:
            __import__(module_name)
        except ImportError as exc:
            raise RuntimeError(
                f"Missing dependency '{pip_name}'. Install with: pip install {pip_name}"
            ) from exc


def main():
    if "GUNICORN_CMD_ARGS" in os.environ:
        print("Running under Gunicorn - worker management handled by Gunicorn")
        return

    if not check_env_file():
        sys.exit(1)

    check_and_install_dependencies()

    from multiprocessing import freeze_support

    freeze_support()

    configure_logging()
    update_uvicorn_mode_config()
    display_splash_screen(global_args)

    setup_signal_handlers()

    app = create_app(global_args)

    uvicorn_config = {
        "app": app,
        "host": global_args.host,
        "port": global_args.port,
        "log_config": None,
    }

    if global_args.ssl:
        uvicorn_config.update(
            {
                "ssl_certfile": global_args.ssl_certfile,
                "ssl_keyfile": global_args.ssl_keyfile,
            }
        )

    print(
        f"Starting Uvicorn server in single-process mode on "
        f"{global_args.host}:{global_args.port}"
    )
    uvicorn.run(**uvicorn_config)


if __name__ == "__main__":
    main()
