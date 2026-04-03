# Gunicorn configuration for GraphSuite API (no LightRAG).
import logging
import os

from config import get_env_value

DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5
DEFAULT_LOG_FILENAME = os.getenv("GRAPHSUITE_LOG_FILENAME", "graphsuite.log")

log_dir = os.getenv("LOG_DIR", os.getcwd())
log_file_path = os.path.abspath(os.path.join(log_dir, DEFAULT_LOG_FILENAME))

os.makedirs(log_dir, exist_ok=True)

log_max_bytes = get_env_value("LOG_MAX_BYTES", DEFAULT_LOG_MAX_BYTES, int)
log_backup_count = get_env_value("LOG_BACKUP_COUNT", DEFAULT_LOG_BACKUP_COUNT, int)

workers = None
bind = None
loglevel = None
certfile = None
keyfile = None

preload_app = True
worker_class = "uvicorn.workers.UvicornWorker"

errorlog = os.getenv("ERROR_LOG", log_file_path)
accesslog = os.getenv("ACCESS_LOG", log_file_path)

logconfig_dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "standard",
            "filename": log_file_path,
            "maxBytes": log_max_bytes,
            "backupCount": log_backup_count,
            "encoding": "utf8",
        },
    },
    "loggers": {
        "gunicorn": {
            "handlers": ["console", "file"],
            "level": loglevel.upper() if loglevel else "INFO",
            "propagate": False,
        },
        "gunicorn.error": {
            "handlers": ["console", "file"],
            "level": loglevel.upper() if loglevel else "INFO",
            "propagate": False,
        },
        "gunicorn.access": {
            "handlers": ["console", "file"],
            "level": loglevel.upper() if loglevel else "INFO",
            "propagate": False,
        },
        "graphsuite": {
            "handlers": ["console", "file"],
            "level": loglevel.upper() if loglevel else "INFO",
            "propagate": False,
        },
    },
}


def on_starting(_server):
    print("=" * 80)
    print(f"GUNICORN MASTER PROCESS: on_starting jobs for {workers} worker(s)")
    print(f"Process ID: {os.getpid()}")
    print("=" * 80)

    try:
        import psutil

        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        print(f"Memory usage after initialization: {memory_info.rss / 1024 / 1024:.2f} MB")
    except ImportError:
        print("psutil not installed, skipping memory usage reporting")

    print(f"GraphSuite log file: {log_file_path}\n")
    print("Gunicorn initialization complete, forking workers...\n")


def on_exit(_server):
    print("=" * 80)
    print("GUNICORN MASTER PROCESS: Shutting down")
    print(f"Process ID: {os.getpid()}")
    print("=" * 80)
    print("Gunicorn shutdown complete")
    print("=" * 80)


def post_fork(_server, _worker):
    log_level = (loglevel or "INFO").upper()
    for name in ("uvicorn", "uvicorn.access", "graphsuite"):
        lg = logging.getLogger(name)
        lg.setLevel(log_level)

    uvicorn_error_logger = logging.getLogger("uvicorn.error")
    uvicorn_error_logger.handlers = []
    uvicorn_error_logger.setLevel(logging.CRITICAL)
    uvicorn_error_logger.propagate = False
