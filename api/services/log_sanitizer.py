"""Log sanitization helpers to prevent sensitive data leakage."""

from __future__ import annotations

import logging
import re


_URL_CREDENTIALS_RE = re.compile(r"(?i)([a-z][a-z0-9+\-.]*://[^:\s/@]+:)([^@\s]+)(@)")
_BEARER_RE = re.compile(r"(?i)\b(Bearer)\s+[A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]+\b")
_ACCESS_TOKEN_RE = re.compile(r'(?i)("access_token"\s*:\s*")([^"]+)(")')
_CLIENT_SECRET_RE = re.compile(r'(?i)("client_secret"\s*:\s*")([^"]+)(")')
_PASSWORD_RE = re.compile(r'(?i)("password"\s*:\s*")([^"]+)(")')
_AUTH_HEADER_RE = re.compile(r'(?i)("Authorization"\s*:\s*")([^"]+)(")')


def sanitize_log_message(message: str) -> str:
    """Redact sensitive values from a log message string."""
    text = str(message)
    text = _URL_CREDENTIALS_RE.sub(r"\1***\3", text)
    text = _BEARER_RE.sub(r"\1 ***", text)
    text = _ACCESS_TOKEN_RE.sub(r"\1***\3", text)
    text = _CLIENT_SECRET_RE.sub(r"\1***\3", text)
    text = _PASSWORD_RE.sub(r"\1***\3", text)
    text = _AUTH_HEADER_RE.sub(r"\1***\3", text)
    return text


class RedactingLogFilter(logging.Filter):
    """Logging filter that redacts sensitive content in-place."""

    def filter(self, record: logging.LogRecord) -> bool:
        rendered = sanitize_log_message(record.getMessage())
        record.msg = rendered
        record.args = ()
        return True

