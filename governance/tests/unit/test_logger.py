"""shared/logger.py の単体テスト"""

import json
import logging

import pytest

from shared.logger import JsonFormatter, get_logger


class TestJsonFormatter:
    def test_basic_format(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello world", args=(), exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "hello world"
        assert "timestamp" in parsed

    def test_format_with_exception(self):
        formatter = JsonFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="", lineno=0,
            msg="failed", args=(), exc_info=exc_info,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]

    def test_format_with_extra_data(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="with data", args=(), exc_info=None,
        )
        record.extra_data = {"key": "value"}
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["data"] == {"key": "value"}


class TestGetLogger:
    def test_returns_logger(self):
        logger = get_logger("test.module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test.module"

    def test_has_json_handler(self):
        logger = get_logger("test.handler_check")
        assert any(
            isinstance(h.formatter, JsonFormatter) for h in logger.handlers
        )
