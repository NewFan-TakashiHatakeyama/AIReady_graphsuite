"""shared/metrics.py の単体テスト"""

from unittest.mock import MagicMock, patch

import pytest

import shared.metrics as metrics_module
from shared.metrics import emit_count, emit_duration, emit_metric


@pytest.fixture(autouse=True)
def reset_cw_client():
    metrics_module._cw_client = None
    yield
    metrics_module._cw_client = None


class TestEmitMetric:
    def test_with_dimensions(self):
        mock_client = MagicMock()
        metrics_module._cw_client = mock_client

        emit_metric("Test.Metric", 5.0, dimensions={"Lambda": "test"})

        mock_client.put_metric_data.assert_called_once()
        call_args = mock_client.put_metric_data.call_args
        metric_data = call_args.kwargs["MetricData"][0]
        assert metric_data["MetricName"] == "Test.Metric"
        assert metric_data["Value"] == 5.0
        assert metric_data["Dimensions"] == [{"Name": "Lambda", "Value": "test"}]

    def test_without_dimensions(self):
        mock_client = MagicMock()
        metrics_module._cw_client = mock_client

        emit_metric("Test.Metric", 1.0)

        call_args = mock_client.put_metric_data.call_args
        metric_data = call_args.kwargs["MetricData"][0]
        assert metric_data["Dimensions"] == []

    def test_exception_swallowed(self):
        mock_client = MagicMock()
        mock_client.put_metric_data.side_effect = Exception("CW error")
        metrics_module._cw_client = mock_client

        emit_metric("Test.Metric", 1.0)


class TestEmitCount:
    def test_default_count(self):
        mock_client = MagicMock()
        metrics_module._cw_client = mock_client

        emit_count("Test.Count")

        call_args = mock_client.put_metric_data.call_args
        metric_data = call_args.kwargs["MetricData"][0]
        assert metric_data["Value"] == 1.0
        assert metric_data["Unit"] == "Count"


class TestEmitDuration:
    def test_emit_milliseconds(self):
        mock_client = MagicMock()
        metrics_module._cw_client = mock_client

        emit_duration("Test.Duration", 150.5, dimensions={"Lambda": "batch"})

        call_args = mock_client.put_metric_data.call_args
        metric_data = call_args.kwargs["MetricData"][0]
        assert metric_data["Value"] == 150.5
        assert metric_data["Unit"] == "Milliseconds"
