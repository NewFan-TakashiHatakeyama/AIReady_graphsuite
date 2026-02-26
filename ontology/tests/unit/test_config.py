from __future__ import annotations

from src.shared import config


class _SsmClient:
    def __init__(self):
        self.calls = 0

    def get_parameter(self, **kwargs):
        self.calls += 1
        return {"Parameter": {"Value": '{"a":1}'}}


def test_get_tenant_parameter_path() -> None:
    assert (
        config.get_tenant_parameter_path("tenant-1", "freshness-thresholds")
        == "/ai-ready/ontology/tenant-1/freshness-thresholds"
    )


def test_ssm_cache_works() -> None:
    config.clear_ssm_cache()
    client = _SsmClient()
    value1 = config.get_ssm_parameter("p1", ssm_client=client, ttl_seconds=300)
    value2 = config.get_ssm_parameter("p1", ssm_client=client, ttl_seconds=300)
    assert value1 == value2
    assert client.calls == 1


def test_get_ssm_json_parameter() -> None:
    config.clear_ssm_cache()
    client = _SsmClient()
    value = config.get_ssm_json_parameter("p2", ssm_client=client)
    assert value["a"] == 1
