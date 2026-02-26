"""shared/config.py の単体テスト — SSM キャッシュ・get_env・get_ssm_*"""

import time

import boto3
import pytest
from moto import mock_aws

from shared.config import (
    _ssm_cache,
    clear_ssm_cache,
    get_env,
    get_ssm_float,
    get_ssm_int,
    get_ssm_parameter,
)
import shared.config as config_module


@pytest.fixture(autouse=True)
def reset_ssm(monkeypatch):
    """テストごとに SSM クライアントとキャッシュをリセット"""
    monkeypatch.setattr(config_module, "_ssm_client", None)
    clear_ssm_cache()
    yield
    clear_ssm_cache()


# ─── get_env ───


class TestGetEnv:
    def test_existing_var(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "hello")
        assert get_env("MY_VAR") == "hello"

    def test_missing_var_with_default(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
        assert get_env("NONEXISTENT_VAR", "fallback") == "fallback"

    def test_missing_var_no_default_raises(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
        with pytest.raises(KeyError, match="NONEXISTENT_VAR"):
            get_env("NONEXISTENT_VAR")


# ─── get_ssm_parameter ───


class TestGetSSMParameter:
    @mock_aws
    def test_fresh_fetch(self):
        """SSM から初回取得 → キャッシュに格納"""
        ssm = boto3.client("ssm", region_name="ap-northeast-1")
        ssm.put_parameter(Name="/test/param", Value="42", Type="String")

        config_module._ssm_client = ssm
        result = get_ssm_parameter("/test/param")
        assert result == "42"
        assert "/test/param" in _ssm_cache

    @mock_aws
    def test_cache_hit(self):
        """キャッシュが有効な間は SSM を再呼出しない"""
        ssm = boto3.client("ssm", region_name="ap-northeast-1")
        ssm.put_parameter(Name="/test/cached", Value="first", Type="String")
        config_module._ssm_client = ssm

        get_ssm_parameter("/test/cached")

        ssm.put_parameter(Name="/test/cached", Value="second", Type="String", Overwrite=True)
        result = get_ssm_parameter("/test/cached")
        assert result == "first"

    @mock_aws
    def test_parameter_not_found_with_default(self):
        """ParameterNotFound でデフォルト値を返す"""
        ssm = boto3.client("ssm", region_name="ap-northeast-1")
        config_module._ssm_client = ssm

        result = get_ssm_parameter("/nonexistent", default="fallback")
        assert result == "fallback"

    @mock_aws
    def test_parameter_not_found_no_default_raises(self):
        """ParameterNotFound でデフォルトなし → 例外送出"""
        ssm = boto3.client("ssm", region_name="ap-northeast-1")
        config_module._ssm_client = ssm

        with pytest.raises(Exception):
            get_ssm_parameter("/nonexistent")


# ─── get_ssm_float / get_ssm_int ───


class TestSSMTypedGetters:
    @mock_aws
    def test_get_ssm_float(self):
        ssm = boto3.client("ssm", region_name="ap-northeast-1")
        ssm.put_parameter(Name="/test/float", Value="3.14", Type="String")
        config_module._ssm_client = ssm

        assert get_ssm_float("/test/float") == 3.14

    @mock_aws
    def test_get_ssm_float_default(self):
        ssm = boto3.client("ssm", region_name="ap-northeast-1")
        config_module._ssm_client = ssm

        assert get_ssm_float("/nonexistent", default=9.9) == 9.9

    @mock_aws
    def test_get_ssm_int(self):
        ssm = boto3.client("ssm", region_name="ap-northeast-1")
        ssm.put_parameter(Name="/test/int", Value="50", Type="String")
        config_module._ssm_client = ssm

        assert get_ssm_int("/test/int") == 50

    @mock_aws
    def test_get_ssm_int_default(self):
        ssm = boto3.client("ssm", region_name="ap-northeast-1")
        config_module._ssm_client = ssm

        assert get_ssm_int("/nonexistent", default=100) == 100


# ─── clear_ssm_cache ───


class TestClearCache:
    def test_clear(self):
        _ssm_cache["foo"] = ("bar", time.time())
        assert len(_ssm_cache) > 0
        clear_ssm_cache()
        assert len(_ssm_cache) == 0
