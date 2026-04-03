"""Shared AWS client/session factory for repository infrastructure."""

from __future__ import annotations

import boto3

from services.runtime_config import AwsRuntimeConfig

_session = None


def get_aws_session(config: AwsRuntimeConfig):
    global _session
    if _session is None:
        _session = boto3.session.Session(region_name=config.aws_region)
    return _session


def get_dynamodb_resource(config: AwsRuntimeConfig):
    return get_aws_session(config).resource("dynamodb")


def get_dynamodb_client(config: AwsRuntimeConfig):
    return get_aws_session(config).client("dynamodb")


def get_s3_client(config: AwsRuntimeConfig):
    return get_aws_session(config).client("s3")


def get_lambda_client(config: AwsRuntimeConfig):
    return get_aws_session(config).client("lambda")


def get_logs_client(config: AwsRuntimeConfig):
    return get_aws_session(config).client("logs")


def get_ssm_client(config: AwsRuntimeConfig):
    return get_aws_session(config).client("ssm")


def get_secretsmanager_client(config: AwsRuntimeConfig):
    return get_aws_session(config).client("secretsmanager")


def get_sns_client(config: AwsRuntimeConfig):
    return get_aws_session(config).client("sns")


def get_stepfunctions_client(config: AwsRuntimeConfig):
    return get_aws_session(config).client("stepfunctions")


def get_rds_client(config: AwsRuntimeConfig):
    return get_aws_session(config).client("rds")


def check_aws_connectivity(config: AwsRuntimeConfig) -> dict[str, str]:
    """
    Validate basic AWS connectivity for fail-fast startup.
    Raises an exception when any required client check fails.
    """
    dynamodb_client = get_dynamodb_client(config)
    s3_client = get_s3_client(config)
    lambda_client = get_lambda_client(config)
    logs_client = get_logs_client(config)

    # Intentionally minimal calls: verify credentials/region/service reachability.
    dynamodb_client.list_tables(Limit=1)
    s3_client.list_buckets()
    lambda_client.list_functions(MaxItems=1)
    logs_client.describe_log_groups(limit=1)

    return {"dynamodb": "ok", "s3": "ok", "lambda": "ok", "logs": "ok"}
