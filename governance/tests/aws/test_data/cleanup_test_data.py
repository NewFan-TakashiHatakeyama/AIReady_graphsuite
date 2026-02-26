"""AWS テストデータ削除スクリプト

テスト完了後にテスト専用テナントの全データを削除する。
単独実行: python -m tests.aws.test_data.cleanup_test_data
"""

from __future__ import annotations

import sys

import boto3

AWS_REGION = "ap-northeast-1"
AWS_ACCOUNT_ID = "565699611973"

FINDING_TABLE_NAME = "AIReadyGov-ExposureFinding"
CONNECT_TABLE_NAME = "AIReadyConnect-FileMetadata"
RAW_PAYLOAD_BUCKET = f"aireadyconnect-raw-payload-{AWS_ACCOUNT_ID}"
REPORT_BUCKET = f"aireadygov-reports-{AWS_ACCOUNT_ID}"
SENSITIVITY_QUEUE_NAME = "AIReadyGov-SensitivityDetectionQueue"

TEST_TENANT_PREFIXES = ["test-tenant-dvt-"]


def _get_resources():
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    s3 = boto3.client("s3", region_name=AWS_REGION)
    sqs = boto3.client("sqs", region_name=AWS_REGION)
    return {
        "finding_table": dynamodb.Table(FINDING_TABLE_NAME),
        "connect_table": dynamodb.Table(CONNECT_TABLE_NAME),
        "s3": s3,
        "sqs": sqs,
    }


def cleanup_dynamodb_table(table, key_schema_names: list[str]):
    """テスト用テナントのレコードを全削除する。"""
    deleted = 0
    scan_kwargs = {}

    while True:
        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])

        with table.batch_writer() as batch:
            for item in items:
                tenant_id = item.get("tenant_id", "")
                if any(tenant_id.startswith(prefix) for prefix in TEST_TENANT_PREFIXES):
                    key = {k: item[k] for k in key_schema_names}
                    batch.delete_item(Key=key)
                    deleted += 1

        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    return deleted


def cleanup_s3_prefix(s3_client, bucket: str, prefix: str) -> int:
    """S3 プレフィックス配下のオブジェクトを全削除する。"""
    deleted = 0
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if objects:
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )
            deleted += len(objects)

    return deleted


def purge_sqs_queue(sqs_client, queue_name: str):
    """SQS キューをパージする。"""
    try:
        url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        sqs_client.purge_queue(QueueUrl=url)
        print(f"  SQS キュー '{queue_name}' パージ完了")
    except sqs_client.exceptions.QueueDoesNotExist:
        print(f"  SQS キュー '{queue_name}' が見つかりません（スキップ）")
    except Exception as e:
        print(f"  SQS パージエラー: {e}（直近のパージから 60 秒以内の可能性）")


def main():
    print("=== AWS テストデータ削除開始 ===")
    resources = _get_resources()

    print("\n--- DynamoDB: ExposureFinding テーブル ---")
    count = cleanup_dynamodb_table(
        resources["finding_table"], ["tenant_id", "finding_id"],
    )
    print(f"  → {count} 件削除")

    print("\n--- DynamoDB: FileMetadata テーブル ---")
    count = cleanup_dynamodb_table(
        resources["connect_table"], ["tenant_id", "item_id"],
    )
    print(f"  → {count} 件削除")

    print("\n--- S3: テストデータ削除 ---")
    for prefix in TEST_TENANT_PREFIXES:
        for bucket in [RAW_PAYLOAD_BUCKET, REPORT_BUCKET]:
            for sub in ["raw/", "reports/", ""]:
                full_prefix = f"{sub}{prefix}" if sub else prefix
                count = cleanup_s3_prefix(resources["s3"], bucket, full_prefix)
                if count > 0:
                    print(f"  s3://{bucket}/{full_prefix}* → {count} 件削除")

    print("\n--- SQS: キューパージ ---")
    purge_sqs_queue(resources["sqs"], SENSITIVITY_QUEUE_NAME)

    print("\n=== テストデータ削除完了 ===")


if __name__ == "__main__":
    main()
