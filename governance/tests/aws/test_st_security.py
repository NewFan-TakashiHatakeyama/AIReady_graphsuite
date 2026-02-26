"""ST-1: セキュリティテスト

IAM ロール・暗号化・PII 漏洩防止・パブリックアクセス制御を検証する。
"""

from __future__ import annotations

import json
import time
import uuid

import pytest

from tests.aws.conftest import (
    ANALYZE_EXPOSURE_FN,
    AWS_REGION,
    BATCH_SCORING_FN,
    DOCUMENT_ANALYSIS_TABLE_NAME,
    FINDING_TABLE_NAME,
    RAW_PAYLOAD_BUCKET,
    REPORT_BUCKET,
    SENSITIVITY_QUEUE_NAME,
    TEST_TENANT_ID,
    VECTORS_BUCKET,
    invoke_lambda,
    make_file_metadata,
    wait_for_finding_by_item,
)


class TestST1Security:
    """AWS リソースのセキュリティ設定と PII 漏洩防止を検証するテスト。"""

    def test_st_1_01_lambda_role_exists(self, iam_client):
        """AIReadyGov-LambdaRole が存在する。"""
        paginator = iam_client.get_paginator("list_roles")
        role_found = False
        for page in paginator.paginate(PathPrefix="/"):
            for role in page["Roles"]:
                if "AIReadyGov" in role["RoleName"] and "Lambda" in role["RoleName"]:
                    role_found = True
                    break
            if role_found:
                break
        assert role_found, "AIReadyGov Lambda IAM role not found"

    def test_st_1_02_no_wildcard_actions(self, iam_client):
        """Lambda ロールのポリシーに Action='*' が含まれない。"""
        paginator = iam_client.get_paginator("list_roles")
        role_name = None
        for page in paginator.paginate(PathPrefix="/"):
            for role in page["Roles"]:
                if "AIReadyGov" in role["RoleName"] and "Lambda" in role["RoleName"]:
                    role_name = role["RoleName"]
                    break
            if role_name:
                break
        assert role_name is not None, "Lambda role not found"

        inline_policies = iam_client.list_role_policies(RoleName=role_name)
        for policy_name in inline_policies.get("PolicyNames", []):
            policy_doc = iam_client.get_role_policy(
                RoleName=role_name, PolicyName=policy_name
            )["PolicyDocument"]
            for statement in policy_doc.get("Statement", []):
                actions = statement.get("Action", [])
                if isinstance(actions, str):
                    actions = [actions]
                assert "*" not in actions, (
                    f"Wildcard Action found in {policy_name}: {statement}"
                )

        attached = iam_client.list_attached_role_policies(RoleName=role_name)
        for policy in attached.get("AttachedPolicies", []):
            arn = policy["PolicyArn"]
            version = iam_client.get_policy(PolicyArn=arn)["Policy"]["DefaultVersionId"]
            doc = iam_client.get_policy_version(
                PolicyArn=arn, VersionId=version
            )["PolicyVersion"]["Document"]
            for statement in doc.get("Statement", []):
                actions = statement.get("Action", [])
                if isinstance(actions, str):
                    actions = [actions]
                assert "*" not in actions, (
                    f"Wildcard Action found in attached policy {arn}: {statement}"
                )

    def test_st_1_03_no_cross_account_access(self, iam_client):
        """Lambda ロールの trust policy が lambda.amazonaws.com のみ許可。"""
        paginator = iam_client.get_paginator("list_roles")
        role_name = None
        for page in paginator.paginate(PathPrefix="/"):
            for role in page["Roles"]:
                if "AIReadyGov" in role["RoleName"] and "Lambda" in role["RoleName"]:
                    role_name = role["RoleName"]
                    break
            if role_name:
                break
        assert role_name is not None

        role = iam_client.get_role(RoleName=role_name)["Role"]
        trust_policy = role["AssumeRolePolicyDocument"]
        for statement in trust_policy.get("Statement", []):
            principal = statement.get("Principal", {})
            service = principal.get("Service", "")
            if isinstance(service, list):
                for svc in service:
                    assert svc == "lambda.amazonaws.com", (
                        f"Unexpected service principal: {svc}"
                    )
            elif service:
                assert service == "lambda.amazonaws.com", (
                    f"Unexpected service principal: {service}"
                )

    def test_st_1_04_s3_encryption(self, s3_client):
        """レポートバケットに AES256 暗号化が設定されている。"""
        encryption = s3_client.get_bucket_encryption(Bucket=REPORT_BUCKET)
        rules = encryption["ServerSideEncryptionConfiguration"]["Rules"]
        assert len(rules) > 0
        algo = rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
        assert algo in ("AES256", "aws:kms"), f"Unexpected encryption: {algo}"

    def test_st_1_05_dynamodb_encryption(self, dynamodb_client):
        """Finding テーブルに暗号化が有効。"""
        desc = dynamodb_client.describe_table(TableName=FINDING_TABLE_NAME)
        sse = desc["Table"].get("SSEDescription", {})
        status = sse.get("Status", "ENABLED")
        assert status in ("ENABLED", "ENABLING"), (
            f"DynamoDB encryption status: {status}"
        )

    @pytest.mark.slow
    def test_st_1_06_pii_not_in_logs(
        self, connect_table, finding_table, s3_client, logs_client
    ):
        """PII 検出後、CloudWatch ログに生の PII 値 "1234 5678 9012" が含まれない。"""
        item_id = f"item-st106-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        pii_value = "1234 5678 9012"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body=f"個人番号 {pii_value}".encode("utf-8"),
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="pii_log_test.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=120, interval=10
        )

        time.sleep(30)

        log_group = f"/aws/lambda/{ANALYZE_EXPOSURE_FN}"
        end_time = int(time.time() * 1000)
        start_time = end_time - (30 * 60 * 1000)

        try:
            resp = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=start_time,
                endTime=end_time,
                filterPattern=f'"{pii_value}"',
                limit=5,
            )
            assert len(resp.get("events", [])) == 0, (
                "Raw PII value found in CloudWatch logs"
            )
        except logs_client.exceptions.ResourceNotFoundException:
            pytest.skip(f"Log group {log_group} not found")

    @pytest.mark.slow
    def test_st_1_07_pii_not_in_finding(
        self, connect_table, finding_table, s3_client
    ):
        """PII 検出後の Finding にはタイプ名のみ記録され、生の値は含まれない。"""
        item_id = f"item-st107-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        pii_value = "1234 5678 9012"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body=f"個人番号 {pii_value}".encode("utf-8"),
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="pii_finding_test.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        finding = wait_for_finding_by_item(
            finding_table, TEST_TENANT_ID, item_id, max_wait=300, interval=10
        )
        assert finding is not None

        finding_json = json.dumps(finding, default=str)
        assert pii_value not in finding_json, (
            "Raw PII value found in Finding record"
        )
        if finding.get("pii_types"):
            pii_types = finding["pii_types"]
            if isinstance(pii_types, str):
                pii_types = json.loads(pii_types)
            for entry in pii_types if isinstance(pii_types, list) else [pii_types]:
                entry_str = json.dumps(entry, default=str)
                assert pii_value not in entry_str

    def test_st_1_08_pii_not_in_report(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """batchScoring レポートに生の PII 値が含まれない。"""
        item_id = f"item-st108-{uuid.uuid4().hex[:8]}"
        raw_key = f"raw/{TEST_TENANT_ID}/{item_id}/payload.txt"
        pii_value = "9876 5432 1098"
        s3_client.put_object(
            Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
            Body=f"個人番号 {pii_value}".encode("utf-8"),
        )
        metadata = make_file_metadata(
            tenant_id=TEST_TENANT_ID, item_id=item_id,
            item_name="pii_report_test.txt", mime_type="text/plain",
            raw_s3_key=raw_key,
        )
        connect_table.put_item(Item=metadata)

        result = invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        assert result["error"] is None

        report_objects = s3_client.list_objects_v2(
            Bucket=REPORT_BUCKET, Prefix=f"{TEST_TENANT_ID}/"
        )
        for obj in report_objects.get("Contents", []):
            body = s3_client.get_object(
                Bucket=REPORT_BUCKET, Key=obj["Key"]
            )["Body"].read().decode("utf-8")
            assert pii_value not in body, (
                f"Raw PII value found in report {obj['Key']}"
            )

    def test_st_1_09_s3_public_access_blocked(self, s3_client):
        """レポートバケットのパブリックアクセスが全てブロックされている。"""
        resp = s3_client.get_public_access_block(Bucket=REPORT_BUCKET)
        config = resp["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is True
        assert config["IgnorePublicAcls"] is True
        assert config["BlockPublicPolicy"] is True
        assert config["RestrictPublicBuckets"] is True

    def test_st_1_10_sqs_no_public_policy(self, sqs_client):
        """Sensitivity Detection キューに '*' プリンシパルのポリシーが無い。"""
        queue_url = sqs_client.get_queue_url(QueueName=SENSITIVITY_QUEUE_NAME)["QueueUrl"]
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["Policy"]
        ).get("Attributes", {})

        policy_str = attrs.get("Policy")
        if policy_str:
            policy = json.loads(policy_str)
            for statement in policy.get("Statement", []):
                principal = statement.get("Principal", {})
                if isinstance(principal, str):
                    assert principal != "*", "SQS policy allows wildcard principal"
                elif isinstance(principal, dict):
                    aws_principal = principal.get("AWS", "")
                    if isinstance(aws_principal, list):
                        assert "*" not in aws_principal
                    else:
                        assert aws_principal != "*"

    def test_st_1_11_document_analysis_table_encrypted(self, dynamodb_client):
        """DocumentAnalysis テーブルのサーバーサイド暗号化が有効。"""
        desc = dynamodb_client.describe_table(TableName=DOCUMENT_ANALYSIS_TABLE_NAME)
        sse = desc["Table"].get("SSEDescription", {})
        status = sse.get("Status", "ENABLED")
        assert status in ("ENABLED", "ENABLING")

    def test_st_1_12_vectors_bucket_public_access_blocked(self, s3_client):
        """Vectors バケットのパブリックアクセスが全てブロックされている。"""
        resp = s3_client.get_public_access_block(Bucket=VECTORS_BUCKET)
        config = resp["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is True
        assert config["IgnorePublicAcls"] is True
        assert config["BlockPublicPolicy"] is True
        assert config["RestrictPublicBuckets"] is True

    def test_st_1_13_detect_sensitivity_bedrock_scope(self, lambda_client, iam_client):
        """detectSensitivity 実行ロールが Bedrock invoke 権限を持つ。"""
        fn = lambda_client.get_function(FunctionName="AIReadyGov-detectSensitivity")
        role_arn = fn["Configuration"]["Role"]
        role_name = role_arn.split("/")[-1]

        has_bedrock_invoke = False

        inline_policies = iam_client.list_role_policies(RoleName=role_name)
        for policy_name in inline_policies.get("PolicyNames", []):
            doc = iam_client.get_role_policy(
                RoleName=role_name, PolicyName=policy_name
            )["PolicyDocument"]
            for statement in doc.get("Statement", []):
                actions = statement.get("Action", [])
                if isinstance(actions, str):
                    actions = [actions]
                if any(a in ("bedrock:InvokeModel", "bedrock:*") for a in actions):
                    has_bedrock_invoke = True
                    break
            if has_bedrock_invoke:
                break

        if not has_bedrock_invoke:
            attached = iam_client.list_attached_role_policies(RoleName=role_name)
            for policy in attached.get("AttachedPolicies", []):
                arn = policy["PolicyArn"]
                version = iam_client.get_policy(PolicyArn=arn)["Policy"]["DefaultVersionId"]
                doc = iam_client.get_policy_version(
                    PolicyArn=arn, VersionId=version
                )["PolicyVersion"]["Document"]
                for statement in doc.get("Statement", []):
                    actions = statement.get("Action", [])
                    if isinstance(actions, str):
                        actions = [actions]
                    if any(a in ("bedrock:InvokeModel", "bedrock:*") for a in actions):
                        has_bedrock_invoke = True
                        break
                if has_bedrock_invoke:
                    break

        assert has_bedrock_invoke, "detectSensitivity role missing Bedrock invoke permission"
