"""E2E-3: マルチテナント分離テスト

テナント間のデータ分離・独立レポート・相互影響の無さを検証する。
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests.aws.conftest import (
    BATCH_SCORING_FN,
    RAW_PAYLOAD_BUCKET,
    REPORT_BUCKET,
    TEST_TENANT_ID,
    TEST_TENANT_ID_2,
    invoke_lambda,
    make_file_metadata,
    wait_for_finding_by_item,
)


class TestE2E3MultiTenant:
    """2 テナント間のデータ分離を検証する E2E テスト。"""

    @pytest.mark.slow
    def test_e2e_3_01_independent_findings(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """2 テナントに FileMetadata を挿入し batchScoring を実行すると、
        各テナントに独立した Finding が生成される。"""
        items_by_tenant: dict[str, list[str]] = {}

        for tenant_id in [TEST_TENANT_ID, TEST_TENANT_ID_2]:
            item_ids = []
            for i in range(3):
                item_id = f"item-e2e301-{tenant_id[-3:]}-{i}-{uuid.uuid4().hex[:6]}"
                raw_key = f"raw/{tenant_id}/{item_id}/payload.txt"
                s3_client.put_object(
                    Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
                    Body=f"multi-tenant test {tenant_id} {i}".encode("utf-8"),
                )
                metadata = make_file_metadata(
                    tenant_id=tenant_id, item_id=item_id,
                    item_name=f"mt_test_{i}.txt", mime_type="text/plain",
                    raw_s3_key=raw_key,
                )
                connect_table.put_item(Item=metadata)
                item_ids.append(item_id)
            items_by_tenant[tenant_id] = item_ids

        for tenant_id in [TEST_TENANT_ID, TEST_TENANT_ID_2]:
            result = invoke_lambda(
                lambda_client, BATCH_SCORING_FN, {"tenant_id": tenant_id}
            )
            assert result["error"] is None, f"batchScoring failed for {tenant_id}"

        for tenant_id, item_ids in items_by_tenant.items():
            resp = finding_table.query(
                KeyConditionExpression="tenant_id = :tid",
                ExpressionAttributeValues={":tid": tenant_id},
            )
            findings = resp.get("Items", [])
            found_items = {f["item_id"] for f in findings}
            for iid in item_ids:
                assert iid in found_items, (
                    f"Finding for {iid} missing in tenant {tenant_id}"
                )

    def test_e2e_3_02_separate_reports(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """batchScoring の結果、各テナントに個別の S3 レポートが生成される。"""
        for tenant_id in [TEST_TENANT_ID, TEST_TENANT_ID_2]:
            item_id = f"item-e2e302-{tenant_id[-3:]}-{uuid.uuid4().hex[:6]}"
            raw_key = f"raw/{tenant_id}/{item_id}/payload.txt"
            s3_client.put_object(
                Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
                Body=b"report separation test",
            )
            metadata = make_file_metadata(
                tenant_id=tenant_id, item_id=item_id,
                item_name="report_sep.txt", mime_type="text/plain",
                raw_s3_key=raw_key,
            )
            connect_table.put_item(Item=metadata)
            invoke_lambda(
                lambda_client, BATCH_SCORING_FN, {"tenant_id": tenant_id}
            )

        for tenant_id in [TEST_TENANT_ID, TEST_TENANT_ID_2]:
            report_objects = s3_client.list_objects_v2(
                Bucket=REPORT_BUCKET, Prefix=f"{tenant_id}/"
            )
            assert report_objects.get("KeyCount", 0) > 0, (
                f"No report for tenant {tenant_id}"
            )

    @pytest.mark.slow
    def test_e2e_3_03_deletion_no_cross_impact(
        self, connect_table, finding_table, lambda_client, s3_client
    ):
        """テナント A の FileMetadata を削除しても、テナント B の Finding に影響しない。"""
        tenant_a_items = []
        tenant_b_items = []

        for i in range(2):
            for tenant_id, items_list in [
                (TEST_TENANT_ID, tenant_a_items),
                (TEST_TENANT_ID_2, tenant_b_items),
            ]:
                item_id = f"item-e2e303-{tenant_id[-3:]}-{i}-{uuid.uuid4().hex[:6]}"
                raw_key = f"raw/{tenant_id}/{item_id}/payload.txt"
                s3_client.put_object(
                    Bucket=RAW_PAYLOAD_BUCKET, Key=raw_key,
                    Body=f"cross impact test {i}".encode("utf-8"),
                )
                metadata = make_file_metadata(
                    tenant_id=tenant_id, item_id=item_id,
                    item_name=f"cross_{i}.txt", mime_type="text/plain",
                    raw_s3_key=raw_key,
                )
                connect_table.put_item(Item=metadata)
                items_list.append(item_id)

        for tenant_id in [TEST_TENANT_ID, TEST_TENANT_ID_2]:
            invoke_lambda(
                lambda_client, BATCH_SCORING_FN, {"tenant_id": tenant_id}
            )

        for item_id in tenant_a_items:
            connect_table.delete_item(
                Key={"tenant_id": TEST_TENANT_ID, "item_id": item_id}
            )

        invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID}
        )
        invoke_lambda(
            lambda_client, BATCH_SCORING_FN, {"tenant_id": TEST_TENANT_ID_2}
        )

        resp_b = finding_table.query(
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": TEST_TENANT_ID_2},
        )
        b_findings = resp_b.get("Items", [])
        b_item_ids = {f["item_id"] for f in b_findings if f.get("status") == "open"}
        for iid in tenant_b_items:
            assert iid in b_item_ids, (
                f"Tenant B Finding for {iid} incorrectly affected by Tenant A deletion"
            )

    def test_e2e_3_04_tenant_isolation(self, finding_table):
        """テナント A のキーでクエリしてもテナント B の Finding は返らない。"""
        resp = finding_table.query(
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": TEST_TENANT_ID},
        )
        findings = resp.get("Items", [])
        for finding in findings:
            assert finding["tenant_id"] == TEST_TENANT_ID, (
                f"Found tenant_id={finding['tenant_id']} when querying for {TEST_TENANT_ID}"
            )
