"""E2E-3: マルチテナント分離テスト

テナント間のデータ分離・ストリーム経由 Finding 生成を検証する。
（batchScoring / 日次レポートは廃止）
"""

from __future__ import annotations

import uuid

import pytest

from tests.aws.conftest import (
    RAW_PAYLOAD_BUCKET,
    TEST_TENANT_ID,
    TEST_TENANT_ID_2,
    make_file_metadata,
    wait_for_finding_by_item,
)


class TestE2E3MultiTenant:
    """2 テナント間のデータ分離を検証する E2E テスト。"""

    @pytest.mark.slow
    def test_e2e_3_01_independent_findings(
        self, connect_table, finding_table, s3_client
    ):
        """2 テナントに FileMetadata を挿入すると、ストリーム経由で各テナントに Finding が生成される。"""
        items_by_tenant: dict[str, list[str]] = {}

        for tenant_id in [TEST_TENANT_ID, TEST_TENANT_ID_2]:
            item_ids = []
            for i in range(3):
                item_id = f"item-e2e301-{tenant_id[-3:]}-{i}-{uuid.uuid4().hex[:6]}"
                raw_key = f"{tenant_id}/raw/{item_id}/payload.txt"
                s3_client.put_object(
                    Bucket=RAW_PAYLOAD_BUCKET,
                    Key=raw_key,
                    Body=f"multi-tenant test {tenant_id} {i}".encode("utf-8"),
                )
                metadata = make_file_metadata(
                    tenant_id=tenant_id,
                    item_id=item_id,
                    item_name=f"mt_test_{i}.txt",
                    mime_type="text/plain",
                    raw_s3_key=raw_key,
                )
                connect_table.put_item(Item=metadata)
                item_ids.append(item_id)
            items_by_tenant[tenant_id] = item_ids

        for tenant_id, item_ids in items_by_tenant.items():
            for iid in item_ids:
                finding = wait_for_finding_by_item(
                    finding_table, tenant_id, iid, max_wait=180
                )
                assert finding is not None, f"Finding for {iid} missing in tenant {tenant_id}"

    @pytest.mark.slow
    def test_e2e_3_03_deletion_no_cross_impact(
        self, connect_table, finding_table, s3_client
    ):
        """テナント A の FileMetadata を削除しても、テナント B の Finding に影響しない。"""
        tenant_a_items = []
        tenant_b_items = []
        tenant_a_drive_map: dict[str, str] = {}

        for i in range(2):
            for tenant_id, items_list in [
                (TEST_TENANT_ID, tenant_a_items),
                (TEST_TENANT_ID_2, tenant_b_items),
            ]:
                item_id = f"item-e2e303-{tenant_id[-3:]}-{i}-{uuid.uuid4().hex[:6]}"
                raw_key = f"{tenant_id}/raw/{item_id}/payload.txt"
                s3_client.put_object(
                    Bucket=RAW_PAYLOAD_BUCKET,
                    Key=raw_key,
                    Body=f"cross impact test {i}".encode("utf-8"),
                )
                metadata = make_file_metadata(
                    tenant_id=tenant_id,
                    item_id=item_id,
                    item_name=f"cross_{i}.txt",
                    mime_type="text/plain",
                    raw_s3_key=raw_key,
                )
                connect_table.put_item(Item=metadata)
                items_list.append(item_id)
                if tenant_id == TEST_TENANT_ID:
                    tenant_a_drive_map[item_id] = metadata["drive_id"]

        for tenant_id, iids in [
            (TEST_TENANT_ID, tenant_a_items),
            (TEST_TENANT_ID_2, tenant_b_items),
        ]:
            for iid in iids:
                fnd = wait_for_finding_by_item(
                    finding_table, tenant_id, iid, max_wait=180
                )
                assert fnd is not None

        for item_id in tenant_a_items:
            connect_table.delete_item(
                Key={"drive_id": tenant_a_drive_map[item_id], "item_id": item_id}
            )

        for item_id in tenant_a_items:
            closed = wait_for_finding_by_item(
                finding_table,
                TEST_TENANT_ID,
                item_id,
                expected_status="closed",
                max_wait=180,
            )
            assert closed is not None

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
