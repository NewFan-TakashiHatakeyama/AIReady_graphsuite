"""FT: tenant-alpha 固定データで A/B/C/D 期待値比較を行う実AWSテスト。

analyzeExposure を DynamoDB Streams 形式イベントで直接 invoke し、
各シナリオに対して exposure/reason/evidence が設計通りに保存されることを確認する。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from tests.aws.conftest import ANALYZE_EXPOSURE_FN, CONNECT_TABLE_NAME, invoke_lambda, wait_for_finding

pytestmark = pytest.mark.aws

TENANT_ALPHA = "tenant-alpha"


def _generate_finding_id(tenant_id: str, source: str, item_id: str) -> str:
    return hashlib.sha256(f"{tenant_id}:{source}:{item_id}".encode()).hexdigest()[:32]


def _to_dynamodb_attr(value: Any) -> dict[str, Any]:
    if value is None:
        return {"NULL": True}
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, (int, float)):
        return {"N": str(value)}
    return {"S": str(value)}


def _build_stream_insert_event(metadata: dict[str, Any]) -> dict[str, Any]:
    image = {k: _to_dynamodb_attr(v) for k, v in metadata.items() if v is not None}
    return {
        "Records": [
            {
                "eventID": uuid.uuid4().hex,
                "eventName": "INSERT",
                "eventVersion": "1.1",
                "eventSource": "aws:dynamodb",
                "awsRegion": "ap-northeast-1",
                "dynamodb": {
                    "Keys": {
                        "drive_id": {"S": metadata["drive_id"]},
                        "item_id": {"S": metadata["item_id"]},
                    },
                    "NewImage": image,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                    "SequenceNumber": "1",
                    "SizeBytes": 512,
                    "ApproximateCreationDateTime": int(datetime.now(timezone.utc).timestamp()),
                },
                "eventSourceARN": (
                    f"arn:aws:dynamodb:ap-northeast-1:565699611973:"
                    f"table/{CONNECT_TABLE_NAME}/stream/2026-01-01T00:00:00.000"
                ),
            }
        ]
    }


def _base_metadata(*, item_id: str, item_name: str, sharing_scope: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "tenant_id": TENANT_ALPHA,
        "drive_id": f"drive-{TENANT_ALPHA}",
        "item_id": item_id,
        "source": "m365",
        "container_id": "site-tenant-alpha-fixed",
        "container_name": "tenant-alpha-fixed-site",
        "container_type": "site",
        "item_name": item_name,
        "web_url": f"https://example.local/{item_id}",
        "sharing_scope": sharing_scope,
        "permissions": json.dumps({"entries": []}, ensure_ascii=False),
        "permissions_count": 10,
        "sensitivity_label": json.dumps({"id": "lbl-fixed", "name": "Confidential"}, ensure_ascii=False),
        "mime_type": "text/plain",
        "size": 2048,
        "modified_at": now,
        "is_deleted": False,
        "raw_s3_key": f"{TENANT_ALPHA}/raw/{item_id}/fixed.txt",
    }


def _invoke_and_fetch_finding(lambda_client, finding_table, metadata: dict[str, Any]) -> dict[str, Any]:
    finding_id = _generate_finding_id(metadata["tenant_id"], metadata["source"], metadata["item_id"])
    result = invoke_lambda(lambda_client, ANALYZE_EXPOSURE_FN, _build_stream_insert_event(metadata))
    assert result["error"] is None, f"analyzeExposure failed: {result['body']}"
    finding = wait_for_finding(
        finding_table,
        metadata["tenant_id"],
        finding_id,
        max_wait=120,
        interval=5,
    )
    assert finding is not None, f"Finding not found for item_id={metadata['item_id']}"
    return finding


def _cleanup_finding(finding_table, *, tenant_id: str, source: str, item_id: str) -> None:
    finding_id = _generate_finding_id(tenant_id, source, item_id)
    finding_table.delete_item(Key={"tenant_id": tenant_id, "finding_id": finding_id})


class TestFTTenantAlphaABCDExpected:
    """tenant-alpha 固定データを使った A/B/C/D の期待値比較。"""

    def test_abcd_scenario_expected_values(self, lambda_client, finding_table):
        fixed_items = [
            {
                "scenario": "A",
                "metadata": {
                    **_base_metadata(
                        item_id="tenant-alpha-fixed-scenario-a",
                        item_name="tenant-alpha-fixed-scenario-a.txt",
                        sharing_scope="organization",
                    ),
                    "source_metadata": json.dumps(
                        {
                            "org_edit_links": [{"scope": "organization", "type": "edit"}],
                            "tenant_domains": ["newfan0908.onmicrosoft.com"],
                            "permission_targets": [],
                        },
                        ensure_ascii=False,
                    ),
                },
                "expected_vectors": {"org_link", "org_link_editable"},
                "expected_detection_reasons": {"scenario_a_org_overshare"},
                "expected_guard_reason_codes": {"g3_org_link_editable"},
                "expected_evidence_key": "org_edit_links",
            },
            {
                "scenario": "B",
                "metadata": {
                    **_base_metadata(
                        item_id="tenant-alpha-fixed-scenario-b",
                        item_name="tenant-alpha-fixed-scenario-b.txt",
                        sharing_scope="specific_users",
                    ),
                    "permissions": json.dumps(
                        {
                            "entries": [
                                {
                                    "grantedToV2": {
                                        "user": {
                                            "email": "stayhungry.stayfoolish.1990@gmail.com",
                                            "userType": "guest",
                                        }
                                    },
                                    "roles": ["write"],
                                }
                            ]
                        },
                        ensure_ascii=False,
                    ),
                    "source_metadata": json.dumps(
                        {
                            "external_recipients": ["stayhungry.stayfoolish.1990@gmail.com"],
                            "tenant_domains": ["newfan0908.onmicrosoft.com"],
                            "permission_targets": [],
                        },
                        ensure_ascii=False,
                    ),
                },
                "expected_vectors": {
                    "guest_direct_share",
                    "external_email_direct_share",
                    "external_domain",
                },
                "expected_detection_reasons": {"scenario_b_external_direct_share"},
                "expected_guard_reason_codes": {"g3_external_direct_share"},
                "expected_evidence_key": "external_recipients",
            },
            {
                "scenario": "C",
                "metadata": {
                    **_base_metadata(
                        item_id="tenant-alpha-fixed-scenario-c",
                        item_name="tenant-alpha-fixed-scenario-c.txt",
                        sharing_scope="anonymous",
                    ),
                    "source_metadata": json.dumps(
                        {
                            "anonymous_links": [{"scope": "anonymous", "type": "view"}],
                            "tenant_domains": ["newfan0908.onmicrosoft.com"],
                            "permission_targets": [],
                        },
                        ensure_ascii=False,
                    ),
                },
                "expected_vectors": {"public_link"},
                "expected_detection_reasons": {"scenario_c_public_link"},
                "expected_guard_reason_codes": {"g3_public_link"},
                "expected_evidence_key": "anonymous_links",
            },
            {
                "scenario": "D",
                "metadata": {
                    **_base_metadata(
                        item_id="tenant-alpha-fixed-scenario-d",
                        item_name="tenant-alpha-fixed-scenario-d.txt",
                        sharing_scope="specific_users",
                    ),
                    "source_metadata": json.dumps(
                        {
                            "effective_permissions_hash": "hash-new-fixed",
                            "baseline_permissions_hash": "hash-old-fixed",
                            "permission_delta": [
                                {
                                    "principal": "guest-user@external.example",
                                    "change": "added",
                                    "role": "read",
                                },
                                {
                                    "principal": "owner-user@newfan0908.onmicrosoft.com",
                                    "change": "escalation",
                                    "from": "read",
                                    "to": "write",
                                },
                            ],
                            "tenant_domains": ["newfan0908.onmicrosoft.com"],
                            "permission_targets": [],
                        },
                        ensure_ascii=False,
                    ),
                },
                "expected_vectors": {
                    "acl_drift_detected",
                    "acl_drift_added_principal",
                    "acl_drift_privilege_escalation",
                },
                "expected_detection_reasons": {"scenario_d_acl_drift"},
                "expected_guard_reason_codes": {"g7_acl_drift"},
                "expected_evidence_key": "acl_drift_diff",
            },
        ]

        created: list[tuple[str, str, str]] = []
        try:
            for case in fixed_items:
                metadata = case["metadata"]
                finding = _invoke_and_fetch_finding(lambda_client, finding_table, metadata)
                created.append((metadata["tenant_id"], metadata["source"], metadata["item_id"]))

                vectors = set(finding.get("exposure_vectors") or [])
                detection_reasons = set(finding.get("detection_reasons") or [])
                guard_reason_codes = set(finding.get("guard_reason_codes") or [])
                evidence = finding.get("finding_evidence") or {}
                evidence_list = evidence.get(case["expected_evidence_key"]) if isinstance(evidence, dict) else None

                aliases = {
                    "org_link": {"org_link", "org_link_view", "org_link_edit"},
                    "org_link_editable": {"org_link_edit", "org_link_editable"},
                    "external_domain_share": {"external_domain_share", "external_domain"},
                    "external_domain": {"external_domain", "external_domain_share", "specific_people_external"},
                }
                if vectors:
                    for expected in case["expected_vectors"]:
                        candidates = {expected, *aliases.get(expected, set())}
                        assert any(candidate in vectors for candidate in candidates), (
                            f"Scenario {case['scenario']} vectors mismatch for '{expected}'. "
                            f"actual={sorted(vectors)}"
                        )
                if case["expected_detection_reasons"] and detection_reasons:
                    assert case["expected_detection_reasons"].issubset(detection_reasons), (
                        f"Scenario {case['scenario']} detection_reasons mismatch. "
                        f"expected_subset={sorted(case['expected_detection_reasons'])}, "
                        f"actual={sorted(detection_reasons)}"
                    )
                if case["expected_guard_reason_codes"] and guard_reason_codes:
                    assert case["expected_guard_reason_codes"].issubset(guard_reason_codes), (
                        f"Scenario {case['scenario']} guard_reason_codes mismatch. "
                        f"expected_subset={sorted(case['expected_guard_reason_codes'])}, "
                        f"actual={sorted(guard_reason_codes)}"
                    )
                assert isinstance(evidence_list, list) and len(evidence_list) > 0, (
                    f"Scenario {case['scenario']} evidence[{case['expected_evidence_key']}] is empty"
                )
        finally:
            for tenant_id, source, item_id in created:
                _cleanup_finding(
                    finding_table,
                    tenant_id=tenant_id,
                    source=source,
                    item_id=item_id,
                )
