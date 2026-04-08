#!/usr/bin/env python3
"""Compare UnifiedMetadata item_ids with ExposureFinding item_ids for one tenant (ops / triage).

Uses DynamoDB Query on tenant_id for both tables. Requires AWS credentials and boto3.

Environment (optional):
  GOVERNANCE_FINDING_TABLE_NAME — default AIReadyGov-ExposureFinding
  UNIFIED_METADATA_TABLE — default AIReadyOntology-UnifiedMetadata (override if tenant binding differs)

Examples (PowerShell, repo root):
  $env:AWS_PROFILE='your-profile'
  python scripts/diff_ontology_governance_item_ids.py --tenant-id tenant-alpha
  python scripts/diff_ontology_governance_item_ids.py --tenant-id tenant-alpha --json-out diff.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any


def _query_all_items(*, table, tenant_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = {
        "KeyConditionExpression": "tenant_id = :t",
        "ExpressionAttributeValues": {":t": tenant_id},
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items


def _item_ids_from_unified(rows: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for row in rows:
        if bool(row.get("is_deleted")):
            continue
        iid = str(row.get("item_id") or "").strip()
        if iid:
            out.add(iid)
    return out


def _item_ids_from_findings(rows: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for row in rows:
        iid = str(row.get("item_id") or "").strip()
        if iid:
            out.add(iid)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Diff UnifiedMetadata vs ExposureFinding item_ids per tenant.")
    parser.add_argument("--tenant-id", required=True, help="Tenant id (partition key value)")
    parser.add_argument(
        "--finding-table",
        default=os.environ.get("GOVERNANCE_FINDING_TABLE_NAME", "AIReadyGov-ExposureFinding"),
        help="ExposureFinding DynamoDB table name",
    )
    parser.add_argument(
        "--unified-table",
        default=os.environ.get("UNIFIED_METADATA_TABLE", "AIReadyOntology-UnifiedMetadata"),
        help="UnifiedMetadata DynamoDB table name",
    )
    parser.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
    parser.add_argument("--json-out", help="Write full report JSON to this path")
    args = parser.parse_args()
    tenant_id = str(args.tenant_id).strip()
    if not tenant_id:
        print("tenant_id is required", file=sys.stderr)
        return 2

    try:
        import boto3
    except ImportError as e:
        print("boto3 is required: pip install boto3", file=sys.stderr)
        raise SystemExit(1) from e

    session_kw: dict[str, Any] = {}
    if args.region:
        session_kw["region_name"] = args.region
    dynamodb = boto3.resource("dynamodb", **session_kw)

    unified_table = dynamodb.Table(args.unified_table)
    finding_table = dynamodb.Table(args.finding_table)

    unified_rows = _query_all_items(table=unified_table, tenant_id=tenant_id)
    finding_rows = _query_all_items(table=finding_table, tenant_id=tenant_id)

    unified_ids = _item_ids_from_unified(unified_rows)
    finding_ids = _item_ids_from_findings(finding_rows)

    only_unified = sorted(unified_ids - finding_ids)
    only_finding = sorted(finding_ids - unified_ids)
    both = sorted(unified_ids & finding_ids)

    report = {
        "tenant_id": tenant_id,
        "tables": {"unified_metadata": args.unified_table, "exposure_finding": args.finding_table},
        "counts": {
            "unified_metadata_rows": len(unified_rows),
            "unified_metadata_item_ids_non_deleted": len(unified_ids),
            "finding_rows": len(finding_rows),
            "finding_item_ids": len(finding_ids),
            "intersection": len(both),
            "only_in_unified_metadata": len(only_unified),
            "only_in_findings": len(only_finding),
        },
        "only_in_unified_metadata": only_unified,
        "only_in_findings": only_finding,
    }

    print(json.dumps(report["counts"], indent=2, ensure_ascii=False))
    print(f"\nonly_in_unified_metadata ({len(only_unified)}):", file=sys.stderr)
    for x in only_unified[:200]:
        print(f"  {x}")
    if len(only_unified) > 200:
        print(f"  ... and {len(only_unified) - 200} more", file=sys.stderr)

    print(f"\nonly_in_findings ({len(only_finding)}):", file=sys.stderr)
    for x in only_finding[:200]:
        print(f"  {x}")
    if len(only_finding) > 200:
        print(f"  ... and {len(only_finding) - 200} more", file=sys.stderr)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\nWrote {args.json_out}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
