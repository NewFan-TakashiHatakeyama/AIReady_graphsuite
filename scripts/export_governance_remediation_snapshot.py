#!/usr/bin/env python3
"""Export Governance Finding remediation fields + Connect FileMetadata permissions for triage.

Usage (repo root, with AWS credentials configured):
  python scripts/export_governance_remediation_snapshot.py --tenant-id TENANT --finding-id FID

Environment:
  FINDING_TABLE_NAME   default: AIReadyGov-ExposureFinding
  CONNECT_TABLE_NAME   default: AIReadyConnect-FileMetadata
  AWS_REGION           optional; passed to boto3 client
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

try:
    import boto3
    from boto3.dynamodb.conditions import Attr, Key
except ImportError:  # pragma: no cover
    print("boto3 is required: pip install boto3", file=sys.stderr)
    raise SystemExit(1) from None


def _parse_permissions(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        entries = parsed.get("entries")
        return entries if isinstance(entries, list) else []
    return []


def _permission_link_scope(permission: dict[str, Any]) -> str:
    link = permission.get("link") or {}
    return str(link.get("scope") or "").strip().lower()


def _permission_id(permission: dict[str, Any]) -> str:
    return str(permission.get("id") or "").strip()


def _is_inherited_permission(permission: dict[str, Any]) -> bool:
    inherited = permission.get("inheritedFrom")
    if isinstance(inherited, dict):
        return len(inherited) > 0
    if isinstance(inherited, list):
        return len(inherited) > 0
    if isinstance(inherited, str):
        return bool(inherited.strip())
    return False


def _is_site_default_permission(permission: dict[str, Any]) -> bool:
    pid = _permission_id(permission).lower()
    if pid.startswith("c:0-.f|rolemanager|"):
        return True
    return False


def _iter_users(permission: dict[str, Any]) -> list[dict[str, Any]]:
    users: list[dict[str, Any]] = []
    for key in ("grantedToV2", "grantedTo"):
        granted = permission.get(key)
        if isinstance(granted, dict):
            user = granted.get("user")
            if isinstance(user, dict):
                users.append(user)
    return users


def _is_owner_permission(permission: dict[str, Any], owner_user_id: str) -> bool:
    roles = permission.get("roles")
    if isinstance(roles, list) and "owner" in {str(r).strip().lower() for r in roles}:
        return True
    if not owner_user_id:
        return False
    return any(str(u.get("id") or "").strip() == owner_user_id for u in _iter_users(permission))


def _is_removable_permission(permission: dict[str, Any], owner_user_id: str) -> bool:
    if not _permission_id(permission):
        return False
    if _is_owner_permission(permission, owner_user_id):
        return False
    if _is_inherited_permission(permission):
        return False
    if _is_site_default_permission(permission):
        return False
    return True


def _get_file_metadata(*, table_name: str, tenant_id: str, item_id: str, region: str | None) -> dict[str, Any] | None:
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)
    try:
        response = table.query(
            IndexName="GSI-ModifiedAt",
            KeyConditionExpression=Key("tenant_id").eq(tenant_id),
            FilterExpression=Attr("item_id").eq(item_id),
            ScanIndexForward=False,
        )
        rows = response.get("Items", [])
        if rows:
            return rows[0]
        last_key = response.get("LastEvaluatedKey")
        while last_key:
            response = table.query(
                IndexName="GSI-ModifiedAt",
                KeyConditionExpression=Key("tenant_id").eq(tenant_id),
                FilterExpression=Attr("item_id").eq(item_id),
                ScanIndexForward=False,
                ExclusiveStartKey=last_key,
            )
            rows = response.get("Items", [])
            if rows:
                return rows[0]
            last_key = response.get("LastEvaluatedKey")
    except Exception:
        pass

    try:
        scan_kwargs: dict[str, Any] = {
            "FilterExpression": Attr("tenant_id").eq(tenant_id) & Attr("item_id").eq(item_id),
        }
        while True:
            scan_response = table.scan(**scan_kwargs)
            scan_rows = scan_response.get("Items", [])
            if scan_rows:
                return scan_rows[0]
            last_key = scan_response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key
    except Exception:
        return None
    return None


def _analyze_permissions(permissions: list[dict[str, Any]], owner_user_id: str) -> dict[str, Any]:
    link_org_anon_removable: list[str] = []
    link_org_anon_not_removable: list[str] = []
    for p in permissions:
        pid = _permission_id(p)
        if not pid:
            continue
        scope = _permission_link_scope(p)
        if scope not in {"anonymous", "organization"}:
            continue
        if _is_removable_permission(p, owner_user_id):
            link_org_anon_removable.append(pid)
        else:
            link_org_anon_not_removable.append(pid)
    return {
        "permission_row_count": len(permissions),
        "link_scope_org_or_anonymous_removable_ids": link_org_anon_removable,
        "link_scope_org_or_anonymous_not_removable_ids": link_org_anon_not_removable,
        "inherited_permission_ids": [_permission_id(p) for p in permissions if _is_inherited_permission(p) and _permission_id(p)],
    }


def _plan_has_remove_permissions(actions: Any) -> bool:
    if not isinstance(actions, list):
        return False
    for a in actions:
        if not isinstance(a, dict):
            continue
        if str(a.get("action_type") or "").strip().lower() == "remove_permissions":
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Export remediation snapshot for one ExposureFinding.")
    parser.add_argument("--tenant-id", required=True, help="tenant_id (PK)")
    parser.add_argument("--finding-id", required=True, help="finding_id (SK)")
    parser.add_argument("--finding-table", default=os.getenv("FINDING_TABLE_NAME", "AIReadyGov-ExposureFinding"))
    parser.add_argument("--connect-table", default=os.getenv("CONNECT_TABLE_NAME", "AIReadyConnect-FileMetadata"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"))
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Single-line JSON (default: indented)",
    )
    args = parser.parse_args()

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    finding_table = dynamodb.Table(args.finding_table)
    raw = finding_table.get_item(Key={"tenant_id": args.tenant_id, "finding_id": args.finding_id})
    item = raw.get("Item")
    if not item:
        print(json.dumps({"error": "Finding not found", "tenant_id": args.tenant_id, "finding_id": args.finding_id}))
        return 2

    item_id = str(item.get("item_id") or "").strip()
    metadata = None
    if item_id:
        metadata = _get_file_metadata(
            table_name=args.connect_table,
            tenant_id=args.tenant_id,
            item_id=item_id,
            region=args.region,
        )

    owner_user_id = str((metadata or {}).get("created_by_user_id") or "").strip()
    permissions = _parse_permissions((metadata or {}).get("permissions"))
    analysis = _analyze_permissions(permissions, owner_user_id)

    remediation_actions = item.get("remediation_actions")
    remediation_result = item.get("remediation_result")

    out: dict[str, Any] = {
        "tenant_id": args.tenant_id,
        "finding_id": args.finding_id,
        "item_id": item_id or None,
        "remediation_state": item.get("remediation_state"),
        "remediation_mode": item.get("remediation_mode"),
        "remediation_actions": remediation_actions,
        "remediation_result": remediation_result,
        "plan_has_remove_permissions": _plan_has_remove_permissions(remediation_actions),
        "connect_filemetadata_found": metadata is not None,
        "connect_snapshot": None,
        "derived": analysis,
    }

    if metadata:
        out["connect_snapshot"] = {
            "drive_id": metadata.get("drive_id"),
            "item_id": metadata.get("item_id"),
            "created_by_user_id": metadata.get("created_by_user_id"),
            "sharing_scope": metadata.get("sharing_scope"),
            "permissions": permissions,
        }

    indent = None if args.compact else 2
    print(json.dumps(out, ensure_ascii=False, indent=indent, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
