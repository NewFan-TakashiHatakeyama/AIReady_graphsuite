"""T-027/028/029: pull_file_metadata Lambda

SQS から通知メッセージを受け取り、以下を実行する:
1. 冪等チェック（処理済みならスキップ）
2. Delta Query で変更アイテム一覧を取得
3. 各アイテムの詳細 + 権限情報を取得（全フィールド）
4. 正規化して DynamoDB に保存
5. S3 に Raw Payload を保存
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
import hashlib
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import boto3

from src.shared.config import get_config
from src.shared.logger import get_logger, log_with_context
from src.shared.dynamodb import (
    get_file_metadata,
    is_already_processed,
    mark_as_processed,
    put_file_metadata,
)
from src.shared.ssm import resolve_connect_param
from src.connectors.m365.graph_client import GraphClient
from src.connectors.m365.delta import fetch_delta, fetch_item_detail, fetch_permissions
from src.connectors.m365.normalizer import (
    normalize_item,
    normalize_deleted_item,
)

# Lambda コールドスタート時に初期化
s3_client = boto3.client("s3", region_name=get_config().region)
TOKYO_TZ = ZoneInfo("Asia/Tokyo")
EDITABLE_ROLES = frozenset({"write", "edit", "owner", "manage", "fullcontrol"})


def _save_raw_payload(
    bucket: str,
    tenant_id: str,
    item_id: str,
    item: dict[str, Any],
    permissions: list[dict[str, Any]],
) -> str:
    """S3 に Raw Payload を保存する

    キー形式: {tenant_id}/raw/{date}/{item_id}_{uuid}.json

    Args:
        bucket: 保存先 S3 バケット名
        tenant_id: テナント識別子
        item_id: DriveItem ID
        item: Graph から取得したアイテム情報
        permissions: アイテムの権限情報

    Returns:
        S3 オブジェクトキー
    """
    date_str = datetime.now(TOKYO_TZ).strftime("%Y-%m-%d")
    uid = str(uuid.uuid4())[:8]
    key = f"{tenant_id}/raw/{date_str}/{item_id}_{uid}.json"

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            {"item": item, "permissions": permissions},
            ensure_ascii=False,
            default=str,
        ),
        ContentType="application/json",
    )
    return key


def _normalize_permission_for_hash(permission: dict[str, Any]) -> dict[str, Any]:
    link = permission.get("link", {}) if isinstance(permission.get("link"), dict) else {}
    roles = sorted(
        {
            str(role).strip().lower()
            for role in (permission.get("roles") or [])
            if str(role).strip()
        }
    )
    principals: list[str] = []
    for key in ("grantedToV2", "grantedTo"):
        obj = permission.get(key)
        if isinstance(obj, dict):
            user = obj.get("user")
            if isinstance(user, dict):
                principal = (
                    str(user.get("email") or "").strip().lower()
                    or str(user.get("id") or "").strip()
                    or str(user.get("displayName") or "").strip()
                )
                if principal:
                    principals.append(principal)
    for key in ("grantedToIdentitiesV2", "grantedToIdentities"):
        identities = permission.get(key)
        if isinstance(identities, list):
            for identity in identities:
                if not isinstance(identity, dict):
                    continue
                user = identity.get("user")
                if isinstance(user, dict):
                    principal = (
                        str(user.get("email") or "").strip().lower()
                        or str(user.get("id") or "").strip()
                        or str(user.get("displayName") or "").strip()
                    )
                    if principal:
                        principals.append(principal)
    normalized = {
        "id": str(permission.get("id") or "").strip(),
        "roles": roles,
        "scope": str(link.get("scope") or "").strip().lower(),
        "type": str(link.get("type") or "").strip().lower(),
        "web_url": str(link.get("webUrl") or "").strip(),
        "principals": sorted(set(principals)),
    }
    return normalized


def _compute_permissions_hash(permissions: list[dict[str, Any]]) -> str:
    normalized = [_normalize_permission_for_hash(p) for p in permissions]
    normalized.sort(
        key=lambda p: (
            p.get("id", ""),
            p.get("scope", ""),
            p.get("type", ""),
            "|".join(p.get("principals", [])),
        )
    )
    payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"sha256:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"


def _role_rank(role: str) -> int:
    normalized = str(role or "").strip().lower()
    if normalized in {"read", "view", "reader"}:
        return 1
    if normalized in {"write", "edit", "writer"}:
        return 2
    if normalized in {"owner", "manage", "fullcontrol"}:
        return 3
    return 0


def _build_permission_target_map(permission_targets: Any) -> dict[str, str]:
    target_map: dict[str, str] = {}
    if not isinstance(permission_targets, list):
        return target_map
    for target in permission_targets:
        if not isinstance(target, dict):
            continue
        principal = str(target.get("principal") or "").strip().lower()
        role = str(target.get("role") or "").strip().lower()
        if not principal:
            continue
        current = target_map.get(principal)
        if current is None or _role_rank(role) > _role_rank(current):
            target_map[principal] = role
    return target_map


def _compute_permission_delta(previous_targets: Any, current_targets: Any) -> list[dict[str, str]]:
    previous_map = _build_permission_target_map(previous_targets)
    current_map = _build_permission_target_map(current_targets)
    delta: list[dict[str, str]] = []

    principals = sorted(set(previous_map.keys()).union(current_map.keys()))
    for principal in principals:
        before = previous_map.get(principal)
        after = current_map.get(principal)
        if before is None and after is not None:
            change = "added"
        elif before is not None and after is None:
            change = "removed"
        elif before != after:
            change = "escalation" if _role_rank(after or "") > _role_rank(before or "") else "reduced"
        else:
            continue
        delta.append({
            "principal": principal,
            "before": before or "",
            "after": after or "",
            "change": change,
        })
    return delta


def _resolve_tenant_domains(
    detail: dict[str, Any],
    permissions: list[dict[str, Any]],
) -> list[str]:
    domains: set[str] = set()
    configured = os.getenv("CONNECT_TENANT_DOMAINS", "")
    for value in configured.split(","):
        domain = value.strip().lower()
        if domain:
            domains.add(domain)

    # DriveItem の作成者・更新者メールから内部ドメイン候補を補完する。
    for path in (
        ("createdBy", "user", "email"),
        ("lastModifiedBy", "user", "email"),
    ):
        current: Any = detail
        for key in path:
            current = current.get(key, {}) if isinstance(current, dict) else {}
        if isinstance(current, str) and "@" in current:
            domains.add(current.rsplit("@", 1)[-1].strip().lower())

    # 明示的な内部ドメイン情報が無い場合でも、permissions 内の onmicrosoft は内部候補とする。
    for perm in permissions:
        for key in ("grantedToV2", "grantedTo"):
            obj = perm.get(key)
            if isinstance(obj, dict):
                user = obj.get("user")
                if isinstance(user, dict):
                    email = str(user.get("email") or "").strip().lower()
                    if email.endswith(".onmicrosoft.com"):
                        domains.add(email.rsplit("@", 1)[-1])

    return sorted(domains)


def _enrich_source_metadata(
    metadata: dict[str, Any],
    previous_item: dict[str, Any] | None,
    permissions: list[dict[str, Any]],
) -> None:
    raw = metadata.get("source_metadata")
    current_source_metadata = {}
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                current_source_metadata = parsed
        except (json.JSONDecodeError, TypeError):
            current_source_metadata = {}

    previous_source_metadata = {}
    if previous_item and isinstance(previous_item.get("source_metadata"), str):
        try:
            parsed_prev = json.loads(previous_item["source_metadata"])
            if isinstance(parsed_prev, dict):
                previous_source_metadata = parsed_prev
        except (json.JSONDecodeError, TypeError):
            previous_source_metadata = {}

    effective_hash = _compute_permissions_hash(permissions)
    baseline_hash = str(previous_source_metadata.get("baseline_permissions_hash") or "").strip() or effective_hash
    previous_targets = previous_source_metadata.get("permission_targets", [])
    current_targets = current_source_metadata.get("permission_targets", [])
    permission_delta = _compute_permission_delta(previous_targets, current_targets)

    current_source_metadata["effective_permissions_hash"] = effective_hash
    current_source_metadata["baseline_permissions_hash"] = baseline_hash
    current_source_metadata["permission_delta"] = permission_delta
    metadata["source_metadata"] = json.dumps(current_source_metadata, ensure_ascii=False)


def _process_single_record(
    record: dict[str, Any],
    client: GraphClient,
    cfg: Any,
    logger: logging.Logger,
) -> dict[str, int]:
    """SQS レコード 1件を処理する

    Args:
        record: SQS レコード
        client: 初期化済み GraphClient
        cfg: 共通設定オブジェクト
        logger: 構造化ロガー

    Returns:
        {"processed": N, "skipped": N, "errors": N}
    """
    stats = {"processed": 0, "skipped": 0, "errors": 0}

    # SQS メッセージボディをパース
    body = json.loads(record["body"])
    resource_type = str(body.get("resource_type") or "drive").strip().lower()
    if resource_type != "drive":
        stats["skipped"] += 1
        return stats

    drive_id = body.get("drive_id", "")
    tenant_id = body.get("tenant_id", cfg.tenant_id)
    connection_id = str(body.get("connection_id") or "").strip()
    change_type = body.get("change_type", "unknown")
    subscription_id = body.get("subscription_id", "")

    # 冪等キー: SQS messageId を使用（各通知で一意）
    # SQS の at-least-once 配信による重複のみスキップし、
    # 同一ドライブへの異なるイベントは個別に処理する
    message_id = record.get("messageId", "")
    event_id = message_id if message_id else f"{subscription_id}:{drive_id}:{change_type}"

    log_with_context(
        logger, logging.INFO,
        f"Processing notification: drive={drive_id}, type={change_type}",
        event_id=event_id,
        extra_data=body,
    )

    # 冪等チェック
    if is_already_processed(event_id):
        log_with_context(
            logger, logging.INFO,
            f"Already processed: {event_id} — skipping",
            event_id=event_id,
        )
        stats["skipped"] += 1
        return stats

    # Delta Query 実行（全変更アイテムを取得）
    log_with_context(
        logger, logging.INFO,
        f"Starting Delta Query for drive={drive_id}",
        event_id=event_id,
    )

    scoped_client = client
    if connection_id or str(tenant_id or "").strip() != str(cfg.tenant_id or "").strip():
        scoped_client = GraphClient.from_ssm(
            tenant_id=str(tenant_id or cfg.tenant_id),
            connection_id=connection_id,
        )
    items = fetch_delta(scoped_client, drive_id)
    if len(items) == 0:
        # Graph 通知が Delta インデックス反映より先に到着する場合がある。
        # 「更新通知なのに 0 件」を減らすため、短時間だけ再試行する。
        retry_count = max(0, min(int(os.getenv("DELTA_EMPTY_RETRY_COUNT", "2")), 5))
        retry_interval_sec = max(0.5, min(float(os.getenv("DELTA_EMPTY_RETRY_INTERVAL_SEC", "2.0")), 10.0))
        for _ in range(retry_count):
            time.sleep(retry_interval_sec)
            items = fetch_delta(scoped_client, drive_id)
            if items:
                break
    if len(items) == 0 and change_type == "manual-sync-check":
        # 手動同期ではコストより可視性を優先する。
        # 差分取得が空のままでも、取りこぼし防止のため 1 回だけフルスキャンする。
        log_with_context(
            logger,
            logging.INFO,
            "Incremental delta returned 0 items on manual sync; retrying with full scan.",
            event_id=event_id,
        )
        items = fetch_delta(scoped_client, drive_id, use_saved_token=False)

    log_with_context(
        logger, logging.INFO,
        f"Delta Query returned {len(items)} items",
        event_id=event_id,
        extra_data={"item_count": len(items)},
    )

    # 各アイテムの詳細取得 + 正規化 + 保存
    for item in items:
        item_id = item.get("id", "")
        if not item_id:
            continue

        try:
            is_deleted = item.get("deleted") is not None

            if is_deleted:
                # 削除アイテムは最小限の正規化
                metadata = normalize_deleted_item(item, drive_id, tenant_id)
                permissions = []
            else:
                # 詳細取得（$expand=permissions で全フィールド + 権限を同時取得）
                try:
                    detail = fetch_item_detail(scoped_client, drive_id, item_id)
                    permissions = detail.get("permissions", [])
                except Exception as detail_err:
                    # 詳細取得失敗 → Delta の基本情報 + 別途 permissions 取得
                    log_with_context(
                        logger, logging.WARNING,
                        f"Failed to fetch item detail for {item_id}: {detail_err}. "
                        "Falling back to delta item + separate permissions.",
                        event_id=event_id,
                    )
                    detail = item
                    try:
                        permissions = fetch_permissions(scoped_client, drive_id, item_id)
                    except Exception:
                        permissions = []

                tenant_domains = _resolve_tenant_domains(detail, permissions)
                metadata = normalize_item(
                    detail,
                    permissions,
                    drive_id,
                    tenant_id,
                    tenant_domains=tenant_domains,
                )
                previous_item = get_file_metadata(drive_id, item_id)
                _enrich_source_metadata(metadata, previous_item, permissions)

            # S3 に Raw Payload を保存
            raw_s3_key = ""
            if cfg.raw_bucket:
                raw_item = detail if not is_deleted else item
                raw_s3_key = _save_raw_payload(
                    cfg.raw_bucket, tenant_id, item_id, raw_item, permissions
                )
            if raw_s3_key:
                metadata["raw_s3_key"] = raw_s3_key
                metadata["raw_s3_bucket"] = cfg.raw_bucket

            # Downstream governance expects item_name/source keys.
            metadata.setdefault("item_name", str(metadata.get("name", "")))
            metadata.setdefault("source", "m365")
            # Reconnection/manual resync 時は Governance 側で差分判定をバイパスできるよう明示する。
            metadata["force_re_evaluate"] = (change_type == "manual-sync-check")
            metadata["last_change_type"] = change_type

            # DynamoDB に保存
            put_file_metadata(metadata)

            stats["processed"] += 1
            log_with_context(
                logger, logging.DEBUG,
                f"Saved metadata for item={item_id}, name={metadata.get('name', '')}",
                event_id=event_id,
            )

        except Exception as item_err:
            stats["errors"] += 1
            log_with_context(
                logger, logging.ERROR,
                f"Failed to process item {item_id}: {item_err}",
                event_id=event_id,
                exc_info=True,
            )

    # 処理済みマーク
    mark_as_processed(event_id, tenant_id)

    return stats


def _manual_sync_record(
    event: dict[str, Any],
    cfg: Any,
    logger: logging.Logger,
) -> dict[str, Any] | None:
    """sync/check からの直接 invoke を SQS 互換レコードに変換する。

    既存の `_process_single_record` を再利用するため、
    手動実行リクエストを SQS レコード形式へ正規化する。

    Args:
        event: 手動同期リクエストイベント
        cfg: 共通設定オブジェクト
        logger: 構造化ロガー

    Returns:
        変換後の SQS 互換レコード。変換不可の場合は None
    """
    tenant_id = str(event.get("tenant_id") or cfg.tenant_id).strip()
    connection_id = str(event.get("connection_id") or "").strip()
    drive_id = str(event.get("drive_id") or "").strip()
    if not drive_id:
        try:
            drive_id = str(
                resolve_connect_param(
                    "drive_id",
                    tenant_id=tenant_id,
                    connection_id=connection_id,
                    decrypt=False,
                    fallback_name=cfg.ssm_drive_id,
                )
            ).strip()
        except Exception:
            drive_id = ""
    if not drive_id:
        log_with_context(
            logger,
            logging.ERROR,
            "Manual sync trigger was received but drive_id is missing.",
            extra_data={"tenant_id": tenant_id, "trigger": event.get("trigger")},
        )
        return None
    correlation_id = str(event.get("correlation_id") or "").strip()
    message_id = correlation_id or f"manual-sync-{uuid.uuid4().hex[:12]}"
    body = {
        "tenant_id": tenant_id,
        "drive_id": drive_id,
        "subscription_id": str(event.get("subscription_id") or "").strip(),
        "connection_id": connection_id,
        "change_type": "manual-sync-check",
        "trigger": str(event.get("trigger") or "manual-sync-check"),
        "requested_by": str(event.get("requested_by") or "").strip(),
        "correlation_id": correlation_id,
    }
    return {"messageId": message_id, "body": json.dumps(body, ensure_ascii=False)}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """SQS Event Source Mapping から呼び出されるハンドラー

    BatchSize=10 で複数レコードが渡される可能性がある。

    Args:
        event: SQS または手動同期トリガーの Lambda イベント
        context: Lambda コンテキスト

    Returns:
        SQS partial batch failure 形式のレスポンス辞書
    """
    cfg = get_config()
    request_id = getattr(context, "aws_request_id", "local")
    logger = get_logger(__name__, tenant_id=cfg.tenant_id, request_id=request_id)

    records = event.get("Records", [])
    trigger = str(event.get("trigger") or "").strip().lower()
    if not records and trigger == "manual-sync-check":
        manual_record = _manual_sync_record(event, cfg, logger)
        if manual_record is not None:
            records = [manual_record]
            log_with_context(
                logger,
                logging.INFO,
                "Manual sync trigger accepted. Executing delta pull directly.",
                event_id=manual_record.get("messageId", ""),
                extra_data={
                    "tenant_id": event.get("tenant_id", cfg.tenant_id),
                    "drive_id": json.loads(manual_record["body"]).get("drive_id", ""),
                    "correlation_id": event.get("correlation_id", ""),
                },
            )
    log_with_context(
        logger, logging.INFO,
        f"Received {len(records)} SQS records",
        extra_data={"record_count": len(records)},
    )

    # GraphClient を初期化（全レコードで共有）
    bootstrap_tenant_id = str(event.get("tenant_id") or cfg.tenant_id).strip() or cfg.tenant_id
    bootstrap_connection_id = str(event.get("connection_id") or "").strip()
    client = GraphClient.from_ssm(
        tenant_id=bootstrap_tenant_id,
        connection_id=bootstrap_connection_id,
    )

    # まずトークンが有効か確認（PLACEHOLDER の場合は更新）
    if client._access_token == "PLACEHOLDER_WILL_BE_UPDATED":
        log_with_context(logger, logging.INFO, "Access token is placeholder — refreshing")
        client.refresh_and_store_token(
            tenant_id=bootstrap_tenant_id,
            connection_id=bootstrap_connection_id,
        )

    total_stats = {"processed": 0, "skipped": 0, "errors": 0}
    failed_record_ids: list[str] = []

    for record in records:
        try:
            stats = _process_single_record(record, client, cfg, logger)
            for key in total_stats:
                total_stats[key] += stats[key]
        except Exception as e:
            log_with_context(
                logger, logging.ERROR,
                f"Failed to process SQS record: {e}",
                exc_info=True,
            )
            # Partial batch failure: 失敗したレコードの messageId を返す
            failed_record_ids.append(record.get("messageId", ""))

    log_with_context(
        logger, logging.INFO,
        f"Processing complete: {total_stats}",
        extra_data=total_stats,
    )

    # Partial batch failure response
    # 失敗したメッセージのみ SQS に戻す
    result: dict[str, Any] = {"batchItemFailures": []}
    for msg_id in failed_record_ids:
        result["batchItemFailures"].append({"itemIdentifier": msg_id})

    return result
