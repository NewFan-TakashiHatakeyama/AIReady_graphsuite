"""T-021: Delta Query 実行

ページング対応 + deltaLink 管理を行う。
Graph API の /drives/{driveId}/root/delta エンドポイントを呼び出し、
全ページの変更アイテムを取得する。
"""

from __future__ import annotations

import logging
from typing import Any

from src.shared.config import get_config
from src.shared.dynamodb import get_delta_token, save_delta_token
from src.connectors.m365.graph_client import GraphClient

logger = logging.getLogger(__name__)


def fetch_delta(
    client: GraphClient,
    drive_id: str,
    *,
    use_saved_token: bool = True,
    select_fields: str | None = None,
) -> list[dict[str, Any]]:
    """Delta Query を実行し、全ページの変更アイテムを返す

    1. DeltaTokens テーブルから保存済み deltaLink を取得
    2. deltaLink が無ければ初回フルスキャン
    3. 全ページをフェッチ (nextLink をたどる)
    4. 新しい deltaLink を DeltaTokens テーブルに保存

    Args:
        client: 認証済み GraphClient
        drive_id: 監視対象ドライブ ID
        use_saved_token: 保存済み deltaLink を使用するか（False=フルスキャン強制）
        select_fields: $select で取得フィールドを制限する場合のカンマ区切り文字列
                       None の場合は全フィールド取得（PoC: 全情報収集）

    Returns:
        変更された DriveItem のリスト
    """
    cfg = get_config()
    all_items: list[dict[str, Any]] = []
    page_count = 0

    # 保存済み deltaLink を取得
    delta_link = None
    if use_saved_token:
        delta_link = get_delta_token(drive_id)

    # 初回 or deltaLink なし → 初回フルスキャン
    if delta_link:
        url = delta_link
        logger.info(f"Using saved deltaLink for drive={drive_id}")
    else:
        url = f"{cfg.graph_base_url}/drives/{drive_id}/root/delta"
        logger.info(f"No saved deltaLink — starting full scan for drive={drive_id}")

    # パラメータ設定（初回のみ。deltaLink にはパラメータが含まれるため不要）
    params: dict[str, str] | None = None
    if not delta_link and select_fields:
        params = {"$select": select_fields}

    # ページング
    while url:
        page_count += 1
        logger.info(f"Fetching delta page {page_count} for drive={drive_id}")

        if url.startswith("http"):
            resp = client.get(url, params=params, timeout=120)
            data = resp.json()
        else:
            data = client.graph_get(url, params=params, timeout=120)

        # ページの結果を追加
        items = data.get("value", [])
        all_items.extend(items)
        logger.info(f"  Page {page_count}: {len(items)} items")

        # 次回以降はパラメータ不要（nextLink に含まれる）
        params = None

        # 次ページ or 終了
        next_link = data.get("@odata.nextLink")
        new_delta_link = data.get("@odata.deltaLink")

        if next_link:
            url = next_link
        elif new_delta_link:
            # 全ページ取得完了 → deltaLink を保存
            save_delta_token(drive_id, new_delta_link)
            logger.info(
                f"Delta query complete: {len(all_items)} total items, "
                f"{page_count} pages. deltaLink saved."
            )
            url = ""  # ループ終了
        else:
            logger.warning("No nextLink or deltaLink in response — ending pagination")
            url = ""

    return all_items


def fetch_item_detail(
    client: GraphClient,
    drive_id: str,
    item_id: str,
) -> dict[str, Any]:
    """個別の DriveItem 詳細を取得する（全フィールド）

    $expand=permissions でアクセス権限情報も一緒に取得する。

    Args:
        client: 認証済み GraphClient
        drive_id: ドライブ ID
        item_id: アイテム ID

    Returns:
        DriveItem JSON (permissions 含む)
    """
    # $expand=permissions で権限情報も同時取得
    path = f"/drives/{drive_id}/items/{item_id}"
    params = {"$expand": "permissions"}
    return client.graph_get(path, params=params)


def fetch_permissions(
    client: GraphClient,
    drive_id: str,
    item_id: str,
) -> list[dict[str, Any]]:
    """個別のアイテムの権限情報を取得する

    Args:
        client: 認証済み GraphClient
        drive_id: ドライブ ID
        item_id: アイテム ID

    Returns:
        Permission オブジェクトのリスト
    """
    path = f"/drives/{drive_id}/items/{item_id}/permissions"
    data = client.graph_get(path)
    return data.get("value", [])
