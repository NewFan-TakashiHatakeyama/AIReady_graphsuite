"""T-033: 初回サブスクリプション作成スクリプト

Graph API に Change Notification サブスクリプションを作成し、
Webhook ハンドシェイクを完了させる。

実行方法:
    cd AI_Ready/connect
    python scripts/init_subscription.py

前提:
    - renew_access_token Lambda が1回以上実行済み (SSM に有効なトークンがある)
    - ALB + receive_notification Lambda がデプロイ済み
    - webhook.graphsuite.jp でハンドシェイク (validationToken) に応答できる状態
"""

from __future__ import annotations

import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv

# Windows cp932 対策
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# .env 読み込み
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-1")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://webhook.graphsuite.jp")
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

ssm = boto3.client("ssm", region_name=REGION)


def get_ssm_param(name: str, decrypt: bool = True) -> str:
    """SSM からパラメータ取得"""
    resp = ssm.get_parameter(Name=name, WithDecryption=decrypt)
    return resp["Parameter"]["Value"]


def put_ssm_param(name: str, value: str, param_type: str = "String") -> None:
    """SSM にパラメータ登録"""
    ssm.put_parameter(Name=name, Value=value, Type=param_type, Overwrite=True)


def main():
    print("=" * 60)
    print("  AI Ready Connect - 初回サブスクリプション作成")
    print("=" * 60)
    print()

    # 認証情報取得
    print("[1] SSM から認証情報を取得...")
    try:
        access_token = get_ssm_param("MSGraphAccessToken")
        client_state = get_ssm_param("MSGraphClientState")
        drive_id = get_ssm_param("MSGraphDriveId", decrypt=False)
    except Exception as e:
        print(f"  [ERROR] SSM パラメータ取得失敗: {e}")
        print("  → renew_access_token Lambda を先に実行してください")
        sys.exit(1)

    if access_token == "PLACEHOLDER_WILL_BE_UPDATED":
        print("  [ERROR] Access Token がプレースホルダーのままです")
        print("  → renew_access_token Lambda を先に実行してください")
        sys.exit(1)

    print(f"  Access Token: {access_token[:20]}...")
    print(f"  Client State: {client_state[:8]}...")
    print(f"  Drive ID: {drive_id[:20]}...")
    print()

    # サブスクリプション作成
    print("[2] サブスクリプションを作成...")

    # 有効期限: 2日後
    expiration = datetime.now(timezone.utc) + timedelta(days=2)
    expiration_str = expiration.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")

    # Webhook エンドポイント
    notification_url = f"{WEBHOOK_URL}/webhook"

    subscription_body = {
        "changeType": "updated",
        "notificationUrl": notification_url,
        "resource": f"drives/{drive_id}/root",
        "expirationDateTime": expiration_str,
        "clientState": client_state,
    }

    print(f"  Notification URL: {notification_url}")
    print(f"  Resource: drives/{drive_id[:20]}..../root")
    print(f"  Expiration: {expiration_str}")
    print()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(
            f"{GRAPH_BASE_URL}/subscriptions",
            headers=headers,
            json=subscription_body,
            timeout=60,
        )
    except requests.exceptions.Timeout:
        print("  [ERROR] リクエストがタイムアウトしました")
        print("  → ALB + Lambda が正常に稼働しているか確認してください")
        sys.exit(1)

    print(f"  Response Status: {resp.status_code}")

    if resp.status_code in (200, 201):
        result = resp.json()
        subscription_id = result.get("id", "")
        actual_expiration = result.get("expirationDateTime", "")

        print()
        print("  [SUCCESS] サブスクリプション作成成功!")
        print(f"  Subscription ID: {subscription_id}")
        print(f"  Expiration: {actual_expiration}")
        print()

        # SSM に subscription_id を保存
        print("[3] SSM に Subscription ID を保存...")
        put_ssm_param("MSGraphSubscriptionId", subscription_id)
        print(f"  [OK] MSGraphSubscriptionId = {subscription_id}")
        print()

        print("=" * 60)
        print("  初回サブスクリプション作成完了!")
        print()
        print("  次のステップ:")
        print("  1. SharePoint でファイルを変更する")
        print("  2. CloudWatch Logs で receive_notification Lambda のログを確認")
        print("  3. SQS キューにメッセージが到達することを確認")
        print("=" * 60)

    else:
        print()
        print("  [ERROR] サブスクリプション作成失敗")
        try:
            error_detail = resp.json()
            print(f"  Error: {json.dumps(error_detail, indent=2, ensure_ascii=False)}")
        except Exception:
            print(f"  Response Body: {resp.text[:500]}")

        print()
        print("  トラブルシューティング:")
        print("  1. Access Token が有効か確認 (renew_access_token Lambda を実行)")
        print("  2. ALB + Lambda がデプロイ済みか確認")
        print(f"  3. {notification_url} にブラウザでアクセスして 200 が返るか確認")
        print("  4. clientState が SSM に登録されているか確認")
        sys.exit(1)


if __name__ == "__main__":
    main()
