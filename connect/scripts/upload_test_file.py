"""E2E テスト用: Graph API で SharePoint にテストファイルをアップロード

SharePoint の Web UI にアクセスできない場合でも、
Graph API 経由で直接ファイルをアップロードして Webhook 通知をトリガーできる。
"""
import sys
import io
import json
import time

import boto3
import requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REGION = "ap-northeast-1"
ssm = boto3.client("ssm", region_name=REGION)


def get_param(name: str) -> str:
    return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]


def main():
    print("=" * 60)
    print("  AI Ready Connect - テストファイルアップロード")
    print("=" * 60)
    print()

    # SSM から認証情報を取得
    print("[1] SSM から認証情報を取得...")
    access_token = get_param("MSGraphAccessToken")
    drive_id = get_param("MSGraphDriveId")
    print(f"  Access Token: {access_token[:20]}...")
    print(f"  Drive ID: {drive_id[:20]}...")
    print()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "text/plain",
    }

    # テストファイル名（タイムスタンプ付き）
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    filename = f"test-webhook-{timestamp}.txt"
    content = (
        f"AI Ready Connect E2E Test\n"
        f"Timestamp: {timestamp}\n"
        f"This file was created by upload_test_file.py\n"
    )

    # Graph API でアップロード（小さいファイル用の PUT エンドポイント）
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{filename}:/content"

    print(f"[2] テストファイルをアップロード...")
    print(f"  ファイル名: {filename}")
    print()

    try:
        response = requests.put(
            url,
            headers=headers,
            data=content.encode("utf-8"),
            timeout=30,
        )
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] リクエスト失敗: {e}")
        sys.exit(1)

    print(f"  Response Status: {response.status_code}")
    print()

    if response.status_code in (200, 201):
        result = response.json()
        print("  [SUCCESS] ファイルアップロード成功!")
        print(f"  Item ID: {result.get('id', 'N/A')}")
        print(f"  Name: {result.get('name', 'N/A')}")
        print(f"  Size: {result.get('size', 'N/A')} bytes")
        print(f"  Web URL: {result.get('webUrl', 'N/A')}")
        print(f"  Created: {result.get('createdDateTime', 'N/A')}")
        print()
        print("=" * 60)
        print("  アップロード完了!")
        print()
        print("  Webhook 通知は数秒〜数十秒以内に送信されます。")
        print("  以下を確認してください:")
        print("  1. CloudWatch Logs (receive_notification Lambda)")
        print("  2. SQS キュー (AIReadyConnect-NotificationQueue)")
        print("  3. DynamoDB (AIReadyConnect-FileMetadata)")
        print("=" * 60)
    else:
        print(f"  [ERROR] アップロード失敗")
        try:
            error = response.json()
            print(f"  Error: {json.dumps(error, indent=2, ensure_ascii=False)}")
        except Exception:
            print(f"  Response: {response.text[:500]}")

        if response.status_code == 401:
            print()
            print("  -> Access Token が期限切れの可能性があります。")
            print("  -> renew_access_token Lambda を実行してください。")
        elif response.status_code == 403:
            print()
            print("  -> Files.ReadWrite.All 権限が必要です。")
            print("  -> Azure AD でアプリの API Permission を確認してください。")


if __name__ == "__main__":
    main()
