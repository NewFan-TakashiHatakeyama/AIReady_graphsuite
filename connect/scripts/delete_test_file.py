"""E2E テスト用: Graph API でテストファイルを削除し、is_deleted の更新を確認"""
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
    print("  AI Ready Connect - テストファイル削除 (T-041)")
    print("=" * 60)
    print()

    access_token = get_param("MSGraphAccessToken")
    drive_id = get_param("MSGraphDriveId")
    headers = {"Authorization": f"Bearer {access_token}"}

    # まずドライブ内のファイル一覧を取得
    print("[1] ドライブ内のファイル一覧...")
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
    resp = requests.get(url, headers=headers, timeout=30)
    items = resp.json().get("value", [])

    test_files = [f for f in items if f["name"].startswith("test-webhook-")]
    if not test_files:
        print("  テストファイルが見つかりません。先に upload_test_file.py を実行してください。")
        sys.exit(1)

    target = test_files[0]
    print(f"  削除対象: {target['name']} (ID: {target['id']})")
    print()

    # ファイル削除
    print("[2] ファイルを削除...")
    del_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{target['id']}"
    del_resp = requests.delete(del_url, headers=headers, timeout=30)
    print(f"  Response Status: {del_resp.status_code}")

    if del_resp.status_code == 204:
        print("  [SUCCESS] ファイル削除成功!")
        print()
        print("[3] Webhook 通知の処理を待機 (60秒)...")

        for i in range(6):
            time.sleep(10)
            print(f"  {(i+1)*10}秒経過...")

        # DynamoDB を確認
        print()
        print("[4] DynamoDB の is_deleted フラグを確認...")
        ddb = boto3.resource("dynamodb", region_name=REGION)
        table = ddb.Table("AIReadyConnect-FileMetadata")
        result = table.get_item(Key={"drive_id": drive_id, "item_id": target["id"]})
        item = result.get("Item")

        if item:
            is_deleted = item.get("is_deleted", False)
            print(f"  item_id: {item.get('item_id')}")
            print(f"  name: {item.get('name')}")
            print(f"  is_deleted: {is_deleted}")
            print(f"  modified_at: {item.get('modified_at')}")
            if is_deleted:
                print()
                print("  [SUCCESS] is_deleted = True に更新されました!")
            else:
                print()
                print("  [INFO] is_deleted はまだ False です。")
                print("  → 通知の遅延がある場合があります。数分後に再確認してください。")
        else:
            print("  [INFO] DynamoDB にアイテムが見つかりません。")
            print("  → 削除後に Delta Query で取得されたアイテムが別 ID の場合があります。")

            # 全レコードを確認
            scan = table.scan()
            deleted_items = [i for i in scan["Items"] if i.get("is_deleted")]
            print(f"  → is_deleted=True のレコード数: {len(deleted_items)}")
            for d in deleted_items:
                print(f"    - {d.get('item_id')}: {d.get('name')}")
    else:
        print(f"  [ERROR] 削除失敗: {del_resp.text[:300]}")


if __name__ == "__main__":
    main()
