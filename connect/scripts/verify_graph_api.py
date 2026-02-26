"""
T-003: Graph API 疎通確認スクリプト

実行方法:
    cd AI_Ready
    pip install requests python-dotenv
    python scripts/verify_graph_api.py

処理内容:
    1. Azure AD から Access Token を取得（client_credentials フロー）
    2. /sites でルートサイト情報を取得
    3. SharePoint サイト一覧を取得
    4. 各サイトのドライブ一覧を取得
    5. 指定ドライブで Delta Query を実行
"""

import io
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

# Windows cp932 対策: UTF-8 出力を強制
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ---------- .env 読み込み ----------
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

CLIENT_ID = os.getenv("MS_GRAPH_CLIENT_ID")
TENANT_ID = os.getenv("MS_GRAPH_TENANT_ID")
CLIENT_SECRET = os.getenv("MS_GRAPH_CLIENT_SECRET")
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# ---------- ユーティリティ ----------

def print_separator(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def print_json(data: dict, max_items: int = 10) -> None:
    """見やすく JSON を表示（大量データは先頭のみ）"""
    if isinstance(data, list) and len(data) > max_items:
        print(json.dumps(data[:max_items], indent=2, ensure_ascii=False))
        print(f"  ... 他 {len(data) - max_items} 件省略")
    else:
        print(json.dumps(data, indent=2, ensure_ascii=False))


# ---------- Step 1: アクセストークン取得 ----------

def get_access_token() -> str:
    """client_credentials フローでトークンを取得"""
    print_separator("Step 1: アクセストークン取得")

    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
    }

    resp = requests.post(url, data=payload, timeout=30)

    if resp.status_code != 200:
        print(f"[ERROR] トークン取得失敗: {resp.status_code}")
        print(resp.text)
        sys.exit(1)

    token_data = resp.json()
    access_token = token_data["access_token"]
    expires_in = token_data.get("expires_in", "?")

    print(f"[OK] トークン取得成功")
    print(f"  Token 先頭: {access_token[:30]}...")
    print(f"  有効期限: {expires_in} 秒")

    return access_token


# ---------- Step 2: ルートサイト確認 ----------

def check_root_site(headers: dict) -> None:
    """ルートサイトにアクセスして基本疎通確認"""
    print_separator("Step 2: ルートサイト情報の確認")

    resp = requests.get(f"{GRAPH_BASE}/sites/root", headers=headers, timeout=30)

    if resp.status_code != 200:
        print(f"[ERROR] ルートサイト取得失敗: {resp.status_code}")
        print(resp.text)
        return

    site = resp.json()
    print(f"[OK] ルートサイト取得成功")
    print(f"  サイト名: {site.get('displayName', 'N/A')}")
    print(f"  サイト ID: {site.get('id', 'N/A')}")
    print(f"  Web URL:  {site.get('webUrl', 'N/A')}")


# ---------- Step 3: SharePoint サイト一覧 ----------

def list_sites(headers: dict) -> list:
    """組織内の SharePoint サイトを一覧表示"""
    print_separator("Step 3: SharePoint サイト一覧")

    sites = []

    # 方法1: search で全サイトを取得
    for attempt in range(3):
        try:
            print(f"  サイト検索中... (試行 {attempt + 1}/3)")
            resp = requests.get(
                f"{GRAPH_BASE}/sites?search=*",
                headers=headers,
                timeout=90,
            )
            if resp.status_code == 200:
                sites = resp.json().get("value", [])
                break
            else:
                print(f"  [WARN] サイト検索失敗: {resp.status_code}")
                error_msg = resp.json().get("error", {}).get("message", "")
                print(f"    {error_msg}")
        except requests.exceptions.Timeout:
            print(f"  [WARN] タイムアウト (試行 {attempt + 1})")
        except requests.exceptions.RequestException as e:
            print(f"  [WARN] リクエストエラー: {e}")

    # 方法2: search が失敗した場合、ルートサイトから直接取得
    if not sites:
        print("  サイト検索がタイムアウトしました。ルートサイトから直接取得します...")
        try:
            resp = requests.get(
                f"{GRAPH_BASE}/sites/root",
                headers=headers,
                timeout=60,
            )
            if resp.status_code == 200:
                sites = [resp.json()]
        except requests.exceptions.RequestException:
            pass

    if sites:
        print(f"\n[OK] {len(sites)} 件のサイトが見つかりました\n")
        for i, site in enumerate(sites):
            print(f"  [{i+1}] {site.get('displayName', 'N/A')}")
            print(f"      ID:  {site.get('id', 'N/A')}")
            print(f"      URL: {site.get('webUrl', 'N/A')}")
            print()
    else:
        print(f"\n[WARN] サイトが見つかりませんでした")

    return sites


# ---------- Step 4: ドライブ一覧 ----------

def list_drives_for_site(headers: dict, site_id: str, site_name: str) -> list:
    """指定サイトのドライブ一覧を取得"""
    resp = requests.get(
        f"{GRAPH_BASE}/sites/{site_id}/drives",
        headers=headers,
        timeout=30,
    )

    if resp.status_code != 200:
        print(f"  [WARN] サイト '{site_name}' のドライブ取得失敗: {resp.status_code}")
        return []

    drives = resp.json().get("value", [])
    return drives


def list_all_drives(headers: dict, sites: list) -> list:
    """全サイトのドライブを一覧表示"""
    print_separator("Step 4: ドライブ一覧（全サイト）")

    all_drives = []

    for site in sites:
        site_id = site.get("id", "")
        site_name = site.get("displayName", "Unknown")
        drives = list_drives_for_site(headers, site_id, site_name)

        for drive in drives:
            drive_info = {
                "site_name": site_name,
                "site_id": site_id,
                "drive_name": drive.get("name", "N/A"),
                "drive_id": drive.get("id", "N/A"),
                "drive_type": drive.get("driveType", "N/A"),
                "web_url": drive.get("webUrl", "N/A"),
                "quota_total": drive.get("quota", {}).get("total", 0),
                "quota_used": drive.get("quota", {}).get("used", 0),
            }
            all_drives.append(drive_info)

    print(f"[OK] 合計 {len(all_drives)} 件のドライブが見つかりました\n")

    for i, d in enumerate(all_drives):
        print(f"  [{i+1}] {d['drive_name']}")
        print(f"      サイト:      {d['site_name']}")
        print(f"      ドライブ ID: {d['drive_id']}")
        print(f"      種類:        {d['drive_type']}")
        print(f"      URL:         {d['web_url']}")
        if d["quota_total"] > 0:
            used_gb = d["quota_used"] / (1024**3)
            total_gb = d["quota_total"] / (1024**3)
            print(f"      使用量:      {used_gb:.2f} GB / {total_gb:.2f} GB")
        print()

    return all_drives


# ---------- Step 4b: ユーザーの OneDrive ドライブ一覧 ----------

def list_user_drives(headers: dict) -> list:
    """組織内ユーザーの OneDrive ドライブを一覧表示"""
    print_separator("Step 4b: ユーザー OneDrive ドライブ確認")

    # まずユーザー一覧を取得
    resp = requests.get(
        f"{GRAPH_BASE}/users?$select=id,displayName,mail,userPrincipalName&$top=50",
        headers=headers,
        timeout=30,
    )

    if resp.status_code != 200:
        print(f"[WARN] ユーザー一覧取得失敗: {resp.status_code}")
        print(f"  エラー: {resp.json().get('error', {}).get('message', resp.text)}")
        return []

    users = resp.json().get("value", [])
    print(f"[OK] {len(users)} 件のユーザーが見つかりました")

    all_drives = []
    for user in users:
        user_id = user.get("id", "")
        user_name = user.get("displayName", "Unknown")
        user_mail = user.get("mail") or user.get("userPrincipalName", "")

        # ユーザーのドライブを取得
        drive_resp = requests.get(
            f"{GRAPH_BASE}/users/{user_id}/drive",
            headers=headers,
            timeout=30,
        )

        if drive_resp.status_code == 200:
            drive = drive_resp.json()
            drive_info = {
                "owner": f"{user_name} ({user_mail})",
                "drive_name": drive.get("name", "OneDrive"),
                "drive_id": drive.get("id", "N/A"),
                "drive_type": drive.get("driveType", "N/A"),
                "web_url": drive.get("webUrl", "N/A"),
            }
            all_drives.append(drive_info)
            print(f"  [OK] {user_name}: ドライブ ID = {drive.get('id', 'N/A')}")
        else:
            error_msg = drive_resp.json().get("error", {}).get("code", "Unknown")
            if error_msg not in ("Request_ResourceNotFound",):
                print(f"  [--] {user_name}: ドライブなし ({error_msg})")

    print(f"\n  合計 {len(all_drives)} 件の OneDrive ドライブが見つかりました")
    return all_drives


# ---------- Step 5: Delta Query テスト ----------

def test_delta_query(headers: dict, drive_id: str, drive_name: str) -> None:
    """指定ドライブで Delta Query を実行してみる"""
    print_separator(f"Step 5: Delta Query テスト — {drive_name}")
    print(f"  ドライブ ID: {drive_id}")

    url = f"{GRAPH_BASE}/drives/{drive_id}/root/delta"
    resp = requests.get(url, headers=headers, timeout=120)

    if resp.status_code != 200:
        print(f"[ERROR] Delta Query 失敗: {resp.status_code}")
        print(resp.text)
        return

    data = resp.json()
    items = data.get("value", [])
    delta_link = data.get("@odata.deltaLink", "")
    next_link = data.get("@odata.nextLink", "")

    print(f"[OK] Delta Query 成功!")
    print(f"  取得アイテム数: {len(items)}")
    if next_link:
        print(f"  @odata.nextLink あり（ページング続きあり）")
    if delta_link:
        print(f"  @odata.deltaLink: {delta_link[:80]}...")

    # 先頭数件のアイテムを表示
    print(f"\n  --- アイテムサンプル（先頭5件） ---")
    for item in items[:5]:
        name = item.get("name", "N/A")
        item_id = item.get("id", "N/A")
        is_folder = "folder" in item
        item_type = "フォルダ" if is_folder else "ファイル"
        size = item.get("size", 0)
        modified = item.get("lastModifiedDateTime", "N/A")
        print(f"    {item_type}: {name}")
        print(f"      ID: {item_id}")
        if not is_folder:
            print(f"      サイズ: {size:,} bytes")
        print(f"      最終更新: {modified}")
        print()


# ---------- メイン ----------

def main():
    print("\n" + "=" * 60)
    print("  GraphSuite — Graph API 疎通確認 (T-003)")
    print("=" * 60)

    # 環境変数チェック
    missing = []
    if not CLIENT_ID:
        missing.append("MS_GRAPH_CLIENT_ID")
    if not TENANT_ID:
        missing.append("MS_GRAPH_TENANT_ID")
    if not CLIENT_SECRET:
        missing.append("MS_GRAPH_CLIENT_SECRET")

    if missing:
        print(f"\n[ERROR] 環境変数が未設定です: {', '.join(missing)}")
        print(f"  .env ファイルを確認してください: {env_path}")
        sys.exit(1)

    print(f"\n  Client ID:  {CLIENT_ID[:8]}...{CLIENT_ID[-4:]}")
    print(f"  Tenant ID:  {TENANT_ID[:8]}...{TENANT_ID[-4:]}")

    # Step 1: トークン取得
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Step 2: ルートサイト
    check_root_site(headers)

    # Step 3: サイト一覧
    sites = list_sites(headers)

    all_drives = []

    if sites:
        # Step 4: SharePoint ドライブ一覧
        all_drives = list_all_drives(headers, sites)
    else:
        print("\n[INFO] SharePoint サイトが見つかりませんでした。")
        print("  OneDrive for Business 経由で確認します...\n")

    # Step 4b: OneDrive ユーザードライブも確認
    onedrive_drives = list_user_drives(headers)
    all_drives.extend(onedrive_drives)

    if not all_drives:
        print_separator("結果")
        print("[WARN] ドライブが見つかりませんでした。")
        print("  以下を確認してください:")
        print("  1. M365 ライセンスに SharePoint Online / OneDrive が含まれているか")
        print("  2. Admin Consent が付与されているか")
        print("  3. API Permission (Files.Read.All, Sites.Read.All) が正しいか")
        return

    # ドライブ一覧表示
    print_separator("発見されたドライブ一覧")
    for i, d in enumerate(all_drives):
        print(f"  [{i+1}] {d['drive_name']}")
        print(f"      所有者:      {d.get('owner', 'N/A')}")
        print(f"      ドライブ ID: {d['drive_id']}")
        print(f"      種類:        {d['drive_type']}")
        print(f"      URL:         {d.get('web_url', 'N/A')}")
        print()

    # Step 5: Delta Query（最初のドライブでテスト）
    print_separator("ドライブ選択")
    print("  Delta Query をテストするドライブを番号で選択してください。")
    print("  (Enter でスキップ、'all' で全ドライブテスト)")

    for i, d in enumerate(all_drives):
        print(f"    [{i+1}] {d['drive_name']} ({d.get('owner', 'N/A')})")

    try:
        choice = input("\n  選択 > ").strip()
    except (EOFError, KeyboardInterrupt):
        choice = ""

    if choice.lower() == "all":
        for d in all_drives:
            test_delta_query(headers, d["drive_id"], d["drive_name"])
    elif choice.isdigit() and 1 <= int(choice) <= len(all_drives):
        d = all_drives[int(choice) - 1]
        test_delta_query(headers, d["drive_id"], d["drive_name"])
    elif choice == "":
        print("  スキップしました。")
    else:
        print("  無効な入力です。スキップします。")

    # 結果サマリー
    print_separator("結果サマリー")
    print("  以下のドライブ ID をメモしてください。")
    print("  監視対象のドライブ ID を .env や Parameter Store に設定します。\n")
    for i, d in enumerate(all_drives):
        print(f"  [{i+1}] {d['drive_name']}")
        print(f"      ドライブ ID: {d['drive_id']}")
        print(f"      所有者:      {d.get('owner', 'N/A')}")
        print()

    print("  完了! 次のステップ:")
    print("  - 監視対象のドライブ ID を決定する")
    print("  - .env に MS_GRAPH_DRIVE_ID=<drive_id> を追加する")
    print("  - T-003 完了 -> T-004 以降へ進む")
    print()


if __name__ == "__main__":
    main()
