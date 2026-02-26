"""
T-014: Parameter Store 初期値登録スクリプト

.env から値を読み取り、AWS SSM Parameter Store に SecureString として登録する。

実行方法:
    cd AI_Ready/connect
    python scripts/seed_parameters.py
"""

import io
import os
import sys
import uuid
from pathlib import Path

import boto3
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# .env 読み込み
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-1")
ssm = boto3.client("ssm", region_name=REGION)


def put_param(name: str, value: str, param_type: str = "SecureString") -> None:
    """Parameter Store にパラメータを登録（上書き可）"""
    try:
        ssm.put_parameter(
            Name=name,
            Value=value,
            Type=param_type,
            Overwrite=True,
        )
        masked = value[:8] + "..." if len(value) > 12 else "***"
        print(f"  [OK] {name} = {masked} ({param_type})")
    except Exception as e:
        print(f"  [ERROR] {name}: {e}")


def main():
    print("=" * 60)
    print("  AI Ready Connect - Parameter Store 初期値登録")
    print("=" * 60)
    print()

    # 必須パラメータのチェック
    client_id = os.getenv("MS_GRAPH_CLIENT_ID")
    tenant_id = os.getenv("MS_GRAPH_TENANT_ID")
    client_secret = os.getenv("MS_GRAPH_CLIENT_SECRET")
    drive_id = os.getenv("MS_GRAPH_DRIVE_ID")

    missing = []
    if not client_id:
        missing.append("MS_GRAPH_CLIENT_ID")
    if not tenant_id:
        missing.append("MS_GRAPH_TENANT_ID")
    if not client_secret:
        missing.append("MS_GRAPH_CLIENT_SECRET")

    if missing:
        print(f"[ERROR] .env に未設定: {', '.join(missing)}")
        print(f"  ファイル: {env_path}")
        sys.exit(1)

    # clientState (Webhook 検証用のランダムシークレット)
    client_state = str(uuid.uuid4())

    print("以下のパラメータを登録します:\n")

    # 認証情報
    print("[認証情報]")
    put_param("MSGraphClientId", client_id, "String")
    put_param("MSGraphTenantId", tenant_id, "String")
    put_param("MSGraphClientSecret", client_secret, "SecureString")

    # Webhook 検証用シークレット
    print("\n[Webhook 検証]")
    put_param("MSGraphClientState", client_state, "SecureString")

    # ドライブ ID
    if drive_id:
        print("\n[監視対象]")
        put_param("MSGraphDriveId", drive_id, "String")

    # アクセストークン (プレースホルダー、renew_access_token Lambda で更新される)
    print("\n[トークン (プレースホルダー)]")
    put_param("MSGraphAccessToken", "PLACEHOLDER_WILL_BE_UPDATED", "SecureString")

    print("\n" + "=" * 60)
    print("  登録完了!")
    print()
    print("  clientState (Webhook 検証用):")
    print(f"    {client_state}")
    print("    ※ この値は自動生成されました。手動で変更する場合は")
    print("    Parameter Store の MSGraphClientState を更新してください。")
    print("=" * 60)


if __name__ == "__main__":
    main()
