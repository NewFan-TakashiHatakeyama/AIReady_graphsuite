# AI Ready Connect

**M365 ファイルイベントのリアルタイム検知・処理システム (PoC)**

Microsoft 365 (SharePoint Online / OneDrive) 上のファイルイベント（作成・変更・削除・共有変更）を
Microsoft Graph Change Notifications (Webhook) と Delta Query でリアルタイムに検知し、
AWS サーバーレス基盤で処理・蓄積するシステムです。

---

## システム概要

### 目的

企業の M365 環境における **AI Readiness スコア** の算出に必要なファイルメタデータを、
リアルタイムかつ網羅的に収集する PoC 基盤を構築すること。

### 特徴

- **リアルタイム検知**: Microsoft Graph Webhook による即座のイベント通知
- **網羅的データ収集**: DriveItem の全属性 (40以上) + 権限情報 + Raw JSON を保存
- **信頼性**: Delta Query による取りこぼし防止、SQS + DLQ による再試行、冪等処理
- **サーバーレス**: Lambda + DynamoDB + S3 で運用コスト最小化
- **拡張性**: 将来的に Slack / Google Drive / Box 等のコネクター追加を想定した設計

---

## アーキテクチャ

```
Microsoft Graph API
    │ Change Notification (Webhook POST)
    ▼
ALB (webhook.graphsuite.jp / HTTPS)
    │
    ▼
┌─────────────────────────────────┐
│  receive_notification (Lambda)  │  ← Validation / Health Check / 通知振り分け
│  - clientState 検証             │
│  - SNS Publish                  │
└────────────┬────────────────────┘
             │
             ▼
         SNS Topic
             │
             ▼
         SQS Queue ──→ DLQ (3回失敗時)
             │
             ▼
┌─────────────────────────────────┐
│  pull_file_metadata (Lambda)    │  ← Delta Query + 詳細取得 + 正規化
│  - 冪等チェック (SQS messageId) │
│  - Graph Delta Query            │
│  - $expand=permissions          │
│  - DynamoDB 保存                │
│  - S3 Raw Payload 保存          │
└─────────────────────────────────┘

┌─────────────────────────────────┐
│  renew_access_token (Lambda)    │  ← EventBridge rate(30 min)
│  - OAuth2 Client Credentials    │
│  - SSM Parameter Store 保存     │
└─────────────────────────────────┘

┌─────────────────────────────────┐
│  renew_subscription (Lambda)    │  ← EventBridge rate(1 day)
│  - Graph PATCH /subscriptions   │
│  - 有効期限 +2日 延長           │
└─────────────────────────────────┘
```

### AWS リソース一覧

| カテゴリ | リソース | 名前 |
|---------|---------|------|
| ネットワーク | VPC / Subnets / NAT / IGW / SG | `AIReadyConnect-vpc` |
| ロードバランサー | ALB + HTTPS Listener | `AIReadyConnect-alb` |
| DNS | Route 53 A Record | `webhook.graphsuite.jp` |
| SSL | ACM Certificate | `webhook.graphsuite.jp` |
| メッセージング | SNS Topic | `AIReadyConnect-NotificationTopic` |
| メッセージング | SQS Queue + DLQ | `AIReadyConnect-NotificationQueue` |
| データベース | DynamoDB (3 テーブル) | `AIReadyConnect-FileMetadata` / `IdempotencyKeys` / `DeltaTokens` |
| ストレージ | S3 Bucket | `aireadyconnect-raw-payload` |
| コンピュート | Lambda (4 関数) | `AIReadyConnect-*` |
| スケジュール | EventBridge Rules (2) | `AIReadyConnect-renewTokenSchedule` / `renewSubSchedule` |
| セキュリティ | IAM Role | `AIReadyConnect-LambdaRole` |

---

## ディレクトリ構成

```
AI_Ready/connect/
├── README.md                    # 本ファイル
├── pyproject.toml               # Poetry 依存管理 / pytest / black 設定
├── .env.example                 # 環境変数テンプレート
├── .gitignore
│
├── src/                         # アプリケーションコード
│   ├── shared/                  # 全 Lambda 共通ユーティリティ
│   │   ├── config.py            #   環境変数・SSM パラメータ名の一元管理
│   │   ├── ssm.py               #   SSM Parameter Store ヘルパー
│   │   ├── dynamodb.py          #   DynamoDB CRUD (メタデータ/冪等/Delta Token)
│   │   └── logger.py            #   JSON 構造化ログ
│   │
│   ├── connectors/m365/         # Microsoft Graph API コネクター
│   │   ├── graph_client.py      #   OAuth2 認証 + API クライアント (retry/backoff)
│   │   ├── webhook.py           #   Webhook 解析 (validation/clientState/parse)
│   │   ├── delta.py             #   Delta Query (ページング/deltaLink 管理)
│   │   ├── messages.py          #   Teams/チャット メッセージ正規化
│   │   └── normalizer.py        #   DriveItem → DynamoDB 正規化 (40+ 属性)
│   │
│   └── handlers/                # Lambda ハンドラー（CDK stack.py の定義と対応）
│       ├── receive_notification.py / pull_file_metadata.py / pull_message_metadata.py
│       ├── renew_access_token.py / renew_subscription.py / init_subscription.py
│       ├── backfill_chat_messages.py / cleanup_connection_artifacts.py
│       └── （ほか `src/shared` に connection_lookup / connection_cleanup など）
│
├── infra/                       # AWS CDK (Python)
│   ├── app.py                   #   CDK アプリエントリポイント
│   ├── stack.py                 #   AIReadyConnectStack (全リソース定義)
│   ├── cdk.json                 #   CDK 設定
│   ├── requirements.txt         #   CDK 依存 (aws-cdk-lib)
│   └── layers/deps/python/      #   Lambda Layer (requests, python-dotenv)
│
├── tests/                       # pytest + moto
│   ├── conftest.py              #   AWS モック / fixtures / 環境変数セットアップ
│   ├── fixtures/                #   テスト用 JSON データ
│   │   ├── drive_item_file.json
│   │   ├── drive_item_deleted.json
│   │   └── notification_payload.json
│   ├── test_webhook.py          #   Webhook 解析テスト (22 tests)
│   ├── test_normalizer.py       #   正規化テスト (16 tests)
│   └── test_delta.py            #   Delta Query + DynamoDB テスト (12 tests)
```

運用タスク（SSM 登録、ログ確認、DynamoDB 確認など）は AWS コンソール、AWS CLI、または該当 Lambda の手動実行で行います。

---

## セットアップ手順

### 前提条件

- Python 3.11+
- AWS CLI (設定済み)
- AWS CDK CLI (`npm install -g aws-cdk`)
- Microsoft 365 テナント (Business Basic 以上)
- Azure AD アプリ登録 (マルチテナント)

### 1. Azure AD アプリ登録

Microsoft Entra ID (Azure AD) で以下を設定:

| 設定項目 | 値 |
|---------|-----|
| アプリ名 | `GraphSuite-FileSync` (任意) |
| アカウントの種類 | 任意の組織ディレクトリ内のアカウント (マルチテナント) |
| API アクセス許可 | `Files.Read.All`, `Files.ReadWrite.All`, `Sites.Read.All`, `User.Read.All` (アプリケーション) |
| 管理者の同意 | 付与済み |

クライアント ID、テナント ID、クライアントシークレットを控えてください。

### 2. 依存インストール

```bash
# アプリケーション依存
pip install requests boto3 python-dotenv

# 開発依存
pip install pytest moto pytest-cov black flake8

# CDK 依存
pip install aws-cdk-lib constructs
```

### 3. 環境変数設定

```bash
cp .env.example .env
# .env を編集して Azure AD / AWS の情報を設定
```

### 4. AWS Parameter Store 初期値登録

Azure AD の値を SSM に登録します（例: `aws ssm put-parameter`）。最低限、Graph 呼び出しに必要なクライアント ID / テナント ID / シークレット、`clientState`、監視対象の `drive_id` などを、運用方針に沿ったパラメータ名で登録してください。

以下は従来スクリプトが想定していたパラメータの例です:

| パラメータ名 | 内容 |
|-------------|------|
| `MSGraphClientId` | Azure AD アプリケーション ID |
| `MSGraphTenantId` | Azure AD テナント ID |
| `MSGraphClientSecret` | クライアントシークレット (SecureString) |
| `MSGraphClientState` | Webhook 検証用シークレット (SecureString) |
| `MSGraphDriveId` | 監視対象 SharePoint ドライブ ID |
| `MSGraphAccessToken` | OAuth2 アクセストークン (自動更新) |
| `MSGraphSubscriptionId` | サブスクリプション ID (`initSubscription` Lambda で設定) |

### 5. CDK デプロイ

```bash
cd infra

# 初回のみ
cdk bootstrap

# ドライラン (差分確認)
cdk diff

# デプロイ
cdk deploy
```

### 6. 初期化

```bash
# アクセストークン初期化 (Lambda 手動実行)
aws lambda invoke --function-name AIReadyConnect-renewAccessToken --payload "{}" response.json

# Webhook サブスクリプション作成（テナント / 接続に合わせて payload を編集）
aws lambda invoke --function-name AIReadyConnect-initSubscription --cli-binary-format raw-in-base64-out --payload "{\"tenant_id\":\"default\",\"connection_id\":\"conn-your-id\"}" init-subscription-out.json
```

Graph の疎通確認は、上記トークン取得後に `renewAccessToken` のログ、または Graph の監視リソースに対する API 呼び出しで行ってください。

### 7. テスト実行

```bash
# 単体テスト (50 tests)
python -m pytest tests/ -v

# カバレッジ付き
python -m pytest tests/ --cov=src --cov-report=html
```

---

## データモデル

### DynamoDB: FileMetadata

| 属性 | 型 | 説明 |
|------|-----|------|
| `drive_id` (PK) | String | SharePoint ドライブ ID |
| `item_id` (SK) | String | DriveItem ID |
| `name` | String | ファイル/フォルダ名 |
| `mime_type` | String | MIME タイプ |
| `size` | Number | ファイルサイズ (bytes) |
| `web_url` | String | SharePoint Web URL |
| `created_at` | String | 作成日時 (ISO 8601) |
| `modified_at` | String | 更新日時 (ISO 8601) |
| `created_by_*` | String | 作成者情報 |
| `modified_by_*` | String | 更新者情報 |
| `parent_drive_id` | String | 親ドライブ ID |
| `parent_path` | String | 親フォルダパス |
| `is_deleted` | Boolean | 削除フラグ |
| `sharing_scope` | String | 共有スコープ (`anonymous` / `organization` / `specific_users` / `unknown`) |
| `sensitivity_label_id` | String | 秘密度ラベル ID |
| `raw_item` | String | Graph API レスポンスの完全な JSON |
| `synced_at` | String | 同期日時 |
| ... | ... | その他 40+ のファセット属性 |

### DynamoDB: DeltaTokens

| 属性 | 型 | 説明 |
|------|-----|------|
| `drive_id` (PK) | String | SharePoint ドライブ ID |
| `delta_token` | String | Graph API deltaLink URL |
| `updated_at` | String | 更新日時 |

### DynamoDB: IdempotencyKeys

| 属性 | 型 | 説明 |
|------|-----|------|
| `event_id` (PK) | String | SQS messageId (冪等キー) |
| `processed_at` | String | 処理日時 |
| `tenant_id` | String | テナント ID |
| `ttl` | Number | TTL (7日後の UNIX timestamp) |

### S3: Raw Payload

```
s3://aireadyconnect-raw-payload/{tenant_id}/raw/{date}/{item_id}_{uuid}.json
```

各ファイルに Graph API レスポンスの完全な JSON (item + permissions) を保存。

---

## 主要コンポーネント

### Lambda 関数

| 関数名 | トリガー | 処理内容 |
|--------|---------|---------|
| `receive_notification` | ALB (HTTPS) | Webhook 受信 → Validation 応答 / clientState 検証 / SNS Publish |
| `pull_file_metadata` | SQS | Delta Query → 詳細取得 ($expand=permissions) → 正規化 → DynamoDB + S3 保存 |
| `renew_access_token` | EventBridge (30分) | OAuth2 Client Credentials → SSM 保存 |
| `renew_subscription` | EventBridge (1日) | Graph PATCH /subscriptions → 有効期限 +2日延長 |

### Graph API クライアント (`graph_client.py`)

- OAuth2 Client Credentials フローによるトークン取得
- 429 (Rate Limit) → `Retry-After` ヘッダー対応
- 401 (Unauthorized) → 自動トークン再取得
- 指数バックオフ付きリトライ (最大3回)

### 正規化 (`normalizer.py`)

DriveItem の全ファセットを網羅的に抽出:

- 基本属性: `id`, `name`, `size`, `mimeType`, `webUrl`, `createdDateTime`, `lastModifiedDateTime`
- ファイル属性: `file.hashes`, `image`, `video`, `audio`, `photo` ファセット
- フォルダ属性: `folder.childCount`, `folder.view`
- 権限: `permissions[]` → `sharing_scope` 判定 (`anonymous` / `organization` / `specific_users`)
- 秘密度: `sensitivityLabel.id`
- Raw JSON: 全レスポンスを `raw_item` として JSON 文字列で保存

---

## テスト

### 単体テスト (50 tests / 5.35 秒)

| テストファイル | テスト数 | カバー範囲 |
|--------------|---------|-----------|
| `test_webhook.py` | 22 | Validation 判定, URL-decoded token, Health check, clientState 検証, Body parse (JSON/Base64/empty/invalid), extract_resource_info |
| `test_normalizer.py` | 16 | ファイル/フォルダ正規化, datetime/user_info/parent_reference, raw_json 保存, 削除アイテム, sharing_scope 判定 (none/specific/org/anonymous/grantedToV2) |
| `test_delta.py` | 12 | DeltaToken CRUD, 冪等キー, FileMetadata CRUD, fetch_delta (単一ページ/複数ページ/保存済み deltaLink) |

### E2E テスト (実環境)

| テスト | 手順 | 結果 |
|-------|------|------|
| Webhook 受信 | ファイルアップロード → Graph Webhook → ALB → Lambda → SNS → SQS | 成功 |
| メタデータ保存 | SQS → Delta Query → DynamoDB (2 レコード) + S3 Raw Payload | 成功 |
| 削除検知 | ファイル削除 → Delta Query → `is_deleted: True` 更新 | 成功 |
| サブスクリプション更新 | Lambda 手動実行 → 有効期限延長 | 成功 |

---

## 運用

### 日次自動処理

| 処理 | スケジュール | Lambda |
|------|------------|--------|
| アクセストークン更新 | 30分ごと | `renew_access_token` |
| サブスクリプション延長 | 1日1回 | `renew_subscription` |

### 監視ポイント

| 対象 | 確認方法 |
|------|------------|
| Lambda ログ | CloudWatch Logs（各関数のロググループ）または `aws logs filter-log-events` |
| DynamoDB データ | AWS コンソールの該当テーブル、または `aws dynamodb scan/query` |
| DLQ (エラー) | AWS Console → SQS → `AIReadyConnect-NotificationDLQ` |

### スタック削除

```bash
cd infra
cdk destroy
```

全リソースが `AIReadyConnect-` プレフィックスで管理されており、
`cdk destroy` で完全にクリーンアップされます。

---

## 今後の拡張 (PoC 後)

| カテゴリ | 内容 |
|---------|------|
| **信頼性** | deltaReconciler Lambda (取りこぼし補完) |
| **信頼性** | DLQ 再投入 Lambda + CloudWatch Alarm |
| **AI** | AI Readiness スコア算出 (DynamoDB Streams → 分析 Lambda) |
| **監視** | CloudWatch Dashboard + Alarms (6件) |
| **セキュリティ** | WAF WebACL / KMS CMK / VPC Endpoints |
| **CI/CD** | GitHub Actions (lint / test / cdk deploy) |
| **マルチテナント** | テナント分離 (Silo モデル) |
| **コネクター** | Slack / Google Drive / Box 等の追加 |

---

## ドキュメント

- [詳細設計書](Docs/詳細設計.md) — システム全体の設計方針・技術選定
- [タスク一覧](Docs/タスク一覧.md) — 全 42 タスクの進捗管理 (全完了)
- [ディレクトリ構成](Docs/ディレクトリ構成.md) — PoC レベルの構成説明

---

## 技術スタック

| レイヤー | 技術 |
|---------|------|
| 言語 | Python 3.11 |
| クラウド | AWS (Lambda, DynamoDB, S3, SQS, SNS, ALB, EventBridge, Route 53, ACM, VPC) |
| IaC | AWS CDK (Python) |
| 外部 API | Microsoft Graph API v1.0 |
| 認証 | OAuth2 Client Credentials (Azure AD) |
| テスト | pytest + moto (AWS モック) |
| フォーマッター | black |
| リンター | flake8 |

---

## ライセンス

Private / Internal Use Only
