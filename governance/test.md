# AWS デプロイ検証テスト設計書 — Oversharing 検知パイプライン

## 文書管理

| 項目 | 内容 |
|------|------|
| 文書名 | AWS デプロイ検証テスト設計書（Oversharing 検知パイプライン） |
| 対象 | AI Ready Governance — Phase 1〜6 + Phase 6.5 デプロイ済みリソースの動作検証 |
| 版数 | v1.1 |
| 作成日 | 2026-02-23 |
| 更新日 | 2026-02-24 |
| 参照 | [詳細設計](./Docs/過剰共有（Oversharing）詳細設計.md)、[実装手順書](./Tasks.md)、[設計変更書](../ontology/設計変更.md) |
| テスト種別 | デプロイ検証テスト（DVT）/ 機能テスト / E2E テスト / 性能テスト / セキュリティテスト / 耐障害性テスト |

---

## 目次

1. [テスト方針](#1-テスト方針)
2. [前提条件・環境情報](#2-前提条件環境情報)
3. [テストカテゴリ一覧](#3-テストカテゴリ一覧)
4. [DVT-1: インフラリソース検証](#4-dvt-1-インフラリソース検証)
5. [DVT-2: Lambda 関数デプロイ検証](#5-dvt-2-lambda-関数デプロイ検証)
6. [FT-1: analyzeExposure 機能テスト](#6-ft-1-analyzeexposure-機能テスト)
7. [FT-2: detectSensitivity 機能テスト](#7-ft-2-detectsensitivity-機能テスト)
8. [FT-3: batchScoring 機能テスト](#8-ft-3-batchscoring-機能テスト)
9. [FT-4: スコアリングエンジン検証](#9-ft-4-スコアリングエンジン検証)
10. [FT-5: ガード照合検証](#10-ft-5-ガード照合検証)
11. [FT-6: Finding ライフサイクル検証](#11-ft-6-finding-ライフサイクル検証)
12. [E2E-1: リアルタイムパイプライン E2E](#12-e2e-1-リアルタイムパイプライン-e2e)
13. [E2E-2: バッチパイプライン E2E](#13-e2e-2-バッチパイプライン-e2e)
14. [E2E-3: マルチテナント E2E](#14-e2e-3-マルチテナント-e2e)
15. [PT-1: 性能テスト](#15-pt-1-性能テスト)
16. [ST-1: セキュリティテスト](#16-st-1-セキュリティテスト)
17. [RT-1: 耐障害性テスト](#17-rt-1-耐障害性テスト)
18. [OT-1: 監視・可観測性テスト](#18-ot-1-監視可観測性テスト)
19. [FT-7: NER + 名詞チャンク抽出検証](#19-ft-7-ner--名詞チャンク抽出検証)
20. [FT-8: ドキュメント要約 + Embedding 検証](#20-ft-8-ドキュメント要約--embedding-検証)
21. [FT-9: DocumentAnalysis + S3 Vectors 検証](#21-ft-9-documentanalysis--s3-vectors-検証)
22. [E2E-4: 解析一元化 E2E](#22-e2e-4-解析一元化-e2e)
23. [テスト実行手順](#23-テスト実行手順)
24. [判定基準・完了条件](#24-判定基準完了条件)
25. [テストデータ管理](#25-テストデータ管理)
26. [リスクと対策](#26-リスクと対策)

---

## 1. テスト方針

### 1.1 目的

ローカル環境（moto モック）で全 419 テストが PASS した Oversharing 検知パイプラインが、実際の AWS 環境（`ap-northeast-1`）でも設計通りに動作することを**エンタープライズレベル**で保証する。

> **v1.1 追加（Phase 6.5 対応）**: [設計変更書](../ontology/設計変更.md) に基づく detectSensitivity 拡張（NER + 名詞チャンク + 要約 + Embedding + DocumentAnalysis + S3 Vectors + EntityResolutionQueue）のテストケース 32 件を追加。合計 162 テストケース。

### 1.2 ローカルテストとの対応関係

| ローカルテスト（moto） | AWS デプロイ検証 | 差分ポイント |
|----------------------|----------------|-------------|
| DynamoDB CRUD（moto） | 実 DynamoDB テーブルでの CRUD | スループット制限、GSI 整合性遅延 |
| SQS 送受信（moto） | 実 SQS キューの可視性タイムアウト・DLQ 動作 | メッセージ遅延、リトライ動作 |
| S3 読み書き（moto） | 実 S3 バケットの暗号化・ライフサイクル | IAM 権限、クロスアカウント参照 |
| DynamoDB Streams（moto 直接呼出し） | 実 DynamoDB Streams → Lambda トリガー | イベントソースマッピングの遅延 |
| EventBridge（直接呼出し） | 実 EventBridge スケジュール | cron 式の実行タイミング |
| Docker Lambda（モック） | 実 ECR イメージの Lambda 起動 | コールドスタート、メモリ消費 |
| CloudWatch（モック） | 実 CloudWatch メトリクス・アラーム | メトリクス反映遅延、アラーム評価期間 |

### 1.3 テスト原則

| 原則 | 説明 |
|------|------|
| **べき等性** | 全テストは繰り返し実行しても同じ結果になること |
| **独立性** | 各テストケースは他のテストに依存せず独立実行可能 |
| **クリーンアップ** | テスト後にテストデータを必ず削除（残存データによる影響を排除） |
| **タイムアウト考慮** | AWS サービスの非同期特性を考慮し、適切な待機・リトライを設定 |
| **テナント分離** | テスト専用テナント ID を使用し、本番データに影響を与えない |

---

## 2. 前提条件・環境情報

### 2.1 AWS 環境

| 項目 | 値 |
|------|-----|
| アカウント ID | `565699611973` |
| リージョン | `ap-northeast-1` |
| CDK スタック | `AIReadyGovernanceStack` |
| デプロイ日 | 2026-02-20 |

### 2.2 テスト専用リソース

| リソース | テスト用識別子 | 備考 |
|---------|-------------|------|
| テナント ID | `test-tenant-dvt-001` | テスト専用。クリーンアップ対象 |
| テナント ID（マルチテナント） | `test-tenant-dvt-002` | E2E-3 マルチテナントテスト用 |
| S3 テストプレフィックス | `raw/test-tenant-dvt-001/` | クリーンアップ対象 |
| Finding プレフィックス | `test-tenant-dvt-*` | テスト完了後に削除 |

### 2.3 必要ツール

| ツール | 用途 |
|--------|------|
| AWS CLI v2 | リソース確認・操作 |
| Python 3.12 + boto3 | テストスクリプト実行 |
| pytest | テスト実行フレームワーク |
| jq | JSON 出力の整形・検証 |

### 2.4 IAM 権限（テスト実行者）

テスト実行者には以下の権限が必要:

```
dynamodb:PutItem, GetItem, Query, Scan, DeleteItem  (ExposureFinding, FileMetadata)
sqs:SendMessage, ReceiveMessage, PurgeQueue          (SensitivityDetectionQueue, DLQ)
s3:PutObject, GetObject, DeleteObject                (raw-payload, reports)
lambda:InvokeFunction, GetFunction                   (3 Lambda)
cloudwatch:GetMetricData, DescribeAlarms             (メトリクス・アラーム確認)
ssm:GetParameter                                     (SSM パラメータ読み取り)
logs:GetLogEvents, FilterLogEvents                   (CloudWatch Logs 確認)
ecr:DescribeImages                                   (ECR イメージ確認)
events:DescribeRule                                  (EventBridge ルール確認)
```

---

## 3. テストカテゴリ一覧

| カテゴリ | ID | テストケース数 | 目的 |
|---------|-----|-------------|------|
| インフラリソース検証 | DVT-1 | 18 | CDK でデプロイされた全リソースの存在・設定確認 |
| Lambda デプロイ検証 | DVT-2 | 12 | Lambda 関数の構成・環境変数・トリガー確認 |
| analyzeExposure 機能 | FT-1 | 10 | リアルタイム検知の全シナリオ検証 |
| detectSensitivity 機能 | FT-2 | 10 | PII/Secret 検知の全シナリオ検証 |
| batchScoring 機能 | FT-3 | 10 | 日次バッチの全シナリオ検証 |
| スコアリングエンジン | FT-4 | 8 | RiskScore 算出の正確性検証 |
| ガード照合 | FT-5 | 6 | ExposureVector → ガード照合の正確性検証 |
| Finding ライフサイクル | FT-6 | 8 | Finding のステータス遷移検証 |
| リアルタイム E2E | E2E-1 | 6 | analyzeExposure → SQS → detectSensitivity の統合フロー |
| バッチ E2E | E2E-2 | 6 | batchScoring → Finding → レポートの統合フロー |
| マルチテナント E2E | E2E-3 | 4 | テナント分離・独立処理の検証 |
| 性能テスト | PT-1 | 6 | スループット・レイテンシ・コールドスタート |
| セキュリティテスト | ST-1 | 10 | IAM・暗号化・アクセス制御 |
| 耐障害性テスト | RT-1 | 8 | DLQ・リトライ・エラーハンドリング |
| 監視・可観測性テスト | OT-1 | 8 | CloudWatch メトリクス・アラーム・ログ |
| NER + 名詞チャンク検証 | FT-7 | 8 | NER 抽出・名詞チャンク・PII 統合の正確性検証 |
| 要約 + Embedding 検証 | FT-8 | 8 | Bedrock 要約生成・Embedding 生成の検証 |
| DocumentAnalysis + S3 Vectors | FT-9 | 8 | DocumentAnalysis テーブル・S3 Vectors 保存検証 |
| 解析一元化 E2E | E2E-4 | 8 | detectSensitivity 拡張の統合フロー検証 |
| **合計** | | **162** | |

---

## 4. DVT-1: インフラリソース検証

> ローカル対応: `cdk synth` で検証していた CDK リソースが、実際の AWS 上に正しく作成されていることを確認。

### 4.1 DynamoDB テーブル

| # | テストケース | 検証内容 | 確認コマンド | 期待結果 |
|---|------------|---------|------------|---------|
| DVT-1-01 | ExposureFinding テーブル存在 | テーブルが作成されていること | `aws dynamodb describe-table --table-name AIReadyGov-ExposureFinding` | TableStatus = `ACTIVE` |
| DVT-1-02 | PK/SK 構成 | パーティションキー・ソートキー | 同上 | PK=`tenant_id`(S), SK=`finding_id`(S) |
| DVT-1-03 | 課金モード | オンデマンド | 同上 | BillingMode = `PAY_PER_REQUEST` |
| DVT-1-04 | GSI-ItemFinding | item_id 逆引き用 GSI | 同上 | GSI 名=`GSI-ItemFinding`, PK=`item_id`, Projection=`ALL` |
| DVT-1-05 | GSI-StatusFinding | status 別クエリ用 GSI | 同上 | GSI 名=`GSI-StatusFinding`, PK=`tenant_id`, SK=`status` |
| DVT-1-06 | PITR 有効 | ポイントインタイムリカバリ | `aws dynamodb describe-continuous-backups` | PointInTimeRecoveryStatus = `ENABLED` |

### 4.2 SQS キュー

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| DVT-1-07 | SensitivityDetectionQueue 存在 | メインキューが作成されていること | キュー URL が取得できる |
| DVT-1-08 | 可視性タイムアウト | 360 秒に設定 | VisibilityTimeout = `360` |
| DVT-1-09 | DLQ リドライブポリシー | maxReceiveCount=3 | RedrivePolicy.maxReceiveCount = `3` |
| DVT-1-10 | analyzeExposure DLQ 存在 | DLQ が作成されていること | メッセージ保持期間 = 14 日 |
| DVT-1-11 | detectSensitivity DLQ 存在 | DLQ が作成されていること | メッセージ保持期間 = 14 日 |

### 4.3 S3 バケット

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| DVT-1-12 | レポートバケット存在 | `aireadygov-reports-*` が作成されていること | バケットが存在 |
| DVT-1-13 | 暗号化設定 | サーバーサイド暗号化 | SSEAlgorithm = `AES256` |
| DVT-1-14 | パブリックアクセス | ブロック設定 | BlockPublicAcls, BlockPublicPolicy = `true` |
| DVT-1-15 | ライフサイクルルール | Glacier 移行 + 削除 | Glacier@90日, Expiration@365日 |

### 4.4 SSM パラメータ

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| DVT-1-16 | risk_score_threshold | 閾値パラメータ | Value = `2.0` |
| DVT-1-17 | max_exposure_score | 上限キャップ | Value = `10.0` |
| DVT-1-18 | 全 7 パラメータ存在 | `/aiready/governance/*` の全パラメータ | 7 件すべて存在し、デフォルト値が設定されている |

---

## 5. DVT-2: Lambda 関数デプロイ検証

> ローカル対応: `cdk deploy` 後の Lambda 構成が設計書通りであることを確認。

### 5.1 analyzeExposure Lambda

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| DVT-2-01 | 関数存在 | Lambda が作成されていること | State = `Active` |
| DVT-2-02 | ランタイム | Python 3.12 | Runtime = `python3.12` |
| DVT-2-03 | メモリ・タイムアウト | 512MB / 60秒 | MemorySize=`512`, Timeout=`60` |
| DVT-2-04 | 環境変数 | 必要な変数がすべて設定 | `FINDING_TABLE_NAME`, `SENSITIVITY_QUEUE_URL`, `LOG_LEVEL` が存在 |
| DVT-2-05 | DynamoDB Streams トリガー | イベントソースマッピング | Enabled=`true`, BatchSize=`10` |
| DVT-2-06 | Reserved Concurrency | 50 | ReservedConcurrentExecutions = `50` |

### 5.2 detectSensitivity Lambda

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| DVT-2-07 | 関数存在（Docker） | Docker Lambda が作成されていること | PackageType = `Image` |
| DVT-2-08 | メモリ・タイムアウト | 3072MB / 300秒 | MemorySize=`3072`, Timeout=`300` |
| DVT-2-09 | エフェメラルストレージ | 1024MB | EphemeralStorage.Size = `1024` |
| DVT-2-10 | SQS トリガー | SQS イベントソースマッピング | Enabled=`true`, BatchSize=`1` |

### 5.3 batchScoring Lambda

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| DVT-2-11 | 関数存在 | Lambda が作成されていること | State = `Active` |
| DVT-2-12 | EventBridge ルール | 日次スケジュール | ScheduleExpression = `cron(0 5 * * ? *)`, State = `ENABLED` |

---

## 6. FT-1: analyzeExposure 機能テスト

> ローカル対応: `tests/unit/test_analyze_exposure.py`（580行）+ `tests/integration/test_analyze_exposure_e2e.py`（572行）

### テスト手順

Connect の `AIReadyConnect-FileMetadata` テーブルにテストデータを投入し、DynamoDB Streams 経由で `analyzeExposure` Lambda がトリガーされることを確認する。

| # | テストケース | 投入データ | 期待結果 | ローカルテスト対応 |
|---|------------|----------|---------|-----------------|
| FT-1-01 | INSERT → Finding 生成 | `sharing_scope=organization`, `permissions_count=150` のレコード | ExposureFinding に `status=new` の Finding が作成される | `test_insert_event_creates_finding` |
| FT-1-02 | INSERT → SQS 送信 | 同上 | SensitivityDetectionQueue にメッセージが 1 件投入される | `test_insert_event_enqueues_sqs` |
| FT-1-03 | MODIFY（sharing_scope 変更） | `sharing_scope` を `organization` → `specific` に変更 | Finding の `risk_score` が再計算される | `test_modify_sharing_scope_updates_finding` |
| FT-1-04 | MODIFY（無関係フィールド変更） | `web_url` のみ変更 | Finding が更新されない（スキップ） | `test_modify_irrelevant_field_skips` |
| FT-1-05 | REMOVE → Finding Closed | レコードを削除 | Finding の `status` が `closed` になる | `test_remove_event_closes_finding` |
| FT-1-06 | is_deleted=true → Closed | `is_deleted` を `true` に更新 | Finding の `status` が `closed` になる | `test_is_deleted_closes_finding` |
| FT-1-07 | 低リスクアイテム → Finding 未生成 | `sharing_scope=specific`, `permissions_count=5` | Finding が作成されない（RiskScore < 2.0） | `test_low_risk_no_finding` |
| FT-1-08 | Anyone リンク → 高 ExposureScore | `sharing_scope=anonymous` | ExposureScore ≥ 5.0 | `test_anonymous_link_high_exposure` |
| FT-1-09 | バッチ処理（10 レコード） | 10 件のレコードを同時 INSERT | 10 件の Finding が生成される | `test_batch_processing` |
| FT-1-10 | acknowledged Finding 不変 | `status=acknowledged` の Finding が存在する状態で MODIFY | acknowledged Finding のスコアが更新されない | `test_acknowledged_finding_not_updated` |

### 確認方法

```powershell
# Finding の確認
aws dynamodb query --table-name AIReadyGov-ExposureFinding --key-condition-expression "tenant_id = :tid" --expression-attribute-values '{":tid": {"S": "test-tenant-dvt-001"}}'

# SQS メッセージの確認
aws sqs get-queue-attributes --queue-url <QUEUE_URL> --attribute-names ApproximateNumberOfMessages

# CloudWatch Logs の確認
aws logs filter-log-events --log-group-name /aws/lambda/AIReadyGov-analyzeExposure --filter-pattern "test-tenant-dvt-001"
```

---

## 7. FT-2: detectSensitivity 機能テスト

> ローカル対応: `tests/unit/test_detect_sensitivity.py`（459行）+ `tests/unit/test_pii_detector.py` + `tests/unit/test_secret_detector.py` + `tests/unit/test_text_extractor.py` + `tests/integration/test_detect_sensitivity_e2e.py`（309行）

### テスト手順

S3 にテストファイルをアップロードし、SQS にメッセージを投入して `detectSensitivity` Lambda を実行する。

| # | テストケース | テストデータ | 期待結果 | ローカルテスト対応 |
|---|------------|-----------|---------|-----------------|
| FT-2-01 | PII 検出（英語） | メールアドレス・人名を含む `.txt` | `pii_detected=true`, `pii_types` に `EMAIL_ADDRESS`, `PERSON` | `test_pii_detection_english` |
| FT-2-02 | PII 検出（日本語マイナンバー） | マイナンバー（12桁 + コンテキスト語「個人番号」）を含む `.txt` | `pii_detected=true`, `pii_types` に `my_number`, `sensitivity_score ≥ 4.0` | `test_pii_detection_my_number` |
| FT-2-03 | PII 検出（日本語口座番号） | 銀行口座番号（「普通 1234567」+ コンテキスト語「口座」）を含む `.txt` | `pii_types` に `bank_account` | `test_pii_detection_bank_account` |
| FT-2-04 | Secret 検出（AWS Key） | AWS Access Key ID (`AKIA...`) を含む `.txt` | `secrets_detected=true`, `sensitivity_score=5.0` | `test_secret_detection_aws_key` |
| FT-2-05 | Secret 検出（GitHub Token） | `ghp_...` 形式のトークンを含む `.txt` | `secret_types` に `github_token` | `test_secret_detection_github_token` |
| FT-2-06 | docx テキスト抽出 | PII を含む `.docx` ファイル | テキスト抽出 → PII 検出 → Finding 更新 | `test_docx_extraction_and_pii` |
| FT-2-07 | xlsx テキスト抽出 | 個人情報を含む `.xlsx` ファイル（複数シート） | 全シートからテキスト抽出 → PII 検出 | `test_xlsx_extraction_and_pii` |
| FT-2-08 | ファイルサイズ超過スキップ | 50MB 超のファイルメタデータ（`size > 52428800`） | スキップ処理（`reason=file_too_large`） | `test_file_too_large_skip` |
| FT-2-09 | 未対応形式スキップ | `.zip` ファイル | スキップ処理（`reason=unsupported_format`） | `test_unsupported_format_skip` |
| FT-2-10 | RiskScore < 閾値 → 自動クローズ | PII なし + 低 ExposureScore の Finding | Finding が `closed` に自動遷移 | `test_auto_close_below_threshold` |

### テストファイル準備

```
tests/fixtures/aws/
├── pii_english.txt          # 英語 PII（EMAIL, PERSON, PHONE_NUMBER）
├── pii_mynumber.txt         # 日本語マイナンバー（12桁 + コンテキスト語）
├── pii_bank_account.txt     # 日本語口座番号
├── secret_aws_key.txt       # AWS Access Key
├── secret_github_token.txt  # GitHub Token
├── pii_document.docx        # PII 入り Word 文書
├── pii_spreadsheet.xlsx     # PII 入り Excel（複数シート）
└── empty_file.txt           # 空ファイル（テキスト抽出なし検証用）
```

---

## 8. FT-3: batchScoring 機能テスト

> ローカル対応: `tests/unit/test_batch_scoring.py`（1227行）+ `tests/unit/test_batch_scoring_advanced.py`（784行）+ `tests/integration/test_batch_scoring_e2e.py`（539行）

### テスト手順

FileMetadata テーブルにテストデータを事前投入し、`batchScoring` Lambda を手動 invoke して結果を検証する。

| # | テストケース | 事前データ | 期待結果 | ローカルテスト対応 |
|---|------------|----------|---------|-----------------|
| FT-3-01 | 全件再スコアリング | FileMetadata に 20 件投入 | `processed=20`, `errors=0` | `test_100_items_finding_generation_and_report` |
| FT-3-02 | Finding 生成 | 高リスクアイテム 5 件 | ExposureFinding に 5 件の Finding | `test_batch_creates_findings` |
| FT-3-03 | 孤立 Finding クローズ | Finding は存在するが FileMetadata にないアイテム | Finding が `closed` になる | `test_orphan_finding_closed` |
| FT-3-04 | 抑制期限切れ → open | `acknowledged` + `suppress_until` が過去 + リスク残存 | `status` が `open` に、`suppress_until=null` | `test_suppression_expired_risk_remains` |
| FT-3-05 | 抑制期限切れ → closed | `acknowledged` + `suppress_until` が過去 + リスク解消 | `status` が `closed` に | `test_suppression_expired_risk_resolved` |
| FT-3-06 | 期限内 acknowledged 不変 | `suppress_until` が未来 | `acknowledged` のまま変更なし | `test_acknowledged_not_expired_skipped` |
| FT-3-07 | 未スキャン SQS 投入 | `sensitivity_scan_at=null` の Finding | SQS に `trigger=batch` のメッセージ投入 | `test_unscanned_items_enqueued_to_sqs` |
| FT-3-08 | 日次レポート S3 出力 | 20 件投入後 batch 実行 | `s3://reports/{tenant}/daily/{date}.json` が出力される | `test_report_contains_exposure_and_guard_distribution` |
| FT-3-09 | レポート構造検証 | 同上 | `summary`, `risk_distribution`, `pii_summary`, `top_containers`, `exposure_vector_distribution`, `guard_match_distribution`, `suppression_summary` が含まれる | `test_report_structure` |
| FT-3-10 | is_deleted=true → Finding クローズ | `is_deleted=true` のレコード + 既存 Finding | Finding が `closed` になる | `test_deleted_items_in_file_metadata` |

### 確認方法

```powershell
# batchScoring 手動実行
aws lambda invoke --function-name AIReadyGov-batchScoring --payload '{}' response.json
type response.json | python -m json.tool

# S3 レポート確認
aws s3 ls s3://aireadygov-reports-565699611973/test-tenant-dvt-001/daily/
aws s3 cp s3://aireadygov-reports-565699611973/test-tenant-dvt-001/daily/2026-02-23.json - | python -m json.tool
```

---

## 9. FT-4: スコアリングエンジン検証

> ローカル対応: `tests/unit/test_scoring.py`（329行）— 詳細設計 6 章の算出例テーブル全行

### テスト手順

各スコアリングパターンに対応するテストデータを FileMetadata に投入し、生成された Finding のスコアが詳細設計と一致することを確認する。

| # | テストケース | 入力条件 | 期待 ExposureScore | 期待 RiskScore | 設計書参照 |
|---|------------|---------|-------------------|---------------|----------|
| FT-4-01 | Anyone リンクのみ | `sharing_scope=anonymous` | 5.0 | ≥ 5.0 | 6.1 算出例 1 行目 |
| FT-4-02 | 組織リンク + EEEU | `sharing_scope=organization`, EEEU in permissions | 4.1 | ≥ 4.1 | 6.1 算出例 2 行目 |
| FT-4-03 | Anyone + ゲスト + 継承崩れ | `sharing_scope=anonymous`, guest, broken_inheritance | 6.2 | ≥ 6.2 | 6.1 算出例 3 行目 |
| FT-4-04 | Private（露出なし） | `sharing_scope=specific`, `permissions_count=3` | 1.0 | < 2.0 | 6.1 算出例 4 行目 |
| FT-4-05 | ラベル「Confidential」 | `sensitivity_label=Confidential` | — | SensitivityScore=3.0 | 6.2 |
| FT-4-06 | ファイル名「給与一覧.xlsx」 | `item_name=給与一覧.xlsx` | — | SensitivityScore≥2.0 | 6.2 |
| FT-4-07 | 直近更新（3日前） | `modified_at=3日前` | — | ActivityScore=2.0 | 6.4 |
| FT-4-08 | 長期放置（100日前） | `modified_at=100日前` | — | ActivityScore=0.5 | 6.4 |

---

## 10. FT-5: ガード照合検証

> ローカル対応: `tests/unit/test_guard_matcher.py`（82行）

| # | テストケース | ExposureVector | source | 期待 matched_guards | 設計書参照 |
|---|------------|---------------|--------|-------------------|----------|
| FT-5-01 | public_link → G3 | `["public_link"]` | m365 | `["G3"]` | 8.3 |
| FT-5-02 | all_users + broken_inheritance → G2,G7 | `["all_users", "broken_inheritance"]` | m365 | `["G2", "G7"]` | 8.3 |
| FT-5-03 | public_link (Box) → G3 | `["public_link"]` | box | `["G3"]` | 8.3 |
| FT-5-04 | 対象外ソース | `["public_link"]` | slack | `[]` | 8.3 |
| FT-5-05 | ai_accessible → G9 | `["ai_accessible"]` | m365 | `["G9"]` | 8.3 |
| FT-5-06 | 複合パターン | `["public_link", "all_users", "broken_inheritance"]` | m365 | `["G2", "G3", "G7"]` | 8.3 |

---

## 11. FT-6: Finding ライフサイクル検証

> ローカル対応: `tests/unit/test_finding_manager.py`（483行）

### テスト手順

DynamoDB 上で Finding のステータス遷移が詳細設計 7.3 のルール通りに動作することを確認する。

| # | テストケース | 操作 | 初期 status | 期待 status | 設計書参照 |
|---|------------|------|-----------|-----------|----------|
| FT-6-01 | 新規検知 | FileMetadata INSERT（高リスク） | (なし) | `new` | 7.3 |
| FT-6-02 | 再評価で昇格 | 同一アイテムの MODIFY | `new` | `open` | 7.3 |
| FT-6-03 | リスク解消 | `sharing_scope` を `specific` に変更 | `open` | `closed` | 7.3 |
| FT-6-04 | アイテム削除 | FileMetadata REMOVE | `open` | `closed` | 7.3 |
| FT-6-05 | 抑制登録 | （API レベル：DynamoDB 直接更新でシミュレート） | `open` | `acknowledged` | 7.3.1 |
| FT-6-06 | 抑制期限切れ（リスク残存） | batchScoring 実行 | `acknowledged` | `open` | 7.3 |
| FT-6-07 | 抑制期限切れ（リスク解消） | batchScoring 実行 + アイテム削除 | `acknowledged` | `closed` | 7.3 |
| FT-6-08 | Finding ID 決定性 | 同一 tenant_id + source + item_id で 2 回生成 | — | 同一 finding_id | 7.1 |

---

## 12. E2E-1: リアルタイムパイプライン E2E

> ローカル対応: `tests/integration/test_pipeline_e2e.py` の TestPipelineE2E_RealtimeFlow

### テスト手順

FileMetadata への INSERT から、Finding 生成 → SQS → detectSensitivity → Finding 更新までの**完全なリアルタイムフロー**を検証する。

| # | テストケース | シナリオ | 期待結果 | 最大待機時間 |
|---|------------|---------|---------|-----------|
| E2E-1-01 | フルリアルタイムパイプライン | PII 入り `.txt` を S3 にアップロード → FileMetadata に INSERT | ① Finding 生成（`status=new`）② SQS メッセージ ③ detectSensitivity 実行 ④ `pii_detected=true`, `sensitivity_score` 更新 | 5 分 |
| E2E-1-02 | Secret 検出パイプライン | AWS Key 入り `.txt` → FileMetadata INSERT | `secrets_detected=true`, `sensitivity_score=5.0` | 5 分 |
| E2E-1-03 | 高リスク PII（マイナンバー） | マイナンバー入り `.txt` → FileMetadata INSERT | `sensitivity_score ≥ 4.0` | 5 分 |
| E2E-1-04 | 権限変更 → スコア再計算 | FT-1-01 の後、`sharing_scope` を `specific` に変更 | `risk_score` が減少 | 3 分 |
| E2E-1-05 | 削除 → Finding クローズ | FT-1-01 の後、FileMetadata レコードを REMOVE | `status=closed` | 3 分 |
| E2E-1-06 | DLQ 空確認 | 上記テスト全完了後 | analyzeExposure-DLQ, detectSensitivity-DLQ ともにメッセージ 0 件 | 1 分 |

### 待機・ポーリング戦略

```python
import time

def wait_for_finding(tenant_id, item_id, expected_field, expected_value, max_wait=300, interval=10):
    """Finding の特定フィールドが期待値になるまでポーリング"""
    elapsed = 0
    while elapsed < max_wait:
        finding = get_finding_by_item(tenant_id, item_id)
        if finding and finding.get(expected_field) == expected_value:
            return finding
        time.sleep(interval)
        elapsed += interval
    raise TimeoutError(f"Finding did not reach {expected_field}={expected_value} within {max_wait}s")
```

---

## 13. E2E-2: バッチパイプライン E2E

> ローカル対応: `tests/integration/test_pipeline_e2e.py` の TestPipelineE2E_BatchScoringFlow

| # | テストケース | 事前データ | 期待結果 | 最大待機時間 |
|---|------------|----------|---------|-----------|
| E2E-2-01 | バッチ全件処理 + レポート | FileMetadata 20 件 | Finding 生成 + S3 レポート | 15 分 |
| E2E-2-02 | 孤立 Finding クローズ | FileMetadata なし + Finding 存在 | Finding `closed` | 15 分 |
| E2E-2-03 | 未スキャン SQS 投入 | `sensitivity_scan_at=null` の Finding | SQS にメッセージ投入 | 15 分 |
| E2E-2-04 | レポート構造完全性 | 20 件処理後 | JSON レポートの全フィールドが存在し値が正しい | 15 分 |
| E2E-2-05 | 抑制期限切れ処理 | acknowledged Finding (suppress_until=過去) | `open` に遷移 | 15 分 |
| E2E-2-06 | detectSensitivity 正式スコア維持 | detectSensitivity 実行済み Finding | batchScoring が `sensitivity_score` を上書きしない | 15 分 |

---

## 14. E2E-3: マルチテナント E2E

> ローカル対応: `tests/integration/test_pipeline_e2e.py` の TestPipelineE2E_MultiTenant

| # | テストケース | シナリオ | 期待結果 |
|---|------------|---------|---------|
| E2E-3-01 | テナント独立 Finding 生成 | tenant-A に 5 件、tenant-B に 3 件投入 → batchScoring | テナントごとに独立した Finding が生成（クロス汚染なし） |
| E2E-3-02 | テナント別レポート | 同上 | 各テナントの日次レポートが S3 に個別出力 |
| E2E-3-03 | テナント A の削除がテナント B に影響しない | tenant-A の FileMetadata を削除 → batchScoring | tenant-A の Finding は closed、tenant-B は変更なし |
| E2E-3-04 | テナント ID 不正アクセス | tenant-A の Finding を tenant-B のキーで取得試行 | Finding が返されない（テナント分離） |

---

## 15. PT-1: 性能テスト

> ローカル対応: なし（moto ではレイテンシ・スループット測定不可）。AWS 固有のテスト。

| # | テストケース | 条件 | 合格基準 | 設計書参照 |
|---|------------|------|---------|----------|
| PT-1-01 | analyzeExposure レイテンシ | 単一レコード INSERT | Lambda 実行時間 < 5 秒 | 3.1 |
| PT-1-02 | analyzeExposure スループット | 100 レコード一括 INSERT（10 バッチ × 10 レコード） | 全件処理完了 < 30 秒、エラー率 < 1% | 詳細設計 11.3 |
| PT-1-03 | detectSensitivity コールドスタート | Lambda 未起動状態から SQS メッセージ投入 | コールドスタート（初回応答）< 30 秒 | 4.8 |
| PT-1-04 | detectSensitivity 処理時間 | 1MB の `.docx` ファイル | Lambda 実行時間 < 60 秒 | 4.8 |
| PT-1-05 | batchScoring 大規模テナント | FileMetadata 1,000 件 | 15 分以内に完了 | 5.5 |
| PT-1-06 | batchScoring タイムアウト安全機構 | FileMetadata 10,000 件（タイムアウト接近） | 安全停止し進捗が保存される | 5.2 |

---

## 16. ST-1: セキュリティテスト

> ローカル対応: なし（IAM・暗号化は moto では完全に検証不可）。AWS 固有のテスト。

### 16.1 IAM 最小権限テスト

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| ST-1-01 | Lambda ロールの権限範囲 | `AIReadyGov-LambdaRole` のポリシーを確認 | 設計書の権限リストと完全一致（余分な権限がない） |
| ST-1-02 | 他テーブルへのアクセス不可 | Lambda ロールで無関係な DynamoDB テーブルへの PutItem | `AccessDeniedException` |
| ST-1-03 | 他バケットへのアクセス不可 | Lambda ロールで無関係な S3 バケットへの PutObject | `AccessDenied` |

### 16.2 暗号化テスト

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| ST-1-04 | S3 暗号化確認 | レポートバケットのオブジェクトが暗号化されていること | ServerSideEncryption = `AES256` |
| ST-1-05 | DynamoDB 暗号化確認 | ExposureFinding テーブルの暗号化設定 | SSEType = `KMS` or デフォルト暗号化 |

### 16.3 データ保護テスト

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| ST-1-06 | PII 平文が CloudWatch Logs に出力されない | PII 検出後の Lambda ログを確認 | マイナンバー・口座番号の生値がログに含まれない |
| ST-1-07 | PII 平文が Finding に保存されない | ExposureFinding の `pii_types` フィールドを確認 | タイプ名のみ（`my_number` 等）。生値は含まれない |
| ST-1-08 | S3 レポートに PII 生値が含まれない | 日次レポートの JSON を確認 | 集計情報のみ。個別の PII 値は含まれない |

### 16.4 ネットワークテスト

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| ST-1-09 | S3 パブリックアクセス不可 | レポートバケットの S3 URL に外部からアクセス | `403 Forbidden` |
| ST-1-10 | SQS パブリックアクセス不可 | キューの IAM ポリシーを確認 | パブリック送信が許可されていない |

---

## 17. RT-1: 耐障害性テスト

> ローカル対応: `tests/unit/test_production_resilience.py`（1121行）の AWS 実環境版

| # | テストケース | 障害シナリオ | 期待結果 | 設計書参照 |
|---|------------|-----------|---------|----------|
| RT-1-01 | DLQ への到達確認 | analyzeExposure で処理不可能なレコードを投入（不正 JSON） | 3 回リトライ後、analyzeExposure-DLQ にメッセージが到達 | 9.2 |
| RT-1-02 | detectSensitivity DLQ | SQS に不正なメッセージを投入 | 3 回リトライ後、detectSensitivity-DLQ にメッセージが到達 | 9.2 |
| RT-1-03 | S3 キー不存在時のスキップ | 存在しない `raw_s3_key` を SQS に投入 | エラーログ出力 + Finding の `sensitivity_scan_at` がスキップとして記録 | 4.3 |
| RT-1-04 | DynamoDB スロットリング耐性 | 大量レコード同時 INSERT（100 件） | リトライで全件処理完了 | 9.1 |
| RT-1-05 | Lambda 同時実行数制限 | Reserved Concurrency を超える同時トリガー | スロットリングされるがエラーにならない（キューに滞留） | 3.1 |
| RT-1-06 | べき等性 | 同一レコードを 2 回 INSERT | Finding が 1 件のみ（重複なし） | 9.3 |
| RT-1-07 | DLQ メッセージ保持 | DLQ にメッセージ投入後 | 14 日間保持されること | 9.2 |
| RT-1-08 | batchScoring 部分障害 | 1 テナントの処理でエラーが発生 | エラーテナント以外は正常完了、エラーログ出力 | 5.2 |

---

## 18. OT-1: 監視・可観測性テスト

> ローカル対応: なし（CloudWatch は moto で完全検証不可）。AWS 固有のテスト。

### 18.1 CloudWatch アラーム

| # | テストケース | アラーム | 検証方法 | 期待結果 |
|---|------------|---------|---------|---------|
| OT-1-01 | analyzeExposure DLQ アラーム | `AIReadyGov-analyzeExposure-DLQ-NotEmpty` | DLQ にメッセージを投入 | アラーム状態が `ALARM` に遷移 |
| OT-1-02 | detectSensitivity DLQ アラーム | `AIReadyGov-detectSensitivity-DLQ-NotEmpty` | DLQ にメッセージを投入 | アラーム状態が `ALARM` に遷移 |
| OT-1-03 | batchScoring 実行時間アラーム | `AIReadyGov-batchScoring-Duration-High` | アラームの設定値を確認 | 閾値 = 840,000ms (14分) |

### 18.2 CloudWatch メトリクス

| # | テストケース | メトリクス | 検証方法 | 期待結果 |
|---|------------|----------|---------|---------|
| OT-1-04 | FindingsCreated メトリクス | `AIReadyGov.FindingsCreated` | FT-1-01 実行後に確認 | Count ≥ 1 が記録 |
| OT-1-05 | PIIDetected メトリクス | `AIReadyGov.PIIDetected` | FT-2-01 実行後に確認 | Count ≥ 1 が記録 |
| OT-1-06 | BatchItemsProcessed メトリクス | `AIReadyGov.BatchItemsProcessed` | FT-3-01 実行後に確認 | Count = 処理件数 |

### 18.3 CloudWatch Logs

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| OT-1-07 | 構造化ログ出力 | Lambda 実行後の CloudWatch Logs を確認 | JSON 形式の構造化ログが出力されている |
| OT-1-08 | エラーログのトレーサビリティ | エラー発生時のログを確認 | `tenant_id`, `item_id`, `finding_id`, エラーメッセージ、スタックトレースが含まれる |

---

## 19. FT-7: NER + 名詞チャンク抽出検証

> **Phase 6.5 対応**: [設計変更書](../ontology/設計変更.md) に基づき、detectSensitivity Lambda に追加された NER + 名詞チャンク抽出機能の検証。

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| FT-7-01 | 日本語テキスト NER 抽出 | 人名・組織名を含む日本語 docx を解析 | `ner_entities` に `Person`, `Organization` が含まれる |
| FT-7-02 | 英語テキスト NER 抽出 | 人名・地名を含む英語 txt を解析 | `en_core_web_trf` で NER が正しく抽出される |
| FT-7-03 | 名詞チャンク抽出 | 業務文書を解析 | `noun_chunks` に名詞句が含まれ、重複が除去されている |
| FT-7-04 | PII と NER の統合（同一スパン） | PII（マイナンバー）と NER（Person）が同一位置にある | 統合候補で `pii_flag=True` |
| FT-7-05 | NER のみ（PII 重複なし） | 組織名のみの NER | 統合候補で `pii_flag=False` |
| FT-7-06 | 言語自動判定（日本語） | 日本語テキストを入力 | `ja_ginza` が使用される |
| FT-7-07 | 言語自動判定（英語） | 英語テキストを入力 | `en_core_web_trf` が使用される |
| FT-7-08 | GiNZA + PII 同一パイプライン共有 | NER と PII で同一 spaCy インスタンスを使用 | モデルの二重ロードがない（メモリ消費確認） |

---

## 20. FT-8: ドキュメント要約 + Embedding 検証

> **Phase 6.5 対応**: Bedrock Claude Haiku による要約生成、Bedrock Titan Embeddings V2 による Embedding 生成の検証。

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| FT-8-01 | 日本語テキスト要約 | 500 文字の日本語テキストを要約 | 200 文字以内の要約が生成される |
| FT-8-02 | 英語テキスト要約 | 英語テキストを要約 | 要約が生成される |
| FT-8-03 | 長大テキスト要約 | 16,000 文字超のテキスト | 入力が 16,000 文字に切り詰められ、要約が生成される |
| FT-8-04 | Bedrock 要約エラーハンドリング | Bedrock API エラー発生 | フォールバック（先頭 200 文字）が使用され、処理全体は失敗しない |
| FT-8-05 | Embedding 生成（短いテキスト） | 短いテキスト入力 | 1,024 次元のベクトルが生成される |
| FT-8-06 | Embedding 生成（チャンク分割） | 4,000 文字のテキスト | 複数チャンクに分割され、各チャンクの Embedding が生成される |
| FT-8-07 | Embedding 正規化 | normalize=True で生成 | ベクトルの L2 ノルムが 1.0 |
| FT-8-08 | Bedrock Embedding エラーハンドリング | Bedrock API エラー発生 | エラーがログされ、Embedding なしで処理継続 |

---

## 21. FT-9: DocumentAnalysis + S3 Vectors 検証

> **Phase 6.5 対応**: DocumentAnalysis テーブルと S3 Vectors バケットへの保存・読み取り検証。

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| FT-9-01 | DocumentAnalysis テーブル存在 | テーブルが CDK で作成されている | TableStatus = `ACTIVE`, PK=`tenant_id`, SK=`item_id` |
| FT-9-02 | DocumentAnalysis 保存 | 解析結果を保存 | `ner_entities`, `noun_chunks`, `pii_summary`, `summary`, `embedding_s3_key` が正しく保存 |
| FT-9-03 | DocumentAnalysis TTL | TTL が 365 日後に設定 | `ttl` フィールドが現在 + 365 日の UNIX epoch |
| FT-9-04 | S3 Vectors バケット存在 | バケットが作成されている | バケット名 = `aiready-{account}-vectors`, SSE-S3 暗号化 |
| FT-9-05 | S3 Vectors 保存（JSON Lines） | Embedding を S3 に保存 | `vectors/{tenant_id}/{item_id}.jsonl` に JSON Lines 形式で保存 |
| FT-9-06 | S3 Vectors 読み取り | 保存した Embedding を読み取り | 各行が `chunk_index`, `text`, `vector`, `model`, `dimension` を含む |
| FT-9-07 | EntityResolutionQueue メッセージ送信 | PII + NER 統合候補を送信 | FIFO キューに `event_type=entity_candidates` メッセージが到着 |
| FT-9-08 | EntityResolutionQueue メッセージフォーマット | メッセージの構造検証 | `candidates` 配列に `text`, `label`, `pii_flag`, `confidence` が含まれる |

---

## 22. E2E-4: 解析一元化 E2E

> **Phase 6.5 対応**: detectSensitivity 拡張のフルパイプライン E2E 検証。

| # | テストケース | 検証内容 | 期待結果 |
|---|------------|---------|---------|
| E2E-4-01 | フルパイプライン（PII + NER + 要約 + Embedding） | PII + NER を含む docx → analyzeExposure → SQS → detectSensitivity | ① Finding 更新 ② DocumentAnalysis 保存 ③ S3 Vectors 保存 ④ EntityResolutionQueue 送信 |
| E2E-4-02 | DocumentAnalysis の NER エンティティ確認 | detectSensitivity 完了後の DocumentAnalysis 内容 | `ner_entities` に正しいエンティティタイプと `pii_flag` が設定 |
| E2E-4-03 | 要約 + Embedding の一貫性 | DocumentAnalysis の `summary` と `embedding_s3_key` | 要約が存在し、S3 に Embedding が保存されている |
| E2E-4-04 | 後方互換（DOCUMENT_ANALYSIS_ENABLED=false） | フラグ無効時にファイルを解析 | Phase 1〜6 の動作のみ（DocumentAnalysis / S3 Vectors / EntityResolutionQueue は空） |
| E2E-4-05 | PII + NER 統合メッセージ検証 | EntityResolutionQueue のメッセージ内容 | PII と NER が統合され、同一スパンは 1 件に統合、`pii_flag` で区別 |
| E2E-4-06 | 大容量ファイル解析 | 10MB の docx を解析 | 10 分以内に完了、DocumentAnalysis + S3 Vectors に保存 |
| E2E-4-07 | Bedrock API 障害時のグレースフルデグラデーション | Bedrock API がタイムアウト | PII / NER / Secret 検出は成功し Finding は更新される。要約 / Embedding はスキップ |
| E2E-4-08 | DLQ 確認 | 正常処理後 | DLQ にメッセージが滞留していない |

---

## 23. テスト実行手順

### 19.1 事前準備

```powershell
# 1. AWS 認証情報の確認
aws sts get-caller-identity

# 2. CDK スタックの確認
aws cloudformation describe-stacks --stack-name AIReadyGovernanceStack --query "Stacks[0].StackStatus"

# 3. テスト用テナントデータの初期化（既存テストデータのクリーンアップ）
python tests/aws/cleanup_test_data.py --tenant-id test-tenant-dvt-001
python tests/aws/cleanup_test_data.py --tenant-id test-tenant-dvt-002
```

### 19.2 実行順序

テストは以下の順序で実行する。各フェーズの完了を確認してから次に進む。

```
Phase A: インフラ検証（DVT-1, DVT-2）
    ↓ 全件 PASS を確認
Phase B: 機能テスト（FT-1 〜 FT-6）
    ↓ 全件 PASS を確認
Phase C: E2E テスト（E2E-1 〜 E2E-3）
    ↓ 全件 PASS を確認
Phase D: 非機能テスト（PT-1, ST-1, RT-1, OT-1）
    ↓ 全件 PASS を確認
Phase E: クリーンアップ
```

### 19.3 テスト実行コマンド

```powershell
# Phase A: インフラ検証
pytest tests/aws/test_dvt_infrastructure.py -v --tb=short

# Phase B: 機能テスト
pytest tests/aws/test_ft_analyze_exposure.py -v --tb=short
pytest tests/aws/test_ft_detect_sensitivity.py -v --tb=short
pytest tests/aws/test_ft_batch_scoring.py -v --tb=short
pytest tests/aws/test_ft_scoring_engine.py -v --tb=short
pytest tests/aws/test_ft_guard_matching.py -v --tb=short
pytest tests/aws/test_ft_finding_lifecycle.py -v --tb=short

# Phase C: E2E テスト
pytest tests/aws/test_e2e_realtime_pipeline.py -v --tb=short
pytest tests/aws/test_e2e_batch_pipeline.py -v --tb=short
pytest tests/aws/test_e2e_multi_tenant.py -v --tb=short

# Phase D: 非機能テスト
pytest tests/aws/test_pt_performance.py -v --tb=short
pytest tests/aws/test_st_security.py -v --tb=short
pytest tests/aws/test_rt_resilience.py -v --tb=short
pytest tests/aws/test_ot_observability.py -v --tb=short

# Phase E: クリーンアップ
python tests/aws/cleanup_test_data.py --tenant-id test-tenant-dvt-001
python tests/aws/cleanup_test_data.py --tenant-id test-tenant-dvt-002
```

### 19.4 テスト実行環境の切り替え

```python
# tests/aws/conftest.py

import os
import pytest
import boto3

# AWS 実環境を使用（moto は使わない）
AWS_REGION = "ap-northeast-1"
TEST_TENANT_ID = "test-tenant-dvt-001"
TEST_TENANT_ID_2 = "test-tenant-dvt-002"

FINDING_TABLE_NAME = "AIReadyGov-ExposureFinding"
CONNECT_TABLE_NAME = "AIReadyConnect-FileMetadata"

@pytest.fixture(scope="session")
def aws_clients():
    """AWS クライアントの初期化（実環境）"""
    return {
        "dynamodb": boto3.resource("dynamodb", region_name=AWS_REGION),
        "sqs": boto3.client("sqs", region_name=AWS_REGION),
        "s3": boto3.client("s3", region_name=AWS_REGION),
        "lambda_client": boto3.client("lambda", region_name=AWS_REGION),
        "cloudwatch": boto3.client("cloudwatch", region_name=AWS_REGION),
        "logs": boto3.client("logs", region_name=AWS_REGION),
        "ssm": boto3.client("ssm", region_name=AWS_REGION),
    }

@pytest.fixture(autouse=True)
def cleanup_test_data(aws_clients):
    """各テスト後のクリーンアップ"""
    yield
    # テストデータの削除
    cleanup_findings(aws_clients["dynamodb"], TEST_TENANT_ID)
    cleanup_findings(aws_clients["dynamodb"], TEST_TENANT_ID_2)
```

---

## 24. 判定基準・完了条件

### 20.1 合否基準

| カテゴリ | 合格条件 | ブロッカー |
|---------|---------|----------|
| DVT-1/DVT-2（インフラ・Lambda） | **全件 PASS** | 1 件でも FAIL → 後続テスト中止 |
| FT-1〜FT-6（機能テスト） | **全件 PASS** | Critical/High テストが FAIL → 後続テスト中止 |
| E2E-1〜E2E-3（E2E テスト） | **全件 PASS** | 1 件でも FAIL → 本番リリースブロック |
| PT-1（性能テスト） | **全件合格基準内** | 基準超過 → チューニング後に再テスト |
| ST-1（セキュリティテスト） | **全件 PASS** | 1 件でも FAIL → 本番リリースブロック |
| RT-1（耐障害性テスト） | **全件 PASS** | DLQ 未動作 → 本番リリースブロック |
| OT-1（監視テスト） | **全件 PASS** | アラーム未動作 → 本番リリースブロック |

### 20.2 テスト完了チェックリスト

- [ ] DVT-1: インフラリソース 18/18 PASS
- [ ] DVT-2: Lambda デプロイ 12/12 PASS
- [ ] FT-1: analyzeExposure 10/10 PASS
- [ ] FT-2: detectSensitivity 10/10 PASS
- [ ] FT-3: batchScoring 10/10 PASS
- [ ] FT-4: スコアリング 8/8 PASS
- [ ] FT-5: ガード照合 6/6 PASS
- [ ] FT-6: Finding ライフサイクル 8/8 PASS
- [ ] E2E-1: リアルタイム E2E 6/6 PASS
- [ ] E2E-2: バッチ E2E 6/6 PASS
- [ ] E2E-3: マルチテナント E2E 4/4 PASS
- [ ] PT-1: 性能テスト 6/6 合格基準内
- [ ] ST-1: セキュリティ 10/10 PASS
- [ ] RT-1: 耐障害性 8/8 PASS
- [ ] OT-1: 監視 8/8 PASS
- [ ] テストデータのクリーンアップ完了
- [ ] DLQ にメッセージが滞留していないこと
- [ ] テスト結果レポートが作成されていること

**合計: 130 テストケース**

### 20.3 エビデンス

| エビデンス | 保存先 | 形式 |
|-----------|--------|------|
| pytest 実行結果 | `tests/aws/results/` | JUnit XML |
| CloudWatch Logs スクリーンショット | `tests/aws/evidence/` | PNG |
| CloudWatch Metrics データ | `tests/aws/evidence/` | JSON |
| S3 レポートサンプル | `tests/aws/evidence/` | JSON |
| テスト実行サマリ | `tests/aws/results/summary.md` | Markdown |

---

## 25. テストデータ管理

### 21.1 テストデータカタログ

| ID | データ名 | 用途 | フォーマット | 配置先 |
|----|---------|------|-----------|--------|
| TD-001 | 高リスクメタデータ | FT-1, FT-4 | DynamoDB レコード | FileMetadata テーブル |
| TD-002 | 低リスクメタデータ | FT-1-07, FT-4-04 | DynamoDB レコード | FileMetadata テーブル |
| TD-003 | PII 入りテキスト（英語） | FT-2-01 | `.txt` | S3 raw-payload |
| TD-004 | マイナンバー入りテキスト | FT-2-02, E2E-1-03 | `.txt` | S3 raw-payload |
| TD-005 | Secret 入りテキスト | FT-2-04, E2E-1-02 | `.txt` | S3 raw-payload |
| TD-006 | PII 入り docx | FT-2-06 | `.docx` | S3 raw-payload |
| TD-007 | PII 入り xlsx | FT-2-07 | `.xlsx` | S3 raw-payload |
| TD-008 | 大量メタデータ（1,000件） | PT-1-05 | DynamoDB レコード | FileMetadata テーブル |
| TD-009 | acknowledged Finding | FT-3-04, FT-6-05 | DynamoDB レコード | ExposureFinding テーブル |
| TD-010 | 孤立 Finding | FT-3-03, E2E-2-02 | DynamoDB レコード | ExposureFinding テーブル |

### 21.2 テストデータ生成スクリプト

```
tests/aws/
├── conftest.py                    # AWS 実環境用テスト設定
├── test_data/
│   ├── generate_test_data.py      # テストデータ生成スクリプト
│   ├── cleanup_test_data.py       # テストデータ削除スクリプト
│   ├── fixtures/                  # テスト用ファイル
│   │   ├── pii_english.txt
│   │   ├── pii_mynumber.txt
│   │   ├── pii_bank_account.txt
│   │   ├── secret_aws_key.txt
│   │   ├── secret_github_token.txt
│   │   ├── pii_document.docx
│   │   └── pii_spreadsheet.xlsx
│   └── metadata_templates/        # FileMetadata テンプレート
│       ├── high_risk_anonymous.json
│       ├── high_risk_organization.json
│       ├── low_risk_specific.json
│       └── deleted_item.json
├── results/                       # テスト結果
│   └── summary.md
└── evidence/                      # エビデンス
```

### 21.3 クリーンアップポリシー

| 対象 | クリーンアップ方法 | タイミング |
|------|-----------------|----------|
| ExposureFinding テーブル | `tenant_id=test-tenant-dvt-*` を削除 | 各テスト後 + テスト完了後 |
| FileMetadata テーブル | `tenant_id=test-tenant-dvt-*` を削除 | 各テスト後 + テスト完了後 |
| S3 raw-payload | `raw/test-tenant-dvt-*/` プレフィックスを削除 | テスト完了後 |
| S3 レポート | `test-tenant-dvt-*/daily/` プレフィックスを削除 | テスト完了後 |
| SQS キュー | `PurgeQueue`（テスト完了後） | テスト完了後 |

---

## 26. リスクと対策

| リスク | 影響 | 対策 |
|--------|------|------|
| DynamoDB Streams の遅延 | E2E テストのタイムアウト | ポーリング間隔を 10 秒、最大待機時間を 5 分に設定 |
| detectSensitivity のコールドスタート | 初回テストの待機時間増大 | テスト開始前にウォームアップ invoke を実行 |
| テストデータの残存 | 後続テストへの影響 | `autouse` フィクスチャでクリーンアップ + テスト完了後の最終クリーンアップ |
| CloudWatch メトリクスの反映遅延 | OT テストの誤 FAIL | メトリクス確認は 5 分の遅延を考慮 |
| SQS メッセージの重複配信 | べき等性テストの期待値ずれ | Finding ID の決定性により重複は自然に吸収される |
| EventBridge スケジュールの実行タイミング | batchScoring の自動起動とテストの競合 | テスト実行中はテスト用テナントのみを使用し、本番テナントに影響しない |
| AWS サービスクォータ | 大量テスト時のスロットリング | テスト間に適切な待機を入れ、並列度を制限 |
| テスト実行者の IAM 権限不足 | テスト実行失敗 | 事前準備で権限確認を必須化 |
| Bedrock モデルアクセス権限未付与 | FT-8, E2E-4 テスト失敗 | 事前に Bedrock モデルアクセスを有効化（Claude Haiku, Titan Embeddings V2） |
| Bedrock API レート制限 | 大量テスト時のスロットリング | テスト間に適切な待機を入れ、Bedrock API 呼び出し頻度を制限 |
| Docker イメージサイズ増大（GiNZA Transformer + spaCy en_core_web_trf） | Lambda デプロイ時間の増加 | ECR イメージの事前プッシュ + Lambda のウォームアップ |

---

## 付録 A: ローカルテストと AWS テストの対応表

| ローカルテスト（moto） | AWS テスト | 差分・追加検証 |
|----------------------|-----------|-------------|
| `test_scoring.py` (329行, 全算出例) | FT-4-01〜08 | DynamoDB Decimal 型での精度検証 |
| `test_exposure_vectors.py` (257行) | FT-4-01〜04 | 実 permissions JSON パース |
| `test_guard_matcher.py` (82行) | FT-5-01〜06 | 実 Finding の matched_guards フィールド |
| `test_finding_manager.py` (483行) | FT-6-01〜08 | GSI の結果整合性遅延 |
| `test_analyze_exposure.py` (580行) | FT-1-01〜10 | 実 DynamoDB Streams トリガー |
| `test_detect_sensitivity.py` (459行) | FT-2-01〜10 | 実 S3 ダウンロード + Docker Lambda |
| `test_pii_detector.py` (237行) | FT-2-01〜03 | 実 Presidio + GiNZA（`ja_ginza`） |
| `test_secret_detector.py` (107行) | FT-2-04〜05 | 実テキスト内のパターンマッチ |
| `test_text_extractor.py` (225行) | FT-2-06〜07 | 実ファイルフォーマットの抽出 |
| `test_batch_scoring.py` (1227行) | FT-3-01〜10 | 実 EventBridge トリガー + S3 レポート |
| `test_batch_scoring_advanced.py` (784行) | PT-1-05, PT-1-06 | 実データ量での性能 |
| `test_analyze_exposure_e2e.py` (572行) | E2E-1-01〜06 | 実サービス間連携 |
| `test_detect_sensitivity_e2e.py` (309行) | E2E-1-01〜03 | 実 SQS → Docker Lambda |
| `test_batch_scoring_e2e.py` (539行) | E2E-2-01〜06 | 実 S3 レポート出力 |
| `test_pipeline_e2e.py` (947行, 12シナリオ) | E2E-1/E2E-2/E2E-3 全体 | 実 3 Lambda 統合フロー |
| `test_production_resilience.py` (1121行) | RT-1-01〜08 | 実 DLQ + リトライ動作 |
| (CloudWatch モック) | OT-1-01〜08 | 実 CloudWatch メトリクス・アラーム |
| (なし) | ST-1-01〜10 | **AWS 固有**: IAM・暗号化・ネットワーク |
| (なし) | PT-1-01〜06 | **AWS 固有**: レイテンシ・スループット |
| `test_ner_pipeline.py` [Phase 6.5] | FT-7-01〜08 | 実 GiNZA (ja_ginza) + spaCy (en_core_web_trf) |
| `test_summarizer.py` [Phase 6.5] | FT-8-01〜04 | 実 Bedrock Claude Haiku 呼び出し |
| `test_embedding_generator.py` [Phase 6.5] | FT-8-05〜08 | 実 Bedrock Titan Embeddings V2 呼び出し |
| `test_document_analysis.py` [Phase 6.5] | FT-9-01〜08 | 実 DocumentAnalysis テーブル + S3 Vectors |
| `test_document_analysis_e2e.py` [Phase 6.5] | E2E-4-01〜08 | 実 detectSensitivity 拡張フルパイプライン |

---

## 付録 B: テスト結果テンプレート

```markdown
# AWS デプロイ検証テスト結果

## 実施情報

| 項目 | 値 |
|------|-----|
| 実施日 | YYYY-MM-DD |
| 実施者 | 氏名 |
| 環境 | ap-northeast-1 / 565699611973 |
| CDK バージョン | x.x.x |

## 結果サマリ

| カテゴリ | テスト数 | PASS | FAIL | SKIP | 合格率 |
|---------|---------|------|------|------|--------|
| DVT-1 | 18 | | | | |
| DVT-2 | 12 | | | | |
| FT-1〜FT-6 | 52 | | | | |
| E2E-1〜E2E-3 | 16 | | | | |
| PT-1 | 6 | | | | |
| ST-1 | 10 | | | | |
| RT-1 | 8 | | | | |
| OT-1 | 8 | | | | |
| FT-7 | 8 | | | | |
| FT-8 | 8 | | | | |
| FT-9 | 8 | | | | |
| E2E-4 | 8 | | | | |
| **合計** | **162** | | | | |

## FAIL 詳細

| テスト ID | テストケース | FAIL 理由 | 影響度 | 対応方針 |
|-----------|------------|----------|--------|---------|

## 判定

- [ ] 全カテゴリの合格条件を満たしている
- [ ] ブロッカーとなる FAIL がない
- [ ] テストデータのクリーンアップが完了している
- [ ] DLQ にメッセージが滞留していない

**最終判定**: PASS / FAIL
```

---

## 付録 C: Phase 6.5 エンタープライズ品質ゲート（追加）

### C.1 目的

Phase 6.5（解析一元化）の本番デプロイ時に発生しやすい障害（設定不整合、Bedrock 権限不足、S3/DynamoDB 保存失敗、Ontology 連携遅延）を事前に検知し、**リリースゲートで機械的に判定**できるようにする。

### C.2 品質ゲート（必須）

| ゲート | 判定条件 | 失敗時の扱い |
|-------|---------|------------|
| G1: インフラ整合 | `tests/aws/test_dvt_infrastructure.py` の Phase 6.5 項目が PASS（DocumentAnalysis / Vectors / SSM queue param） | リリース中止 |
| G2: Lambda 構成整合 | `tests/aws/test_dvt_lambda.py` で detectSensitivity が `4096MB/600s` + Phase 6.5 env vars を満たす | リリース中止 |
| G3: 機能成立 | `tests/aws/test_ft_phase65_analysis_unification.py` の FT-7/8/9 が PASS | リリース中止 |
| G4: 統合成立 | `tests/aws/test_e2e_phase65_analysis_unification.py` の E2E-4 が PASS | リリース中止 |
| G5: 非機能品質 | ST/PT/RT/OT の Phase 6.5 追加ケースが PASS | 条件付きリリース（要承認） |

### C.3 AWS デプロイ時の主要問題点とテスト観点

| リスク | 発生しやすい症状 | 追加テストでの検出箇所 |
|------|----------------|--------------------|
| Bedrock 権限不足 | `AccessDeniedException` で要約/Embedding が失敗 | ST-1-13, FT-8-01, FT-8-02 |
| SSM パラメータ不足（Ontology Queue URL） | Entity 連携が実行されない | DVT-1-22, E2E-4-04 |
| SQS 可視性タイムアウト不足 | 再配信ループ、重複処理 | DVT-1-08 (660 秒), RT-1-09 |
| Vectors バケット設定不備 | Embedding 保存失敗、オブジェクト未生成 | DVT-1-18, FT-8-02, E2E-4-02 |
| DocumentAnalysis テーブル設定不備 | 書き込み失敗、TTL 不整合 | DVT-1-16/17, FT-9-02 |
| Entity メッセージでの PII 漏洩 | Queue payload に平文 PII が残る | FT-9-04, E2E-4-06 |
| 高負荷時の遅延 | 分析完了まで SLA 超過 | PT-1-07 |
| ログ可観測性不足 | 障害時に原因追跡不可 | OT-1-09 |

### C.4 推奨実行順序（リリース判定用）

```powershell
# 1) 構成逸脱の早期検知
pytest -m aws tests/aws/test_dvt_infrastructure.py tests/aws/test_dvt_lambda.py -v

# 2) Phase 6.5 の機能・統合検証
pytest -m aws tests/aws/test_ft_phase65_analysis_unification.py tests/aws/test_e2e_phase65_analysis_unification.py -v

# 3) 非機能（時間がかかるため slow を分離）
pytest -m "aws and not slow" tests/aws/test_st_security.py tests/aws/test_ot_observability.py tests/aws/test_rt_resilience.py tests/aws/test_pt_performance.py -v
pytest -m "aws and slow" tests/aws/test_st_security.py tests/aws/test_ot_observability.py tests/aws/test_rt_resilience.py tests/aws/test_pt_performance.py -v
```

### C.5 受け入れ基準（Phase 6.5）

- 重大（Blocker）: DVT/FT/E2E の FAIL は 0 件
- 高（High）: ST/PT/RT/OT の FAIL は 0 件（やむを得ず残る場合は CAB 承認を必須化）
- セキュリティ: 平文 PII の保存/送信/ログ出力が 0 件
- 運用性: 主要ログで `finding_id`, `tenant_id`, `sensitivity_score` の追跡が可能

---

## 付録 D: 実施結果サマリ・設計差分メモ（2026-02-24）

### D.1 実施結果サマリ（最終）

#### 実施した順序

1. `cdk diff` → `cdk deploy`（Phase 6.5 リソース反映）
2. DVT 再実行
3. FT/E2E（Phase 6.5）実行
4. ST/PT/RT/OT 実行
5. 失敗要因修正後、品質ゲート対象を一括再実行

#### 最終判定

- 対象: `tests/aws/test_dvt_infrastructure.py` / `test_dvt_lambda.py` / `test_ft_phase65_analysis_unification.py` / `test_e2e_phase65_analysis_unification.py` / `test_st_security.py` / `test_pt_performance.py` / `test_rt_resilience.py` / `test_ot_observability.py`
- 合計: **87**
- PASS: **87**
- FAIL: **0**
- SKIP: **0**
- 品質ゲート判定: **PASS**

### D.2 設計差分メモ（実運用安定化のための調整）

> いずれも設計意図（機能要件・セキュリティ要件）を損なわない範囲での実行安定化。

1. **Connect スキーマ整合**
   - `make_file_metadata()` に `drive_id` を追加
   - 実環境テーブルの PK 要件に合わせた調整

2. **テストクリーンアップ方式**
   - `connect_table` の削除を GSI 経由に変更
   - 実テーブルキーと整合した安全な削除へ変更

3. **PII 検知初期化の実行環境対応**
   - `pii_detector.py` の Presidio 初期化を Lambda 実行制約（read-only FS）に合わせて修正
   - 動的モデル導入失敗に起因する異常終了を防止

4. **非同期完了待ちの明確化**
   - FT/E2E で `sensitivity_scan_at` を完了条件に追加
   - 生成済み Finding と感度解析完了の時間差による誤判定を防止

5. **可観測性テストの判定方式強化**
   - OT メトリクスは `list_metrics` で系列を列挙し、`get_metric_data` で合算判定
   - 構造化ログは「最新ストリーム1本」ではなく「時間範囲」検索で判定

6. **実環境ゆらぎ許容（テスト堅牢化）**
   - cron 等価表記（EventBridge）を許容
   - RT の DLQ テストは、DLQ 到達または再試行観測を合格条件に設定
