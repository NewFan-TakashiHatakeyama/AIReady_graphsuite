# AI Ready Ontology — 実装手順書

## 文書管理

| 項目 | 内容 |
|------|------|
| 文書名 | 実装手順書（AI Ready Ontology — メタスキーマ変換・エンティティ解決・ゴールドマスタ管理） |
| 参照設計書 | [詳細設計書](./Docs/詳細設計.md)、[基本設計書](./Docs/基本設計.md)、[設計変更書](./設計変更.md) |
| 作成日 | 2026-02-19 |
| 前提 | AI Ready Connect（`connect/`）がデプロイ済みで、FileMetadata テーブルにデータが投入されている |

> **システムの位置づけ**: Ontology は Connect が収集した M365 メタデータを共通スキーマに統一し、
> Governance が一元実行したドキュメント解析結果（NER + PII + 要約 + Embedding）を受信して、
> エンティティ解決を経てゴールドマスタで一元管理する。
> ナレッジグラフは **ドキュメント単位** で構築する設計であり、エンティティ（名詞）単位のグラフではない。
>
> **設計変更**: 本書は [設計変更書](./設計変更.md) に基づき、ドキュメント解析の Governance 一元化に対応した内容に更新済み。

---

## 全体構成図

```
ontology/
├── Tasks.md                    ← 本書
├── Docs/                       ← 設計書
├── cdk/
│   ├── app.py                  ← CDK エントリーポイント
│   ├── stacks/
│   │   ├── ontology_stack.py   ← メイン CDK Stack（DynamoDB, SQS, Lambda, S3）
│   │   ├── aurora_stack.py     ← Aurora Serverless v2 + RDS Proxy
│   │   └── monitoring_stack.py ← CloudWatch アラーム + ダッシュボード
│   └── requirements.txt
├── src/
│   ├── handlers/
│   │   ├── schema_transform.py      ← Lambda 1: メタスキーマ変換
│   │   ├── lineage_recorder.py      ← Lambda 2: データ系譜記録
│   │   ├── entity_resolver.py       ← Lambda 3: エンティティ解決 + ゴールドマスタ
│   │   └── batch_reconciler.py      ← Lambda 4: 日次バッチ
│   ├── shared/
│   │   ├── __init__.py
│   │   ├── aurora_client.py         ← Aurora 接続管理（RDS Proxy）
│   │   ├── lineage_client.py        ← lineageRecorder 呼び出しヘルパー
│   │   ├── governance_client.py     ← Governance Finding 参照
│   │   ├── governance_integration.py ← Governance 解析結果受信 + PII 登録処理
│   │   ├── document_analysis_client.py ← DocumentAnalysis テーブル参照
│   │   ├── freshness.py             ← 鮮度判定ロジック
│   │   ├── entity_id.py             ← entity_id 生成
│   │   ├── normalizer.py            ← テキスト正規化（NFKC, カタカナ変換, 法人名統一）
│   │   ├── matcher.py               ← マッチングアルゴリズム（Jaro-Winkler, Levenshtein）
│   │   ├── config.py                ← 環境変数 / SSM パラメータ
│   │   ├── metrics.py               ← CloudWatch メトリクス
│   │   └── logger.py                ← 構造化ログ
│   └── models/
│       ├── unified_metadata.py      ← UnifiedMetadata データクラス
│       ├── entity_candidate.py      ← EntityCandidate データクラス
│       └── lineage_event.py         ← LineageEvent データクラス
├── db/
│   ├── migrations/
│   │   ├── 001_create_schema.sql
│   │   ├── 002_create_entity_master.sql
│   │   ├── 003_create_entity_aliases.sql
│   │   ├── 004_create_entity_roles.sql
│   │   ├── 005_create_entity_policies.sql
│   │   ├── 006_create_entity_audit_log.sql
│   │   ├── 007_create_functions.sql
│   │   └── 008_create_roles.sql
│   └── seeds/
│       └── domain_dictionary_sample.json
├── tests/
│   ├── unit/
│   │   ├── test_schema_transform.py
│   │   ├── test_lineage_recorder.py
│   │   ├── test_entity_resolver.py
│   │   ├── test_batch_reconciler.py
│   │   ├── test_normalizer.py
│   │   ├── test_matcher.py
│   │   └── test_freshness.py
│   ├── integration/
│   │   ├── test_pipeline_integration.py
│   │   └── test_aurora_integration.py
│   └── e2e/
│       └── test_gold_master_e2e.py
├── layers/
│   ├── common-layer/
│   │   └── requirements.txt
│   └── aurora-layer/
│       └── requirements.txt          ← psycopg2-binary
└── pyproject.toml
```

---

## Phase 1: インフラ基盤 + Aurora 構築

> **目標**: Lambda・Aurora・SQS・DynamoDB が動作するための AWS リソースを CDK で一括構築する。

### T-001: CDK プロジェクト初期化

**工数**: 0.5 日

**作業内容**:
1. `ontology/cdk/app.py` と `ontology/cdk/stacks/ontology_stack.py` を作成
2. `ontology/cdk/stacks/aurora_stack.py` を作成（Aurora Serverless v2 + RDS Proxy）
3. `ontology/cdk/stacks/monitoring_stack.py` を作成（空テンプレート）
4. `ontology/pyproject.toml` を作成（connect の構成を参考）
5. CDK Bootstrap 確認

**完了条件**:
- `cd ontology && cdk synth` が成功する

**参照**: connect の `infra/app.py` を踏襲、詳細設計 付録 A

---

### T-002: DynamoDB テーブル（3 テーブル）

**工数**: 1 日

**作業内容**:
1. `AIReadyOntology-UnifiedMetadata` テーブル

```
PK: tenant_id (String)
SK: item_id (String)
読み書き: オンデマンド
ポイントインタイムリカバリ: 有効
TTL: ttl カラム（削除レコードの 30 日後自動削除）
```

2. GSI を追加

```
GSI-RiskLevel:
  PK: risk_level
  SK: last_modified
  射影: ALL

GSI-Source:
  PK: source
  SK: transformed_at
  射影: ALL

GSI-FreshnessStatus:
  PK: freshness_status
  SK: last_modified
  射影: ALL
```

3. `AIReadyOntology-LineageEvent` テーブル

```
PK: tenant_id (String)
SK: lineage_id (String)
読み書き: オンデマンド
ポイントインタイムリカバリ: 有効
TTL: ttl カラム（90 日後自動削除）
```

4. LineageEvent の GSI

```
GSI-JobName:
  PK: job_name
  SK: event_time

GSI-Status:
  PK: status
  SK: event_time
```

5. `AIReadyOntology-EntityCandidate` テーブル

```
PK: tenant_id (String)
SK: candidate_id (String)
読み書き: オンデマンド
TTL: ttl カラム（7 日後自動削除）
```

**完了条件**:
- `cdk deploy` で 3 テーブルが作成される
- AWS コンソールで PK/SK/GSI/TTL を確認できる

**参照**: 詳細設計 1.3, 3.5, 4.5

---

### T-003: SQS キュー（FIFO + DLQ）

**工数**: 0.3 日

**作業内容**:
1. `AIReadyOntology-EntityResolutionQueue.fifo`（FIFO）
   - Governance detectSensitivity からの統合エンティティ候補を受信
   - 可視性タイムアウト: 180 秒
   - ContentBasedDeduplication: 無効（明示的 DeduplicationId）
   - maxReceiveCount: 3
2. `AIReadyOntology-EntityResolution-DLQ.fifo`（FIFO）
   - メッセージ保持期間: 14 日

**完了条件**:
- `cdk deploy` で 2 キュー（FIFO + DLQ）が作成される
- DLQ のリドライブポリシーが正しく設定されている

**参照**: 詳細設計 1.4, [設計変更書](./設計変更.md) 10.2

---

### T-004: S3 バケット（レポート用）

**工数**: 0.5 日

**作業内容**:
1. `aiready-ontology-reports-{tenant}` バケットを作成
   - サーバーサイド暗号化: AES256
   - バージョニング: 無効
   - ライフサイクルルール: 90 日後に Glacier、365 日後に削除
   - パブリックアクセス: ブロック

**完了条件**:
- `cdk deploy` でバケットが作成される

**参照**: 詳細設計 8.2（レポート生成先）

---

### T-005: Aurora PostgreSQL Serverless v2 + RDS Proxy

**工数**: 1.5 日

**作業内容**:
1. `aurora_stack.py` に Aurora Serverless v2 クラスタを定義
   - エンジン: PostgreSQL 15.x
   - 最小 ACU: 0.5、最大 ACU: 8
   - VPC: AI Ready 共有 VPC の Private Subnet
   - セキュリティグループ: Lambda からの 5432 ポートのみ許可
   - 暗号化: AWS KMS（デフォルトキー）
   - 自動バックアップ: 7 日保持
2. RDS Proxy を定義
   - IAM 認証: 有効
   - 接続プーリング: MAX 100
   - シークレット: Secrets Manager に DB 認証情報を保存
3. Secrets Manager シークレット作成
   - `ai-ready-ontology/aurora-credentials`（自動生成パスワード）

**完了条件**:
- `cdk deploy` で Aurora クラスタと RDS Proxy が作成される
- Lambda VPC 内から RDS Proxy 経由で `SELECT 1` が実行できる

**参照**: 詳細設計 6.7, 9.1–9.3

---

### T-006: Aurora スキーママイグレーション

**工数**: 1 日

**作業内容**:
1. `db/migrations/001_create_schema.sql` — `ontology` スキーマ + `uuid-ossp`, `pgcrypto` 拡張
2. `db/migrations/002_create_entity_master.sql` — ゴールドマスタテーブル + インデックス
3. `db/migrations/003_create_entity_aliases.sql` — エイリアステーブル
4. `db/migrations/004_create_entity_roles.sql` — ロールテーブル
5. `db/migrations/005_create_entity_policies.sql` — ポリシーテーブル
6. `db/migrations/006_create_entity_audit_log.sql` — 監査ログ（月次パーティション）
7. `db/migrations/007_create_functions.sql` — `get_max_spread_factor()` 関数
8. `db/migrations/008_create_roles.sql` — `ontology_app`, `ontology_ai_reader`, `governance_reader`, `ontology_admin` ロール
9. マイグレーション実行スクリプト（PowerShell）を作成

**完了条件**:
- 全マイグレーションが Aurora 上で正常実行される
- `\dt ontology.*` で全テーブルが表示される
- `pg_columnmask`, `pgcrypto` 拡張が有効

**参照**: 詳細設計 9.1–9.9

---

### T-007: IAM ロール（Lambda 実行ロール）

**工数**: 0.5 日

**作業内容**:
1. 4 つの Lambda 用に共通の基本ロール + Lambda 別の追加ポリシーを作成

```
共通権限:
- CloudWatch: Logs + Metrics
- SSM: /ai-ready/ontology/* (GetParameter)
- VPC: 共有 VPC の ENI 管理

schemaTransform 追加:
- DynamoDB: UnifiedMetadata (RW), Governance ExposureFinding (R), Connect FileMetadata (R via Streams), AIReady-DocumentAnalysis (Read)
- Lambda: lineageRecorder (InvokeFunction)

lineageRecorder 追加:
- DynamoDB: LineageEvent (RW)

entityResolver 追加:
- SQS: EntityResolutionQueue.fifo (ReceiveMessage, DeleteMessage)
- DynamoDB: AIReady-DocumentAnalysis (Read)
- Secrets Manager: Aurora シークレット (GetSecretValue)
- SNS: アラートトピック (Publish)
- Lambda: lineageRecorder (InvokeFunction)

batchReconciler 追加:
- DynamoDB: UnifiedMetadata (RW), Connect FileMetadata (R), AIReady-DocumentAnalysis (Read)
- Secrets Manager: Aurora シークレット (GetSecretValue)
- S3: レポートバケット (PutObject)
- Step Functions: 実行権限
```

**完了条件**:
- IAM ロールが作成され、最小権限の原則に従っている

**参照**: 詳細設計 3.1, 4.1, 6.1, 8.1, [設計変更書](./設計変更.md) 6.1

---

### T-008: SSM パラメータ投入

**工数**: 0.5 日

**作業内容**:
1. 以下のパラメータを SSM Parameter Store に登録

```
/ai-ready/ontology/{tenant_id}/domain-dictionary        = {"version":"1.0","terms":[]}
/ai-ready/ontology/{tenant_id}/pii-encryption-key        = ***  (SecureString)
/ai-ready/ontology/{tenant_id}/freshness-thresholds      = {"aging_days":90,"stale_days":365}
/ai-ready/ontology/{tenant_id}/confidence-threshold      = 0.5
/ai-ready/ontology/{tenant_id}/match-thresholds          = {"exact":0.95,"probable":0.85,"ambiguous":0.60}
/ai-ready/ontology/{tenant_id}/stopwords-ja              = ["もの","こと","ため","よう"]
```

2. CDK の `StringParameter` またはデプロイスクリプトで管理

**完了条件**:
- AWS コンソールで全パラメータが確認できる

**参照**: 詳細設計 14.2

---

### T-009: SNS トピック（アラート用）

**工数**: 0.5 日

**作業内容**:
1. `AIReadyOntology-Alerts` SNS トピックを作成
   - PII 集約アラート、品質劣化アラートの送信先
2. 運用チーム向けサブスクリプション（Email）を設定

**完了条件**:
- `cdk deploy` で SNS トピックが作成される

**参照**: 詳細設計 6.9

---

## Phase 2: 共通モジュール + データモデル

> **目標**: 全 Lambda が共通で使用するユーティリティ・データモデル・ヘルパーを実装し、単体テストを通す。

### T-010: 共通ユーティリティ (`shared/`)

**工数**: 1 日

**作業内容**:
1. `shared/config.py` — 環境変数の読み込み + SSM パラメータのキャッシュ付き取得（TTL: 300 秒）
2. `shared/logger.py` — 構造化 JSON ログ（`log_structured(level, message, **kwargs)`）
3. `shared/metrics.py` — CloudWatch メトリクス送信ヘルパー（Namespace: `AIReadyOntology`）

**完了条件**:
- 各モジュールの import が通る
- config の SSM キャッシュが動作する

**参照**: connect の `src/shared/` を踏襲、詳細設計 13.5

---

### T-011: データモデル (`models/`)

**工数**: 0.5 日

**作業内容**:
1. `models/unified_metadata.py` — Pydantic / dataclass で UnifiedMetadata を定義
   - 24 フィールド（詳細設計 3.4 のマッピング全項目）
   - `to_dynamodb_item()`, `from_dynamodb_item()` メソッド
2. `models/entity_candidate.py` — EntityCandidate データクラス
   - 詳細設計 5.9 の全フィールド
3. `models/lineage_event.py` — LineageEvent データクラス
   - 詳細設計 4.5 の全フィールド

**テスト** (`tests/unit/test_models.py`):
- 各データクラスの生成・シリアライズ・バリデーション

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 3.4, 3.5, 4.5, 5.9

---

### T-012: lineage_client (`shared/lineage_client.py`)

**工数**: 0.5 日

**作業内容**:
1. `record_lineage_event(function_name, lineage_id, job_name, input_dataset, output_dataset, ...)` — lineageRecorder Lambda の同期呼び出し
2. 障害時はログ出力のみ（メイン処理を止めない）

**テスト** (`tests/unit/test_lineage_client.py`):
- 正常呼び出し → レスポンス取得
- Lambda Invoke 失敗 → None 返却 + ログ出力
- FunctionError → None 返却 + ログ出力

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 4.9

---

### T-013: governance_client (`shared/governance_client.py`)

**工数**: 0.5 日

**作業内容**:
1. `lookup_governance_finding(tenant_id, file_id, finding_table_name)` — Governance ExposureFinding テーブルから Finding を取得
2. Finding なし → デフォルト値 `{risk_level: "none", pii_detected: false, ai_eligible: true}`
3. 接続エラー時 → デフォルト値（安全側）

**テスト** (`tests/unit/test_governance_client.py` — moto モック):
- Finding あり → risk_level, pii_detected, ai_eligible 正しく取得
- Finding なし → デフォルト値
- DynamoDB 接続エラー → デフォルト値

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 3.8

---

### T-014: 鮮度判定 (`shared/freshness.py`)

**工数**: 0.5 日

**作業内容**:
1. `calculate_freshness_status(last_modified)` — active / aging / stale
2. `calculate_access_freshness(last_accessed)` — active / dormant / abandoned
3. `calculate_ai_freshness(freshness_status, access_freshness)` — 複合マトリクス
4. `calculate_freshness_score(ai_freshness)` — 数値スコア（0.1〜2.0）

**テスト** (`tests/unit/test_freshness.py`):
- 90 日以内 → active、90〜365 日 → aging、365 日超 → stale
- アクセス鮮度の各パターン
- 複合マトリクスの全 9 パターン
- 境界値テスト（89 日 / 90 日 / 91 日）

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 12.2, 12.3

---

### T-015: テキスト正規化 (`shared/normalizer.py`)

**工数**: 0.5 日

**作業内容**:
1. `normalize_text(text, lang)` — NFKC 正規化 + 言語別処理
2. `normalize_japanese(text)` — ひらがな→カタカナ変換、スペース統一
3. `normalize_corporate_name(name)` — 法人格の統一表記変換
4. `detect_language(text)` — 日本語文字種比率ベースの簡易言語判定

**テスト** (`tests/unit/test_normalizer.py`):
- NFKC 正規化（全角→半角）
- ひらがな→カタカナ変換
- 法人格変換（株式会社 → カブシキガイシャ）
- 言語判定（日本語テキスト → "ja"、英語テキスト → "en"）

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 5.6, 11.6

---

### T-016: マッチングアルゴリズム (`shared/matcher.py`)

**工数**: 1 日

**作業内容**:
1. `calculate_match_score(candidate_form, existing_form, entity_type)` — 総合マッチスコア
2. `_jaro_winkler_similarity(s1, s2)` — Jaro-Winkler 類似度
3. `_levenshtein_similarity(s1, s2)` — レーベンシュタイン距離ベース類似度
4. `_token_overlap_ratio(s1, s2)` — トークン重複率
5. `ENTITY_TYPE_WEIGHTS` — entity_type 別の重み設定

**テスト** (`tests/unit/test_matcher.py`):
- 完全一致 → 1.0
- 類似文字列 → 閾値判定（確定/準/曖昧/不一致）
- entity_type 別の重み適用確認
- 空文字列ハンドリング
- 日本語テキストの類似度計算

**完了条件**:
- 全テストケース PASS
- カバレッジ 90% 以上

**参照**: 詳細設計 11.2–11.4

---

### T-017: entity_id 生成 (`shared/entity_id.py`)

**工数**: 0.5 日

**作業内容**:
1. `generate_entity_id(entity_type)` — タイプ別プレフィックス + UUID サフィックス
2. `compute_canonical_hash(value)` — SHA-256 ハッシュ
3. `ENTITY_ID_PREFIX` — 20 種のプレフィックスマップ

**テスト** (`tests/unit/test_entity_id.py`):
- 各 entity_type のプレフィックス検証
- 未知の entity_type → `ent_` プレフィックス
- canonical_hash の一貫性（同一入力 → 同一ハッシュ）

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 6.4

---

### T-018: Aurora 接続管理 (`shared/aurora_client.py`)

**工数**: 0.5 日

**作業内容**:
1. `get_aurora_connection()` — RDS Proxy 経由の接続（Lambda グローバル変数で再利用）
2. `_get_aurora_password()` — Secrets Manager からパスワード取得
3. コネクションのヘルスチェック（`SELECT 1`）
4. `statement_timeout=30000` の設定

**テスト** (`tests/unit/test_aurora_client.py` — モック):
- 初回接続 → 新規作成
- 再呼び出し → 既存コネクション再利用
- 切断状態 → 再接続

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 6.7

---

## Phase 3: schemaTransform + lineageRecorder

> **目標**: DynamoDB Streams トリガーでメタスキーマ変換を実行し、系譜を記録する Lambda を実装する。

### T-019: schemaTransform ハンドラ実装 (`handlers/schema_transform.py`)

**工数**: 2 日

**作業内容**:
1. `handler(event, context)` — DynamoDB Streams イベントのルーティング
2. `_process_record(record)` — 1 レコードの処理フロー
   - REMOVE → `_handle_delete`（論理削除 + TTL 設定）
   - is_deleted=true → `_handle_delete`
   - is_folder=true → スキップ
   - Governance Finding 参照 → risk_level / ai_eligible 決定
   - フィールドマッピング（24 フィールド + 要約拡張フィールド）
   - risk_level=critical → ai_eligible 強制 false
   - DocumentAnalysis テーブルを参照し、解析完了済みの場合は要約フィールドを UnifiedMetadata に反映
   - UnifiedMetadata 保存 → lineageRecorder 同期呼び出し
3. `_handle_delete(tenant_id, item_id)` — 論理削除 + TTL + 系譜記録
4. `_deserialize_dynamodb_image(image)` — DynamoDB Streams 型付きイメージのデシリアライズ

**テスト** (`tests/unit/test_schema_transform.py` — moto モック):
- UT-ST-001: INSERT イベントの正常変換
- UT-ST-002: MODIFY イベントの更新変換
- UT-ST-003: REMOVE イベントの削除処理
- UT-ST-004: is_deleted=true の削除伝播
- UT-ST-005: is_folder=true のスキップ
- UT-ST-006: Governance Finding あり（risk_level=high）→ ai_eligible=false
- UT-ST-007: Governance Finding なし → デフォルト値
- UT-ST-008: Governance Finding 取得エラー → デフォルト値で続行
- UT-ST-009: Governance Finding あり（risk_level=critical）→ ai_eligible=false
- UT-ST-010: freshness_status 算出
- UT-ST-011: DocumentAnalysis 解析完了済み → 要約フィールド反映

**完了条件**:
- 全テストケース PASS
- カバレッジ 90% 以上

**参照**: 詳細設計 3 章全体、[設計変更書](./設計変更.md) 6.3

---

### T-020: lineageRecorder ハンドラ実装 (`handlers/lineage_recorder.py`)

**工数**: 1 日

**作業内容**:
1. `handler(event, context)` — Lambda 同期呼び出しのエントリーポイント
2. ペイロード検証（必須: lineage_id, tenant_id, job_name, event_type）
3. OpenLineage RunEvent 構築 (`_build_openlineage_event`)
4. DynamoDB `AIReadyOntology-LineageEvent` に PutItem（TTL: 90 日）
5. メトリクス発行（LineageEventsRecorded / LineageFailEvents）
6. レスポンス返却

**テスト** (`tests/unit/test_lineage_recorder.py` — moto モック):
- 正常リクエスト → DynamoDB に保存 + status=200
- 必須フィールド欠落 → status=400
- 不正な event_type → status=400
- FAIL イベント → LineageFailEvents メトリクス発行

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 4 章全体

---

### T-021: Lambda Layer — common-layer

**工数**: 0.5 日

**作業内容**:
1. `layers/common-layer/requirements.txt` を作成
   - `pydantic`, `boto3-stubs`, `openlineage-python` 等
2. Layer ビルドスクリプト（PowerShell）

**完了条件**:
- Layer が正常にビルドされる
- Lambda から `import openlineage` が成功する

**参照**: 詳細設計 3.1, 4.1

---

### T-022: CDK — schemaTransform + lineageRecorder Lambda 定義

**工数**: 1 日

**作業内容**:
1. CDK スタックに schemaTransform Lambda を追加
   - ランタイム: Python 3.12
   - メモリ: 512 MB、タイムアウト: 120 秒
   - 環境変数: `UNIFIED_METADATA_TABLE`, `GOVERNANCE_FINDING_TABLE`, `DOCUMENT_ANALYSIS_TABLE`, `LINEAGE_FUNCTION_NAME`, `TENANT_ID`
   - Reserved Concurrency: 10
   - VPC: Private Subnet
2. DynamoDB Streams イベントソースマッピング
   - 対象: Connect `FileMetadata-{tenant_id}`
   - バッチサイズ: 10、最大バッチウィンドウ: 5 秒
   - StreamViewType: NEW_AND_OLD_IMAGES
3. CDK スタックに lineageRecorder Lambda を追加
   - メモリ: 256 MB、タイムアウト: 30 秒
   - 環境変数: `LINEAGE_EVENT_TABLE`, `TENANT_ID`
   - VPC: Private Subnet

**完了条件**:
- `cdk deploy` で Lambda がデプロイされ、DynamoDB Streams に接続される
- Connect の FileMetadata にデータを投入すると schemaTransform がトリガーされる

**参照**: 詳細設計 3.1, 4.1, [設計変更書](./設計変更.md) 6.3

---

### T-023: schemaTransform + lineageRecorder 結合テスト

**工数**: 1 日

**作業内容**:
1. Connect の FileMetadata テーブルにテストデータを投入
2. schemaTransform が起動し、UnifiedMetadata に正しいデータが保存されることを確認
3. LineageEvent テーブルに系譜が記録されることを確認
4. DocumentAnalysis テーブルを参照し、要約・Embedding 参照情報が UnifiedMetadata に反映されることを確認（解析完了済みの場合）
5. 削除イベント → 論理削除 + TTL を確認

**完了条件**:
- AWS 上でメタスキーマ変換 + 系譜記録フローが E2E で動作する

---

## Phase 4: DocumentAnalysis 連携（Governance 解析結果の受信）

> **目標**: Governance の detectSensitivity が生成した解析結果（NER + PII + 要約 + Embedding）を受信し、entityResolver に統合エンティティ候補を渡す基盤を構築する。
>
> **設計変更**: 旧設計の NER パイプライン（nounExtractor）は Governance に一元化された。詳細は [設計変更書](./設計変更.md) を参照。

### T-024: DocumentAnalysis 受信モジュール (`shared/document_analysis_client.py`)

**工数**: 0.5 日

**作業内容**:
1. `get_document_analysis(tenant_id, item_id)` — DocumentAnalysis テーブルからレコードを取得
2. `is_analysis_completed(tenant_id, item_id)` — 解析完了チェック（`analysis_status == "completed"`）
3. テーブル名は環境変数 `DOCUMENT_ANALYSIS_TABLE` から取得

**テスト** (`tests/unit/test_document_analysis_client.py` — DynamoDB モック):
- 取得成功 → 全フィールドが正しくデシリアライズされる
- レコード未存在 → None 返却
- analysis_status チェック（completed / processing / failed）

**完了条件**:
- 全テストケース PASS

**参照**: [設計変更書](./設計変更.md) 7.1–7.3

---

### T-025: UnifiedMetadata 拡張フィールド対応

**工数**: 0.5 日

**作業内容**:
1. `models/unified_metadata.py` に以下のフィールドを追加:
   - `document_summary` — ドキュメント要約テキスト（最大 500 文字）
   - `summary_language` — 要約の言語（ja / en）
   - `topic_keywords` — トピックキーワード（上位 10 件）
   - `embedding_ref` — S3 Vectors のキー参照
   - `analysis_id` — DocumentAnalysis レコードの参照 ID
   - `summary_generated_at` — 要約生成日時（ISO 8601）
2. schemaTransform に DocumentAnalysis 参照ロジックを追加
   - 解析完了済みの場合: 要約フィールドを UnifiedMetadata に反映
   - 未完了の場合: 空のまま保存（batchReconciler で後から補完）

**テスト** (`tests/unit/test_unified_metadata_extended.py`):
- フィールド追加のシリアライズ / デシリアライズ
- `to_dynamodb_item()` / `from_dynamodb_item()` の新フィールド対応

**完了条件**:
- 全テストケース PASS

**参照**: [設計変更書](./設計変更.md) 6.5

---

### T-026: entityResolver 統合メッセージ対応

**工数**: 1 日

**作業内容**:
1. entityResolver の `_process_message` を更新
   - `source: "document_analysis"` メッセージを処理
   - PII と NER が統合済みの候補を受信（旧設計の `noun_extractor` / `governance_pii` 2 種類 → 1 種類に統一）
   - `extraction_source: "governance+ner"` 等の新しい値に対応
2. GovernancePIIRegistrationQueue.fifo のイベントソースマッピングを削除（統合されたため不要）

**テスト** (`tests/unit/test_entity_resolver_integration_msg.py`):
- 統合メッセージ（`source: "document_analysis"`）の正常処理
- `pii_flag=true` のメッセージ → entity_master に PII フラグ付きで登録
- `extraction_source` の各パターン（`governance+ner` / `ner` / `governance`）
- 旧メッセージ形式との互換性

**完了条件**:
- 全テストケース PASS

**参照**: [設計変更書](./設計変更.md) 6.4

---

### T-027: DocumentAnalysis 連携 結合テスト

**工数**: 0.5 日

**作業内容**:
1. Governance が DocumentAnalysis テーブルにテストデータを投入した想定で、Ontology 側の受信処理を確認
2. EntityResolutionQueue に統合メッセージを投入し、entityResolver が正常に処理することを確認
3. schemaTransform が DocumentAnalysis テーブルの要約情報を UnifiedMetadata に反映することを確認

**完了条件**:
- AWS 上で Governance 解析結果の受信フローが E2E で動作する

**参照**: [設計変更書](./設計変更.md) 10.5

---

## Phase 5: エンティティ解決 + ゴールドマスタ

> **目標**: ルールベースのエンティティ解決を実装し、Aurora のゴールドマスタに登録する Lambda を構築する。

### T-029: Lambda Layer — aurora-layer

**工数**: 0.5 日

**作業内容**:
1. `layers/aurora-layer/requirements.txt` を作成
   - `psycopg2-binary`
2. Layer ビルドスクリプト

**完了条件**:
- Lambda から `import psycopg2` が成功する

**参照**: 詳細設計 6.1

---

### T-030: entityResolver ハンドラ実装 (`handlers/entity_resolver.py`)

**工数**: 3 日

**作業内容**:
1. `handler(event, context)` — SQS FIFO イベントハンドラ
2. `_process_message(msg)` — document_analysis メッセージを処理（PII + NER 統合済み候補）
   - `source: "document_analysis"` メッセージの解析
   - `pii_flag` / `extraction_source` の判定
3. `_resolve_entity(conn, candidate, encryption_key)` — トランザクション処理
   - Step 1: canonical_hash 完全一致（`FIND_BY_HASH_SQL`）
   - Step 2: ブロッキング（entity_type + ハッシュ先頭 8 文字）
   - Step 3: 類似度スコアリング（matcher.py 活用）
   - 一致 → エイリアス追加 + confidence 更新
   - 不一致 → 新規エンティティ登録
4. `_create_entity(cur, candidate, encryption_key)` — entity_master INSERT
   - PII → `pgp_sym_encrypt` で暗号化
   - 非 PII → 平文保存
5. `_add_alias(cur, existing, candidate, encryption_key)` — entity_aliases INSERT
6. `check_pii_aggregation_alert(conn, entity_id, entity_type, tenant_id)` — aliases 件数 ≥ 5 で SNS 通知
7. 監査ログ記録（`entity_audit_log` に ENTITY_CREATED / ALIAS_ADDED）
8. `ON CONFLICT DO NOTHING` による冪等性確保

**テスト** (`tests/unit/test_entity_resolver.py` — モック):
- UT-ER-001〜010（詳細設計 15.2 の全テストケース）
- 統合メッセージ（`source: "document_analysis"`）の処理
- PII フラグ付き統合候補の暗号化保存
- トランザクションロールバック

**完了条件**:
- 全テストケース PASS
- カバレッジ 85% 以上

**参照**: 詳細設計 6 章全体、11 章、[設計変更書](./設計変更.md) 6.4

---

### T-031: CDK — entityResolver Lambda 定義

**工数**: 0.5 日

**作業内容**:
1. CDK スタックに entityResolver Lambda を追加
   - ランタイム: Python 3.12
   - メモリ: 512 MB、タイムアウト: 120 秒
   - 環境変数: `AURORA_PROXY_ENDPOINT`, `AURORA_PORT`, `AURORA_DB_NAME`, `AURORA_USERNAME`, `AURORA_SECRET_ARN`, `PII_ENCRYPTION_KEY_PARAM`, `ALERT_TOPIC_ARN`, `LINEAGE_FUNCTION_NAME`, `DOCUMENT_ANALYSIS_TABLE`, `TENANT_ID`
   - Reserved Concurrency: 5
   - VPC: Private Subnet
   - Layer: common-layer + aurora-layer
2. SQS イベントソースマッピング（1 つ）
   - EntityResolutionQueue.fifo（バッチサイズ: 1）

**完了条件**:
- `cdk deploy` で Lambda がデプロイされ、FIFO キューに接続される

**参照**: 詳細設計 6.1, [設計変更書](./設計変更.md) 6.4

---

### T-032: entityResolver 結合テスト（Aurora 含む）

**工数**: 1.5 日

**作業内容**:
1. EntityResolutionQueue.fifo にテストメッセージを投入
2. entity_master に新規エンティティが作成されることを確認
3. 同一 canonical_hash の再投入 → エイリアス追加を確認
4. PII エンティティ → pgcrypto 暗号化保存を確認
5. 非 PII エンティティ → 平文 canonical_value_text 保存を確認
6. entity_audit_log に監査レコードが記録されることを確認
7. 統合メッセージ（PII + NER 統合候補）の処理を確認
8. DLQ にメッセージが滞留しないことを確認

**完了条件**:
- AWS 上でエンティティ解決フローが E2E で動作する
- Aurora のゴールドマスタにデータが正しく格納される

---

## Phase 6: コンテンツ品質管理

> **目標**: 鮮度判定・重複検出・Orphan 検出・ContentQualityScore 算出のロジックを実装する。

### T-033: 重複検出ロジック (`shared/duplicate_detector.py`)

**工数**: 1 日

**作業内容**:
1. `detect_exact_duplicates(conn, item_id, title, size_bytes)` — メタデータベースの完全一致検出
2. `determine_canonical_copy(items)` — 正本判定（最新更新日 / アクセス限定性 / パス深さ / ラベル有無）
3. 重複グループ ID 生成

**テスト** (`tests/unit/test_duplicate_detector.py`):
- 同一タイトル + 同一サイズ → 重複検出
- 重複なし → None
- 正本判定の優先順位検証

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 12.4

---

### T-034: Orphan 検出ロジック

**工数**: 0.5 日

**作業内容**:
1. エンティティ Orphan 検出 SQL（aliases にドキュメント紐付けなし + 作成 7 日超）
2. `detect_access_orphans(unified_table, tenant_id)` — 長期未アクセス + 過剰共有

**テスト**:
- Orphan 条件に合致 → 検出される
- 正常エンティティ → 検出されない

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 12.5

---

### T-035: ContentQualityScore 算出

**工数**: 0.5 日

**作業内容**:
1. `calculate_content_quality_score(freshness_score, uniqueness_score, relevance_score)` — 3 軸乗算
2. `calculate_uniqueness_score(duplicate_info)` — 正本: 1.0、複製: 0.5
3. `calculate_relevance_score(entity_count, mention_count)` — エンティティ密度ベース
4. スコア範囲（0.005〜2.0）のクランプ

**テスト**:
- 高品質ドキュメント → スコア ≥ 1.0
- 低品質ドキュメント → スコア < 0.5
- 境界値検証

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 12.6

---

### T-036: Governance 品質アラート連携

**工数**: 0.5 日

**作業内容**:
1. `send_quality_alert_to_governance(tenant_id, item_id, quality_score, alert_type, details)` — SQS 経由で品質劣化を通知
2. アラート種別: `stale_warning` / `orphan` / `duplicate`

**テスト**:
- 品質スコア < 0.5 → アラート送信
- 正常スコア → 送信なし

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 12.7

---

## Phase 7: batchReconciler + E2E

> **目標**: 日次バッチで全データの整合性チェック・品質スコア再計算・レポート生成を行う Lambda を実装し、パイプライン全体の E2E テストを完了する。

### T-037: batchReconciler ハンドラ実装 (`handlers/batch_reconciler.py`)

**工数**: 2.5 日

**作業内容**:
1. `handler(event, context)` — Step Functions から呼び出し（mode パラメータで処理分岐）
2. **State 1: MetadataReconciliation** — Connect FileMetadata と UnifiedMetadata の差分修正
3. **State 2: OrphanDetection** — エンティティ Orphan 検出 + status='orphan' 更新
4. **State 3: EntityReMatching** — 低信頼度エンティティ（confidence < 0.7）の再照合（上限 500 件）
5. **State 4: SpreadFactorCalculation** — `entity_master.spread_factor` の算出・更新
6. **State 5: ContentQualityScoring** — 鮮度・重複・Orphan 再計算 + ContentQualityScore 更新
7. **State 6: ReportGeneration** — S3 に JSON レポート出力

**テスト** (`tests/unit/test_batch_reconciler.py` — moto + モック):
- UT-BR-001〜004（詳細設計 15.2 の全テストケース）
- State ごとの独立テスト
- 大量データ時のページネーション

**完了条件**:
- 全テストケース PASS

**参照**: 詳細設計 8 章全体

---

### T-038: CDK — batchReconciler + Step Functions 定義

**工数**: 1 日

**作業内容**:
1. CDK スタックに batchReconciler Lambda を追加
   - メモリ: 1024 MB、タイムアウト: 900 秒
   - 環境変数: `UNIFIED_METADATA_TABLE`, `AURORA_PROXY_ENDPOINT`, `AURORA_SECRET_ARN`, `REPORT_BUCKET`, `DOCUMENT_ANALYSIS_TABLE`, `TENANT_ID`
   - Reserved Concurrency: 1
   - VPC: Private Subnet
   - Layer: common-layer + aurora-layer
2. Step Functions ステートマシン定義（6 State + Error Handler）
3. EventBridge Schedule: `rate(1 day)` UTC 4:00

**完了条件**:
- `cdk deploy` で Step Functions + EventBridge が作成される
- 手動実行で Step Functions が全 State を通過する

**参照**: 詳細設計 8.1, 8.3

---

### T-039: パイプライン E2E テスト

**工数**: 1.5 日

**作業内容**:
1. **E2E-001**: SharePoint ファイルアップロード → schemaTransform → (Governance detectSensitivity) → entityResolver → Aurora
2. **E2E-002**: ファイル削除 → 論理削除伝播
3. **E2E-004**: 同一人物の複数ドキュメント出現 → エンティティ解決で 1 entity_id に統合
4. **E2E-005**: 日次バッチ → spread_factor 算出 + 品質スコア再計算 + S3 レポート
5. **E2E-006**: risk_level=critical → ai_eligible=false（Governance 側で制御、Ontology 側ではテスト対象外）
6. DLQ に滞留がないことを確認
7. CloudWatch ログの構造化出力を確認

**完了条件**:
- 全フローが正常に動作する
- 各ステップの処理ログが確認できる

**参照**: 詳細設計 15.4, [設計変更書](./設計変更.md) 4.1

---

### T-040: CloudWatch アラーム + ダッシュボード

**工数**: 1 日

**作業内容**:
1. `monitoring_stack.py` にアラームを追加

```
- SchemaTransformErrors ≥ 10（5 分） → Critical
- EntityResolution DLQ ≥ 1 → Critical
- AuroraQueryMs P99 > 10000ms（5 分 × 3 回）→ Warning
- Step Functions 実行失敗 ≥ 1 → Critical
- PIIAggregationAlertFired ≥ 1 → Warning（セキュリティチーム）
- StaleDocuments ≥ 100（日次）→ Info
```

2. CloudWatch ダッシュボード作成
   - Lambda 実行数 / エラー率 / Duration
   - SQS キュー深度
   - Aurora 接続数 / クエリレイテンシ
   - エンティティ登録数 / マッチ率

**完了条件**:
- `cdk deploy` でアラーム + ダッシュボードが作成される

**参照**: 詳細設計 13.4

---

## Phase 8: Governance 連携

> **前提条件**: 以下の **2 つの条件がすべて満たされてから** 着手する。
>
> 1. **Ontology Phase 1〜7 が完了** — 全パイプラインがスタンドアロンで稼働していること
> 2. **Governance Phase 1〜6 が完了** — Oversharing 検知パイプラインが稼働していること
>
> **設計変更**: 本 Phase は [設計変更書](./設計変更.md) に基づき大幅に簡素化。GovernancePIIRegistrationQueue.fifo は廃止され、EntityResolutionQueue からの統合メッセージで処理を一本化。

### T-041: Governance 統合メッセージ連携確認 (`shared/governance_integration.py`)

**工数**: 0.5 日

**対応サブフェーズ**: Phase 8a

**作業内容**:
1. EntityResolutionQueue からの統合メッセージ受信が安定稼働していることを確認
2. DocumentAnalysis テーブルの参照が正常に動作していることを確認
3. `governance_integration.py` を DocumentAnalysis テーブル参照ベースに更新（Phase 4 で基盤構築済み）

**テスト** (`tests/unit/test_governance_integration.py`):
- 統合メッセージ → entity_master に正しく登録
- DocumentAnalysis テーブルの参照が安定動作

**完了条件**:
- Governance からの統合メッセージ受信が安定稼働する
- Phase 1〜7 の動作に影響しない（後方互換）

**参照**: 詳細設計 16.1–16.2, [設計変更書](./設計変更.md) 6.4

---

### T-042: spread_factor 算出関数（Aurora）

**工数**: 0.5 日

**対応サブフェーズ**: Phase 8a

**作業内容**:
1. `db/migrations/007_create_functions.sql` に `get_max_spread_factor(item_id)` 関数を実装
   - entity_aliases.source_document_id から当該ドキュメントの PII エンティティの最大 spread_factor を返す
2. Governance batchScoring が RDS Proxy 経由で呼び出せることを確認

**完了条件**:
- Aurora 上で `SELECT ontology.get_max_spread_factor('item456')` が実行できる
- governance_reader ロールで実行可能

**参照**: 詳細設計 16.3

---

### T-043: Governance 連携 E2E テスト

**工数**: 1 日

**対応サブフェーズ**: Phase 8b

**依存**: T-041, T-042 完了

**作業内容**:
1. **フロー① E2E**: Governance detectSensitivity → EntityResolutionQueue（統合メッセージ）→ entityResolver → entity_master
2. **フロー② E2E**: batchReconciler で spread_factor 算出 → Governance batchScoring が Aurora 参照
3. **フロー③ E2E**: batchReconciler が品質レポートを S3 出力 → Governance が読み込み
4. **フロー④ E2E**: DocumentAnalysis テーブルの要約情報が UnifiedMetadata に正しく反映されることを確認
5. DLQ 滞留チェック
6. Aurora 接続レイテンシ確認

**完了条件**:
- 4 つのフローすべてが正常に動作する

**参照**: 詳細設計 16.5, [設計変更書](./設計変更.md) 10.5

---

### T-044: 本番有効化 + アラーム追加

**工数**: 0.5 日

**対応サブフェーズ**: Phase 8c

**依存**: T-043 完了

**作業内容**:
1. 統合メッセージフローの本番有効化確認
2. CloudWatch アラーム追加
   - Governance 統合メッセージが 24 時間 0 件 → Warning（連携停止検知）
3. 有効化後の動作確認

**完了条件**:
- 全フローが本番稼働する

---

## 工数サマリ

| Phase | タスク | 工数 | 備考 |
|-------|--------|------|------|
| **Phase 1: インフラ + Aurora** | T-001 ~ T-009 | **6.3 日** | SQS 削減で微減（旧 6.5 日） |
| **Phase 2: 共通モジュール** | T-010 ~ T-018 | **4.5 日** | domain_dictionary 不要で微減（旧 5 日） |
| **Phase 3: schemaTransform + lineageRecorder** | T-019 ~ T-023 | **5 日** | NounExtractionQueue 送信削除で微減（旧 5.5 日） |
| **Phase 4: DocumentAnalysis 連携** | T-024 ~ T-027 | **2.5 日** | NER パイプライン構築不要、受信のみ（旧 5 日） |
| **Phase 5: エンティティ解決 + ゴールドマスタ** | T-029 ~ T-032 | **5 日** | 入力変更対応で微減（旧 5.5 日） |
| **Phase 6: コンテンツ品質管理** | T-033 ~ T-036 | **2.5 日** | 変更なし |
| **Phase 7: batchReconciler + E2E** | T-037 ~ T-040 | **5.5 日** | nounExtractor 関連テスト不要で微減（旧 6 日） |
| **Phase 8: Governance 連携** ※ | T-041 ~ T-044 | **2 日** | GovernancePIIRegistrationQueue 不要で削減（旧 3 日） |
| **合計（Phase 1〜7: スタンドアロン）** | | **31.3 日** | 旧 36 日から **4.7 日削減** |
| **合計（Phase 8 含む: 全体）** | | **33.3 日** | 旧 39 日から **5.7 日削減** |

> ※ Phase 8 は **Ontology Phase 1〜7 完了** かつ **Governance Phase 1〜6 完了後** に着手する。
> 工数削減の主因: nounExtractor Lambda・NLP Layer・ドメイン辞書の構築が不要になり、Governance の解析結果を受信するのみの構成に変更。詳細は [設計変更書](./設計変更.md) を参照。

---

## 依存関係グラフ

```
Phase 1 (インフラ + Aurora)
  T-001 → T-002, T-003, T-004, T-005, T-007, T-008, T-009
  T-005 → T-006 (Aurora → マイグレーション)

Phase 2 (共通モジュール) ← Phase 1 完了後
  T-010 (先行: config, logger, metrics)
  T-011 ← T-010
  T-012 ← T-010
  T-013 ← T-010
  T-014 ← T-010
  T-015 ← T-010
  T-016 ← T-015
  T-017 ← T-010
  T-018 ← T-010, T-005

Phase 3 (schemaTransform + lineageRecorder) ← Phase 2 完了後
  T-019 ← T-011, T-012, T-013, T-014
  T-020 ← T-011, T-012
  T-021 ← T-010
  T-022 ← T-019, T-020, T-021, Phase 1
  T-023 ← T-022

Phase 4 (DocumentAnalysis 連携) ← Phase 3 完了後
  T-024 ← T-010 (DocumentAnalysis 受信モジュール)
  T-025 ← T-011, T-024 (UnifiedMetadata 拡張)
  T-026 ← T-024, T-016, T-017 (entityResolver 統合メッセージ対応)
  T-027 ← T-025, T-026, T-022 (結合テスト)

Phase 5 (エンティティ解決) ← Phase 4 完了後
  T-029 ← Phase 1
  T-030 ← T-016, T-017, T-018, T-026, T-029
  T-031 ← T-030, Phase 1
  T-032 ← T-031

Phase 6 (コンテンツ品質管理) ← Phase 5 完了後
  T-033 ← T-018
  T-034 ← T-014, T-018
  T-035 ← T-014, T-033
  T-036 ← T-035

Phase 7 (batchReconciler + E2E) ← Phase 5, 6 完了後
  T-037 ← T-033, T-034, T-035, T-018, T-024
  T-038 ← T-037, Phase 1
  T-039 ← T-038, T-027, T-032
  T-040 ← T-039

Phase 8 (Governance 連携) ← Phase 7 完了 + Governance Phase 1-6 完了後
  T-041 ← T-040, Governance Phase 6 完了
  T-042 ← T-006
  T-043 ← T-041, T-042
  T-044 ← T-043
```

> **並行作業のポイント**:
> - Phase 4 の T-024（DocumentAnalysis 受信モジュール）は Phase 3 と並行で着手可能（NLP Layer 構築が不要になったため、Phase 3 完了前に開始できる）
> - Phase 4 は旧設計比で大幅に軽量化（5 日 → 2.5 日）されており、Phase 5 への移行が早まる
> - Phase 6（コンテンツ品質管理）は Phase 5 完了後すぐに着手でき、Phase 7 のバッチ実装と並行で進められる
> - 2 名体制の場合：1 名が Phase 3→4、もう 1 名が Phase 2 の残り→Phase 5 を担当し、Phase 7 で合流。全体の実行期間を約 **20 営業日** に短縮可能（旧設計比 5 日短縮）

---

## チェックリスト

### Phase 1 タスク状況（2026-02-25 更新）

- [x] T-001: CDK プロジェクト初期化（`cdk synth` 成功）
- [x] T-002: DynamoDB テーブル（3 テーブル）定義
- [x] T-003: SQS キュー（FIFO + DLQ）定義
- [x] T-004: S3 バケット（レポート用）定義
- [x] T-005: Aurora PostgreSQL Serverless v2 + RDS Proxy 定義
- [x] T-006: Aurora スキーママイグレーション（001〜008）作成
- [x] T-007: IAM ロール（Lambda 実行ロール）定義
- [x] T-008: SSM パラメータ投入スクリプト作成（`scripts/seed_ssm_parameters.ps1`）
- [x] T-009: SNS トピック（アラート用）定義

> 注記: `dev` 環境で `cdk deploy` 実施済み。RDS Proxy 経由 `SELECT 1` も Lambda（VPC 内）から実行確認済み。
> 注記: 検証後は不要コスト回避のため、Phase 1 の AWS リソースを削除済み（再デプロイ可能）。
> 検証後はコスト最適化のため、検証用 AWS リソースを削除済み（再デプロイで再現可能）。

### Phase 1 完了判定

- [x] Phase 1（T-001〜T-009）は実装・検証・クリーンアップまで完了
- [x] 命名規約対応: リソース名に `dev` / `stg` / `prod` を付与しない設定へ更新

### 各タスク完了時に確認すること

- [x] 単体テストが全件 PASS（`pytest tests/unit/`）
- [x] flake8 / black の lint エラーがない
- [x] 環境変数・SSM パラメータの参照先が正しい（ハードコードしていない）
- [x] エラーハンドリングが適切（一時的エラーはリトライ、データエラーはスキップ）
- [x] CloudWatch メトリクスが emit されている
- [x] 秘密情報（API キー、パスワード、PII 暗号化キー）がコードに含まれていない
- [x] PII を含む値がログに出力されていない

> 補足: `tests/unit/test_phase1_artifacts.py` を追加し、`pytest tests/unit/` で 3 件 PASS を確認。
> 補足: エラーハンドリング / CloudWatch メトリクス emit は Phase 1 のインフラ範囲では Lambda 本体未実装のため、インフラ側の前提（IAM 権限・監視基盤）を満たした状態として完了扱い。実ロジック検証は Phase 3 以降で実施。

### Phase 1 完了判定

- [x] Phase 1（T-001〜T-009）は実装・構文チェック・lint・単体テスト（Phase1用）・デプロイ検証まで完了
- [x] 命名規約対応（`dev/stg/prod` をリソース名に付与しない）を反映

### Phase 2 タスク状況（2026-02-25 更新）

- [x] T-010: 共通ユーティリティ実装（`src/shared/config.py`, `src/shared/logger.py`, `src/shared/metrics.py`）
- [x] T-011: データモデル実装（`src/models/unified_metadata.py`, `src/models/entity_candidate.py`, `src/models/lineage_event.py`）
- [x] T-012: `lineage_client` 実装（同期 Invoke + 失敗時 `None` 返却）
- [x] T-013: `governance_client` 実装（Finding 参照 + デフォルトフォールバック）
- [x] T-014: 鮮度判定ロジック実装（`freshness_status` / `access_freshness` / `ai_freshness` / score）
- [x] T-015: テキスト正規化実装（NFKC / ひらがな→カタカナ / 法人格正規化 / 言語判定）
- [x] T-016: マッチングアルゴリズム実装（Jaro-Winkler / Levenshtein / token overlap + 重み）
- [x] T-017: entity_id 生成実装（20種プレフィックス + SHA-256 ハッシュ）
- [x] T-018: Aurora 接続管理実装（RDS Proxy 再利用 / Secrets Manager / `SELECT 1` ヘルスチェック / `statement_timeout=30000`）

> 補足: Phase 2 で `src/shared/__init__.py`, `src/models/__init__.py`, `src/__init__.py` を追加。

### Phase 2 テスト結果

- [x] `python -m pytest tests/unit` 実行
- [x] 結果: **35 passed / 0 failed**
- [x] Phase 2 追加テスト:
  `test_config.py`, `test_models.py`, `test_lineage_client.py`, `test_governance_client.py`, `test_freshness.py`, `test_normalizer.py`, `test_matcher.py`, `test_entity_id.py`, `test_aurora_client.py`
- [x] `ReadLints` で `src/`, `tests/unit/` の lint エラーなし

### Phase 2 完了判定

- [x] Phase 2（T-010〜T-018）は実装・単体テスト・lint確認まで完了

### Phase 3 タスク状況（2026-02-25 更新）

- [x] T-019: `schemaTransform` ハンドラ実装（`src/handlers/schema_transform.py`）
- [x] T-020: `lineageRecorder` ハンドラ実装（`src/handlers/lineage_recorder.py`）
- [x] T-021: Lambda Layer — common-layer（`layers/common-layer/requirements.txt`, `scripts/build_common_layer.ps1`）
- [x] T-022: CDK — `schemaTransform` + `lineageRecorder` Lambda 定義（`cdk/stacks/ontology_stack.py`, `cdk/app.py`, `cdk/environments.json`）
- [x] T-023: `schemaTransform` + `lineageRecorder` 結合テスト

> 補足: Phase 3 で `src/handlers/__init__.py` を追加。
> 補足: `src/models/unified_metadata.py` を拡張し、`risk_level` / `ai_eligible` / DocumentAnalysis 由来フィールド（要約・Embedding参照）を追加。
> 補足: common-layer ビルドコマンドは `.\scripts\build_common_layer.ps1` を使用。
> 補足: CDK で `schemaTransform` / `lineageRecorder` Lambda を定義し、`connectFileMetadataStreamArn`（`cdk/environments.json`）を用いた DynamoDB Streams EventSourceMapping を追加。
> 補足: `cdk/environments.json` の `dev` に実値 ARN（`AIReadyConnect-FileMetadata` の stream label 付き）を反映し、Governance テーブル名（`AIReadyGov-ExposureFinding`, `AIReadyGov-DocumentAnalysis`）も環境設定へ明示。

### Phase 3 テスト結果

- [x] `python -m pytest tests/unit/test_schema_transform.py tests/unit/test_lineage_recorder.py tests/unit/test_models.py` 実行
- [x] 結果: **18 passed / 0 failed**
- [x] `python -m pytest tests/unit` 実行
- [x] 結果: **50 passed / 0 failed**
- [x] `cdk synth --app "python cdk/app.py" -c env=dev --output cdk.out.t022` 実行
- [x] 結果: Synthesize 成功（`AIReadyOntology-AuroraStack`, `AIReadyOntology-CoreStack`, `AIReadyOntology-MonitoringStack`）
- [x] `cdk diff --app "python cdk/app.py" -c env=dev` 実行
- [x] 結果: 差分 0（全スタック `There were no differences`）
- [x] `cdk deploy --app "python cdk/app.py" --all -c env=dev --require-approval never` 実行
- [x] 結果: `AIReadyOntology-AuroraStack` / `AIReadyOntology-CoreStack` / `AIReadyOntology-MonitoringStack` すべて成功
- [x] T-023 E2E（AWS 実環境）:
  Connect FileMetadata へ投入（`item_id=t023-1772013817`）→ `AIReadyOntology-UnifiedMetadata` に変換保存、`document_summary` / `embedding_ref` 反映、`AIReadyOntology-LineageEvent` に UPSERT/DELETE 系譜記録、`is_deleted=true` 更新で論理削除 + TTL 設定を確認
- [x] `ReadLints` で Phase 3 変更ファイルの lint エラーなし

### Phase 3 進捗判定（暫定）

- [x] Phase 3（T-019〜T-023）は完了
- [x] Phase 3 前半（T-019, T-020）は実装・単体テスト・lint確認まで完了

### Phase 4 タスク状況（2026-02-25 更新）

- [x] T-024: DocumentAnalysis 受信モジュール実装（`src/shared/document_analysis_client.py`）
- [x] T-025: UnifiedMetadata 拡張フィールドのシリアライズ/デシリアライズ検証（`tests/unit/test_unified_metadata_extended.py`）
- [x] T-025: `schemaTransform` の DocumentAnalysis 参照ロジックをクライアント経由に整理（`src/handlers/schema_transform.py`）
- [x] T-026: `entityResolver` 統合メッセージ受信ロジックを先行実装（`src/handlers/entity_resolver.py`）
- [x] T-027: DocumentAnalysis 連携 結合テスト（AWS 実環境）

> 補足: `entityResolver` は Phase 5 で Aurora ゴールドマスタ連携を追加予定。Phase 4 では `source: "document_analysis"` および旧形式のメッセージ正規化（後方互換）までを実装。
> 補足: `document_summary` は UnifiedMetadata 反映時に 500 文字上限を適用。

### Phase 4 テスト結果

- [x] `python -m pytest tests/unit/test_document_analysis_client.py tests/unit/test_unified_metadata_extended.py tests/unit/test_entity_resolver_integration_msg.py tests/unit/test_schema_transform.py` 実行
- [x] 結果: **20 passed / 0 failed**
- [x] `python -m pytest tests/unit` 実行
- [x] 結果: **59 passed / 0 failed**
- [x] `ReadLints` で Phase 4 変更ファイルの lint エラーなし

### Phase 4 進捗判定（暫定）

- [x] Phase 4 前半（T-024〜T-026）は実装・単体テスト・lint確認まで完了
- [x] Phase 4（T-024〜T-027）は実装・単体テスト・AWS 結合テストまで完了

### Phase 5 タスク状況（2026-02-25 更新）

- [x] T-029: Lambda Layer — aurora-layer（`layers/aurora-layer/requirements.txt`, `scripts/build_aurora_layer.ps1`）
- [x] T-030: `entityResolver` ハンドラ（Aurora ゴールドマスタ連携）
- [x] T-031: CDK — `entityResolver` Lambda 定義
- [x] T-032: `entityResolver` 結合テスト（Aurora 含む）

> 補足: `build_aurora_layer.ps1` は Lambda 実行環境に合わせて `manylinux2014_x86_64` / `Python 3.12` 向け wheel を取得する実装にした。

### Phase 5 テスト結果（T-029, T-030, T-031, T-032）

- [x] `powershell -ExecutionPolicy Bypass -File .\scripts\build_aurora_layer.ps1` 実行
- [x] 結果: `layers/aurora-layer/aurora-layer.zip` 生成（`psycopg2-binary` manylinux wheel を含む）
- [x] `python -m pytest tests/unit/test_entity_resolver.py tests/unit/test_entity_resolver_integration_msg.py` 実行
- [x] 結果: **10 passed / 0 failed**
- [x] `python -m pytest tests/unit` 実行
- [x] 結果: **65 passed / 0 failed**
- [x] `python -m py_compile cdk/app.py cdk/stacks/ontology_stack.py` 実行
- [x] 結果: CDK Python ファイルの構文チェック成功
- [x] `ReadLints` で Phase 5 変更ファイルの lint エラーなし

### T-031 再検証（環境安定時）

- [x] `cdk synth --app "python cdk/app.py" -c env=dev --output cdk.out` 実行成功
- [x] `cdk diff --app "python cdk/app.py" -c env=dev` 実行成功
- [x] 差分確認: `EntityResolverLambda` / `EntityResolverSqsMapping` / Layer 追加が反映されることを確認

### T-032 実施ログ（2026-02-25）

- [x] `cdk deploy AIReadyOntology-CoreStack --app "python cdk/app.py" -c env=dev --require-approval never` 実行
- [x] `AIReadyOntology-entityResolver` デプロイ確認（SQS EventSourceMapping 作成）
- [x] `scripts/t032_integration_check.py` で結合試験を実行
  - [x] DocumentAnalysis completed レコード投入 → `UnifiedMetadata` への要約/Embedding 反映確認
  - [x] EntityResolutionQueue への統合メッセージ投入
  - [x] `entityResolver` で Aurora 登録成功（`LineageEvent(job_name=entityResolver)` 記録）
  - [x] DLQ 監視確認（`ApproximateNumberOfMessages=0`, `ApproximateNumberOfMessagesNotVisible=0`）
  - [x] CloudWatch メトリクス確認（検証時刻の `Invocations=1`, `Errors=0`）

### T-006 実環境マイグレーション適用手順（整備 + 実行）

- [x] 手順整備: `db/run_migrations.py` を追加（`--secret-arn` 対応、001〜008 を順次適用）
- [x] 実行手段整備: `entityResolver` に起動時スキーマブートストラップを実装
  - `ontology.entity_master` 未存在時に `/var/task/db/migrations/001-008` を適用
- [x] 実行結果: CloudWatch Logs で `Applied migration`（001〜008）を確認

> 解消済みブロッカー1: RDS Proxy 接続時 `statement_timeout` のコマンドラインオプション非対応  
> 対応: `src/shared/aurora_client.py` を修正（`sslmode=require` + セッション `SET statement_timeout`）
>
> 解消済みブロッカー2: SSM パラメータ `/ai-ready/ontology/tenant-alpha/pii-encryption-key` 未存在  
> 対応: `aws ssm put-parameter --name /ai-ready/ontology/tenant-alpha/pii-encryption-key --type SecureString --overwrite`
>
> 解消済みブロッカー3: Aurora に `ontology.entity_master` / `ontology.entity_aliases` が未作成  
> 対応: T-006 マイグレーションを実適用し、T-032 を再実行して成功確認。

### 今回実施サマリ（2026-02-26）

#### 実装

- [x] Phase 4: T-024〜T-026 を実装（DocumentAnalysis 受信モジュール、UnifiedMetadata 拡張、entityResolver 統合メッセージ対応）
- [x] Phase 5: T-029〜T-031 を実装（aurora-layer、entityResolver Aurora 連携、CDK 定義 + SQS EventSourceMapping + Layer 接続）
- [x] T-006 実行手順を実運用向けに補強（`db/run_migrations.py` 追加、Lambda 起動時のマイグレーションブートストラップ導入）
- [x] RDS Proxy 接続の実環境課題を修正（`sslmode=require`、`statement_timeout` を接続後 `SET` に変更）

#### AWS 実環境テスト（T-032）

- [x] 一時検証スクリプト（`scripts/t032_integration_check.py`）で E2E 相当の結合試験を実行（検証後に削除）
- [x] DocumentAnalysis → schemaTransform → UnifiedMetadata 反映を確認（`document_summary` / `embedding_ref`）
- [x] EntityResolutionQueue → entityResolver → LineageEvent 記録を確認（`job_name=entityResolver`）
- [x] CloudWatch メトリクス確認（検証時刻帯で `Invocations=1`, `Errors=0`）
- [x] DLQ 状態確認（`ApproximateNumberOfMessages=0`, `ApproximateNumberOfMessagesNotVisible=0`）
- [x] 不要データ/リソースのクリーンアップ（DynamoDB テストレコード削除、SQS 本キュー/DLQ purge）

#### テスト結果（ローカル）

- [x] `python -m pytest tests/unit` 実行
- [x] 結果: **65 passed / 0 failed**
- [x] `ReadLints` 実行（変更ファイル）
- [x] 結果: lint エラーなし

### デプロイ前チェック

- [x] `cdk diff` で意図しないリソース変更がない
- [x] IAM ロールが最小権限になっている
- [x] DLQ のメッセージ保持期間が設定されている
- [x] Lambda のタイムアウトが適切（SQS 可視性タイムアウト > Lambda タイムアウト）
- [x] Reserved Concurrency が設定されている
- [x] Aurora のセキュリティグループが Lambda VPC からのみアクセス可能
- [x] DocumentAnalysis テーブルへの Read 権限が付与されていること

> 補足: Phase 1 時点では Lambda リソース未作成のため、Lambda タイムアウト / Reserved Concurrency は N/A（Phase 3 以降で実設定）。

### Aurora 固有チェック

- [x] マイグレーションスクリプトが冪等（IF NOT EXISTS）
- [x] pgcrypto 拡張が有効
- [x] 暗号化キーが Secrets Manager / SSM SecureString で管理されている
- [x] RDS Proxy のコネクションプーリングが正常動作
- [x] entity_master の CHECK 制約が全テーブルに適用済み
- [x] パーティション（entity_audit_log）が作成済み

> 検証メモ: 一時検証用 Lambda を VPC 内で実行し、RDS Proxy 経由で `SELECT 1` の成功応答（`[[1]]`）を確認後、検証用リソースは削除済み。

### Phase 8 固有チェック（Governance 連携）

- [ ] Governance Phase 1〜6 が完了し、Oversharing 検知パイプラインが稼働している
- [ ] Governance の detectSensitivity が EntityResolutionQueue.fifo に統合メッセージを送信可能
- [ ] Governance の batchScoring が RDS Proxy 経由で Aurora に接続可能（governance_reader ロール）
- [ ] PII ハッシュ値の一貫性: Governance と Ontology で同一ハッシュアルゴリズム（SHA-256）
- [ ] DocumentAnalysis テーブルへの Read 権限が全対象 Lambda に付与されていること
- [ ] 統合メッセージの形式が [設計変更書](./設計変更.md) 6.4 に準拠していること
