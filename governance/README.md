# AI Ready Governance

生成AI（Copilot / 社内RAG / AIエージェント）がデータを参照する際の**過剰共有（Oversharing）リスク**を検知・評価するガバナンス基盤。
さらに、**ドキュメントコンテンツ解析の一元担当**として、PII 検知・NER・名詞チャンク・要約・Embedding 生成を統合的に実行し、解析結果を Ontology に提供する。

## 概要

AI Ready Connect が Microsoft 365 から収集したファイルメタデータ・権限情報を入力として、以下のパイプラインでリスクを評価する。

```
Connect (FileMetadata)
    │ DynamoDB Streams
    ▼
analyzeExposure ──▶ ExposureFinding (DynamoDB)
    │ SQS                    ▲
    ▼                        │
detectSensitivity ───────────┘   (PII/Secret 検知 → SensitivityScore 更新)
    │
    ├──▶ DocumentAnalysis (DynamoDB)   ← NER + PII 統合 + 要約 + メタ情報
    ├──▶ S3 Vectors                    ← Embedding（ベクトル表現）
    └──▶ EntityResolutionQueue (SQS)   ← エンティティ候補 → Ontology

EventBridge (日次)
    │
    ▼
batchScoring ──▶ 全件再スコアリング + 日次レポート (S3)
```

## スコアリングモデル

```
RiskScore = ExposureScore × SensitivityScore × ActivityScore × AIAmplification
```

| スコア | 評価対象 |
|--------|---------|
| ExposureScore | 権限・共有の広さ（Anyone リンク, EEEU, 外部ゲスト等） |
| SensitivityScore | 中身の機微度（PII, Secret, 秘密度ラベル, ファイル名ヒューリスティック） |
| ActivityScore | 実際の利用状況（modified_at ベース） |
| AIAmplification | 生成AIによる増幅リスク（v1.0 は 1.0 固定） |

## プロジェクト構成

```
governance/
├── Docs/                       設計書
├── infra/
│   ├── app.py                  CDK エントリーポイント
│   └── stack.py                CDK スタック
├── src/
│   ├── handlers/               Lambda ハンドラ
│   ├── services/               ビジネスロジック
│   │   ├── pii_detector.py             Presidio + GiNZA PII 検出
│   │   ├── secret_detector.py          Secret/Credential 検出
│   │   ├── text_extractor.py           ファイルからのテキスト抽出
│   │   ├── ner_pipeline.py             NER + 名詞チャンク抽出（Ontology から移管）
│   │   ├── domain_dictionary.py        ドメイン辞書マッチ（Ontology から移管）
│   │   ├── summarizer.py              ドキュメント要約（Bedrock Claude Haiku）
│   │   ├── embedding_generator.py      Embedding 生成（Bedrock Titan Embeddings V2）
│   │   └── entity_integration.py       Ontology 連携 [Phase 7]
│   └── shared/                 共通ユーティリティ
├── tests/
│   ├── unit/                   単体テスト（moto モック）
│   ├── integration/            結合テスト（AWS リソース使用）
│   └── aws/                    AWS デプロイ検証テスト
├── pyproject.toml
├── Tasks.md                    実装手順書
└── README.md
```

## 前提条件

- Python 3.11+
- AWS CDK v2
- AI Ready Connect がデプロイ済みで、FileMetadata テーブルにデータが投入されていること

## セットアップ

```bash
# 依存パッケージのインストール
cd governance
pip install poetry
poetry install

# CDK デプロイ
cd infra
pip install -r requirements.txt
cdk synth
cdk deploy
```

## テスト実行

```bash
# 単体テスト
cd governance
poetry run pytest tests/unit/ -v

# カバレッジ付き
poetry run pytest tests/unit/ --cov=src --cov-report=term-missing

# 結合テスト（AWS リソースが必要）
poetry run pytest tests/integration/ -v

# AWS テスト文言の v1.2 用語統一チェック
python scripts/check_aws_test_terminology.py
```

### CI 2段階検証フロー（推奨）

1. **ローカル代替検証（PR/Push 自動）**  
   `governance-unit-tests.yml` で unit テストに加えて、次の件数ベース検証を実行します。  
   - `tests/integration/test_risk_count_aggregation_local.py`
   - `tests/integration/test_analyze_exposure_e2e.py`
   - `tests/integration/test_batch_scoring_e2e.py`

2. **実 AWS 検証（手動）**  
   `governance-aws-smoke.yml` を `workflow_dispatch` で実行し、`--run-aws` 付きテストを実施します。  
   `run_ft_scoring_engine=true` の場合、`tests/aws/test_ft_scoring_engine.py` も実行します。

### 是正フローとコンテンツ信頼度

`GOVERNANCE_CONTENT_CONFIDENCE_THRESHOLD` 未満の `analysis_confidence` でも、露出ベクトルが権限スキャン由来（例: `public_link`, `org_link` 系）のときは `remediation_mode=approval` / `remediation_action=remove_permissions` へ誘導し、承認後に Graph API で権限削除できるようにします（既定）。従来どおり低信頼度で常に `owner_review` に落とす場合は `GOVERNANCE_CONFIDENCE_FAILSAFE_IGNORE_PERMISSION_VECTORS=false` を設定してください。

### ステージング / 本番相当環境での検証（実 AWS）

単体テストだけでは Graph・実テナントデータを踏んだ end-to-end は保証しません。デプロイ後は次を併用してください。

1. **実 AWS スモーク（Lambda 到達性・入力検証）**  
   認証済みプロファイルで `ap-northeast-1` を向け、次を実行します（`AIReadyGov-remediateFinding` が存在するアカウントであること）。

   ```bash
   cd governance
   poetry run pytest tests/aws/test_ft_remediate_finding_smoke.py tests/aws/test_dvt_lambda.py -k "dvt_2_14 or dvt_2_15 or dvt_2_16" -v --run-aws
   ```

2. **GitHub Actions（手動）**  
   リポジトリに `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`（必要なら `AWS_SESSION_TOKEN`）を設定し、ワークフロー **Governance AWS Smoke (manual)** を `workflow_dispatch` で実行します。

3. **是正 execute の実結合（Graph + DynamoDB）**  
   上記スモークは **Graph を呼びません**。PoC など **意図したテナント上のテスト用 Finding** で Graph まで踏む場合は、環境変数を付与したうえで次の pytest を `--run-aws` で実行します（**権限削除・Finding 更新が発生**します）。リスク再計算は **Connect ストリーム経由**のため、本テストでは `post_verification.deferred_to=connect_filemetadata_stream` を検証します。

   | 変数 | 必須 | 説明 |
   |------|------|------|
   | `GOVERNANCE_E2E_GRAPH_EXECUTE` | はい | `1` / `true` / `yes` で E2E を有効化 |
   | `GOVERNANCE_E2E_EXECUTE_TENANT_ID` | はい | 対象テナント ID |
   | `GOVERNANCE_E2E_EXECUTE_FINDING_ID` | はい | 対象 Finding ID（`remediation_state=approved` で実行、または下記で承認から） |
   | `GOVERNANCE_E2E_APPROVE_BEFORE_EXECUTE` | いいえ | `1` で先に `approve`（Lambda が `approval_then_auto_execute` なら承認時に execute まで実行） |
   | `GOVERNANCE_E2E_EXPECT_ITEM_ID` | いいえ | 設定時、invoke 前に DynamoDB の `item_id` と一致検証 |
   | `GOVERNANCE_E2E_OPERATOR` | いいえ | Lambda に渡す操作者（既定: `graph-e2e-pytest`） |

   ```bash
   cd governance
   export GOVERNANCE_E2E_GRAPH_EXECUTE=1
   export GOVERNANCE_E2E_EXECUTE_TENANT_ID="<tenant>"
   export GOVERNANCE_E2E_EXECUTE_FINDING_ID="<finding_id>"
   # 任意: 誤爆防止
   # export GOVERNANCE_E2E_EXPECT_ITEM_ID="<item_id>"
   poetry run pytest tests/aws/test_e2e_remediate_finding_graph_execute.py -v --run-aws
   ```

   PowerShell の例:

   ```powershell
   $env:GOVERNANCE_E2E_GRAPH_EXECUTE = "1"
   $env:GOVERNANCE_E2E_EXECUTE_TENANT_ID = "<tenant>"
   $env:GOVERNANCE_E2E_EXECUTE_FINDING_ID = "<finding_id>"
   cd governance; poetry run pytest tests/aws/test_e2e_remediate_finding_graph_execute.py -v --run-aws
   ```

4. **是正後の Graph 実状態の確認（読み取りのみ）**  
   DynamoDB の FileMetadata が古いままでも、**Graph 上の permissions が正であること**は次で検証できます（SSM の Connect 用 Graph クレデンシャルを使用）。

   ```bash
   cd governance
   export AWS_DEFAULT_REGION=ap-northeast-1
   poetry run python scripts/verify_graph_item_permissions.py \
     --connect-tenant-id tenant-alpha \
     --drive-id "<drive_id>" \
     --item-id "<item_id>" \
     --expect-permission-absent "<removed_permission_id>" \
     --expect-no-anonymous-link
   ```

5. **是正用テストデータ（オーバーシェア）再現**  
   シナリオ A（organization+edit）/B（guest invite）/C（anonymous link）/D（ACL drift 手順）を再現する場合は、次を参照してください。  
   - `governance/Docs/ガバナンス再現シナリオ手順.md`  
   - 実行スクリプト: `governance/scripts/reproduce_governance_risk_scenarios.py`

### Web UI（モック API 経由の E2E）

フロントと remediation 表示の配線確認は、リポジトリルートの `webui` で Playwright を実行します（`e2e/mock-governance-api.mjs` がバックエンドを模倣）。

```bash
cd webui
npx playwright test e2e/governance-remediation-completed-flow.spec.ts
```

### 是正実行後のリスク更新方針（既定）

- **正のソース**は **Connect の FileMetadata** が M365 の実状態に収束した内容とし、**DynamoDB Streams → `analyzeExposure`** による再評価に任せる（ストリーム駆動）。
- 是正直後に Governance が Graph を再読みして `process_item_batch` を即時実行する経路は **実装しない**（削除済み）。

## Lambda 一覧

| Lambda | トリガー | 処理内容 |
|--------|---------|---------|
| `AIReadyGov-analyzeExposure` | DynamoDB Streams | メタデータ変更の検知 → ExposureScore 算出 → Finding 生成 |
| `AIReadyGov-detectSensitivity` | SQS | ファイルコンテンツの PII/Secret 検知 + **NER + 名詞チャンク + 要約 + Embedding 生成** → SensitivityScore 更新 + **DocumentAnalysis 保存** + **S3 Vectors 保存** + **EntityResolutionQueue 送信** |
| `AIReadyGov-batchScoring` | EventBridge (日次) | 全件再スコアリング + 日次レポート生成 |
| `AIReadyGov-remediateFinding` | API / 手動 invoke | Finding 単位の是正提案・承認・**Graph 上の実行**（リスク更新は Connect ストリーム側） |

## 設計書

- [トリガー・DynamoDB リソース調査（IaC / API 経路）](../Docs/実装内容/Governance-トリガーとリソース調査.md)
- [基本設計書](./Docs/基本設計.md)
- [過剰共有（Oversharing）](./Docs/過剰共有（Oversharing）.md)
- [詳細設計書](./Docs/詳細設計.md)
- （別名） [過剰共有 詳細設計（リダイレクト）](./Docs/過剰共有（Oversharing）詳細設計.md)
- [ドキュメントスタイルガイド（実装コメント/Docstring規約）](../Docs/実装内容/Governance-ドキュメントスタイルガイド.md)
- [設計変更書](../ontology/設計変更.md) — ドキュメント解析の一元化
