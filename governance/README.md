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
├── docker/
│   ├── Dockerfile.sensitivity  detectSensitivity 用 Docker イメージ
│   └── requirements.sensitivity.txt
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
```

## Lambda 一覧

| Lambda | トリガー | 処理内容 |
|--------|---------|---------|
| `AIReadyGov-analyzeExposure` | DynamoDB Streams | メタデータ変更の検知 → ExposureScore 算出 → Finding 生成 |
| `AIReadyGov-detectSensitivity` | SQS | ファイルコンテンツの PII/Secret 検知 + **NER + 名詞チャンク + 要約 + Embedding 生成** → SensitivityScore 更新 + **DocumentAnalysis 保存** + **S3 Vectors 保存** + **EntityResolutionQueue 送信** |
| `AIReadyGov-batchScoring` | EventBridge (日次) | 全件再スコアリング + 日次レポート生成 |

## 設計書

- [基本設計書](./Docs/基本設計.md)
- [過剰共有（Oversharing）](./Docs/過剰共有（Oversharing）.md)
- [詳細設計書](./Docs/過剰共有（Oversharing）詳細設計.md)
- [実装手順書](./Tasks.md)
- [設計変更書](../ontology/設計変更.md) — ドキュメント解析の一元化
