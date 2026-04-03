"""EntityCandidateモデル定義。

ガバナンス分析/文書解析の結果から抽出されたエンティティ候補を
`entityResolver` に渡すための共通DTOとして扱う。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class EntityCandidate:
    """抽出済みエンティティ候補を表す不変に近いデータ構造。

    Args:
        candidate_id: 候補単位の一意ID（キュー重複排除や監査追跡で利用）。
        tenant_id: テナント境界を担保するための識別子。
        source_item_id: 元ドキュメントID。
        surface_form: 元文書に出現した生テキスト。
        normalized_form: 正規化済みの照合キー文字列。
        entity_type: ontology 側の正規エンティティ種別（person/organization 等）。
        pii_flag: PII として扱うかどうか。
        extraction_source: 抽出手段（ner/governance/domain_dict など）。
        confidence: 抽出または推定の信頼度。
        mention_count: 同一候補の出現回数。
        context_snippet: 抽出時の周辺コンテキスト。
        ner_label: 上流 NER の元ラベル。
        language: 原文言語。
        source_title: 元文書タイトル。
        extracted_at: 抽出時刻（ISO8601想定）。

    Notes:
        - `candidate_id`: 候補単位の一意ID。
        - `source_item_id`: 元ドキュメントID（Findings や Metadata と紐付く）。
        - `normalized_form`: マッチング・ハッシュ計算に使う正規化済み表記。
        - `pii_flag` / `pii_category`: PII 保護（暗号化保存やアラート）判断に利用。
    """

    candidate_id: str
    tenant_id: str
    source_item_id: str
    surface_form: str
    normalized_form: str
    entity_type: str
    pii_flag: bool
    extraction_source: str
    confidence: float
    mention_count: int
    context_snippet: str
    ner_label: str
    language: str
    source_title: str
    extracted_at: str
    pii_category: str = ""
    analysis_id: str = ""
    lineage_id: str = ""
    source: str = "document_analysis"
