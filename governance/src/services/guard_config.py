"""ガードカテゴリ定義（コード定数版）

ExposureVector からガードカテゴリへのマッピングを管理する。
将来テナントごとのカスタマイズが必要になった場合は DynamoDB に昇格させる。

詳細設計 8.1 節準拠
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class GuardCategory:
    guard_id: str
    guard_name: str
    severity: str  # "critical" | "high" | "medium" | "low"
    vectors: frozenset[str]
    applicable_sources: frozenset[str]


GUARD_CATEGORIES: dict[str, GuardCategory] = {
    "G2": GuardCategory(
        guard_id="G2",
        guard_name="EEEU 抑止ガード",
        severity="critical",
        vectors=frozenset({"eeeu", "all_users"}),
        applicable_sources=frozenset({"m365"}),
    ),
    "G3": GuardCategory(
        guard_id="G3",
        guard_name="共有リンク健全化ガード",
        severity="critical",
        vectors=frozenset({
            "public_link",
            "org_link",
            "guest",
            "external_domain",
            "excessive_permissions",
        }),
        applicable_sources=frozenset({"m365", "box", "google_drive"}),
    ),
    "G7": GuardCategory(
        guard_id="G7",
        guard_name="ラベル適用状況ガード",
        severity="high",
        vectors=frozenset({"no_label", "broken_inheritance"}),
        applicable_sources=frozenset({"m365", "box", "google_drive"}),
    ),
    "G9": GuardCategory(
        guard_id="G9",
        guard_name="生成AI露出ガード",
        severity="medium",
        vectors=frozenset({"ai_accessible"}),
        applicable_sources=frozenset({"m365", "box", "google_drive"}),
    ),
}
