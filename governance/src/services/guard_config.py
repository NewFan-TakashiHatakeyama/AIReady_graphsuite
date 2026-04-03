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
            "org_link_editable",
            "guest",
            "guest_direct_share",
            "external_email_direct_share",
            "external_domain_share",
            "external_domain",
            "excessive_permissions",
        }),
        applicable_sources=frozenset({"m365", "box", "google_drive"}),
    ),
}
