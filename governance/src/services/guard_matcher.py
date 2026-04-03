"""ガード照合ロジック — ExposureVector からガードカテゴリを照合する

詳細設計 8.2 節準拠
"""

from __future__ import annotations

from services.guard_config import GUARD_CATEGORIES

_G3_PUBLIC = frozenset({"public_link"})
_G3_EXTERNAL_DIRECT = frozenset({
    "guest_direct_share",
    "external_email_direct_share",
    "external_domain_share",
    "guest",
    "external_domain",
    "specific_people_external",
    "external_domain_not_allowlisted",
})
_G3_ORG_EDITABLE = frozenset({"org_link_editable", "org_link_edit"})

_SCENARIO_A = frozenset({"org_link_editable", "org_link_edit"})
_SCENARIO_B = frozenset(
    {
        "guest_direct_share",
        "external_email_direct_share",
        "external_domain_share",
        "specific_people_external",
        "external_domain_not_allowlisted",
    }
)
_SCENARIO_C = frozenset({"public_link"})


def match_guards(
    exposure_vectors: list[str],
    source: str,
) -> list[str]:
    """ExposureVector からガードカテゴリを照合する。

    Args:
        exposure_vectors: アイテムの露出要因リスト（例: ["public_link", "eeeu"]）
        source: データソース識別子（例: "m365", "box"）

    Returns:
        マッチしたガード ID のソート済みリスト（例: ["G2", "G3"]）

    Notes:
        `guard_config.GUARD_CATEGORIES` の `applicable_sources` と
        `vectors` の両条件を満たすガードのみ採用する。
    """
    vector_set = set(exposure_vectors)
    matched: list[str] = []

    for guard in GUARD_CATEGORIES.values():
        if source not in guard.applicable_sources:
            continue

        if vector_set & guard.vectors:
            matched.append(guard.guard_id)

    return sorted(matched)


def resolve_guard_reason_codes(
    exposure_vectors: list[str],
    matched_guards: list[str],
) -> list[str]:
    """ExposureVector と matched guard から理由コードを解決する。"""
    vector_set = set(exposure_vectors)
    guard_set = set(matched_guards)
    reason_codes: set[str] = set()

    if "G3" in guard_set:
        if vector_set & _G3_PUBLIC:
            reason_codes.add("g3_public_link")
        if vector_set & _G3_EXTERNAL_DIRECT:
            reason_codes.add("g3_external_direct_share")
        if vector_set & _G3_ORG_EDITABLE:
            reason_codes.add("g3_org_link_editable")

    return sorted(reason_codes)


def resolve_detection_reasons(exposure_vectors: list[str]) -> list[str]:
    """A/B/C/D シナリオ理由コードを返す。"""
    vector_set = set(exposure_vectors)
    reasons: set[str] = set()

    if vector_set & _SCENARIO_A:
        reasons.add("scenario_a_org_overshare")
    if vector_set & _SCENARIO_B:
        reasons.add("scenario_b_external_direct_share")
    if vector_set & _SCENARIO_C:
        reasons.add("scenario_c_public_link")
    return sorted(reasons)
