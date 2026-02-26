"""ガード照合ロジックの単体テスト

詳細設計 8.3 節の照合ルールを網羅する。
"""

import pytest

from services.guard_matcher import match_guards


class TestMatchGuards:
    def test_public_link_m365(self):
        """public_link + m365 → G3"""
        result = match_guards(["public_link"], source="m365")
        assert result == ["G3"]

    def test_all_users_and_broken_inheritance_m365(self):
        """all_users + broken_inheritance + m365 → G2, G7"""
        result = match_guards(["all_users", "broken_inheritance"], source="m365")
        assert result == ["G2", "G7"]

    def test_public_link_box(self):
        """public_link + box → G3（マルチソース対応）"""
        result = match_guards(["public_link"], source="box")
        assert result == ["G3"]

    def test_public_link_slack(self):
        """public_link + slack → []（対象外ソース）"""
        result = match_guards(["public_link"], source="slack")
        assert result == []

    def test_ai_accessible_m365(self):
        """ai_accessible + m365 → G9"""
        result = match_guards(["ai_accessible"], source="m365")
        assert result == ["G9"]

    def test_eeeu_m365(self):
        """eeeu + m365 → G2"""
        result = match_guards(["eeeu"], source="m365")
        assert result == ["G2"]

    def test_no_label_m365(self):
        """no_label + m365 → G7"""
        result = match_guards(["no_label"], source="m365")
        assert result == ["G7"]

    def test_guest_m365(self):
        """guest + m365 → G3"""
        result = match_guards(["guest"], source="m365")
        assert result == ["G3"]

    def test_multiple_vectors_multiple_guards(self):
        """複数の ExposureVector → 複数ガードにマッチ"""
        result = match_guards(
            ["public_link", "all_users", "broken_inheritance", "ai_accessible"],
            source="m365",
        )
        assert result == ["G2", "G3", "G7", "G9"]

    def test_empty_vectors(self):
        """空の ExposureVector → マッチなし"""
        result = match_guards([], source="m365")
        assert result == []

    def test_excessive_permissions_google_drive(self):
        """excessive_permissions + google_drive → G3"""
        result = match_guards(["excessive_permissions"], source="google_drive")
        assert result == ["G3"]

    def test_eeeu_box_not_applicable(self):
        """eeeu + box → []（G2 は m365 のみ対象）"""
        result = match_guards(["eeeu"], source="box")
        assert result == []

    def test_result_is_sorted(self):
        """結果がソート済みであること"""
        result = match_guards(
            ["ai_accessible", "public_link", "all_users"],
            source="m365",
        )
        assert result == sorted(result)
