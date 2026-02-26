from __future__ import annotations

from services.domain_dictionary import enrich_noun_chunks


def test_enrich_noun_chunks_merges_and_deduplicates():
    result = enrich_noun_chunks(["契約書", "個人情報"], ["個人情報", "取引先"])
    assert result == ["個人情報", "取引先", "契約書"]
