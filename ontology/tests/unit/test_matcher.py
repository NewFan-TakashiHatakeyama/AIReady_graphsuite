from __future__ import annotations

from src.shared.matcher import calculate_match_score


def test_complete_match_returns_one() -> None:
    assert calculate_match_score("Tanaka Taro", "Tanaka Taro", "person") == 1.0


def test_similar_strings_has_reasonable_score() -> None:
    score = calculate_match_score("Tanaka Taro", "Tanaka Taro.", "person")
    assert score >= 0.8


def test_entity_type_weights_affect_score() -> None:
    person = calculate_match_score("Acme Cloud", "Acme Clouds", "person")
    organization = calculate_match_score("Acme Cloud", "Acme Clouds", "organization")
    assert person != organization


def test_empty_strings_handling() -> None:
    assert calculate_match_score("", "abc", "person") == 0.0
    assert calculate_match_score("abc", "", "person") == 0.0


def test_japanese_similarity() -> None:
    score = calculate_match_score("タナカタロウ", "タナカタロー", "person")
    assert 0.6 <= score <= 1.0
