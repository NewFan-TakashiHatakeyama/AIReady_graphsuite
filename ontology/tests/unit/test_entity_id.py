from __future__ import annotations

from src.shared.entity_id import compute_canonical_hash


def test_canonical_hash_consistency() -> None:
    assert compute_canonical_hash("abc") == compute_canonical_hash("abc")


def test_canonical_hash_differs_for_different_inputs() -> None:
    assert compute_canonical_hash("a") != compute_canonical_hash("b")
