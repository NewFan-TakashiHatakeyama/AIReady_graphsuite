from __future__ import annotations

from src.shared.entity_id import (
    ENTITY_ID_PREFIX,
    compute_canonical_hash,
    generate_entity_id,
)


def test_entity_prefixes() -> None:
    for entity_type, prefix in ENTITY_ID_PREFIX.items():
        value = generate_entity_id(entity_type)
        assert value.startswith(prefix)


def test_unknown_entity_type_prefix() -> None:
    value = generate_entity_id("unknown_type")
    assert value.startswith("ent_")


def test_canonical_hash_consistency() -> None:
    assert compute_canonical_hash("abc") == compute_canonical_hash("abc")
