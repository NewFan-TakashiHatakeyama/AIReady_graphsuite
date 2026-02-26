"""ExposureVector 抽出の単体テスト

詳細設計 3.5 節の各パターンを網羅する。
"""

import json

import pytest

from services.exposure_vectors import (
    FileMetadata,
    extract_exposure_vectors,
    has_eeeu_access,
    has_external_domain_users,
    has_external_guests,
    is_broken_inheritance,
    parse_permissions,
)


def _make_metadata(**kwargs) -> FileMetadata:
    defaults = {"tenant_id": "t-001", "item_id": "item-001"}
    defaults.update(kwargs)
    return FileMetadata(**defaults)


# ─── sharing_scope パターン ───


class TestSharingScope:
    def test_anonymous_scope_yields_public_link(self):
        m = _make_metadata(sharing_scope="anonymous")
        vectors = extract_exposure_vectors(m)
        assert "public_link" in vectors

    def test_organization_scope_yields_org_link(self):
        m = _make_metadata(sharing_scope="organization")
        vectors = extract_exposure_vectors(m)
        assert "org_link" in vectors

    def test_specific_scope_yields_nothing(self):
        m = _make_metadata(sharing_scope="specific")
        vectors = extract_exposure_vectors(m)
        assert "public_link" not in vectors
        assert "org_link" not in vectors

    def test_empty_scope_yields_nothing(self):
        m = _make_metadata(sharing_scope="")
        vectors = extract_exposure_vectors(m)
        assert vectors == [] or all(
            v not in ("public_link", "org_link") for v in vectors
        )


# ─── EEEU 判定 ───


class TestEEEUAccess:
    def test_eeeu_display_name_detected(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"displayName": "Everyone except external users"}}
            ]
        })
        m = _make_metadata(permissions=perms)
        vectors = extract_exposure_vectors(m)
        assert "all_users" in vectors

    def test_eeeu_email_detected(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"email": "everyone@contoso.com"}}
            ]
        })
        assert has_eeeu_access(parse_permissions(perms))

    def test_non_eeeu_not_detected(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"displayName": "Legal Team"}}
            ]
        })
        assert not has_eeeu_access(parse_permissions(perms))


# ─── 外部ゲスト判定 ───


class TestExternalGuests:
    def test_guest_user_type(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"userType": "guest", "email": "ext@partner.com"}}
            ]
        })
        m = _make_metadata(permissions=perms)
        vectors = extract_exposure_vectors(m)
        assert "guest" in vectors

    def test_ext_indicator_in_email(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"email": "user_partner.com#ext#@contoso.onmicrosoft.com"}}
            ]
        })
        assert has_external_guests(parse_permissions(perms))

    def test_internal_user_not_guest(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"userType": "member", "email": "user@contoso.com"}}
            ]
        })
        assert not has_external_guests(parse_permissions(perms))


# ─── 外部ドメインユーザー ───


class TestExternalDomainUsers:
    def test_is_external_user_flag(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"isExternalUser": True}}
            ]
        })
        assert has_external_domain_users(parse_permissions(perms))

    def test_different_domain(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"domain": "partner.com", "orgDomain": "contoso.com"}}
            ]
        })
        assert has_external_domain_users(parse_permissions(perms))

    def test_same_domain(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"domain": "contoso.com", "orgDomain": "contoso.com"}}
            ]
        })
        assert not has_external_domain_users(parse_permissions(perms))


# ─── 継承崩れ判定 ───


class TestBrokenInheritance:
    def test_broken_inheritance_detected(self):
        sm = json.dumps({"has_unique_permissions": True})
        m = _make_metadata(source_metadata=sm)
        assert is_broken_inheritance(m)

    def test_normal_inheritance(self):
        sm = json.dumps({"has_unique_permissions": False})
        m = _make_metadata(source_metadata=sm)
        assert not is_broken_inheritance(m)

    def test_no_source_metadata(self):
        m = _make_metadata()
        assert not is_broken_inheritance(m)


# ─── excessive_permissions 判定 ───


class TestExcessivePermissions:
    def test_over_threshold(self):
        m = _make_metadata(permissions_count=100)
        vectors = extract_exposure_vectors(m)
        assert "excessive_permissions" in vectors

    def test_under_threshold(self):
        m = _make_metadata(permissions_count=10)
        vectors = extract_exposure_vectors(m)
        assert "excessive_permissions" not in vectors

    def test_at_threshold(self):
        m = _make_metadata(permissions_count=50)
        vectors = extract_exposure_vectors(m)
        assert "excessive_permissions" not in vectors


# ─── 複合パターン ───


class TestCombined:
    def test_anonymous_with_guest_and_broken(self):
        perms = json.dumps({
            "entries": [
                {"identity": {"userType": "guest", "email": "ext@partner.com"}}
            ]
        })
        sm = json.dumps({"has_unique_permissions": True})
        m = _make_metadata(
            sharing_scope="anonymous",
            permissions=perms,
            source_metadata=sm,
        )
        vectors = extract_exposure_vectors(m)
        assert "public_link" in vectors
        assert "guest" in vectors
        assert "broken_inheritance" in vectors

    def test_private_no_vectors(self):
        m = _make_metadata(
            sharing_scope="specific",
            permissions="{}",
            permissions_count=5,
        )
        vectors = extract_exposure_vectors(m)
        assert vectors == []


# ─── parse_permissions エッジケース ───


class TestParsePermissions:
    def test_empty_string(self):
        assert parse_permissions("") == []

    def test_none(self):
        assert parse_permissions(None) == []

    def test_invalid_json(self):
        assert parse_permissions("not json") == []

    def test_list_format(self):
        result = parse_permissions('[{"identity": {}}]')
        assert len(result) == 1

    def test_dict_with_entries(self):
        result = parse_permissions('{"entries": [{"identity": {}}]}')
        assert len(result) == 1

    def test_non_dict_non_list_returns_empty(self):
        """JSON パース結果が dict でも list でもない場合（Line 100）"""
        assert parse_permissions("42") == []
        assert parse_permissions('"just a string"') == []
        assert parse_permissions("true") == []


# ─── is_broken_inheritance 追加エッジケース ───


class TestBrokenInheritanceEdge:
    def test_invalid_json_source_metadata(self):
        """source_metadata が壊れた JSON の場合 False を返す（Lines 160-161）"""
        m = _make_metadata(source_metadata="not valid json {")
        assert not is_broken_inheritance(m)

    def test_non_dict_source_metadata(self):
        """source_metadata が dict でない場合"""
        m = _make_metadata(source_metadata='"just a string"')
        assert not is_broken_inheritance(m)
