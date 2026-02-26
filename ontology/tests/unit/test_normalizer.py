from __future__ import annotations

from src.shared.normalizer import (
    detect_language,
    normalize_corporate_name,
    normalize_japanese,
    normalize_text,
)


def test_nfkc_normalization() -> None:
    assert normalize_text("ＡＢＣ　１２３", "en") == "abc 123"


def test_hiragana_to_katakana() -> None:
    assert normalize_japanese("たなか たろう") == "タナカ タロウ"


def test_corporate_suffix_normalization() -> None:
    value = normalize_corporate_name("株式会社サンプル")
    assert "カブシキガイシャ" in value


def test_detect_language() -> None:
    assert detect_language("これは日本語の文章です") == "ja"
    assert detect_language("this is english text") == "en"
