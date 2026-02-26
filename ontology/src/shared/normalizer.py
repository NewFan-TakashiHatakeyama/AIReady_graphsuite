"""Text normalization utilities."""

from __future__ import annotations

import re
import unicodedata


HIRAGANA_TO_KATAKANA = str.maketrans(
    "ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞ"
    "ただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽ"
    "まみむめもゃやゅゆょよらりるれろゎわゐゑをん",
    "ァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾ"
    "タダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポ"
    "マミムメモャヤュユョヨラリルレロヮワヰヱヲン",
)

CORPORATE_SUFFIXES = {
    "株式会社": "カブシキガイシャ",
    "(株)": "カブシキガイシャ",
    "㈱": "カブシキガイシャ",
    "有限会社": "ユウゲンガイシャ",
    "(有)": "ユウゲンガイシャ",
    "合同会社": "ゴウドウガイシャ",
    "Inc.": "INC",
    "Corp.": "CORP",
    "Ltd.": "LTD",
    "LLC": "LLC",
    "Co.": "CO",
}


def normalize_japanese(text: str) -> str:
    """Normalize Japanese text (NFKC, kana and spacing)."""
    normalized = unicodedata.normalize("NFKC", text.strip())
    normalized = normalized.translate(HIRAGANA_TO_KATAKANA)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def normalize_corporate_name(name: str) -> str:
    """Normalize company names to canonical corporate suffix form."""
    normalized = normalize_japanese(name)
    for suffix, replacement in CORPORATE_SUFFIXES.items():
        normalized = normalized.replace(suffix, f" {replacement} ")
    return re.sub(r"\s+", " ", normalized).strip()


def detect_language(text: str) -> str:
    """Detect language by rough Japanese character ratio."""
    if not text:
        return "en"
    ja_chars = sum(1 for c in text if _is_japanese_char(c))
    ratio = ja_chars / len(text)
    return "ja" if ratio >= 0.2 else "en"


def normalize_text(text: str, lang: str | None = None) -> str:
    """Normalize text based on language."""
    if not text:
        return ""

    normalized_lang = (lang or detect_language(text)).lower()
    if normalized_lang == "ja":
        return normalize_japanese(text)

    normalized = unicodedata.normalize("NFKC", text.strip()).lower()
    return re.sub(r"\s+", " ", normalized)


def _is_japanese_char(char: str) -> bool:
    code = ord(char)
    return (
        0x3040 <= code <= 0x309F  # Hiragana
        or 0x30A0 <= code <= 0x30FF  # Katakana
        or 0x4E00 <= code <= 0x9FFF  # CJK Unified Ideographs
    )
