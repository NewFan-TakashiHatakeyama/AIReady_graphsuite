from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from services.ner_pipeline import detect_language, extract_ner_and_noun_chunks


class _FakeEnt:
    def __init__(self, text, label, start, end):
        self.text = text
        self.label_ = label
        self.start_char = start
        self.end_char = end


def test_detect_language_ja():
    assert detect_language("これは日本語です") == "ja"


def test_detect_language_en():
    assert detect_language("This is English text") == "en"


def test_extract_ner_and_noun_chunks_with_mocked_spacy():
    fake_doc = SimpleNamespace(
        ents=[_FakeEnt("田中太郎", "Person", 0, 4)],
        noun_chunks=[SimpleNamespace(text="田中太郎"), SimpleNamespace(text="契約書")],
    )
    fake_nlp = MagicMock(return_value=fake_doc)
    fake_spacy = MagicMock()
    fake_spacy.load.return_value = fake_nlp

    import services.ner_pipeline as mod

    mod._nlp_ja = None
    mod._nlp_en = None
    with patch.dict(sys.modules, {"spacy": fake_spacy}):
        result = extract_ner_and_noun_chunks("田中太郎の契約書")

    assert result.language == "ja"
    assert len(result.entities) == 1
    assert result.entities[0].label == "Person"
    assert set(result.noun_chunks) == {"田中太郎", "契約書"}
