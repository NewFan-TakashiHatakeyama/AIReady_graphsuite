"""Secret/Credential 検出 — 正規表現ベースのシークレット検知

詳細設計 4.6 節準拠
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from shared.logger import get_logger

logger = get_logger(__name__)

SECRET_PATTERNS: list[dict[str, str]] = [
    {
        "type": "aws_access_key",
        "pattern": r"(?:AKIA|ABIA|ACCA|ASIA)[0-9A-Z]{16}",
        "description": "AWS Access Key ID",
    },
    {
        "type": "aws_secret_key",
        "pattern": r"(?i)aws_secret_access_key\s*[=:]\s*[A-Za-z0-9/+=]{40}",
        "description": "AWS Secret Access Key",
    },
    {
        "type": "generic_api_key",
        "pattern": r"(?i)(?:api[_\-]?key|apikey)\s*[=:]\s*['\"]?[A-Za-z0-9_\-]{20,}['\"]?",
        "description": "Generic API Key",
    },
    {
        "type": "connection_string",
        "pattern": (
            r"(?i)(?:Server|Data Source)\s*=\s*[^;]+;\s*"
            r"(?:User ID|uid)\s*=\s*[^;]+;\s*"
            r"(?:Password|pwd)\s*=\s*[^;]+"
        ),
        "description": "Database Connection String",
    },
    {
        "type": "private_key",
        "pattern": r"-----BEGIN (?:RSA |EC |DSA )?PRIVATE KEY-----",
        "description": "Private Key (PEM)",
    },
    {
        "type": "github_token",
        "pattern": r"gh[pousr]_[A-Za-z0-9_]{36,}",
        "description": "GitHub Token",
    },
    {
        "type": "slack_token",
        "pattern": r"xox[baprs]-[0-9a-zA-Z\-]{10,}",
        "description": "Slack Token",
    },
    {
        "type": "generic_password",
        "pattern": r"(?i)(?:password|passwd|pwd)\s*[=:]\s*['\"]?[^\s'\"]{8,}['\"]?",
        "description": "Generic Password Assignment",
    },
    {
        "type": "jwt_token",
        "pattern": r"eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_\-]{10,}",
        "description": "JWT Token",
    },
]

_compiled_patterns: list[tuple[dict[str, str], re.Pattern]] | None = None


def _get_compiled_patterns() -> list[tuple[dict[str, str], re.Pattern]]:
    global _compiled_patterns
    if _compiled_patterns is None:
        _compiled_patterns = [
            (pdef, re.compile(pdef["pattern"])) for pdef in SECRET_PATTERNS
        ]
    return _compiled_patterns


@dataclass
class SecretEntity:
    type: str
    start: int
    end: int
    description: str = ""


@dataclass
class SecretDetectionResult:
    detected: bool = False
    types: list[str] = field(default_factory=list)
    count: int = 0
    details: list[SecretEntity] = field(default_factory=list)


def detect_secrets(text: str) -> SecretDetectionResult:
    """テキスト内の Secret/Credential を正規表現で検出する。"""
    if not text:
        return SecretDetectionResult()

    findings: list[SecretEntity] = []
    for pattern_def, compiled in _get_compiled_patterns():
        for match in compiled.finditer(text):
            findings.append(
                SecretEntity(
                    type=pattern_def["type"],
                    start=match.start(),
                    end=match.end(),
                    description=pattern_def["description"],
                )
            )

    deduplicated = _deduplicate_by_position(findings)
    unique_types = sorted(set(f.type for f in deduplicated))

    return SecretDetectionResult(
        detected=len(deduplicated) > 0,
        types=unique_types,
        count=len(deduplicated),
        details=deduplicated,
    )


def _deduplicate_by_position(entities: list[SecretEntity]) -> list[SecretEntity]:
    """位置が重なる検出結果を統合する（より広い範囲を優先）。"""
    if not entities:
        return []

    sorted_entities = sorted(entities, key=lambda e: (e.start, -(e.end - e.start)))
    result: list[SecretEntity] = [sorted_entities[0]]

    for entity in sorted_entities[1:]:
        last = result[-1]
        if entity.start < last.end:
            if entity.end > last.end:
                result[-1] = SecretEntity(
                    type=last.type,
                    start=last.start,
                    end=entity.end,
                    description=last.description,
                )
        else:
            result.append(entity)

    return result
