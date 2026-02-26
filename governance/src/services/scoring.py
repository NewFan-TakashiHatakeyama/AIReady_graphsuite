"""スコアリングエンジン — ExposureScore / SensitivityScore / RiskScore 算出

詳細設計 6.1–6.5 節準拠
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from services.exposure_vectors import FileMetadata, extract_exposure_vectors
from shared.config import SSM_MAX_EXPOSURE_SCORE, get_ssm_float
from shared.logger import get_logger

logger = get_logger(__name__)

# ─── ExposureScore 重み定義 ───

EXPOSURE_WEIGHTS: dict[str, float] = {
    "public_link": 5.0,
    "guest": 4.0,
    "all_users": 3.5,
    "org_link": 3.0,
    "external_domain": 3.0,
    "broken_inheritance": 2.0,
    "excessive_permissions": 1.5,
}

# ─── SensitivityScore: ラベル名 → スコアマッピング ───

LABEL_SCORE_MAP: dict[str, float] = {
    "Highly Confidential": 4.0,
    "Confidential": 3.0,
    "Internal": 2.0,
    "General": 1.0,
    "Public": 1.0,
    "極秘": 4.0,
    "秘": 3.0,
    "社内限定": 2.0,
    "一般": 1.0,
}

# ─── SensitivityScore: ファイル名ヒューリスティック ───

SENSITIVE_FILENAME_PATTERNS: list[tuple[str, float]] = [
    (r"(?i)(給与|salary|payroll)", 2.0),
    (r"(?i)(契約|contract|agreement)", 2.0),
    (r"(?i)(見積|quote|estimate)", 1.5),
    (r"(?i)(人事|hr|personnel)", 2.0),
    (r"(?i)(顧客|customer|client).*(?:リスト|list|一覧)", 2.0),
    (r"(?i)(パスワード|password|credential)", 2.5),
    (r"(?i)(機密|confidential|secret)", 2.5),
    (r"(?i)(予算|budget)", 1.5),
    (r"(?i)(個人情報|PII|個人データ)", 2.5),
]

# ─── SensitivityScore: PII 密度スコア ───

PII_DENSITY_SCORES: dict[str, float] = {
    "high": 4.0,
    "medium": 3.5,
    "low": 2.5,
    "none": 1.0,
}

# ─── RiskScore 閾値 ───

RISK_SCORE_THRESHOLD_DEFAULT = 2.0


@dataclass
class ExposureResult:
    score: float
    vectors: list[str]
    details: dict[str, float] = field(default_factory=dict)


@dataclass
class SensitivityResult:
    score: float
    factors: list[str] = field(default_factory=list)
    is_preliminary: bool = True


# ─── ExposureScore 算出 (6.1) ───


def calculate_exposure_score(metadata: FileMetadata) -> ExposureResult:
    """ExposureScore を算出（最大要因ベース + 追加要因の加算）。"""
    vectors = extract_exposure_vectors(metadata)

    if not vectors:
        return ExposureResult(score=1.0, vectors=[], details={})

    weighted_scores = {v: EXPOSURE_WEIGHTS.get(v, 1.0) for v in vectors}
    max_score = max(weighted_scores.values())

    additional = sum(w * 0.2 for w in weighted_scores.values() if w < max_score)

    max_exposure = _get_max_exposure_score()
    final_score = min(max_score + additional, max_exposure)

    return ExposureResult(
        score=round(final_score, 2),
        vectors=vectors,
        details=weighted_scores,
    )


# ─── SensitivityScore 暫定算出 (6.2) ───


def calculate_preliminary_sensitivity(metadata: FileMetadata) -> SensitivityResult:
    """暫定 SensitivityScore（ラベル + ファイル名ヒューリスティック）。"""
    score = 1.0
    factors: list[str] = []

    if metadata.sensitivity_label:
        label_name = _parse_label_name(metadata.sensitivity_label)
        if label_name:
            label_score = LABEL_SCORE_MAP.get(label_name, 1.0)
            if label_score > score:
                score = label_score
                factors.append(f"label:{label_name}")

    for pattern, weight in SENSITIVE_FILENAME_PATTERNS:
        if re.search(pattern, metadata.item_name or ""):
            if weight > score:
                score = weight
                factors.append(f"filename:{pattern}")

    return SensitivityResult(score=score, factors=factors, is_preliminary=True)


# ─── SensitivityScore 正式算出 (6.3) ───


def calculate_sensitivity_score(
    pii_results: dict,
    secret_results: dict,
    existing_label_score: float = 1.0,
) -> float:
    """正式 SensitivityScore（PII/Secret 検知結果に基づく）。

    Args:
        pii_results: PIIDetectionResult 相当の dict
            {"detected": bool, "high_risk_detected": bool, "density": str}
        secret_results: SecretDetectionResult 相当の dict
            {"detected": bool}
        existing_label_score: ラベルによるベーススコア
    """
    score = existing_label_score

    if secret_results.get("detected", False):
        score = max(score, 5.0)
        return round(score, 2)

    if pii_results.get("high_risk_detected", False):
        score = max(score, 4.0)
        return round(score, 2)

    density = pii_results.get("density", "none")
    density_score = PII_DENSITY_SCORES.get(density, 1.0)
    score = max(score, density_score)

    return round(score, 2)


# ─── ActivityScore 算出 (6.4) ───


def calculate_activity_score(metadata: FileMetadata) -> float:
    """ActivityScore の簡易算出（modified_at ベース）。"""
    if metadata.modified_at is None:
        return 1.0

    try:
        modified = datetime.fromisoformat(metadata.modified_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days_since = (now - modified).days

        if days_since <= 7:
            return 2.0
        elif days_since <= 30:
            return 1.5
        elif days_since <= 90:
            return 1.0
        else:
            return 0.5
    except (ValueError, TypeError):
        return 1.0


# ─── RiskScore 総合算出 (6.5) ───


def calculate_risk_score(
    exposure: float,
    sensitivity: float,
    activity: float,
    ai_amp: float,
) -> float:
    """RiskScore = ExposureScore x SensitivityScore x ActivityScore x AIAmplification"""
    return round(exposure * sensitivity * activity * ai_amp, 2)


def classify_risk_level(risk_score: float) -> str:
    """RiskScore からリスクレベルを判定する。"""
    if risk_score >= 50.0:
        return "critical"
    elif risk_score >= 20.0:
        return "high"
    elif risk_score >= 5.0:
        return "medium"
    elif risk_score >= 2.0:
        return "low"
    else:
        return "none"


# ─── ヘルパー ───


def _parse_label_name(sensitivity_label: str | None) -> str | None:
    """sensitivity_label フィールドからラベル名を抽出する。"""
    if not sensitivity_label:
        return None

    try:
        label_data = json.loads(sensitivity_label)
        if isinstance(label_data, dict):
            return label_data.get("name")
        return str(label_data)
    except (json.JSONDecodeError, TypeError):
        return sensitivity_label


def _get_max_exposure_score() -> float:
    try:
        return get_ssm_float(SSM_MAX_EXPOSURE_SCORE, default=10.0)
    except Exception:
        return 10.0
