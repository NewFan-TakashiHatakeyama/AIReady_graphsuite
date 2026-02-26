"""Entity candidate matching algorithms."""

from __future__ import annotations


ENTITY_TYPE_WEIGHTS = {
    "person": {"jaro_winkler": 0.5, "levenshtein": 0.3, "token_overlap": 0.2},
    "organization": {"jaro_winkler": 0.3, "levenshtein": 0.2, "token_overlap": 0.5},
    "technology": {"jaro_winkler": 0.4, "levenshtein": 0.4, "token_overlap": 0.2},
    "concept": {"jaro_winkler": 0.3, "levenshtein": 0.3, "token_overlap": 0.4},
}
DEFAULT_WEIGHTS = {"jaro_winkler": 0.4, "levenshtein": 0.3, "token_overlap": 0.3}


def calculate_match_score(
    candidate_form: str,
    existing_form: str,
    entity_type: str,
) -> float:
    """Calculate weighted similarity score between two entity forms."""
    if candidate_form == existing_form and candidate_form != "":
        return 1.0
    if not candidate_form or not existing_form:
        return 0.0

    jw_score = _jaro_winkler_similarity(candidate_form, existing_form)
    lev_score = _levenshtein_similarity(candidate_form, existing_form)
    token_score = _token_overlap_ratio(candidate_form, existing_form)

    weights = ENTITY_TYPE_WEIGHTS.get(entity_type, DEFAULT_WEIGHTS)
    score = (
        weights["jaro_winkler"] * jw_score
        + weights["levenshtein"] * lev_score
        + weights["token_overlap"] * token_score
    )
    return round(score, 4)


def _jaro_winkler_similarity(s1: str, s2: str) -> float:
    """Compute Jaro-Winkler similarity."""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    max_dist = max(len(s1), len(s2)) // 2 - 1
    if max_dist < 0:
        max_dist = 0

    s1_matches = [False] * len(s1)
    s2_matches = [False] * len(s2)
    matches = 0
    transpositions = 0

    for i in range(len(s1)):
        start = max(0, i - max_dist)
        end = min(i + max_dist + 1, len(s2))
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len(s1)):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (
        matches / len(s1)
        + matches / len(s2)
        + (matches - transpositions / 2) / matches
    ) / 3

    prefix_len = 0
    for i in range(min(4, len(s1), len(s2))):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break

    return jaro + prefix_len * 0.1 * (1 - jaro)


def _levenshtein_similarity(s1: str, s2: str) -> float:
    """Compute Levenshtein distance based similarity."""
    if s1 == s2:
        return 1.0
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    distance = _levenshtein_distance(s1, s2)
    return 1.0 - distance / max_len


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein distance."""
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row
    return prev_row[-1]


def _token_overlap_ratio(s1: str, s2: str) -> float:
    """Compute token overlap ratio using Jaccard index."""
    tokens1 = set(s1.lower().split())
    tokens2 = set(s2.lower().split())
    if not tokens1 or not tokens2:
        return 0.0
    intersection = tokens1 & tokens2
    union = tokens1 | tokens2
    return len(intersection) / len(union) if union else 0.0
