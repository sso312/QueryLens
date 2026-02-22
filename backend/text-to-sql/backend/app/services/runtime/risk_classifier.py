from __future__ import annotations

import re
from typing import Any


_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[가-힣]+")

_WRITE_KEYWORDS = re.compile(
    r"\b(delete|update|insert|merge|drop|alter|truncate|create|grant|revoke)\b"
    r"|삭제|지워|지우기|업데이트|수정|변경|삽입|추가|생성|초기화|드롭",
    re.IGNORECASE,
)
_DDL_KEYWORDS = re.compile(r"\b(drop|alter|truncate|create)\b|드롭|초기화|생성", re.IGNORECASE)
_JOIN_KEYWORDS = re.compile(r"\b(join|left join|right join|inner join|outer join)\b", re.IGNORECASE)

_DERIVED_METRIC_PATTERNS = (
    re.compile(
        r"(비율|사망률|생존율|재입원율|발생률|rate|ratio|percentage|percent|mortality|survival|readmission|평균|중앙값|중위수|median|mean)",
        re.IGNORECASE,
    ),
)
_STRATIFICATION_PATTERNS = (
    re.compile(
        r"(연도별|월별|주별|일별|분기별|성별|연령별|군별|그룹별|추이|비교|대비|차이|사분위|분위수|하위군)",
        re.IGNORECASE,
    ),
    re.compile(r"\b(vs|versus|comparison|compared|by|stratified|quartile|q[1-4]|decile)\b", re.IGNORECASE),
)
_TEMPORAL_CONSTRAINT_PATTERNS = (
    re.compile(
        r"(최근|지난|작년|올해|전년|기간|이내|이후|전후|입원 후|수술 후|\d+\s*(일|주|개월|월|년)|between|from|to|within|after|before)",
        re.IGNORECASE,
    ),
)
_COHORT_CONSTRAINT_PATTERNS = (
    re.compile(
        r"(진단|질환|수술|처치|투약|약물|중환자|icu|입원|환자군|코호트|고혈압|패혈증|뇌졸중|copd|aki|cabg)",
        re.IGNORECASE,
    ),
)
_MULTI_CONDITION_PATTERNS = (
    re.compile(r"\b(and|or)\b|및|그리고|또는|이면서|동시에|,"),
)
_BROAD_SCOPE_PATTERNS = (
    re.compile(r"\b(all|everything|entire)\b|전체 데이터|모든|전부", re.IGNORECASE),
)


def _token_count(text: str) -> int:
    return len(_TOKEN_RE.findall(text))


def _has_any(text: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def classify(question: str) -> dict[str, Any]:
    text = str(question or "")
    lowered = text.lower()
    risk = 0
    complexity = 0

    if _WRITE_KEYWORDS.search(lowered):
        risk += 5
    if _DDL_KEYWORDS.search(lowered):
        risk += 1

    join_hits = len(_JOIN_KEYWORDS.findall(lowered))
    complexity += join_hits
    if join_hits > 0:
        complexity += 1

    has_derived_metric = _has_any(lowered, _DERIVED_METRIC_PATTERNS)
    has_stratification = _has_any(lowered, _STRATIFICATION_PATTERNS)
    has_temporal_constraint = _has_any(lowered, _TEMPORAL_CONSTRAINT_PATTERNS)
    has_cohort_constraint = _has_any(lowered, _COHORT_CONSTRAINT_PATTERNS)
    has_multi_condition = _has_any(lowered, _MULTI_CONDITION_PATTERNS)

    if has_derived_metric:
        complexity += 1
    if has_stratification:
        complexity += 1
    if has_temporal_constraint:
        complexity += 1
    if has_cohort_constraint:
        complexity += 1
    if has_multi_condition:
        complexity += 1

    if _token_count(text) >= 20:
        complexity += 1

    # Semantic-risk booster for Korean/English analytical intents.
    if has_derived_metric and has_stratification:
        risk += 2
    if has_derived_metric and has_temporal_constraint:
        risk += 1
    if has_multi_condition and has_cohort_constraint:
        risk += 1

    if complexity >= 3:
        risk += 1
    if complexity >= 4:
        risk += 1
    if complexity >= 5:
        risk += 1

    if _has_any(lowered, _BROAD_SCOPE_PATTERNS):
        risk += 1

    intent = "read"
    if risk >= 3:
        intent = "risky"

    return {
        "intent": intent,
        "complexity": complexity,
        "risk": risk,
    }
