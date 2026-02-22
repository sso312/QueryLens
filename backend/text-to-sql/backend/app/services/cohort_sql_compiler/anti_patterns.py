from __future__ import annotations

import re


_ANTI_PATTERNS: tuple[tuple[str, str, bool], ...] = (
    (
        "chartevents_correlated_exists",
        r"EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+[A-Za-z0-9_\.\"]*CHARTEVENTS\s+[A-Za-z0-9_]+\s+WHERE\s+[A-Za-z0-9_]+\.STAY_ID\s*=\s*P\.STAY_ID",
        True,
    ),
    (
        "trim_icd_code",
        r"TRIM\s*\(\s*[A-Za-z0-9_\.\"]*ICD_CODE\s*\)",
        False,
    ),
)


def find_anti_patterns(sql: str) -> list[dict[str, str | bool]]:
    text = str(sql or "")
    violations: list[dict[str, str | bool]] = []
    for code, pattern, blocking in _ANTI_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL):
            violations.append({"code": code, "blocking": blocking, "message": f"anti_pattern:{code}"})
    return violations
