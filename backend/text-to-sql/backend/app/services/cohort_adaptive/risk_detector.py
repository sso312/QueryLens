from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RiskSignal:
    key: str
    score: int
    reason: str


_RISK_PATTERNS: dict[str, tuple[int, tuple[str, ...]]] = {
    "episode_unit": (
        3,
        (
            r"\bfirst\b.*\b(icu|admission|stay)\b",
            r"\binitial\b.*\b(icu|admission|stay)\b",
            r"\bprimary\b.*\b(icu|admission|stay)\b",
        ),
    ),
    "time_window": (
        3,
        (
            r"\bwithin\s+\d+\s*(day|days|hour|hours)\b",
            r"\blast\s*24\s*hours?\s*before\s*(discharge|outtime)\b",
            r"\b퇴실\s*전\s*24\s*시간\b",
        ),
    ),
    "measurement_required": (
        4,
        (
            r"\bmissing\b|\bunavailable\b|\binsufficient\s+data\b",
            r"\b(vital|measurement|charttime|itemid)\b",
            r"\b결측\b|\b측정\b",
        ),
    ),
    "code_normalization": (
        4,
        (
            r"\bicd[-\s]?(9|10)?\s*[:：]?\s*[a-z]?\d{2,4}[a-zx]?\b",
            r"\bi50x\b|\b\d{3}x\b",
            r"\bcode list\b.*\btable\b",
        ),
    ),
    "etiology_scope": (
        2,
        (
            r"\bprimary diagnosis\b|\bsecondary diagnosis\b",
            r"\bdue to\b",
        ),
    ),
}


def _collect_text(snippets: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for item in snippets:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if text:
            parts.append(text)
    return "\n".join(parts)


def detect_risk_signals(*, full_text: str, snippets: list[dict[str, Any]]) -> dict[str, Any]:
    text = f"{str(full_text or '')}\n{_collect_text(snippets)}".lower()
    found: list[RiskSignal] = []
    flags: dict[str, bool] = {}

    for key, (score, patterns) in _RISK_PATTERNS.items():
        matched = False
        for pattern in patterns:
            if re.search(pattern, text, flags=re.IGNORECASE):
                matched = True
                break
        flags[key] = matched
        if matched:
            found.append(RiskSignal(key=key, score=score, reason=f"risk:{key}"))

    total_score = sum(item.score for item in found)
    return {
        "signals": [{"key": s.key, "score": s.score, "reason": s.reason} for s in found],
        "flags": flags,
        "risk_score": total_score,
    }
