from __future__ import annotations

from typing import Any

from .extract_budget import BudgetLevel, get_extract_budget, normalize_level
from .risk_detector import detect_risk_signals
from .snippet_extractor import extract_snippets


_LEVEL_ORDER: tuple[BudgetLevel, ...] = ("fast", "accurate", "strict")


def _next_level(level: BudgetLevel) -> BudgetLevel:
    idx = _LEVEL_ORDER.index(level)
    if idx >= len(_LEVEL_ORDER) - 1:
        return level
    return _LEVEL_ORDER[idx + 1]


def run_adaptive_extraction(
    full_text: str,
    *,
    start_level: str | None = None,
    force_level: str | None = None,
    expanded_keywords: bool = True,
) -> dict[str, Any]:
    level = normalize_level(start_level, default="fast")
    log: list[dict[str, Any]] = []

    budget = get_extract_budget(level)
    snippets = extract_snippets(full_text=full_text, budget=budget, expanded_keywords=expanded_keywords)
    risk = detect_risk_signals(full_text=full_text, snippets=snippets)
    log.append(
        {
            "phase": "initial",
            "level": level,
            "snippet_count": len(snippets),
            "risk_score": int(risk.get("risk_score") or 0),
            "reason": "initial extraction",
        }
    )

    if force_level:
        target = normalize_level(force_level, default=level)
        while level != target:
            level = _next_level(level)
            budget = get_extract_budget(level)
            snippets = extract_snippets(full_text=full_text, budget=budget, expanded_keywords=True)
            risk = detect_risk_signals(full_text=full_text, snippets=snippets)
            log.append(
                {
                    "phase": "forced_upgrade",
                    "level": level,
                    "snippet_count": len(snippets),
                    "risk_score": int(risk.get("risk_score") or 0),
                    "reason": f"force_level={target}",
                }
            )

    return {
        "level": level,
        "snippets": snippets,
        "risk": risk,
        "log": log,
    }
