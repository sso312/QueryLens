from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

BudgetLevel = Literal["fast", "accurate", "strict"]


@dataclass(frozen=True)
class ExtractBudget:
    level: BudgetLevel
    max_snippets: int
    include_tables: bool
    table_capture_chars: int
    context_chars: int


_BUDGETS: dict[BudgetLevel, ExtractBudget] = {
    "fast": ExtractBudget(
        level="fast",
        max_snippets=30,
        include_tables=False,
        table_capture_chars=1600,
        context_chars=1400,
    ),
    "accurate": ExtractBudget(
        level="accurate",
        max_snippets=60,
        include_tables=True,
        table_capture_chars=4000,
        context_chars=2400,
    ),
    "strict": ExtractBudget(
        level="strict",
        max_snippets=90,
        include_tables=True,
        table_capture_chars=7000,
        context_chars=3600,
    ),
}


def normalize_level(level: str | None, default: BudgetLevel = "fast") -> BudgetLevel:
    value = str(level or "").strip().lower()
    if value in _BUDGETS:
        return value  # type: ignore[return-value]
    return default


def get_extract_budget(level: str | None) -> ExtractBudget:
    return _BUDGETS[normalize_level(level)]
