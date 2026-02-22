from __future__ import annotations

from typing import Any


PRIORITY_AMBIGUITY_PREFIXES: tuple[str, ...] = (
    "amb_first_icu_unit",
    "amb_index_unit",
    "amb_icd_",
    "amb_icd_version",
    "amb_missing_evidence_",
)


def _priority(amb_id: str) -> int:
    amb = amb_id.lower()
    if amb.startswith("amb_first_icu_unit") or amb.startswith("amb_index_unit"):
        return 0
    if amb.startswith("amb_icd"):
        return 1
    if amb.startswith("amb_signal_map"):
        return 2
    if amb.startswith("amb_missing_evidence"):
        return 3
    return 4


def rank_ambiguities(ambiguities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [item for item in ambiguities if isinstance(item, dict)],
        key=lambda x: (_priority(str(x.get("id") or "")), str(x.get("id") or "")),
    )


def top_questions(ambiguities: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    ranked = rank_ambiguities(ambiguities)
    return ranked[: max(1, limit)]
