from __future__ import annotations

from typing import Any

SUPPORTED_CONDITION_TYPES: set[str] = {
    "age_rule",
    "age_range",
    "diagnosis_icd_prefix",
    "diagnosis_icd_range",
    "diagnosis_icd_exact",
    "primary_dx_only",
    "icu_los_min_days",
    "admission_los_min_days",
    "death_within_days_of_index_event",
    "measurement_required",
    "medication_exposure",
    "lab_threshold",
    "time_window_definition",
    "criterion_text",
}


def _iter_conditions(spec: dict[str, Any]):
    for section in ("inclusion", "exclusion", "requirements"):
        items = spec.get(section)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                yield section, item


def validate_supported_types(spec: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    for section, item in _iter_conditions(spec):
        ctype = str(item.get("type") or "").strip().lower()
        if not ctype:
            warnings.append(f"{section}:missing_type")
            continue
        if ctype not in SUPPORTED_CONDITION_TYPES:
            warnings.append(f"{section}:{ctype}:unsupported_type")
    return sorted(set(warnings))


def has_measurement_required(spec: dict[str, Any]) -> bool:
    for _, item in _iter_conditions(spec):
        if str(item.get("type") or "").strip().lower() == "measurement_required":
            return True
    return False


def has_icd_shorthand_risk(spec: dict[str, Any]) -> bool:
    for _, item in _iter_conditions(spec):
        if str(item.get("type") or "").strip().lower() != "diagnosis_icd_prefix":
            continue
        codes = item.get("codes")
        if not isinstance(codes, list):
            continue
        for code in codes:
            if not isinstance(code, dict):
                continue
            prefix = str(code.get("prefix") or "").strip().upper()
            if prefix and (prefix.endswith("X") or len(prefix) <= 3 and prefix.isdigit()):
                return True
    return False
