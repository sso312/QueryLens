from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from app.core.paths import project_path


_RULES_PATH = project_path("var/metadata/sql_postprocess_rules.json")
_RULES_CACHE_MTIME: float = -1.0
_RULES_CACHE: dict[str, Any] = {}

_DEFAULT_RULES: dict[str, Any] = {
    "execution": {
        "mode": "conservative",
    },
    "schema_aliases": {
        "use_schema_hints": False,
        "table_aliases": {},
        "column_aliases": {},
    },
    "diagnosis_rewrite": {
        "enabled": True,
        "table_name": "DIAGNOSES_ICD",
        "icd_like_template": "{alias}.ICD_CODE LIKE '{prefix}%'",
        "join_operator": " OR ",
    },
    "procedure_rewrite": {
        "enabled": True,
        "table_name": "PROCEDURES_ICD",
        "icd_like_template": "{alias}.ICD_CODE LIKE '{prefix}%'",
        "join_operator": " OR ",
    },
    "icd_version_inference": {
        "enabled": True,
        "table_names": ["DIAGNOSES_ICD", "PROCEDURES_ICD"],
        "version_column": "ICD_VERSION",
        "letter_prefix_version": 10,
        "digit_prefix_version": 9,
        "prefix_version_overrides": {},
        "predicate_template": "({version_col} = {version} AND {code_expr} LIKE '{prefix}%')",
    },
    "mortality_rewrite": {
        "enabled": True,
        "join_tables": ["DIAGNOSES_ICD", "PROCEDURES_ICD"],
        "admissions_table": "ADMISSIONS",
        "outcome_column": "HOSPITAL_EXPIRE_FLAG",
        "key_column": "HADM_ID",
        "numerator_template": "COUNT(DISTINCT CASE WHEN {expire_ref} = 1 THEN {key_ref} END)",
        "denominator_template": "NULLIF(COUNT(DISTINCT {key_ref}), 0)",
    },
    "time_window_rewrite": {
        "enabled": True,
        "death_anchor_column": "DEATHTIME",
        "from_column": "DISCHTIME",
        "to_column": "ADMITTIME",
        "exclude_question_keywords": ["퇴원 후", "퇴원후", "after discharge", "post-discharge"],
    },
    "admissions_icu_alignment": {
        "enabled": True,
        "admissions_table": "ADMISSIONS",
        "icustays_table": "ICUSTAYS",
    },
    "label_intent_rewrite": {
        "enabled": True,
        "use_metadata_profiles": True,
        "max_metadata_profiles": 4,
        "min_metadata_score": 1,
        "profiles": [],
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    keys = set(base.keys()) | set(override.keys())
    for key in keys:
        base_value = base.get(key)
        override_value = override.get(key)
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            merged[key] = _deep_merge(base_value, override_value)
            continue
        if override_value is None:
            merged[key] = base_value
            continue
        merged[key] = override_value
    return merged


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def load_sql_postprocess_rules() -> dict[str, Any]:
    global _RULES_CACHE_MTIME
    global _RULES_CACHE

    if not _RULES_PATH.exists():
        _RULES_CACHE_MTIME = -1.0
        _RULES_CACHE = dict(_DEFAULT_RULES)
        return _RULES_CACHE

    mtime = _RULES_PATH.stat().st_mtime
    if _RULES_CACHE and _RULES_CACHE_MTIME == mtime:
        return _RULES_CACHE

    override = _load_json(_RULES_PATH)
    _RULES_CACHE = _deep_merge(_DEFAULT_RULES, override)
    _RULES_CACHE_MTIME = mtime
    return _RULES_CACHE
