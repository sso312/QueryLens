from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from app.core.paths import project_path


_SCHEMA_HINTS_PATH = project_path("var/metadata/sql_postprocess_schema_hints.json")
_SCHEMA_HINTS_CACHE_MTIME: float = -1.0
_SCHEMA_HINTS_CACHE: dict[str, Any] = {}

_DEFAULT_HINTS: dict[str, Any] = {
    "table_aliases": {},
    "column_aliases": {},
    "patients_only_cols": set(),
    "admissions_only_cols": set(),
    "tables_with_subject_id": set(),
    "tables_with_hadm_id": set(),
    "micro_only_cols": set(),
    "timestamp_cols": set(),
}


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


def _normalize_alias_map(value: Any, *, upper_values: bool) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, str] = {}
    for key, raw in value.items():
        src = str(key).strip()
        dest = str(raw).strip()
        if not src or not dest:
            continue
        normalized[src] = dest.upper() if upper_values else dest
    return normalized


def _normalize_upper_set(value: Any) -> set[str]:
    if not isinstance(value, (list, tuple, set)):
        return set()
    normalized: set[str] = set()
    for item in value:
        text = str(item).strip().upper()
        if text:
            normalized.add(text)
    return normalized


def _build_hints(raw: dict[str, Any]) -> dict[str, Any]:
    table_aliases = _normalize_alias_map(raw.get("table_aliases"), upper_values=True)
    column_aliases = _normalize_alias_map(raw.get("column_aliases"), upper_values=True)
    return {
        "table_aliases": table_aliases,
        "column_aliases": column_aliases,
        "patients_only_cols": _normalize_upper_set(raw.get("patients_only_cols")),
        "admissions_only_cols": _normalize_upper_set(raw.get("admissions_only_cols")),
        "tables_with_subject_id": _normalize_upper_set(raw.get("tables_with_subject_id")),
        "tables_with_hadm_id": _normalize_upper_set(raw.get("tables_with_hadm_id")),
        "micro_only_cols": _normalize_upper_set(raw.get("micro_only_cols")),
        "timestamp_cols": _normalize_upper_set(raw.get("timestamp_cols")),
    }


def load_sql_schema_hints() -> dict[str, Any]:
    global _SCHEMA_HINTS_CACHE_MTIME
    global _SCHEMA_HINTS_CACHE

    if not _SCHEMA_HINTS_PATH.exists():
        _SCHEMA_HINTS_CACHE_MTIME = -1.0
        _SCHEMA_HINTS_CACHE = dict(_DEFAULT_HINTS)
        return _SCHEMA_HINTS_CACHE

    mtime = _SCHEMA_HINTS_PATH.stat().st_mtime
    if _SCHEMA_HINTS_CACHE and _SCHEMA_HINTS_CACHE_MTIME == mtime:
        return _SCHEMA_HINTS_CACHE

    raw = _load_json(_SCHEMA_HINTS_PATH)
    built = _build_hints(raw)
    if not built.get("table_aliases") and not built.get("column_aliases"):
        built = dict(_DEFAULT_HINTS)
    _SCHEMA_HINTS_CACHE = built
    _SCHEMA_HINTS_CACHE_MTIME = mtime
    return _SCHEMA_HINTS_CACHE
