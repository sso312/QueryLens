from __future__ import annotations

from pathlib import Path
from typing import Any
import hashlib
import json
import re
import time
import uuid

from app.core.paths import project_path


_RULES_PATH = project_path("var/metadata/sql_error_repair_rules.json")
_RULES_CACHE_MTIME: float = -1.0
_RULES_CACHE: dict[str, Any] = {}

_SPACE_RE = re.compile(r"\s+")
_ORA_ERR_RE = re.compile(r"\bORA-\d{5}\b", re.IGNORECASE)
_DPI_ERR_RE = re.compile(r"\bDPI-\d{4}\b", re.IGNORECASE)
_DPY_ERR_RE = re.compile(r"\bDPY-\d{4}\b", re.IGNORECASE)

_DEFAULT_RULES: dict[str, Any] = {
    "enabled": True,
    "max_rules": 200,
    "rules": [],
}


def _normalize_sql(sql: str) -> str:
    text = str(sql or "").strip().rstrip(";")
    text = _SPACE_RE.sub(" ", text)
    return text.upper()


def _sql_hash(sql: str) -> str:
    normalized = _normalize_sql(sql)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _error_signature(error_message: str | None) -> str:
    text = _SPACE_RE.sub(" ", str(error_message or "")).strip()
    if not text:
        return "UNKNOWN"
    for pattern in (_ORA_ERR_RE, _DPI_ERR_RE, _DPY_ERR_RE):
        match = pattern.search(text)
        if match:
            return match.group(0).upper()

    lowered = text.lower()
    if "table not allowed" in lowered:
        return "TABLE_NOT_ALLOWED"
    if "join limit exceeded" in lowered:
        return "JOIN_LIMIT_EXCEEDED"
    if "where clause required" in lowered:
        return "WHERE_REQUIRED"
    return lowered[:80]


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_rules(payload: dict[str, Any]) -> None:
    global _RULES_CACHE_MTIME
    global _RULES_CACHE
    _RULES_PATH.parent.mkdir(parents=True, exist_ok=True)
    _RULES_PATH.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _RULES_CACHE = payload
    _RULES_CACHE_MTIME = _RULES_PATH.stat().st_mtime


def load_sql_error_repair_rules() -> dict[str, Any]:
    global _RULES_CACHE_MTIME
    global _RULES_CACHE

    if not _RULES_PATH.exists():
        _RULES_CACHE_MTIME = -1.0
        _RULES_CACHE = dict(_DEFAULT_RULES)
        return _RULES_CACHE

    mtime = _RULES_PATH.stat().st_mtime
    if _RULES_CACHE and _RULES_CACHE_MTIME == mtime:
        return _RULES_CACHE

    payload = _load_json(_RULES_PATH)
    enabled = bool(payload.get("enabled", _DEFAULT_RULES["enabled"]))
    max_rules = int(payload.get("max_rules", _DEFAULT_RULES["max_rules"]) or _DEFAULT_RULES["max_rules"])
    rules_raw = payload.get("rules", [])
    rules = rules_raw if isinstance(rules_raw, list) else []
    _RULES_CACHE = {
        "enabled": enabled,
        "max_rules": max_rules,
        "rules": rules,
    }
    _RULES_CACHE_MTIME = mtime
    return _RULES_CACHE


def find_learned_sql_fix(
    sql: str,
    *,
    error_message: str | None = None,
    allow_without_error: bool = False,
) -> dict[str, Any] | None:
    cfg = load_sql_error_repair_rules()
    if not bool(cfg.get("enabled", True)):
        return None

    sql_hash = _sql_hash(sql)
    signature = _error_signature(error_message) if error_message is not None else None
    # Blindly applying learned rewrites without the original error context can
    # degrade semantic accuracy. Only permit this when explicitly requested.
    if signature is None and not allow_without_error:
        return None
    rules = cfg.get("rules", [])
    if not isinstance(rules, list):
        return None

    for raw in reversed(rules):
        if not isinstance(raw, dict):
            continue
        if str(raw.get("failed_sql_hash") or "") != sql_hash:
            continue
        if signature is not None and str(raw.get("error_signature") or "") != signature:
            continue
        fixed_sql = str(raw.get("fixed_sql") or "").strip()
        if not fixed_sql:
            continue
        return raw
    return None


def mark_learned_sql_fix_used(rule_id: str) -> None:
    cfg = load_sql_error_repair_rules()
    rules = cfg.get("rules", [])
    if not isinstance(rules, list):
        return
    touched = False
    now_ts = int(time.time())
    for idx, raw in enumerate(rules):
        if not isinstance(raw, dict):
            continue
        if str(raw.get("id") or "") != str(rule_id):
            continue
        updated = dict(raw)
        updated["hit_count"] = int(updated.get("hit_count") or 0) + 1
        updated["last_used_at"] = now_ts
        rules[idx] = updated
        touched = True
        break
    if touched:
        cfg["rules"] = rules
        _save_rules(cfg)


def upsert_learned_sql_fix(
    *,
    failed_sql: str,
    fixed_sql: str,
    error_message: str | None = None,
    resolution_notes: list[str] | None = None,
) -> dict[str, Any] | None:
    failed_text = str(failed_sql or "").strip()
    fixed_text = str(fixed_sql or "").strip()
    if not failed_text or not fixed_text:
        return None
    if _normalize_sql(failed_text) == _normalize_sql(fixed_text):
        return None

    cfg = load_sql_error_repair_rules()
    if not bool(cfg.get("enabled", True)):
        return None

    rules = cfg.get("rules", [])
    if not isinstance(rules, list):
        rules = []

    now_ts = int(time.time())
    failed_hash = _sql_hash(failed_text)
    fixed_hash = _sql_hash(fixed_text)
    signature = _error_signature(error_message)
    notes: list[str] = []
    if isinstance(resolution_notes, list):
        for item in resolution_notes:
            token = str(item).strip()
            if token and token not in notes:
                notes.append(token)

    existing_idx: int | None = None
    for idx, raw in enumerate(rules):
        if not isinstance(raw, dict):
            continue
        if str(raw.get("failed_sql_hash") or "") != failed_hash:
            continue
        if str(raw.get("error_signature") or "") != signature:
            continue
        existing_idx = idx
        break

    if existing_idx is not None:
        updated = dict(rules[existing_idx])
        updated["fixed_sql"] = fixed_text
        updated["fixed_sql_hash"] = fixed_hash
        updated["error_signature"] = signature
        updated["success_count"] = int(updated.get("success_count") or 0) + 1
        updated["updated_at"] = now_ts
        updated["last_used_at"] = now_ts
        if notes:
            updated["resolution_notes"] = notes
        rules[existing_idx] = updated
        cfg["rules"] = rules
        _save_rules(cfg)
        return updated

    created = {
        "id": uuid.uuid4().hex,
        "error_signature": signature,
        "failed_sql_hash": failed_hash,
        "fixed_sql_hash": fixed_hash,
        "failed_sql_sample": failed_text[:500],
        "fixed_sql": fixed_text,
        "success_count": 1,
        "hit_count": 0,
        "created_at": now_ts,
        "updated_at": now_ts,
        "last_used_at": now_ts,
    }
    if notes:
        created["resolution_notes"] = notes
    rules.append(created)

    max_rules = int(cfg.get("max_rules") or _DEFAULT_RULES["max_rules"])
    if max_rules <= 0:
        max_rules = _DEFAULT_RULES["max_rules"]
    if len(rules) > max_rules:
        rules = sorted(
            [item for item in rules if isinstance(item, dict)],
            key=lambda item: int(item.get("updated_at") or 0),
            reverse=True,
        )[:max_rules]
        rules.reverse()

    cfg["rules"] = rules
    _save_rules(cfg)
    return created
