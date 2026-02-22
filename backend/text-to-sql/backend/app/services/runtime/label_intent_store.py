from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import re

from app.core.paths import project_path


_LABEL_INTENT_PATH = project_path("var/metadata/label_intent_profiles.jsonl")
_LABEL_INTENT_CACHE_MTIME: float = -1.0
_LABEL_INTENT_CACHE: list[dict[str, Any]] = []


def _normalize(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "").lower())


def _as_token_list(values: Any, *, upper: bool = False) -> list[str]:
    if not isinstance(values, list):
        return []
    tokens: list[str] = []
    for raw in values:
        token = str(raw or "").strip()
        if upper:
            token = token.upper()
        if token and token not in tokens:
            tokens.append(token)
    return tokens


def _normalize_profile(item: dict[str, Any], idx: int) -> dict[str, Any] | None:
    profile_id = str(item.get("id") or item.get("name") or f"profile_{idx}").strip()
    if not profile_id:
        return None

    anchor_terms = _as_token_list(item.get("anchor_terms"), upper=True)
    if not anchor_terms:
        return None

    normalized: dict[str, Any] = {
        "id": profile_id,
        "name": str(item.get("name") or "").strip() or profile_id,
        "table": str(item.get("table") or "D_ITEMS").strip().upper() or "D_ITEMS",
        "event_table": str(item.get("event_table") or "PROCEDUREEVENTS").strip().upper() or "PROCEDUREEVENTS",
        "allow_sql_pattern_only": bool(item.get("allow_sql_pattern_only", False)),
        "question_any": _as_token_list(item.get("question_any")),
        "question_all": _as_token_list(item.get("question_all")),
        "question_intent_any": _as_token_list(item.get("question_intent_any")),
        "require_if_question_any": _as_token_list(item.get("require_if_question_any")),
        "anchor_terms": anchor_terms,
        "co_terms": _as_token_list(item.get("co_terms"), upper=True)
        or _as_token_list(item.get("insert_verb_terms"), upper=True),
        "required_terms_with_anchor": _as_token_list(item.get("required_terms_with_anchor"), upper=True),
        "exclude_terms_with_anchor": _as_token_list(item.get("exclude_terms_with_anchor"), upper=True),
        "normalize_or_groups": [],
    }

    normalize_groups = item.get("normalize_or_groups")
    if isinstance(normalize_groups, list):
        groups: list[dict[str, Any]] = []
        for group in normalize_groups:
            if not isinstance(group, dict):
                continue
            any_of = _as_token_list(group.get("any_of"), upper=True)
            to_value = str(group.get("to") or "").strip().upper()
            if not any_of or not to_value:
                continue
            groups.append({"any_of": any_of, "to": to_value})
        normalized["normalize_or_groups"] = groups

    return normalized


def load_label_intent_profiles() -> list[dict[str, Any]]:
    global _LABEL_INTENT_CACHE_MTIME
    global _LABEL_INTENT_CACHE

    if not _LABEL_INTENT_PATH.exists():
        _LABEL_INTENT_CACHE_MTIME = -1.0
        _LABEL_INTENT_CACHE = []
        return []

    mtime = _LABEL_INTENT_PATH.stat().st_mtime
    if _LABEL_INTENT_CACHE and _LABEL_INTENT_CACHE_MTIME == mtime:
        return _LABEL_INTENT_CACHE

    profiles: list[dict[str, Any]] = []
    for idx, line in enumerate(_LABEL_INTENT_PATH.read_text(encoding="utf-8").splitlines()):
        raw = line.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue
        normalized = _normalize_profile(item, idx)
        if normalized is None:
            continue
        profiles.append(normalized)

    _LABEL_INTENT_CACHE_MTIME = mtime
    _LABEL_INTENT_CACHE = profiles
    return profiles


def _token_hits(normalized_question: str, tokens: list[str]) -> int:
    if not tokens:
        return 0
    count = 0
    for token in tokens:
        if _normalize(token) in normalized_question:
            count += 1
    return count


def match_label_intent_profiles(
    question: str,
    *,
    profiles: list[dict[str, Any]] | None = None,
    k: int = 3,
) -> list[dict[str, Any]]:
    normalized_question = _normalize(question)
    if not normalized_question:
        return []

    source = profiles if profiles is not None else load_label_intent_profiles()
    matched: list[dict[str, Any]] = []
    for item in source:
        question_all = _as_token_list(item.get("question_all"))
        if question_all and any(_normalize(token) not in normalized_question for token in question_all):
            continue

        question_any = _as_token_list(item.get("question_any"))
        question_intent_any = _as_token_list(item.get("question_intent_any"))
        any_hits = _token_hits(normalized_question, question_any)
        intent_hits = _token_hits(normalized_question, question_intent_any)
        if question_any and any_hits <= 0:
            continue
        if question_intent_any and intent_hits <= 0:
            continue
        if not question_any and not question_intent_any and not question_all:
            continue

        score = 0
        score += any_hits * 2
        score += intent_hits * 3
        score += len(question_all) * 2
        if any_hits > 0 and intent_hits > 0:
            score += 2
        matched.append({**item, "_score": score})

    matched.sort(key=lambda entry: int(entry.get("_score") or 0), reverse=True)
    if k <= 0:
        return matched
    return matched[:k]
