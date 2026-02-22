from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import json
import re
import time

from app.core.paths import project_path
from app.core.config import get_settings
from app.services.oracle.connection import acquire_connection
from app.services.runtime.settings_store import load_connection_settings


_DIAGNOSIS_MAP_PATH = project_path("var/metadata/diagnosis_icd_map.jsonl")
_EVAL_DIAGNOSIS_SOURCE_PATH = project_path("docs/query_visualization_eval_aside.jsonl")
_DIAGNOSIS_MAP_CACHE_KEY: tuple[float, float, int, int] | None = None
_DIAGNOSIS_MAP_CACHE: list[dict[str, Any]] = []
_D_ICD_DIAGNOSES_CACHE_LOADED = False
_D_ICD_DIAGNOSES_CACHE_AT = 0.0
_D_ICD_DIAGNOSES_CACHE: list[dict[str, str]] = []
_D_ICD_DIAGNOSES_CACHE_TTL_SEC = 300

_DIAGNOSES_ALIAS_RE = re.compile(
    r"(?is)\b(?:from|join)\s+[A-Za-z0-9_$.\"]*diagnoses_icd\b(?:\s+(?:as\s+)?([A-Za-z][A-Za-z0-9_]*))?"
)
_HAS_DIAGNOSES_TABLE_RE = re.compile(r"(?i)\bdiagnoses_icd\b")
_QUOTED_CODE_RE = re.compile(r"'([A-Za-z0-9\.]+)'")
_GENERIC_ICD_LIKE_RE = re.compile(r"(?i)\bicd_code\s+like\s+'([A-Za-z0-9\.]+)%'")
_GENERIC_ICD_EQ_RE = re.compile(r"(?i)\bicd_code\s*=\s*'([A-Za-z0-9\.]+)'")
_GENERIC_ICD_IN_RE = re.compile(r"(?i)\bicd_code\s+in\s*\((.*?)\)", re.DOTALL)

_KO_DIAG_TERM_RE = re.compile(r"([가-힣A-Za-z0-9/()+,\-\s]{2,}?)\s*진단")
_EN_DIAG_TERM_RE = re.compile(r"([A-Za-z][A-Za-z0-9/()+,\-\s]{1,}?)\s*(?:diagnosis|diagnosed)", re.IGNORECASE)
_TERM_BEFORE_PATIENT_RE = re.compile(r"([가-힣A-Za-z0-9/()+,\-\s]{2,}?)\s*환자")
_VS_TERM_RE = re.compile(r"([가-힣A-Za-z0-9/()+\-]{2,})\s*vs\s*([가-힣A-Za-z0-9/()+\-]{2,})", re.IGNORECASE)
_TERM_SPLIT_RE = re.compile(r"\s*(?:,|\+|vs|및|and)\s*", re.IGNORECASE)

_SQL_RESERVED = {
    "where",
    "on",
    "group",
    "order",
    "left",
    "right",
    "full",
    "inner",
    "outer",
    "join",
    "union",
}

_TERM_STOPWORDS = {
    "진단",
    "사망률",
    "ICU 사망률",
    "병원 사망률",
    "비율",
    "유병률",
    "분포",
    "재입원",
    "재입원율",
    "연도별",
    "비교",
    "패턴 동일",
    "환자",
    "입원",
    "치료",
    "사용",
    "미사용",
    "동반",
    "여부",
    "기간",
    "수술",
    "시행",
    "미시행",
    "ICU",
    "ER",
    "LOS",
    "MAP",
    "SQL",
    "HOUR",
    "HOURS",
    "DAY",
    "DAYS",
}
_TRAILING_QUALIFIERS = ("환자", "진단", "동반", "대상", "그룹", "코호트", "질환", "증후군")
_TERM_NOISE_SUBSTRINGS = (
    "사망률",
    "재입원",
    "비율",
    "유병률",
    "분포",
    "진단을받은",
    "패턴동일",
    "기계환기",
    "치료",
    "미시행",
    "시행",
    "사용",
    "평균",
    "연도별",
    "에서",
    "이내",
)
_TERM_ALIAS_HINTS: dict[str, list[str]] = {
    "암": ["cancer", "carcinoma", "neoplasm", "malignan"],
}


def _normalize_match_text(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "").lower())


_TERM_STOPWORDS_NORMALIZED = {_normalize_match_text(term) for term in _TERM_STOPWORDS}


def _dedupe_ordered(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = _normalize_match_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _safe_close(resource: Any) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except Exception:
        pass


def _normalize_free_text(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", str(text or "").lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _valid_schema_name(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_$#]+", str(value or "").strip()))


def _resolve_runtime_schema() -> str:
    settings = get_settings()
    overrides = load_connection_settings()
    schema = str(overrides.get("defaultSchema") or settings.oracle_default_schema or "").strip()
    return schema if _valid_schema_name(schema) else ""


def _normalize_icd_prefix(raw: str) -> str:
    value = str(raw or "").strip().upper().replace(".", "").replace("%", "")
    if len(value) < 2:
        return ""
    if not re.fullmatch(r"[A-Z0-9]+", value):
        return ""
    return value


def _extract_diagnoses_aliases(sql: str) -> set[str]:
    aliases: set[str] = set()
    for alias in _DIAGNOSES_ALIAS_RE.findall(sql):
        token = str(alias or "").strip().strip('"').lower()
        if not token or token in _SQL_RESERVED:
            continue
        aliases.add(token)
    if re.search(r"(?i)\bdiagnoses_icd\s*\.\s*icd_code\b", sql):
        aliases.add("diagnoses_icd")
    return aliases


def _extract_icd_prefixes_with_alias(sql: str, alias: str) -> list[str]:
    prefixes: list[str] = []
    like_re = re.compile(rf"(?i)\b{re.escape(alias)}\s*\.\s*icd_code\s+like\s+'([A-Za-z0-9\.]+)%'")
    eq_re = re.compile(rf"(?i)\b{re.escape(alias)}\s*\.\s*icd_code\s*=\s*'([A-Za-z0-9\.]+)'")
    in_re = re.compile(rf"(?i)\b{re.escape(alias)}\s*\.\s*icd_code\s+in\s*\((.*?)\)", re.DOTALL)

    for code in like_re.findall(sql):
        normalized = _normalize_icd_prefix(code)
        if normalized:
            prefixes.append(normalized)
    for code in eq_re.findall(sql):
        normalized = _normalize_icd_prefix(code)
        if normalized:
            prefixes.append(normalized)
    for block in in_re.findall(sql):
        for code in _QUOTED_CODE_RE.findall(block):
            normalized = _normalize_icd_prefix(code)
            if normalized:
                prefixes.append(normalized)
    return prefixes


def _extract_icd_prefixes_from_sql(sql: str) -> list[str]:
    text = str(sql or "")
    if not text:
        return []
    if not _HAS_DIAGNOSES_TABLE_RE.search(text):
        return []

    aliases = _extract_diagnoses_aliases(text)
    prefixes: list[str] = []
    if aliases:
        for alias in aliases:
            prefixes.extend(_extract_icd_prefixes_with_alias(text, alias))
    else:
        for code in _GENERIC_ICD_LIKE_RE.findall(text):
            normalized = _normalize_icd_prefix(code)
            if normalized:
                prefixes.append(normalized)
        for code in _GENERIC_ICD_EQ_RE.findall(text):
            normalized = _normalize_icd_prefix(code)
            if normalized:
                prefixes.append(normalized)
        for block in _GENERIC_ICD_IN_RE.findall(text):
            for code in _QUOTED_CODE_RE.findall(block):
                normalized = _normalize_icd_prefix(code)
                if normalized:
                    prefixes.append(normalized)
    return _dedupe_ordered(prefixes)


def _load_d_icd_diagnoses_rows() -> list[dict[str, str]]:
    global _D_ICD_DIAGNOSES_CACHE_LOADED
    global _D_ICD_DIAGNOSES_CACHE_AT
    global _D_ICD_DIAGNOSES_CACHE

    now = time.time()
    if (
        _D_ICD_DIAGNOSES_CACHE_LOADED
        and now - _D_ICD_DIAGNOSES_CACHE_AT < _D_ICD_DIAGNOSES_CACHE_TTL_SEC
    ):
        return _D_ICD_DIAGNOSES_CACHE

    rows: list[dict[str, str]] = []
    conn: Any = None
    cur: Any = None
    schema_cur: Any = None
    try:
        conn = acquire_connection()
        schema = _resolve_runtime_schema()
        if schema:
            schema_cur = conn.cursor()
            schema_cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema}")
            _safe_close(schema_cur)
            schema_cur = None

        cur = conn.cursor()
        cur.execute(
            """
            SELECT ICD_CODE, LONG_TITLE
            FROM D_ICD_DIAGNOSES
            WHERE ICD_CODE IS NOT NULL
              AND LONG_TITLE IS NOT NULL
            """
        )
        for icd_code, long_title in cur.fetchall():
            code = _normalize_icd_prefix(str(icd_code or ""))
            title = _normalize_free_text(str(long_title or ""))
            if not code or not title:
                continue
            rows.append({"icd_code": code, "long_title_norm": title})
    except Exception:
        rows = []
    finally:
        _safe_close(cur)
        _safe_close(schema_cur)
        _safe_close(conn)

    _D_ICD_DIAGNOSES_CACHE_LOADED = True
    _D_ICD_DIAGNOSES_CACHE_AT = now
    _D_ICD_DIAGNOSES_CACHE = rows
    return rows


def _clean_term(raw: str) -> str:
    term = str(raw or "").strip().strip("[]{}.,;:!?\"'")
    term = re.sub(r"\s+", " ", term).strip()
    return term


def _normalize_term_candidate(raw: str) -> str:
    term = _clean_term(raw)
    if not term:
        return ""
    for suffix in _TRAILING_QUALIFIERS:
        if term.endswith(suffix) and len(term) > len(suffix):
            term = term[: -len(suffix)].strip()
    if term.startswith("(") and term.endswith(")") and len(term) > 2:
        term = term[1:-1].strip()
    return term


def _is_noise_term(raw: str) -> bool:
    term = _normalize_term_candidate(raw)
    if not term:
        return True
    normalized = _normalize_match_text(term)
    if not normalized:
        return True
    if normalized in _TERM_STOPWORDS_NORMALIZED:
        return True
    if len(term.split()) >= 4:
        return True
    if any(token in normalized for token in _TERM_NOISE_SUBSTRINGS):
        return True
    if normalized.endswith("사망률") or normalized.endswith("재입원율"):
        return True
    if re.fullmatch(r"[0-9]+", normalized):
        return True
    if re.fullmatch(r"[A-Za-z0-9]+", term) and len(term) < 2:
        return True
    if not re.search(r"[A-Za-z가-힣]", term):
        return True
    return False


def _match_candidate_position(question: str, candidate: str) -> int:
    query = str(question or "")
    target = str(candidate or "").strip()
    if not query or not target:
        return -1

    if re.fullmatch(r"[A-Za-z0-9]{2,6}", target):
        pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(target)}(?![A-Za-z0-9])", re.IGNORECASE)
        matched = pattern.search(query)
        return matched.start() if matched else -1

    pos = query.lower().find(target.lower())
    if pos >= 0:
        return pos
    normalized_query = _normalize_match_text(query)
    normalized_target = _normalize_match_text(target)
    return 0 if normalized_target and normalized_target in normalized_query else -1


def _split_candidate_terms(raw_term: str) -> list[str]:
    text = _normalize_term_candidate(raw_term)
    if not text:
        return []

    chunks: list[str] = []
    if "(" in text and ")" in text:
        head = _normalize_term_candidate(text.split("(", 1)[0])
        if head:
            chunks.append(head)
        for block in re.findall(r"\(([^)]{1,120})\)", text):
            chunks.extend([part for part in _TERM_SPLIT_RE.split(block) if part])
    else:
        chunks = [chunk for chunk in _TERM_SPLIT_RE.split(text) if chunk]

    terms: list[str] = []
    for chunk in chunks or [text]:
        token = _normalize_term_candidate(chunk)
        if not token:
            continue
        if "/" in token:
            split_tokens = [part for part in token.split("/") if part]
            terms.extend([_normalize_term_candidate(part) for part in split_tokens if _normalize_term_candidate(part)])
        else:
            terms.append(token)

    filtered = [term for term in terms if not _is_noise_term(term)]
    return _dedupe_ordered(filtered)


def _load_base_diagnosis_map(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    entries: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue

        term = str(item.get("term") or "").strip()
        if not term:
            continue
        aliases_raw = item.get("aliases") or []
        aliases = [str(alias).strip() for alias in aliases_raw if str(alias).strip()] if isinstance(aliases_raw, list) else []
        prefixes_raw = item.get("icd_prefixes") or item.get("prefixes") or []
        prefixes = [str(prefix).strip().upper().replace(".", "") for prefix in prefixes_raw if str(prefix).strip()] if isinstance(prefixes_raw, list) else []
        dedup_prefixes: list[str] = []
        for prefix in prefixes:
            if prefix and prefix not in dedup_prefixes:
                dedup_prefixes.append(prefix)
        if not dedup_prefixes:
            continue
        entries.append(
            {
                "term": term,
                "aliases": aliases,
                "icd_prefixes": dedup_prefixes,
            }
        )
    return entries


def _extract_terms_from_base_map(question: str, base_entries: list[dict[str, Any]]) -> list[dict[str, str]]:
    query = str(question or "").strip()
    if not query or not base_entries:
        return []

    hits: list[tuple[int, int, str, str]] = []
    for entry in base_entries:
        if not isinstance(entry, dict):
            continue
        term = str(entry.get("term") or "").strip()
        if not term:
            continue
        aliases_raw = entry.get("aliases") or []
        aliases = [str(alias).strip() for alias in aliases_raw if str(alias).strip()] if isinstance(aliases_raw, list) else []
        candidates = _dedupe_ordered([term, *aliases])

        best: tuple[int, int, str] | None = None
        for candidate in candidates:
            if _is_noise_term(candidate):
                continue
            pos = _match_candidate_position(query, candidate)
            if pos < 0:
                continue
            score = len(_normalize_match_text(candidate))
            if best is None or pos < best[0] or (pos == best[0] and score > best[1]):
                best = (pos, score, candidate)
        if best is None:
            continue
        hits.append((best[0], best[1], term, best[2]))

    hits.sort(key=lambda row: (row[0], -row[1], _normalize_match_text(row[2])))
    records: list[dict[str, str]] = []
    seen: set[str] = set()
    for _, _, canonical, surface in hits:
        key = _normalize_match_text(canonical)
        if not key or key in seen:
            continue
        seen.add(key)
        records.append({"term": canonical, "alias": surface})
    return records


def _extract_query_terms_for_icd_mapping(question: str, base_entries: list[dict[str, Any]]) -> list[dict[str, str]]:
    query = str(question or "").strip()
    if not query:
        return []

    records = _extract_terms_from_base_map(query, base_entries)

    candidates: list[str] = []
    for pattern in (_KO_DIAG_TERM_RE, _EN_DIAG_TERM_RE, _TERM_BEFORE_PATIENT_RE):
        for match in pattern.finditer(query):
            candidates.extend(_split_candidate_terms(match.group(1)))
    for match in _VS_TERM_RE.finditer(query):
        candidates.extend(_split_candidate_terms(match.group(1)))
        candidates.extend(_split_candidate_terms(match.group(2)))

    known_keys = {_normalize_match_text(str(item.get("term") or "")) for item in records}
    for candidate in candidates:
        if _is_noise_term(candidate):
            continue
        normalized_candidate = _normalize_match_text(candidate)
        if not normalized_candidate:
            continue

        matched_base = _extract_terms_from_base_map(candidate, base_entries)
        if matched_base:
            canonical = str(matched_base[0].get("term") or "").strip()
            if canonical:
                records.append({"term": canonical, "alias": candidate})
                known_keys.add(_normalize_match_text(canonical))
            continue

        related_key = next((key for key in known_keys if normalized_candidate in key or key in normalized_candidate), "")
        if related_key:
            related_term = next(
                (
                    str(item.get("term") or "").strip()
                    for item in records
                    if _normalize_match_text(str(item.get("term") or "")) == related_key
                ),
                "",
            )
            if related_term:
                records.append({"term": related_term, "alias": candidate})
            continue
        records.append({"term": candidate, "alias": candidate})
        known_keys.add(normalized_candidate)

    deduped: list[dict[str, str]] = []
    seen_pairs: set[str] = set()
    for item in records:
        term = _normalize_term_candidate(item.get("term", ""))
        alias = _normalize_term_candidate(item.get("alias", ""))
        if _is_noise_term(term):
            continue
        pair_key = f"{_normalize_match_text(term)}::{_normalize_match_text(alias)}"
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)
        deduped.append({"term": term, "alias": alias or term})
    return deduped


def _build_term_match_phrases(term: str, aliases: list[str]) -> list[str]:
    phrases: list[str] = []
    raw_values = [term, *aliases, *(_TERM_ALIAS_HINTS.get(term, []))]
    for raw in raw_values:
        value = _normalize_term_candidate(raw)
        if not value:
            continue
        normalized = _normalize_free_text(value)
        if not normalized:
            continue
        if normalized in _TERM_STOPWORDS_NORMALIZED:
            continue
        if len(normalized) < 3:
            continue
        phrases.append(normalized)
    return _dedupe_ordered(phrases)


def _title_matches_term(title_norm: str, phrases: list[str]) -> bool:
    text = str(title_norm or "")
    if not text:
        return False
    for phrase in phrases:
        token = phrase.strip()
        if not token:
            continue
        if token in text:
            return True
    return False


def _filter_prefixes_with_d_icd_diagnoses(
    *,
    term: str,
    aliases: list[str],
    prefixes: list[str],
    d_icd_rows: list[dict[str, str]],
) -> list[str]:
    if not prefixes:
        return []
    if not d_icd_rows:
        return prefixes

    phrases = _build_term_match_phrases(term, aliases)
    if not phrases:
        return prefixes

    filtered: list[str] = []
    title_cache: dict[str, list[str]] = {}
    for prefix in prefixes:
        norm_prefix = _normalize_icd_prefix(prefix)
        if not norm_prefix:
            continue
        titles = title_cache.get(norm_prefix)
        if titles is None:
            titles = [
                row.get("long_title_norm", "")
                for row in d_icd_rows
                if str(row.get("icd_code", "")).startswith(norm_prefix)
            ]
            title_cache[norm_prefix] = titles
        if titles and any(_title_matches_term(title, phrases) for title in titles):
            filtered.append(norm_prefix)
    return _dedupe_ordered(filtered)


def _merge_term_alias(existing_aliases: list[str], alias: str, term: str) -> list[str]:
    normalized_term = _normalize_match_text(term)
    aliases = [str(item).strip() for item in existing_aliases if str(item).strip()]
    if alias and _normalize_match_text(alias) != normalized_term and not _is_noise_term(alias):
        aliases.append(alias)
    return _dedupe_ordered(aliases)


def _build_eval_diagnosis_map(
    path: Path,
    base_entries: list[dict[str, Any]],
    d_icd_rows: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    resolved_d_icd_rows = d_icd_rows if d_icd_rows is not None else _load_d_icd_diagnoses_rows()
    base_alias_index: dict[str, list[str]] = {}
    for item in base_entries:
        if not isinstance(item, dict):
            continue
        term = _normalize_term_candidate(str(item.get("term") or ""))
        if not term:
            continue
        key = _normalize_match_text(term)
        aliases_raw = item.get("aliases") or []
        aliases = [str(alias).strip() for alias in aliases_raw if str(alias).strip()] if isinstance(aliases_raw, list) else []
        base_alias_index[key] = _dedupe_ordered([*base_alias_index.get(key, []), *aliases])

    buckets: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue
        query = str(item.get("user_query") or "").strip()
        sql = str(item.get("sql") or "").strip()
        if not query or not sql:
            continue
        if "패턴 동일" in query:
            continue

        prefixes = _extract_icd_prefixes_from_sql(sql)
        if not prefixes:
            continue

        term_records = _extract_query_terms_for_icd_mapping(query, base_entries=base_entries)
        if not term_records:
            continue

        for term_record in term_records:
            term = _normalize_term_candidate(term_record.get("term", ""))
            alias = _normalize_term_candidate(term_record.get("alias", ""))
            if _is_noise_term(term):
                continue
            term_key = _normalize_match_text(term)
            term_aliases = _dedupe_ordered(
                [
                    *([alias] if alias else []),
                    *base_alias_index.get(term_key, []),
                ]
            )
            validated_prefixes = _filter_prefixes_with_d_icd_diagnoses(
                term=term,
                aliases=term_aliases,
                prefixes=prefixes,
                d_icd_rows=resolved_d_icd_rows,
            )
            if resolved_d_icd_rows and validated_prefixes:
                effective_prefixes = validated_prefixes
            elif resolved_d_icd_rows and len(term_records) > 1:
                # When multiple diagnosis terms appear in one question, require
                # dictionary-supported prefix evidence to prevent cross-contamination.
                effective_prefixes = []
            else:
                effective_prefixes = prefixes
            if not effective_prefixes:
                continue

            key = term_key
            if not key:
                continue

            bucket = buckets.get(key)
            if bucket is None:
                bucket = {
                    "term": term,
                    "aliases": [],
                    "icd_prefixes": [],
                }
                buckets[key] = bucket

            bucket["aliases"] = _merge_term_alias(bucket.get("aliases", []), alias, term)
            bucket["icd_prefixes"] = _dedupe_ordered([*bucket.get("icd_prefixes", []), *effective_prefixes])

    return sorted(
        [
            {
                "term": str(item.get("term") or "").strip(),
                "aliases": [str(alias).strip() for alias in item.get("aliases", []) if str(alias).strip()],
                "icd_prefixes": [
                    _normalize_icd_prefix(prefix)
                    for prefix in item.get("icd_prefixes", [])
                    if _normalize_icd_prefix(prefix)
                ],
            }
            for item in buckets.values()
            if str(item.get("term") or "").strip() and item.get("icd_prefixes")
        ],
        key=lambda entry: _normalize_match_text(str(entry.get("term") or "")),
    )


def _merge_diagnosis_maps(base: list[dict[str, Any]], extras: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = [dict(item) for item in base]
    index: dict[str, int] = {
        _normalize_match_text(str(item.get("term") or "")): idx
        for idx, item in enumerate(merged)
        if str(item.get("term") or "").strip()
    }

    for extra in extras:
        term = str(extra.get("term") or "").strip()
        if not term:
            continue
        key = _normalize_match_text(term)
        if not key:
            continue
        extra_aliases = [str(alias).strip() for alias in extra.get("aliases", []) if str(alias).strip()]
        extra_prefixes = [
            _normalize_icd_prefix(prefix)
            for prefix in extra.get("icd_prefixes", [])
            if _normalize_icd_prefix(prefix)
        ]
        if not extra_prefixes:
            continue

        existing_idx = index.get(key)
        if existing_idx is None:
            merged.append(
                {
                    "term": term,
                    "aliases": _dedupe_ordered(extra_aliases),
                    "icd_prefixes": _dedupe_ordered(extra_prefixes),
                }
            )
            index[key] = len(merged) - 1
            continue

        existing = merged[existing_idx]
        existing_aliases = [str(alias).strip() for alias in existing.get("aliases", []) if str(alias).strip()]
        existing["aliases"] = _dedupe_ordered([*existing_aliases, *extra_aliases])
        if not existing.get("icd_prefixes"):
            existing["icd_prefixes"] = _dedupe_ordered(extra_prefixes)
    return merged


def load_diagnosis_icd_map() -> list[dict[str, Any]]:
    global _DIAGNOSIS_MAP_CACHE_KEY
    global _DIAGNOSIS_MAP_CACHE

    base_mtime = _DIAGNOSIS_MAP_PATH.stat().st_mtime if _DIAGNOSIS_MAP_PATH.exists() else -1.0
    eval_mtime = _EVAL_DIAGNOSIS_SOURCE_PATH.stat().st_mtime if _EVAL_DIAGNOSIS_SOURCE_PATH.exists() else -1.0
    d_icd_rows = _load_d_icd_diagnoses_rows()
    d_icd_cache_bucket = int(_D_ICD_DIAGNOSES_CACHE_AT // _D_ICD_DIAGNOSES_CACHE_TTL_SEC)
    cache_key = (base_mtime, eval_mtime, d_icd_cache_bucket, len(d_icd_rows))
    if _DIAGNOSIS_MAP_CACHE and _DIAGNOSIS_MAP_CACHE_KEY == cache_key:
        return _DIAGNOSIS_MAP_CACHE

    base_entries = _load_base_diagnosis_map(_DIAGNOSIS_MAP_PATH)
    eval_entries = _build_eval_diagnosis_map(
        _EVAL_DIAGNOSIS_SOURCE_PATH,
        base_entries=base_entries,
        d_icd_rows=d_icd_rows,
    )
    entries = _merge_diagnosis_maps(base_entries, eval_entries)

    _DIAGNOSIS_MAP_CACHE_KEY = cache_key
    _DIAGNOSIS_MAP_CACHE = entries
    return entries


def match_diagnosis_mappings(question: str, diagnosis_map: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    normalized_question = _normalize_match_text(question)
    if not normalized_question:
        return []

    matched: list[dict[str, Any]] = []
    source = diagnosis_map if diagnosis_map is not None else load_diagnosis_icd_map()
    for item in source:
        term = str(item.get("term") or "").strip()
        aliases = [str(alias).strip() for alias in item.get("aliases", []) if str(alias).strip()]
        prefixes = [str(prefix).strip().upper().replace(".", "") for prefix in item.get("icd_prefixes", []) if str(prefix).strip()]
        if not term or not prefixes:
            continue
        candidates = [term, *aliases]
        hit_keywords = [candidate for candidate in candidates if _normalize_match_text(candidate) in normalized_question]
        if not hit_keywords:
            continue
        hit_score = max(len(keyword) for keyword in hit_keywords)
        matched.append(
            {
                "term": term,
                "aliases": aliases,
                "icd_prefixes": prefixes,
                "_score": hit_score,
            }
        )
    matched.sort(key=lambda entry: int(entry.get("_score", 0)), reverse=True)
    return matched


def map_prefixes_for_terms(diagnosis_map: list[dict[str, Any]], terms: Iterable[str]) -> list[str]:
    normalized_terms = {_normalize_match_text(str(term)) for term in terms if str(term).strip()}
    if not normalized_terms:
        return []
    prefixes: list[str] = []
    for item in diagnosis_map:
        candidates = [str(item.get("term") or ""), *[str(alias) for alias in item.get("aliases", [])]]
        normalized_candidates = {_normalize_match_text(candidate) for candidate in candidates if candidate}
        if not normalized_candidates.intersection(normalized_terms):
            continue
        for prefix in item.get("icd_prefixes", []):
            value = str(prefix).strip().upper().replace(".", "")
            if value and value not in prefixes:
                prefixes.append(value)
    return prefixes
