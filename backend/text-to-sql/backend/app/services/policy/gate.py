from __future__ import annotations

import re
from fastapi import HTTPException

from app.core.config import get_settings
from app.services.runtime.settings_store import load_table_scope


_WRITE_KEYWORDS = re.compile(r"\b(delete|update|insert|merge|drop|alter|truncate)\b", re.IGNORECASE)
_CTE_REF = re.compile(r"(?:with|,)\s*([A-Za-z0-9_]+)\s+as\s*\(", re.IGNORECASE)
_AGG_FN_RE = re.compile(r"\b(count|avg|sum|min|max)\s*\(", re.IGNORECASE)
_SQL_TOKEN_RE = re.compile(r'"[^"]+"|[A-Za-z_][A-Za-z0-9_.$#]*|[(),]')
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--[^\r\n]*")
_SINGLE_QUOTED_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")
_ROWNUM_LIMIT_RE = re.compile(r"\bROWNUM\s*<=\s*\d+", re.IGNORECASE)
_FETCH_FIRST_RE = re.compile(r"\bFETCH\s+FIRST\s+\d+\s+ROWS\s+ONLY\b", re.IGNORECASE)
_LIMIT_RE = re.compile(r"\bLIMIT\s+\d+\b", re.IGNORECASE)
_DISTINCT_SELECT_RE = re.compile(r"^\s*SELECT\s+DISTINCT\b", re.IGNORECASE)

_FROM_CLAUSE_END_KEYWORDS = {
    "where",
    "group",
    "having",
    "order",
    "union",
    "intersect",
    "minus",
    "connect",
    "start",
    "model",
    "qualify",
}

_WHERE_OPTIONAL_QUESTION_HINTS = (
    "count",
    "how many",
    "number of",
    "distribution",
    "trend",
    "compare",
    "comparison",
    "average",
    "mean",
    "median",
    "ratio",
    "rate",
    "share",
    "proportion",
    "breakdown",
    "top",
    "most",
    "least",
    "summary",
    "aggregate",
    "분포",
    "추이",
    "비교",
    "평균",
    "중앙",
    "비율",
    "비중",
    "구성비",
    "점유율",
    "건수",
    "통계",
    "요약",
    "상위",
    "하위",
    "몇 명",
    "몇건",
    "여부",
    "상태",
    "플래그",
    "트렌드",
)

_WHERE_OPTIONAL_SAMPLE_HINTS = (
    "sample",
    "preview",
    "distinct",
    "list distinct",
    "value list",
    "미리보기",
    "샘플",
    "예시",
    "고유값",
    "distinct 값",
)


def _check(name: str, passed: bool, message: str) -> dict[str, str | bool]:
    return {"name": name, "passed": passed, "message": message}


def _strip_literals_and_comments(sql: str) -> str:
    # Avoid false positives (e.g., LIKE '%INSERT%') in write-keyword detection.
    text = _BLOCK_COMMENT_RE.sub(" ", sql)
    text = _LINE_COMMENT_RE.sub(" ", text)
    return _SINGLE_QUOTED_LITERAL_RE.sub("''", text)


def _table_ref_candidates(raw: str) -> list[str]:
    cleaned = raw.strip()
    cleaned = re.sub(r"[(),;]", "", cleaned)
    cleaned = cleaned.replace('"', "").strip()
    if not cleaned:
        return []
    parts = [part for part in cleaned.split(".") if part]
    candidates: list[str] = []
    if cleaned:
        candidates.append(cleaned)
    if parts:
        candidates.append(parts[-1])
        if len(parts) >= 2:
            candidates.append(parts[-2])
        candidates.append(parts[0])
    deduped: list[str] = []
    for item in candidates:
        if item and item not in deduped:
            deduped.append(item)
    return deduped


def _extract_table_refs(sql: str) -> list[str]:
    refs: list[str] = []
    depth = 0
    expecting_from_depths: set[int] = set()
    in_from_clause_depths: set[int] = set()
    awaiting_table_depth: int | None = None

    for match in _SQL_TOKEN_RE.finditer(sql):
        token = match.group(0)
        lowered = token.lower()

        if token == "(":
            depth += 1
            continue
        if token == ")":
            depth = max(0, depth - 1)
            expecting_from_depths = {d for d in expecting_from_depths if d <= depth}
            in_from_clause_depths = {d for d in in_from_clause_depths if d <= depth}
            if awaiting_table_depth is not None and awaiting_table_depth > depth:
                awaiting_table_depth = None
            continue

        if lowered == "select":
            expecting_from_depths.add(depth)
            continue

        if lowered == "from":
            if depth in expecting_from_depths:
                in_from_clause_depths.add(depth)
                awaiting_table_depth = depth
            continue

        if lowered == "join":
            awaiting_table_depth = depth
            continue

        if depth in in_from_clause_depths and lowered in _FROM_CLAUSE_END_KEYWORDS:
            in_from_clause_depths.discard(depth)
            if awaiting_table_depth == depth:
                awaiting_table_depth = None
            continue

        if token == "," and depth in in_from_clause_depths:
            awaiting_table_depth = depth
            continue

        if awaiting_table_depth is not None and depth == awaiting_table_depth:
            if token in {",", "("}:
                continue
            refs.append(token)
            awaiting_table_depth = None

    return refs


def _resolve_table_refs(sql: str, *, allowed_tables: set[str], cte_names: set[str]) -> tuple[list[str], list[str]]:
    resolved_tables: list[str] = []
    disallowed: list[str] = []
    for raw in _extract_table_refs(sql):
        candidates = _table_ref_candidates(raw)
        if not candidates:
            continue
        lowered = [candidate.lower() for candidate in candidates]
        if any(item in cte_names for item in lowered):
            continue

        matched = next((candidate for candidate in candidates if candidate.lower() in allowed_tables), None)
        if matched:
            resolved_tables.append(matched)
            continue

        # Fall back to the least-surprising token for diagnostics.
        fallback = candidates[-1]
        resolved_tables.append(fallback)
        disallowed.append(fallback)
    return resolved_tables, disallowed


def _has_safe_full_scope_shape(sql: str) -> bool:
    # Full-scope reads are allowed when the query shape is inherently bounded
    # (aggregation/grouping) or explicitly row-limited.
    if bool(_AGG_FN_RE.search(sql)) or bool(re.search(r"\bgroup\s+by\b", sql, re.IGNORECASE)):
        return True
    if _ROWNUM_LIMIT_RE.search(sql) or _FETCH_FIRST_RE.search(sql) or _LIMIT_RE.search(sql):
        return True
    return False


def _can_skip_where(question: str | None, sql: str) -> tuple[bool, str]:
    if _has_safe_full_scope_shape(sql):
        return True, "Safe full-scope read: WHERE optional"
    if not question:
        return False, ""
    q = question.lower()
    if _DISTINCT_SELECT_RE.search(sql) and any(hint in q for hint in _WHERE_OPTIONAL_SAMPLE_HINTS):
        return True, "Distinct sample/list question: WHERE optional"
    if not any(hint in q for hint in _WHERE_OPTIONAL_QUESTION_HINTS):
        return False, ""
    has_aggregate_shape = bool(_AGG_FN_RE.search(sql)) or bool(re.search(r"\bgroup\s+by\b", sql, re.IGNORECASE))
    if has_aggregate_shape:
        return True, "Aggregate question: WHERE optional"

    # Status/flag listing requests (e.g., "... 여부/상태") are often valid full-scope reads
    # without additional predicates.
    has_flag_projection = bool(re.search(r"\b[A-Za-z0-9_]*_FLAG\b", sql, re.IGNORECASE))
    mentions_status_intent = any(token in q for token in ("여부", "상태", "플래그", "status", "flag"))
    if has_flag_projection and mentions_status_intent:
        return True, "Status/flag question: WHERE optional"
    return False, ""


def precheck_sql(sql: str, question: str | None = None) -> dict[str, object]:
    text = sql.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Empty SQL")
    checks: list[dict[str, str | bool]] = []

    scan_text = _strip_literals_and_comments(text)
    if _WRITE_KEYWORDS.search(scan_text):
        checks.append(_check("Read-only", False, "Write keyword detected"))
        raise HTTPException(status_code=403, detail="Write operations are not allowed")
    checks.append(_check("Read-only", True, "No write keyword detected"))

    # Allow SELECT and CTE-based read-only queries (WITH ... SELECT ...).
    # Write keywords are already blocked by _WRITE_KEYWORDS above.
    statement_ok = bool(re.match(r"^\s*(select|with)\b", text, re.IGNORECASE))
    checks.append(_check("Statement type", statement_ok, "SELECT/CTE only"))
    if not statement_ok:
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
    if re.match(r"^\s*with\b", text, re.IGNORECASE):
        cte_has_select = bool(re.search(r"\bselect\b", text, re.IGNORECASE))
        checks.append(_check("CTE", cte_has_select, "WITH clause includes SELECT"))
        if not cte_has_select:
            raise HTTPException(status_code=400, detail="CTE query must include SELECT")

    settings = get_settings()
    join_count = len(re.findall(r"\bjoin\b", text, re.IGNORECASE))
    join_ok = join_count <= settings.max_db_joins
    checks.append(_check("Join limit", join_ok, f"{join_count}/{settings.max_db_joins} joins"))
    if join_count > settings.max_db_joins:
        raise HTTPException(status_code=400, detail="Join limit exceeded")

    has_where = re.search(r"\bwhere\b", text, re.IGNORECASE) is not None
    where_optional, where_reason = _can_skip_where(question, text)
    where_ok = has_where or where_optional
    if has_where:
        where_message = "WHERE clause present"
    elif where_optional:
        where_message = where_reason or "WHERE optional"
    else:
        where_message = "WHERE clause required"
    checks.append(_check("WHERE rule", where_ok, where_message))
    if not has_where and not where_optional:
        raise HTTPException(status_code=403, detail="WHERE clause required")

    # Keep table-scope enforcement stable across per-user/global settings:
    # when user-specific scope is absent, fall back to global scope instead
    # of treating scope as unrestricted.
    allowed_tables = {
        name.lower()
        for name in load_table_scope(include_global_fallback=True)
        if name
    }
    if allowed_tables:
        # Oracle pseudo-table used in scalar SELECT patterns; safe to allow even with scope enabled.
        allowed_tables.add("dual")
        cte_names = {name.lower() for name in _CTE_REF.findall(text)}
        found_tables, disallowed = _resolve_table_refs(
            text,
            allowed_tables=allowed_tables,
            cte_names=cte_names,
        )
        scope_ok = not disallowed
        if scope_ok:
            checks.append(_check("Table scope", True, f"{len(found_tables)} table references allowed"))
        else:
            checks.append(_check("Table scope", False, f"Disallowed: {', '.join(sorted(set(disallowed)))}"))
        if disallowed:
            raise HTTPException(
                status_code=403,
                detail=f"Table not allowed: {', '.join(sorted(set(disallowed)))}",
            )
    else:
        checks.append(_check("Table scope", True, "No table scope restriction"))

    return {"passed": True, "checks": checks}
