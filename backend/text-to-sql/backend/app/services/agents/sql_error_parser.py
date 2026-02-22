from __future__ import annotations

from typing import Any
import re


_ERR_CODE_RE = re.compile(r"\b(ORA-\d{5}|DPY-\d{4}|DPI-\d{4})\b", re.IGNORECASE)
_ORA_00904_RE = re.compile(
    r'ORA-00904:\s*(?:"(?P<owner>[A-Za-z0-9_$#]+)"\."(?P<identifier>[A-Za-z0-9_$#]+)"|"(?P<identifier_only>[A-Za-z0-9_$#]+)")',
    re.IGNORECASE,
)
_ORA_00979_RE = re.compile(r"ORA-00979", re.IGNORECASE)
_ORA_00933_RE = re.compile(r"ORA-00933", re.IGNORECASE)


def _extract_top_level_clause(sql: str, clause: str, *, stop_at: tuple[str, ...]) -> str:
    text = str(sql or "").strip()
    if not text:
        return ""
    upper = text.upper()
    clause_marker = f" {clause.upper()} "
    idx = upper.find(clause_marker)
    if idx < 0:
        return ""
    start = idx + len(clause_marker)
    end = len(text)
    for marker in stop_at:
        marker_idx = upper.find(f" {marker.upper()} ", start)
        if marker_idx >= 0:
            end = min(end, marker_idx)
    return text[start:end].strip()


def _split_top_level_csv(text: str) -> list[str]:
    value = str(text or "").strip()
    if not value:
        return []
    parts: list[str] = []
    depth = 0
    token: list[str] = []
    in_single = False
    i = 0
    while i < len(value):
        ch = value[i]
        if in_single:
            token.append(ch)
            if ch == "'":
                if i + 1 < len(value) and value[i + 1] == "'":
                    token.append(value[i + 1])
                    i += 1
                else:
                    in_single = False
            i += 1
            continue
        if ch == "'":
            in_single = True
            token.append(ch)
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            item = "".join(token).strip()
            if item:
                parts.append(item)
            token = []
            i += 1
            continue
        token.append(ch)
        i += 1
    tail = "".join(token).strip()
    if tail:
        parts.append(tail)
    return parts


def parse_sql_error(error_message: str, *, sql: str = "") -> dict[str, Any]:
    raw = str(error_message or "").strip()
    result: dict[str, Any] = {
        "error_code": "",
        "error_message": raw,
        "hint": "",
    }

    code_match = _ERR_CODE_RE.search(raw)
    if code_match:
        result["error_code"] = code_match.group(1).upper()

    ident_match = _ORA_00904_RE.search(raw)
    if ident_match:
        invalid_identifier = str(
            ident_match.group("identifier") or ident_match.group("identifier_only") or ""
        ).strip().upper()
        owner_or_alias = str(ident_match.group("owner") or "").strip().upper()
        result["invalid_identifier"] = invalid_identifier
        result["invalid_column"] = invalid_identifier
        if owner_or_alias:
            result["owner_or_alias"] = owner_or_alias
        result["hint"] = (
            f"Invalid identifier '{invalid_identifier}'. "
            "Use only existing table/column names from schema context."
        )
        return result

    if _ORA_00979_RE.search(raw):
        select_clause = _extract_top_level_clause(
            sql,
            "SELECT",
            stop_at=("FROM",),
        )
        group_by_clause = _extract_top_level_clause(
            sql,
            "GROUP BY",
            stop_at=("HAVING", "ORDER BY"),
        )
        select_items = _split_top_level_csv(select_clause)
        group_by_items = _split_top_level_csv(group_by_clause)
        result["error_code"] = result["error_code"] or "ORA-00979"
        result["select_items"] = select_items
        result["group_by_items"] = group_by_items
        result["hint"] = (
            "Every non-aggregated SELECT expression must appear in GROUP BY, "
            "or be wrapped with an aggregate function."
        )
        return result

    if _ORA_00933_RE.search(raw):
        result["error_code"] = result["error_code"] or "ORA-00933"
        result["hint"] = (
            "SQL command not properly ended. Check Oracle syntax, "
            "including SELECT-only constraints and trailing clauses."
        )
        return result

    if result["error_code"] == "ORA-00942":
        result["hint"] = "Table or view does not exist or is not accessible with current schema/permissions."
    elif result["error_code"] in {"DPY-4024", "DPI-1067", "ORA-03156"}:
        result["hint"] = "Connection timeout or closed connection. Reduce scan scope or retry with lighter query."
    elif result["error_code"] == "ORA-01031":
        result["hint"] = "Insufficient privileges. Use only tables/columns granted to current user."
    elif result["error_code"] == "ORA-01722":
        result["hint"] = "Invalid number conversion. Avoid implicit/forced numeric casts on text columns."
    return result
