from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable


KEYWORDS = {
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER",
    "ON", "GROUP", "BY", "ORDER", "HAVING", "AS", "AND", "OR", "NOT", "IN", "IS",
    "NULL", "LIKE", "BETWEEN", "DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "END",
    "ASC", "DESC", "UNION", "ALL", "EXISTS", "OVER", "PARTITION", "ROWS",
    "CURRENT_DATE", "SYSDATE", "INTERVAL", "ROWNUM", "DUAL",
    "DAY", "MONTH", "YEAR",
}

FUNCTIONS = {
    "COUNT", "AVG", "SUM", "MIN", "MAX",
    "CAST", "TO_DATE", "TRUNC", "NVL", "COALESCE", "DECODE",
    "UPPER", "LOWER", "SUBSTR", "ROUND", "FLOOR", "CEIL",
    "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE",
}

IGNORE_TOKENS = KEYWORDS | FUNCTIONS


def _strip_literals(sql: str) -> str:
    text = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    text = re.sub(r"'([^']|'')*'", "''", text)
    return text


def _normalize_table_name(name: str) -> str:
    return name.split(".")[-1].upper()


def _find_tables(sql: str) -> tuple[dict[str, str], list[str]]:
    alias_map: dict[str, str] = {}
    tables: list[str] = []
    pattern = re.compile(r"\b(from|join)\s+([A-Za-z0-9_\.]+)(?:\s+([A-Za-z0-9_]+))?", re.IGNORECASE)
    for _, table, alias in pattern.findall(sql):
        table_name = _normalize_table_name(table)
        alias_name = (alias or table_name).upper()
        alias_map[alias_name] = table_name
        tables.append(table_name)
    return alias_map, tables


def _extract_clause(sql: str, start_kw: str, end_kws: Iterable[str]) -> str:
    lower = sql.lower()
    start = lower.find(start_kw)
    if start < 0:
        return ""
    start += len(start_kw)
    end = len(sql)
    for kw in end_kws:
        idx = lower.find(kw, start)
        if idx != -1 and idx < end:
            end = idx
    return sql[start:end]


def _extract_on_clauses(sql: str) -> list[str]:
    return re.findall(
        r"\bon\b(.*?)(?=\bjoin\b|\bwhere\b|\bgroup\b|\border\b|\bhaving\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )


def _extract_select_aliases(select_clause: str) -> set[str]:
    aliases = set(re.findall(r"\bas\s+([A-Za-z_][A-Za-z0-9_$#]*)", select_clause, flags=re.IGNORECASE))
    return {a.upper() for a in aliases}


def _load_jsonl(path: Path) -> list[tuple[int, dict[str, Any]]]:
    items: list[tuple[int, dict[str, Any]]] = []
    if not path.exists():
        return items
    for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            items.append((idx, obj))
    return items


def _validate_sql(sql: str, schema: dict[str, set[str]]) -> list[str]:
    issues: list[str] = []
    text = _strip_literals(sql)
    alias_map, tables = _find_tables(text)

    # Table existence
    for table in tables:
        if table not in schema and table != "DUAL":
            issues.append(f"unknown table: {table}")

    # Qualified columns
    for alias, col in re.findall(r"\b([A-Za-z_][A-Za-z0-9_$#]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_$#]*)\b", text):
        alias_up = alias.upper()
        col_up = col.upper()
        if alias_up not in alias_map:
            continue
        table = alias_map[alias_up]
        if table in schema and col_up not in schema[table]:
            issues.append(f"unknown column: {alias}.{col} (table {table})")

    # Unqualified columns in key clauses (skip if derived tables are used)
    if re.search(r"\bfrom\s*\(", text, re.IGNORECASE) or re.search(r"\bjoin\s*\(", text, re.IGNORECASE):
        return issues

    tables_in_query = [t for t in tables if t in schema]
    select_clause = _extract_clause(text, "select", ["from"])
    select_aliases = _extract_select_aliases(select_clause)
    clause_texts = [
        select_clause,
        _extract_clause(text, "where", ["group by", "order by", "having"]),
        _extract_clause(text, "group by", ["order by", "having"]),
        _extract_clause(text, "having", ["order by"]),
        _extract_clause(text, "order by", []),
    ]
    clause_texts.extend(_extract_on_clauses(text))

    ignore = set(IGNORE_TOKENS)
    ignore.update(alias_map.keys())
    ignore.update(tables)
    ignore.update(select_aliases)

    for clause in clause_texts:
        clause_clean = re.sub(
            r"\b[A-Za-z_][A-Za-z0-9_$#]*\s*\.\s*[A-Za-z_][A-Za-z0-9_$#]*\b",
            " ",
            clause,
        )
        tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_$#]*", clause_clean)
        for tok in tokens:
            tok_up = tok.upper()
            if tok_up in ignore:
                continue
            if tok_up.isdigit():
                continue
            matches = [t for t in tables_in_query if tok_up in schema.get(t, set())]
            if not matches:
                issues.append(f"unknown column (unqualified): {tok}")
            elif len(matches) > 1:
                issues.append(f"ambiguous column (unqualified): {tok} in {matches}")

    return issues


def _build_schema_map(schema_path: Path) -> dict[str, set[str]]:
    schema_json = json.loads(schema_path.read_text(encoding="utf-8"))
    tables = schema_json.get("tables", {})
    schema: dict[str, set[str]] = {}
    for table_name, entry in tables.items():
        cols = {c.get("name", "").upper() for c in entry.get("columns", []) if c.get("name")}
        schema[table_name.upper()] = cols
    return schema


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate RAG SQL semantics against schema_catalog.")
    parser.add_argument(
        "--metadata-dir",
        default="var/metadata",
        help="Path to metadata directory.",
    )
    args = parser.parse_args()

    base = Path(args.metadata_dir)
    schema_path = base / "schema_catalog.json"
    if not schema_path.exists():
        print("FAIL: schema_catalog.json not found")
        return 1

    schema = _build_schema_map(schema_path)

    files = [
        ("sql_examples.jsonl", "question"),
        ("join_templates.jsonl", "name"),
        ("sql_templates.jsonl", "name"),
    ]

    total = 0
    issue_count = 0
    for filename, label_key in files:
        path = base / filename
        for line_no, item in _load_jsonl(path):
            sql = item.get("sql", "")
            if not sql:
                continue
            total += 1
            issues = _validate_sql(sql, schema)
            if issues:
                issue_count += len(issues)
                label = item.get(label_key, "") or "unknown"
                for msg in issues:
                    print(f"{filename}:{line_no} [{label}] {msg}")

    if issue_count == 0:
        print("OK: no semantic issues detected")
        return 0

    print(f"FOUND: {issue_count} issues across {total} SQL items")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
