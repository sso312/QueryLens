from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


_WS_RE = re.compile(r"\s+")
_SQL_WS_RE = re.compile(r"\s+")


def _norm_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    return _WS_RE.sub(" ", text)


def _norm_sql(value: Any) -> str:
    text = str(value or "").strip()
    return _SQL_WS_RE.sub(" ", text).strip().lower()


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    payload = "\n".join(json.dumps(row, ensure_ascii=True) for row in rows)
    if payload:
        payload += "\n"
    path.write_text(payload, encoding="utf-8")


def _unique_strs(values: list[Any], *, upper: bool = False) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        if upper:
            text = text.upper()
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _is_readonly_sql(sql: str) -> bool:
    head = str(sql or "").strip().upper()
    return head.startswith("SELECT") or head.startswith("WITH")


def clean_sql_examples(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        question = str(row.get("question") or "").strip()
        sql = str(row.get("sql") or "").strip()
        if not question or not sql:
            continue
        if not _is_readonly_sql(sql):
            continue
        key = (_norm_text(question), _norm_sql(sql))
        if key in seen:
            continue
        seen.add(key)
        cleaned.append({"question": question, "sql": sql})
    return cleaned


def clean_templates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        name = str(row.get("name") or "").strip()
        sql = str(row.get("sql") or "").strip()
        if not name or not sql:
            continue
        if not _is_readonly_sql(sql):
            continue
        key = (_norm_text(name), _norm_sql(sql))
        if key in seen:
            continue
        seen.add(key)
        cleaned.append({"name": name, "sql": sql})
    return cleaned


def clean_glossary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    seen_exact: set[tuple[str, str]] = set()
    for row in rows:
        term = str(row.get("term") or row.get("key") or row.get("name") or "").strip()
        definition = str(row.get("definition") or row.get("desc") or row.get("value") or "").strip()
        if not term or not definition:
            continue
        key = (_norm_text(term), _norm_text(definition))
        if key in seen_exact:
            continue
        seen_exact.add(key)
        out = dict(row)
        if "term" not in out or not str(out.get("term") or "").strip():
            out["term"] = term
        if "definition" not in out or not str(out.get("definition") or "").strip():
            out["definition"] = definition
        cleaned.append(out)
    return cleaned


def _merge_term_prefix_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for row in rows:
        term = str(row.get("term") or "").strip()
        if not term:
            continue
        key = _norm_text(term)
        prefixes = _unique_strs(list(row.get("icd_prefixes") or row.get("prefixes") or []), upper=True)
        if not prefixes:
            continue
        aliases = _unique_strs(list(row.get("aliases") or []), upper=False)
        if key not in merged:
            merged[key] = {"term": term, "aliases": aliases, "icd_prefixes": prefixes}
            order.append(key)
            continue
        current = merged[key]
        current["aliases"] = _unique_strs(list(current.get("aliases") or []) + aliases, upper=False)
        current["icd_prefixes"] = _unique_strs(list(current.get("icd_prefixes") or []) + prefixes, upper=True)
    return [merged[key] for key in order]


def clean_label_intents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    list_fields = (
        "question_any",
        "question_intent_any",
        "anchor_terms",
        "co_terms",
        "required_terms_with_anchor",
        "exclude_terms_with_anchor",
        "forbid_terms_any",
    )
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or row.get("id") or "").strip()
        if not name:
            continue
        key = _norm_text(name)
        if key not in merged:
            base = dict(row)
            merged[key] = base
            order.append(key)
            continue
        current = merged[key]
        for field in list_fields:
            current[field] = _unique_strs(list(current.get(field) or []) + list(row.get(field) or []), upper=False)
        for scalar in ("table", "event_table"):
            if not str(current.get(scalar) or "").strip() and str(row.get(scalar) or "").strip():
                current[scalar] = row.get(scalar)
    out: list[dict[str, Any]] = []
    for key in order:
        row = merged[key]
        if not list(row.get("anchor_terms") or []):
            continue
        out.append(row)
    return out


def clean_column_values(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    order: list[tuple[str, str, str]] = []
    for row in rows:
        table = str(row.get("table") or "").strip().upper()
        column = str(row.get("column") or "").strip().upper()
        value = str(row.get("value") or "").strip()
        if not table or not column or not value:
            continue
        key = (table, column, value.upper())
        desc = str(row.get("description") or "").strip()
        sheet = str(row.get("sheet") or "").strip()
        if key not in merged:
            merged[key] = dict(row)
            order.append(key)
            continue
        cur = merged[key]
        if len(desc) > len(str(cur.get("description") or "")):
            cur["description"] = desc
        if not str(cur.get("sheet") or "").strip() and sheet:
            cur["sheet"] = sheet
    return [merged[key] for key in order]


def clean_table_value_profiles(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    order: list[tuple[str, str]] = []
    for row in rows:
        table = str(row.get("table") or "").strip().upper()
        column = str(row.get("column") or "").strip().upper()
        if not table or not column:
            continue
        key = (table, column)
        if key not in merged:
            merged[key] = dict(row)
            order.append(key)
            continue
        current = merged[key]
        prev_distinct = int(current.get("num_distinct") or 0)
        new_distinct = int(row.get("num_distinct") or 0)
        if new_distinct > prev_distinct:
            merged[key] = dict(row)
    return [merged[key] for key in order]


def _clean_one(path: Path) -> tuple[int, int]:
    rows = _load_jsonl(path)
    before = len(rows)
    name = path.name
    if name in {"sql_examples.jsonl", "sql_examples_augmented.jsonl"}:
        cleaned = clean_sql_examples(rows)
    elif name in {"join_templates.jsonl", "sql_templates.jsonl"}:
        cleaned = clean_templates(rows)
    elif name == "glossary_docs.jsonl":
        cleaned = clean_glossary(rows)
    elif name in {"diagnosis_icd_map.jsonl", "procedure_icd_map.jsonl"}:
        cleaned = _merge_term_prefix_rows(rows)
    elif name == "label_intent_profiles.jsonl":
        cleaned = clean_label_intents(rows)
    elif name == "column_value_docs.jsonl":
        cleaned = clean_column_values(rows)
    elif name == "table_value_profiles.jsonl":
        cleaned = clean_table_value_profiles(rows)
    else:
        cleaned = rows
    if cleaned != rows:
        _write_jsonl(path, cleaned)
    return before, len(cleaned)


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean RAG metadata documents (dedupe + prune low-value rows).")
    parser.add_argument("--metadata-dir", default="var/metadata", help="Metadata directory path.")
    args = parser.parse_args()

    base = Path(args.metadata_dir)
    targets = [
        "glossary_docs.jsonl",
        "sql_examples.jsonl",
        "sql_examples_augmented.jsonl",
        "join_templates.jsonl",
        "sql_templates.jsonl",
        "diagnosis_icd_map.jsonl",
        "procedure_icd_map.jsonl",
        "label_intent_profiles.jsonl",
        "column_value_docs.jsonl",
        "table_value_profiles.jsonl",
    ]

    report: list[dict[str, Any]] = []
    for filename in targets:
        path = base / filename
        if not path.exists():
            continue
        before, after = _clean_one(path)
        report.append({"file": filename, "before": before, "after": after, "removed": before - after})

    print(json.dumps({"metadata_dir": str(base), "files": report}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
