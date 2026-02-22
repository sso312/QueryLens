from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _compact(value: Any, *, max_chars: int = 240) -> str:
    text = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
    text = " ".join(text.split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _append_doc(
    docs: list[dict[str, Any]],
    *,
    text: str,
    source: str,
    section: str,
    term: str = "",
    metadata: dict[str, Any] | None = None,
) -> None:
    clean = " ".join((text or "").split()).strip()
    if not clean:
        return
    payload = {"source": source, "section": section}
    if term:
        payload["term"] = term
    if isinstance(metadata, dict):
        for key, value in metadata.items():
            if key and value is not None and key not in payload:
                payload[key] = value
    docs.append(
        {
            "term": term or section,
            "desc": clean,
            "text": clean,
            "metadata": payload,
        }
    )


def _emit_derived_docs(docs: list[dict[str, Any]], derived_path: Path) -> None:
    root = _load_json(derived_path)
    source_name = derived_path.name
    version = str(root.get("version") or "").strip()
    dialect = str(root.get("dialect_target") or "").strip()
    variables = root.get("derived_variables") if isinstance(root.get("derived_variables"), list) else []

    _append_doc(
        docs,
        text=(
            f"Derived-variable metadata version {version or '-'} for dialect {dialect or '-'}; "
            f"contains {len(variables)} derived concepts for clinical analytics."
        ),
        source=source_name,
        section="overview",
        term="derived variables overview",
    )

    for item in variables:
        if not isinstance(item, dict):
            continue
        name = str(item.get("derived_name") or "").strip()
        if not name:
            continue
        aliases = [str(v).strip() for v in (item.get("aliases") or []) if str(v).strip()]
        item_type = str(item.get("type") or "").strip()
        definition = str(item.get("definition") or "").strip()
        sql_url = str(item.get("sql_url") or "").strip()

        input_parts: list[str] = []
        for entry in item.get("inputs") or []:
            if not isinstance(entry, dict):
                continue
            input_name = str(entry.get("name") or "").strip()
            signal = str(entry.get("signal") or "").strip()
            domain = str(entry.get("domain") or "").strip()
            if input_name or signal:
                input_parts.append(f"{input_name}:{signal}:{domain}".strip(":"))

        window_parts: list[str] = []
        for entry in item.get("time_windows") or []:
            if not isinstance(entry, dict):
                continue
            window_name = str(entry.get("window_name") or "").strip()
            anchor = str(entry.get("anchor") or "").strip()
            start_h = entry.get("start_offset_hours")
            end_h = entry.get("end_offset_hours")
            if start_h is not None or end_h is not None:
                window_parts.append(f"{window_name}@{anchor}[{start_h},{end_h}]")
            else:
                window_parts.append(f"{window_name}@{anchor}")

        oracle_template = item.get("oracle_template")
        template_text = ""
        if isinstance(oracle_template, dict):
            template_text = _compact(
                {
                    "template_name": oracle_template.get("template_name"),
                    "strategy": oracle_template.get("strategy"),
                    "requires_signal_map": oracle_template.get("requires_signal_map"),
                },
                max_chars=420,
            )

        normalization = [str(v).strip() for v in (item.get("normalization_rules") or []) if str(v).strip()]
        sanity_checks = item.get("sanity_checks") or []

        summary = (
            f"Derived variable {name}. "
            f"Type: {item_type or '-'}. "
            f"Definition: {definition or '-'}. "
            f"Aliases: {', '.join(aliases) if aliases else '-'}. "
            f"Inputs: {', '.join(input_parts) if input_parts else '-'}. "
            f"Time windows: {', '.join(window_parts) if window_parts else '-'}. "
            f"Oracle template: {template_text or '-'}. "
            f"Normalization rules: {', '.join(normalization) if normalization else '-'}. "
            f"Sanity checks: {_compact(sanity_checks, max_chars=300)}. "
            f"Reference SQL URL: {sql_url or '-'}."
        )
        _append_doc(
            docs,
            text=summary,
            source=source_name,
            section="derived_variables",
            term=name,
            metadata={"dialect_target": dialect or None},
        )


def _emit_sql_metadata_docs(docs: list[dict[str, Any]], metadata_path: Path) -> None:
    root = _load_json(metadata_path)
    source_name = metadata_path.name
    version = str(root.get("version") or "").strip()
    notes = root.get("source")
    _append_doc(
        docs,
        text=(
            f"MIMIC SQL metadata version {version or '-'} with source notes: "
            f"{_compact(notes, max_chars=460)}"
        ),
        source=source_name,
        section="overview",
        term="sql metadata overview",
    )

    schema_hints = root.get("schema_hints") if isinstance(root.get("schema_hints"), dict) else {}
    tables_present = schema_hints.get("tables_present") if isinstance(schema_hints.get("tables_present"), list) else []
    if tables_present:
        _append_doc(
            docs,
            text=f"Tables present in metadata scope: {', '.join(str(v) for v in tables_present if str(v).strip())}.",
            source=source_name,
            section="schema_hints.tables_present",
            term="tables present",
        )

    time_columns = schema_hints.get("time_columns") if isinstance(schema_hints.get("time_columns"), dict) else {}
    for table_name, columns in time_columns.items():
        cols = [str(v).strip() for v in (columns or []) if str(v).strip()]
        if not cols:
            continue
        _append_doc(
            docs,
            text=f"Time columns for {table_name}: {', '.join(cols)}.",
            source=source_name,
            section="schema_hints.time_columns",
            term=f"{table_name} time columns",
        )

    value_columns = schema_hints.get("value_columns") if isinstance(schema_hints.get("value_columns"), dict) else {}
    for table_name, detail in value_columns.items():
        if not isinstance(detail, dict):
            continue
        _append_doc(
            docs,
            text=f"Value-column mapping for {table_name}: {_compact(detail, max_chars=420)}.",
            source=source_name,
            section="schema_hints.value_columns",
            term=f"{table_name} value columns",
        )

    join_graph = root.get("join_graph") if isinstance(root.get("join_graph"), list) else []
    for edge in join_graph:
        if not isinstance(edge, dict):
            continue
        src = str(edge.get("from") or "").strip()
        dst = str(edge.get("to") or "").strip()
        keys = edge.get("keys")
        level = str(edge.get("level") or "").strip()
        cardinality = str(edge.get("cardinality") or "").strip()
        notes_text = str(edge.get("notes") or "").strip()
        preferred = edge.get("preferred")
        _append_doc(
            docs,
            text=(
                f"Preferred join path {src} -> {dst}. "
                f"Keys: {_compact(keys, max_chars=260)}. "
                f"Level: {level or '-'}. Cardinality: {cardinality or '-'}. "
                f"Preferred: {preferred}. Notes: {notes_text or '-'}."
            ),
            source=source_name,
            section="join_graph",
            term=f"{src} to {dst}",
        )

    window_templates = root.get("window_templates") if isinstance(root.get("window_templates"), list) else []
    for item in window_templates:
        if not isinstance(item, dict):
            continue
        template_id = str(item.get("template_id") or item.get("name") or "").strip()
        if not template_id:
            continue
        _append_doc(
            docs,
            text=f"Window template {template_id}: {_compact(item, max_chars=600)}.",
            source=source_name,
            section="window_templates",
            term=template_id,
        )

    guardrails = root.get("guardrails")
    if guardrails is not None:
        if isinstance(guardrails, list):
            for idx, item in enumerate(guardrails):
                _append_doc(
                    docs,
                    text=f"Guardrail {idx + 1}: {_compact(item, max_chars=560)}.",
                    source=source_name,
                    section="guardrails",
                    term=f"guardrail {idx + 1}",
                )
        else:
            _append_doc(
                docs,
                text=f"Guardrails: {_compact(guardrails, max_chars=760)}.",
                source=source_name,
                section="guardrails",
                term="guardrails",
            )

    signal_dictionary = root.get("signal_dictionary")
    if isinstance(signal_dictionary, list):
        for entry in signal_dictionary:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("signal") or entry.get("name") or "").strip()
            _append_doc(
                docs,
                text=f"Signal dictionary entry {name or '-'}: {_compact(entry, max_chars=560)}.",
                source=source_name,
                section="signal_dictionary",
                term=name or "signal dictionary",
            )
    elif isinstance(signal_dictionary, dict):
        for name, entry in signal_dictionary.items():
            _append_doc(
                docs,
                text=f"Signal dictionary entry {name}: {_compact(entry, max_chars=560)}.",
                source=source_name,
                section="signal_dictionary",
                term=str(name),
            )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def build_docs(derived_path: Path, metadata_path: Path) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    _emit_derived_docs(docs, derived_path)
    _emit_sql_metadata_docs(docs, metadata_path)
    return docs


def main() -> int:
    parser = argparse.ArgumentParser(description="Build external RAG docs from JSON metadata files.")
    parser.add_argument(
        "--derived",
        default=str(Path("C:/Users/KDT_03/Downloads/derived_variables.json")),
        help="Path to derived_variables.json",
    )
    parser.add_argument(
        "--sql-meta",
        default=str(Path("C:/Users/KDT_03/Downloads/mimiciv_sql_metadata.json")),
        help="Path to mimiciv_sql_metadata.json",
    )
    parser.add_argument(
        "--output",
        action="append",
        required=True,
        help="Output JSONL path. Repeat this argument to write multiple copies.",
    )
    args = parser.parse_args()

    derived_path = Path(args.derived)
    sql_meta_path = Path(args.sql_meta)
    if not derived_path.exists():
        raise FileNotFoundError(f"Missing file: {derived_path}")
    if not sql_meta_path.exists():
        raise FileNotFoundError(f"Missing file: {sql_meta_path}")

    docs = build_docs(derived_path, sql_meta_path)
    for output in args.output:
        _write_jsonl(Path(output), docs)
    print(f"Wrote {len(docs)} docs to {len(args.output)} output path(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
