from __future__ import annotations

from pathlib import Path
import json


def _load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def main() -> int:
    base = Path("var/metadata")
    schema = _load_json(base / "schema_catalog.json") or {"tables": {}}
    schema_docs = len(schema.get("tables", {}))
    sql_examples_docs = _count_jsonl(base / "sql_examples.jsonl")
    join_templates_docs = _count_jsonl(base / "join_templates.jsonl")
    sql_templates_docs = _count_jsonl(base / "sql_templates.jsonl")
    templates_docs = join_templates_docs + sql_templates_docs
    glossary_docs = _count_jsonl(base / "glossary_docs.jsonl")

    ok = True
    if sql_examples_docs < 50:
        print(f"FAIL: sql_examples_docs {sql_examples_docs} < 50")
        ok = False
    if templates_docs < 5:
        print(f"FAIL: templates_docs {templates_docs} < 5")
        ok = False
    if schema_docs == 0:
        print("FAIL: schema_docs is 0")
        ok = False

    print("schema_docs=", schema_docs)
    print("sql_examples_docs=", sql_examples_docs)
    print("join_templates_docs=", join_templates_docs)
    print("sql_templates_docs=", sql_templates_docs)
    print("templates_docs=", templates_docs)
    print("glossary_docs=", glossary_docs)

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
