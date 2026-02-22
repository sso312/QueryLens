from __future__ import annotations

from pathlib import Path
import json
import sys

REQUIRED_GLOSSARY_TERMS = {"LOS"}


def _count_jsonl(path: Path) -> tuple[int, list[dict]]:
    items = []
    if not path.exists():
        return 0, items
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return len(items), items


def main() -> int:
    base = Path("var/metadata")
    examples_count, _ = _count_jsonl(base / "sql_examples.jsonl")
    templates_count, _ = _count_jsonl(base / "join_templates.jsonl")
    glossary_count, glossary_items = _count_jsonl(base / "glossary_docs.jsonl")

    ok = True
    if examples_count < 50:
        print(f"FAIL: sql_examples.jsonl has {examples_count}, expected >= 50")
        ok = False
    if templates_count < 5:
        print(f"FAIL: join_templates.jsonl has {templates_count}, expected >= 5")
        ok = False

    glossary_terms = {str(item.get("term", "")).upper() for item in glossary_items}
    missing = REQUIRED_GLOSSARY_TERMS - glossary_terms
    if missing:
        print(f"FAIL: glossary missing terms: {sorted(missing)}")
        ok = False

    if ok:
        print("OK: assets validation passed")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
