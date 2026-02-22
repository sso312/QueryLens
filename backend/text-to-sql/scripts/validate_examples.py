from __future__ import annotations

from pathlib import Path
import sys
import json
import re

from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "backend"))

from app.services.oracle.executor import execute_sql


def _load_examples(path: Path) -> list[dict]:
    if not path.exists():
        return []
    items = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return items


def _extract_ora_code(message: str) -> str | None:
    match = re.search(r"ORA-\d+", message)
    return match.group(0) if match else None


def main() -> int:
    examples = _load_examples(Path("var/metadata/sql_examples.jsonl"))
    if not examples:
        print("No examples found")
        return 1

    ok = True
    for idx, item in enumerate(examples, 1):
        sql = item.get("sql")
        if not sql:
            continue
        try:
            execute_sql(sql)
        except HTTPException as exc:
            ora = _extract_ora_code(str(exc.detail))
            print(f"FAIL [{idx}]: {ora or exc.detail}")
            ok = False
        except Exception as exc:
            print(f"FAIL [{idx}]: {exc}")
            ok = False

    if ok:
        print("OK: all examples executed")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
