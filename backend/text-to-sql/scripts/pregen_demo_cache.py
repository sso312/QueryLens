from __future__ import annotations

from pathlib import Path
import sys
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "backend"))

from fastapi import HTTPException

from app.services.agents.orchestrator import run_oneshot
from app.services.oracle.executor import execute_sql


def _load_questions(path: Path) -> list[str]:
    if not path.exists():
        return []
    questions: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
            if isinstance(item, dict) and "question" in item:
                questions.append(str(item["question"]))
            elif isinstance(item, str):
                questions.append(item)
        except json.JSONDecodeError:
            questions.append(line)
    return questions


def _load_examples(path: Path) -> list[dict]:
    if not path.exists():
        return []
    items: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("sql"):
            items.append(obj)
    return items


def _safe_execute(sql: str) -> tuple[dict | None, str | None]:
    try:
        return execute_sql(sql), None
    except HTTPException as exc:
        return None, str(exc.detail)
    except Exception as exc:  # pragma: no cover - unexpected
        return None, str(exc)


def main() -> int:
    questions = _load_questions(Path("var/metadata/demo_questions.jsonl"))
    if not questions:
        print("No demo questions found")
        return 1

    examples = _load_examples(Path("var/metadata/sql_examples.jsonl"))

    cache: dict[str, dict] = {}
    for idx, question in enumerate(questions):
        payload = run_oneshot(question, skip_policy=True)
        final_sql = None
        if payload.get("mode") == "demo":
            cache[question] = payload.get("result", {})
            continue
        final = payload.get("final", {})
        final_sql = final.get("final_sql")
        preview = None
        error = None
        source = "llm"
        if final_sql:
            preview, error = _safe_execute(final_sql)

        if preview is None and examples:
            fallback = examples[idx % len(examples)]
            fallback_sql = fallback.get("sql")
            if fallback_sql:
                preview, error = _safe_execute(fallback_sql)
                if preview is not None:
                    final_sql = fallback_sql
                    source = "example_fallback"

        if preview is None:
            cache[question] = {
                "sql": final_sql,
                "error": error or "No preview available",
                "summary": "Preview failed",
                "source": source,
            }
            continue

        cache[question] = {
            "sql": final_sql,
            "preview": preview,
            "summary": f"Rows: {preview.get('row_count', 0)}",
            "source": source,
        }

    out_path = Path("var/cache/demo_cache.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(cache, ensure_ascii=True, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"Wrote {len(cache)} items to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
