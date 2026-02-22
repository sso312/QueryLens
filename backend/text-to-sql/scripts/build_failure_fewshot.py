from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            item = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _normalize_question(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _collect_seed_questions(*rows_list: list[dict[str, Any]]) -> set[str]:
    seen: set[str] = set()
    for rows in rows_list:
        for item in rows:
            question = _normalize_question(str(item.get("question") or ""))
            if question:
                seen.add(question)
    return seen


def build_augmented_examples(
    report_rows: list[dict[str, Any]],
    *,
    seed_rows: list[dict[str, Any]],
    existing_augmented_rows: list[dict[str, Any]],
    max_new: int,
) -> tuple[list[dict[str, Any]], int]:
    existing = list(existing_augmented_rows)
    seen_questions = _collect_seed_questions(seed_rows, existing_augmented_rows)
    added = 0
    for item in report_rows:
        status = str(item.get("status") or "").strip().lower()
        if status not in {"mismatch", "exec_error"}:
            continue
        expected_error = str(item.get("expected_error") or "").strip()
        if expected_error:
            continue
        question = str(item.get("question") or "").strip()
        sql = str(item.get("expected_sql") or "").strip()
        if not question or not sql:
            continue
        normalized = _normalize_question(question)
        if not normalized or normalized in seen_questions:
            continue
        payload = {
            "question": question,
            "sql": sql,
            "source": "eval_failure_replay",
            "status": status,
        }
        existing.append(payload)
        seen_questions.add(normalized)
        added += 1
        if max_new > 0 and added >= max_new:
            break
    return existing, added


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build augmented SQL few-shot examples from failed eval report rows."
    )
    parser.add_argument(
        "--report",
        required=True,
        help="Input eval report jsonl path (must contain question/expected_sql/status).",
    )
    parser.add_argument(
        "--base",
        default="var/metadata/sql_examples.jsonl",
        help="Base examples jsonl path for duplicate filtering.",
    )
    parser.add_argument(
        "--output",
        default="var/metadata/sql_examples_augmented.jsonl",
        help="Augmented examples output jsonl path.",
    )
    parser.add_argument(
        "--max-new",
        type=int,
        default=50,
        help="Maximum number of new rows to append (0 = unlimited).",
    )
    args = parser.parse_args()

    report_path = Path(args.report)
    if not report_path.exists():
        print(f"ERROR: report not found: {report_path}")
        return 1

    report_rows = _load_jsonl(report_path)
    base_rows = _load_jsonl(Path(args.base))
    output_path = Path(args.output)
    existing_augmented_rows = _load_jsonl(output_path)

    merged_rows, added = build_augmented_examples(
        report_rows,
        seed_rows=base_rows,
        existing_augmented_rows=existing_augmented_rows,
        max_new=max(0, int(args.max_new)),
    )
    _write_jsonl(output_path, merged_rows)
    print(
        json.dumps(
            {
                "report_rows": len(report_rows),
                "base_rows": len(base_rows),
                "existing_augmented_rows": len(existing_augmented_rows),
                "added_rows": added,
                "final_augmented_rows": len(merged_rows),
                "output": str(output_path),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

