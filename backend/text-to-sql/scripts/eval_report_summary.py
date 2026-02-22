from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def load_report(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    items: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return items


def summarize(items: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(items)
    counts = {
        "match": 0,
        "mismatch": 0,
        "exec_error": 0,
        "no_generated_sql": 0,
        "demo_used": 0,
    }

    for item in items:
        status = item.get("status")
        if status in counts:
            counts[status] += 1

    executed_both = counts["match"] + counts["mismatch"]
    accuracy = (counts["match"] / executed_both) if executed_both else 0.0
    exec_success_rate = (executed_both / total) if total else 0.0

    same_rows = 0
    same_cols = 0
    alias_only_mismatch = 0
    for item in items:
        status = item.get("status")
        if status not in {"match", "mismatch"}:
            continue
        compare = item.get("compare") or {}
        rows_equal = bool(compare.get("same_rows"))
        cols_equal = bool(compare.get("same_cols"))
        if rows_equal:
            same_rows += 1
        if cols_equal:
            same_cols += 1
        if status == "mismatch" and rows_equal and not cols_equal:
            alias_only_mismatch += 1

    row_accuracy = (same_rows / executed_both) if executed_both else 0.0
    col_accuracy = (same_cols / executed_both) if executed_both else 0.0

    return {
        "total": total,
        **counts,
        "executed_both": executed_both,
        "accuracy": round(accuracy, 4),
        "exec_success_rate": round(exec_success_rate, 4),
        "row_accuracy": round(row_accuracy, 4),
        "col_accuracy": round(col_accuracy, 4),
        "alias_only_mismatch": alias_only_mismatch,
    }


def write_csv(path: Path, items: list[dict[str, Any]]) -> None:
    fieldnames = [
        "idx",
        "question",
        "status",
        "expected_sql",
        "generated_sql",
        "expected_error",
        "generated_error",
        "same_cols",
        "same_rows",
        "expected_row_count",
        "generated_row_count",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            compare = item.get("compare") or {}
            row = {
                "idx": item.get("idx"),
                "question": item.get("question"),
                "status": item.get("status"),
                "expected_sql": item.get("expected_sql"),
                "generated_sql": item.get("generated_sql"),
                "expected_error": item.get("expected_error"),
                "generated_error": item.get("generated_error"),
                "same_cols": compare.get("same_cols"),
                "same_rows": compare.get("same_rows"),
                "expected_row_count": compare.get("expected_row_count"),
                "generated_row_count": compare.get("generated_row_count"),
            }
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize eval report and optionally export CSV.")
    parser.add_argument(
        "--input",
        default="var/logs/eval_report.jsonl",
        help="Path to eval report jsonl.",
    )
    parser.add_argument("--csv", default="", help="Optional CSV output path.")
    args = parser.parse_args()

    items = load_report(Path(args.input))
    if not items:
        print("No report items found.")
        return 1

    summary = summarize(items)
    print(json.dumps(summary, ensure_ascii=True, indent=2))

    if args.csv:
        write_csv(Path(args.csv), items)
        print(f"CSV written to {args.csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
