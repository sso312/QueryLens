from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


_ID_RE = re.compile(r"^aside_qv_(\d+)$")
_FENCE_OPEN_RE = re.compile(r"^\s*```(?:sql|dart)?\s*$", re.IGNORECASE)
_FENCE_CLOSE_RE = re.compile(r"^\s*```\s*$")
_LINE_COMMENT_RE = re.compile(r"--[^\r\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_sql(sql: str) -> str:
    text = str(sql or "")
    text = _BLOCK_COMMENT_RE.sub(" ", text)
    text = _LINE_COMMENT_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        rows.append(json.loads(text))
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _next_id(rows: list[dict[str, Any]]) -> int:
    max_id = 0
    for row in rows:
        raw = str(row.get("id", ""))
        m = _ID_RE.match(raw)
        if m:
            max_id = max(max_id, int(m.group(1)))
    return max_id + 1


def _extract_sql_after_heading(lines: list[str], heading_marker: str) -> tuple[int, str]:
    heading_line = 0
    for idx, line in enumerate(lines):
        if heading_marker in line:
            heading_line = idx + 1
            i = idx + 1
            while i < len(lines):
                if _FENCE_OPEN_RE.match(lines[i]):
                    i += 1
                    chunk: list[str] = []
                    while i < len(lines) and not _FENCE_CLOSE_RE.match(lines[i]):
                        chunk.append(lines[i].rstrip())
                        i += 1
                    sql = _normalize_sql("\n".join(chunk).strip())
                    if not sql:
                        raise ValueError(f"empty sql block after heading: {heading_marker}")
                    return heading_line, sql
                i += 1
            break
    raise ValueError(f"heading not found or no sql block: {heading_marker}")


def _build_missing_cases(l2_source: Path) -> list[dict[str, Any]]:
    lines = l2_source.read_text(encoding="utf-8").splitlines()

    specs = [
        {
            "heading_marker": "## 심부전 30일 재입원율",
            "level": "L2",
            "section": "A",
            "section_title": "질환 코호트·예후·합병증 (20문항)",
            "question_no": "4",
            "user_query": "심부전 진단 환자의 30일 재입원율을 연도별로 보여줘.",
            "expected_chart_type": "line",
            "accepted_chart_types": ["line", "bar"],
            "chart_rationale": "연도별 재입원율 추세 질문",
        },
        {
            "heading_marker": "## 심근경색 30일 재입원율",
            "level": "L2",
            "section": "A",
            "section_title": "질환 코호트·예후·합병증 (20문항)",
            "question_no": "5",
            "user_query": "심근경색(STEMI/NSTEMI) 진단 환자의 30일 재입원율을 연도별로 보여줘.",
            "expected_chart_type": "line",
            "accepted_chart_types": ["line", "bar"],
            "chart_rationale": "연도별 재입원율 추세 질문",
        },
        {
            "heading_marker": "## COPD 30일 재입원율",
            "level": "L2",
            "section": "A",
            "section_title": "질환 코호트·예후·합병증 (20문항)",
            "question_no": "6",
            "user_query": "COPD 진단 환자의 30일 재입원율을 연도별로 보여줘.",
            "expected_chart_type": "line",
            "accepted_chart_types": ["line", "bar"],
            "chart_rationale": "연도별 재입원율 추세 질문",
        },
        {
            "heading_marker": "5-5) 뇌졸중 환자에서 기계환기 비율 (연도별)",
            "level": "L2",
            "section": "A",
            "section_title": "질환 코호트·예후·합병증 (20문항)",
            "question_no": "16",
            "user_query": "뇌졸중 진단 환자에서 기계환기 치료를 받은 환자 비율을 연도별로 보여줘.",
            "expected_chart_type": "line",
            "accepted_chart_types": ["line", "bar"],
            "chart_rationale": "연도별 비율 추세 질문",
        },
        {
            "heading_marker": "## 4. 첫 ICU 입실에서 vasopressor 사용 vs 이후 입원에서만 사용한 환자의 ICU 사망률",
            "level": "L2",
            "section": "C",
            "section_title": "약물 사용 및 예후·용량 구간 (10문항)",
            "question_no": "10",
            "user_query": "ICU 입실 후 첫 입원에서 vasopressor를 사용한 환자와 이후 입원에서만 vasopressor를 사용한 환자의 ICU 사망률을 비교해줘.",
            "expected_chart_type": "bar",
            "accepted_chart_types": ["bar", "line"],
            "chart_rationale": "그룹 간 사망률 비교 질문",
        },
    ]

    missing: list[dict[str, Any]] = []
    for spec in specs:
        source_line, sql = _extract_sql_after_heading(lines, spec["heading_marker"])
        row = {
            "source": l2_source.name,
            "source_line": source_line,
            "level": spec["level"],
            "section": spec["section"],
            "section_title": spec["section_title"],
            "question_no": spec["question_no"],
            "user_query": spec["user_query"],
            "sql": sql,
            "expected_chart_type": spec["expected_chart_type"],
            "accepted_chart_types": spec["accepted_chart_types"],
            "chart_rationale": spec["chart_rationale"],
        }
        missing.append(row)
    return missing


def main() -> int:
    parser = argparse.ArgumentParser(description="Append missing 5 L2 cases to query-visualization eval set.")
    parser.add_argument(
        "--eval-set",
        default="backend/query-visualization/data/eval_sets/query_visualization_eval_aside.jsonl",
        help="Target eval set JSONL path.",
    )
    parser.add_argument(
        "--l2-source",
        default=r"c:\Users\KDT_03\Downloads\L2 (중간) 다중 조인, 코호트 정의, 그룹 비교 (40문항).txt",
        help="L2 source txt path.",
    )
    args = parser.parse_args()

    eval_path = Path(args.eval_set)
    l2_path = Path(args.l2_source)

    if not l2_path.exists():
        raise FileNotFoundError(f"L2 source not found: {l2_path}")

    rows = _load_jsonl(eval_path)
    existing_queries = {str(r.get("user_query", "")).strip() for r in rows}

    missing_rows = _build_missing_cases(l2_path)
    to_add: list[dict[str, Any]] = []
    next_id = _next_id(rows)
    for row in missing_rows:
        if row["user_query"] in existing_queries:
            continue
        row["id"] = f"aside_qv_{next_id:03d}"
        next_id += 1
        to_add.append(row)

    rows.extend(to_add)
    _write_jsonl(eval_path, rows)

    print(
        json.dumps(
            {
                "eval_set": str(eval_path),
                "l2_source": str(l2_path),
                "added": len(to_add),
                "total": len(rows),
                "added_ids": [r["id"] for r in to_add],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
