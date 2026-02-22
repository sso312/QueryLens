from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_PATH = BASE_DIR / "data" / "eval_sets" / "query_visualization_eval_aside.jsonl"


_LEVEL_RE = re.compile(r"^\s*##\s*\*{0,2}L([123])\b", re.IGNORECASE)
_SECTION_RE = re.compile(r"^\s*##\s*([A-Z])\.\s*(.+?)\s*$")
_QUESTION_RE = re.compile(r"^\s*##\s*([0-9]+(?:[-–][0-9]+)?)\)\s*(.+?)\s*$")
_FENCE_RE = re.compile(r"^\s*```(?:sql|dart)?\s*$", re.IGNORECASE)
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


def _infer_chart_types(title: str, sql: str) -> tuple[str, list[str], str]:
    question_text = " ".join(title.lower().split())
    sql_text = sql.lower()
    has_vs = bool(re.search(r"\bvs\b", question_text))
    has_compare = has_vs or ("비교" in question_text)

    if ("히스토그램" in question_text) or ("분포" in question_text and not has_compare):
        return "hist", ["hist", "box"], "분포 중심 질문"
    if ("분포" in question_text) and has_compare:
        return "box", ["box", "violin", "hist"], "그룹 간 분포 비교 질문"
    if any(token in question_text for token in ("연도별", "추이", "시간대별", "변화", "선그래프")):
        return "line", ["line", "bar"], "시간축 추세 질문"
    if (
        "group by" in sql_text
        and re.search(
            r"\b(admit_year|event_year|year|month|hour_since|time_bin|time_window|icu_admit_month)\b",
            sql_text,
        )
    ):
        return "line", ["line", "bar"], "시간 단위 그룹 집계"
    if any(token in question_text for token in ("비율", "사망률", "비교", "평균", "환자 수", "건수")):
        return "bar", ["bar", "line"], "범주/그룹 비교 질문"
    return "bar", ["bar"], "기본 막대 차트"


def _extract_sql(lines: list[str], start: int, end: int) -> str:
    i = start
    while i < end:
        if _FENCE_RE.match(lines[i]):
            i += 1
            chunk: list[str] = []
            while i < end and not _FENCE_CLOSE_RE.match(lines[i]):
                chunk.append(lines[i].rstrip())
                i += 1
            return _normalize_sql("\n".join(chunk).strip())
        i += 1
    return ""


def build_rows(source_path: Path) -> list[dict[str, Any]]:
    lines = source_path.read_text(encoding="utf-8").splitlines()

    level = ""
    section = ""
    section_title = ""
    headings: list[dict[str, Any]] = []

    for idx, line in enumerate(lines):
        level_match = _LEVEL_RE.match(line)
        if level_match:
            level = f"L{level_match.group(1)}"
            section = ""
            section_title = ""
            continue

        section_match = _SECTION_RE.match(line)
        if section_match:
            section = section_match.group(1).strip()
            section_title = section_match.group(2).strip()
            continue

        question_match = _QUESTION_RE.match(line)
        if question_match:
            headings.append(
                {
                    "line_no": idx + 1,
                    "question_no": question_match.group(1).replace("–", "-"),
                    "question_title": question_match.group(2).strip(),
                    "level": level,
                    "section": section,
                    "section_title": section_title,
                }
            )

    rows: list[dict[str, Any]] = []
    for i, item in enumerate(headings):
        start = item["line_no"]
        end = headings[i + 1]["line_no"] - 1 if i + 1 < len(headings) else len(lines)
        sql = _extract_sql(lines, start, end)
        if not sql:
            continue

        expected_chart_type, accepted_chart_types, rationale = _infer_chart_types(
            item["question_title"], sql
        )

        rows.append(
            {
                "id": f"aside_qv_{len(rows) + 1:03d}",
                "source": "aside.txt",
                "source_line": item["line_no"],
                "level": item["level"] or "unknown",
                "section": item["section"] or "unknown",
                "section_title": item["section_title"] or "",
                "question_no": item["question_no"],
                "user_query": item["question_title"],
                "sql": sql,
                "expected_chart_type": expected_chart_type,
                "accepted_chart_types": accepted_chart_types,
                "chart_rationale": rationale,
            }
        )

    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build query-visualization eval set from aside.txt."
    )
    parser.add_argument(
        "--source",
        default=r"c:\Users\KDT_03\Downloads\aside.txt",
        help="Path to aside.txt (UTF-8).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output JSONL path.",
    )
    args = parser.parse_args()

    source_path = Path(args.source)
    output_path = Path(args.output)

    if not source_path.exists():
        raise FileNotFoundError(f"source not found: {source_path}")

    rows = build_rows(source_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    level_counts: dict[str, int] = {}
    section_counts: dict[str, int] = {}
    for row in rows:
        level_counts[row["level"]] = level_counts.get(row["level"], 0) + 1
        section_key = f'{row["level"]}.{row["section"]}'
        section_counts[section_key] = section_counts.get(section_key, 0) + 1

    print(
        json.dumps(
            {
                "source": str(source_path),
                "output": str(output_path),
                "count": len(rows),
                "level_counts": level_counts,
                "section_counts": section_counts,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
