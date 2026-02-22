from __future__ import annotations

import argparse
import json
import random
import re
from pathlib import Path
from typing import Any


_HANGUL_RE = re.compile(r"[\uac00-\ud7a3]")
_AGG_RE = re.compile(r"\b(COUNT|AVG|SUM|STDDEV|MIN|MAX)\s*\(", re.IGNORECASE)
_JOIN_RE = re.compile(r"\bJOIN\b", re.IGNORECASE)
_CASE_RE = re.compile(r"\bCASE\b", re.IGNORECASE)


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _contains_hangul(text: str) -> bool:
    return bool(_HANGUL_RE.search(text))


def _analyze_sql(sql: str) -> tuple[str, int, int]:
    upper = sql.upper()
    join_count = len(_JOIN_RE.findall(upper))
    has_subquery = "( SELECT" in upper or " FROM (" in upper
    has_case = bool(_CASE_RE.search(upper))
    agg_count = len(_AGG_RE.findall(upper))
    score = (join_count * 2) + (2 if has_subquery else 0) + (1 if has_case else 0) + (1 if agg_count > 0 else 0)
    if score <= 2:
        level = "simple"
    elif score <= 5:
        level = "medium"
    else:
        level = "hard"
    return level, join_count, score


def _bucket(question: str, sql: str) -> tuple[str, str]:
    lang = "ko" if _contains_hangul(question) else "en"
    complexity, _, _ = _analyze_sql(sql)
    return lang, complexity


def _load_excluded_questions(base_dir: Path, exclude_glob: str) -> set[str]:
    excluded: set[str] = set()
    for path in sorted(base_dir.glob(exclude_glob)):
        for row in _load_jsonl(path):
            q = str(row.get("question") or "").strip()
            if q:
                excluded.add(q)
    return excluded


def _initial_targets(size: int) -> dict[tuple[str, str], int]:
    # Balanced for bilingual+complexity coverage; falls back automatically by capacity.
    weights: dict[tuple[str, str], float] = {
        ("ko", "simple"): 0.20,
        ("ko", "medium"): 0.35,
        ("ko", "hard"): 0.10,
        ("en", "simple"): 0.05,
        ("en", "medium"): 0.25,
        ("en", "hard"): 0.05,
    }
    targets = {bucket: int(round(size * weight)) for bucket, weight in weights.items()}
    # round sum drift correction
    drift = size - sum(targets.values())
    priority = [
        ("ko", "hard"),
        ("en", "hard"),
        ("ko", "medium"),
        ("en", "medium"),
        ("ko", "simple"),
        ("en", "simple"),
    ]
    idx = 0
    while drift != 0:
        key = priority[idx % len(priority)]
        if drift > 0:
            targets[key] += 1
            drift -= 1
        else:
            if targets[key] > 0:
                targets[key] -= 1
                drift += 1
        idx += 1
    return targets


def _choose_examples(
    *,
    candidates: list[dict[str, Any]],
    size: int,
    seed: int,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    by_bucket: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in candidates:
        key = _bucket(str(item["question"]), str(item["sql"]))
        by_bucket.setdefault(key, []).append(item)

    for rows in by_bucket.values():
        rng.shuffle(rows)

    targets = _initial_targets(size)
    selected: list[dict[str, Any]] = []
    used_q: set[str] = set()

    # 1) initial allocation by target
    for key, target in targets.items():
        rows = by_bucket.get(key, [])
        take = min(target, len(rows))
        for item in rows[:take]:
            q = str(item["question"])
            if q in used_q:
                continue
            selected.append(item)
            used_q.add(q)
        by_bucket[key] = rows[take:]

    # 2) fill remainder from remaining capacity with complexity-priority
    priority = [
        ("ko", "hard"),
        ("en", "hard"),
        ("ko", "medium"),
        ("en", "medium"),
        ("ko", "simple"),
        ("en", "simple"),
    ]
    while len(selected) < size:
        progressed = False
        for key in priority:
            rows = by_bucket.get(key, [])
            while rows and len(selected) < size:
                item = rows.pop(0)
                q = str(item["question"])
                if q in used_q:
                    continue
                selected.append(item)
                used_q.add(q)
                progressed = True
                break
            by_bucket[key] = rows
            if len(selected) >= size:
                break
        if not progressed:
            break

    rng.shuffle(selected)
    return selected[:size]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a new evaluation set from sql_examples.jsonl.")
    parser.add_argument("--source", default="var/metadata/sql_examples.jsonl", help="Input examples jsonl path.")
    parser.add_argument("--output", default="var/metadata/mimic_eval_new_v1.jsonl", help="Output eval set path.")
    parser.add_argument(
        "--exclude-glob",
        default="mimic_eval*.jsonl",
        help="Glob (under source directory) for existing eval sets to exclude.",
    )
    parser.add_argument("--size", type=int, default=30, help="Number of examples to generate.")
    parser.add_argument("--seed", type=int, default=20260213, help="Random seed for deterministic sampling.")
    args = parser.parse_args()

    src_path = Path(args.source)
    if not src_path.exists():
        print(f"Source not found: {src_path}")
        return 1
    if args.size <= 0:
        print("size must be > 0")
        return 1

    all_rows = _load_jsonl(src_path)
    base_dir = src_path.parent
    excluded_q = _load_excluded_questions(base_dir, args.exclude_glob)

    dedup_by_question: dict[str, dict[str, Any]] = {}
    for row in all_rows:
        q = str(row.get("question") or "").strip()
        s = str(row.get("sql") or "").strip()
        if not q or not s or q in excluded_q:
            continue
        if q not in dedup_by_question:
            dedup_by_question[q] = {"question": q, "sql": s}

    candidates = list(dedup_by_question.values())
    if not candidates:
        print("No candidates left after exclusions.")
        return 1

    selected = _choose_examples(candidates=candidates, size=args.size, seed=args.seed)
    if not selected:
        print("Failed to select examples.")
        return 1

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        for item in selected:
            q = str(item["question"])
            s = str(item["sql"])
            level, join_count, score = _analyze_sql(s)
            payload = {
                "question": q,
                "sql": s,
                "meta": {
                    "lang": "ko" if _contains_hangul(q) else "en",
                    "complexity": level,
                    "join_count": join_count,
                    "sql_score": score,
                    "source": src_path.name,
                },
            }
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    # summary
    summary: dict[tuple[str, str], int] = {}
    for item in selected:
        key = _bucket(str(item["question"]), str(item["sql"]))
        summary[key] = summary.get(key, 0) + 1
    printable = {f"{k[0]}_{k[1]}": v for k, v in sorted(summary.items())}
    print(
        json.dumps(
            {
                "source": str(src_path),
                "excluded_questions": len(excluded_q),
                "candidates": len(candidates),
                "selected": len(selected),
                "output": str(out_path),
                "bucket_counts": printable,
                "seed": args.seed,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
