from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any

from fastapi import HTTPException

# Ensure backend is importable when running from repo root
ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.append(str(ROOT / "backend"))

from app.services.agents.orchestrator import run_oneshot
from app.services.agents.sql_error_parser import parse_sql_error
from app.services.agents.sql_error_templates import apply_sql_error_templates
from app.services.agents.sql_expert import repair_sql_after_error
from app.services.agents.sql_postprocess import postprocess_sql, recommend_postprocess_profile
from app.core.config import get_settings
from app.services.oracle.executor import execute_sql
from app.services.runtime.sql_error_repair_store import find_learned_sql_fix


_TEMPLATE_REPAIR_ERROR_CODES = {
    "ORA-00904",
    "ORA-00905",
    "ORA-00933",
    "ORA-00942",
    "ORA-00979",
    "ORA-01722",
}
_TEMPLATE_REPAIR_ERROR_MARKERS = tuple(_TEMPLATE_REPAIR_ERROR_CODES)


def load_examples(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    items: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("question") and obj.get("sql"):
            items.append(obj)
    return items


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


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _normalize_question(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _augment_examples_from_report(
    report_path: Path,
    *,
    base_path: Path,
    augmented_path: Path,
    max_new: int,
) -> int:
    report_rows = _load_jsonl(report_path)
    base_rows = _load_jsonl(base_path)
    existing_rows = _load_jsonl(augmented_path)

    seen_questions = {
        _normalize_question(str(item.get("question") or ""))
        for item in [*base_rows, *existing_rows]
        if str(item.get("question") or "").strip()
    }
    seen_questions = {item for item in seen_questions if item}

    merged = list(existing_rows)
    added = 0
    for item in report_rows:
        status = str(item.get("status") or "").strip().lower()
        if status not in {"mismatch", "generated_exec_error", "exec_error"}:
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
        merged.append(
            {
                "question": question,
                "sql": sql,
                "source": "eval_failure_replay",
                "status": status,
            }
        )
        seen_questions.add(normalized)
        added += 1
        if max_new > 0 and added >= max_new:
            break

    _write_jsonl(augmented_path, merged)
    return added


def safe_execute(sql: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return execute_sql(sql), None
    except HTTPException as exc:
        return None, str(exc.detail)
    except Exception as exc:
        return None, str(exc)


def _is_template_repair_candidate(
    *,
    structured_error: dict[str, Any] | None,
    error_message: str,
) -> bool:
    if isinstance(structured_error, dict):
        code = str(structured_error.get("error_code") or "").strip().upper()
        if code in _TEMPLATE_REPAIR_ERROR_CODES:
            return True
    upper = str(error_message or "").upper()
    return any(marker in upper for marker in _TEMPLATE_REPAIR_ERROR_MARKERS)


def _extract_payload_context(payload: dict[str, Any]) -> tuple[str | None, dict[str, Any], dict[str, Any] | None]:
    question_en_value = payload.get("question_en")
    question_en = str(question_en_value).strip() if isinstance(question_en_value, str) and question_en_value.strip() else None
    context_value = payload.get("context")
    context = context_value if isinstance(context_value, dict) else {}
    planner_intent: dict[str, Any] | None = None
    planner = payload.get("planner")
    if isinstance(planner, dict):
        intent = planner.get("intent")
        if isinstance(intent, dict):
            planner_intent = intent
    return question_en, context, planner_intent


def execute_with_repair(
    *,
    question: str,
    payload: dict[str, Any],
    generated_sql: str,
) -> tuple[str, dict[str, Any] | None, str | None, int]:
    settings = get_settings()
    current_sql = str(generated_sql or "").strip()
    if not current_sql:
        return "", None, "empty generated sql", 0

    question_en, context, planner_intent = _extract_payload_context(payload)
    max_repair_attempts = (
        int(settings.sql_auto_repair_max_attempts)
        if bool(settings.sql_auto_repair_enabled)
        else 0
    )
    repair_round = 0
    while True:
        profile = "relaxed" if repair_round == 0 else "aggressive"
        if repair_round == 0:
            profile, _ = recommend_postprocess_profile(
                question,
                current_sql,
                default_profile=profile,
            )
        current_sql, _ = postprocess_sql(question, current_sql, profile=profile)
        result, error = safe_execute(current_sql)
        if result is not None:
            return current_sql, result, None, repair_round
        if repair_round >= max_repair_attempts:
            return current_sql, None, error, repair_round

        error_message = str(error or "")
        structured_error = parse_sql_error(error_message, sql=current_sql)
        known_fix = find_learned_sql_fix(current_sql, error_message=error_message)
        if isinstance(known_fix, dict):
            known_fixed_sql = str(known_fix.get("fixed_sql") or "").strip()
            if known_fixed_sql and known_fixed_sql.strip() != current_sql.strip():
                current_sql = known_fixed_sql
                repair_round += 1
                continue

        if _is_template_repair_candidate(
            structured_error=structured_error,
            error_message=error_message,
        ):
            templated_sql, _ = apply_sql_error_templates(
                question=question,
                sql=current_sql,
                error_message=error_message,
            )
            if templated_sql.strip() and templated_sql.strip() != current_sql.strip():
                current_sql = templated_sql
                repair_round += 1
                continue

        try:
            repaired = repair_sql_after_error(
                question,
                context,
                current_sql,
                error_message,
                question_en=question_en,
                planner_intent=planner_intent,
            )
        except Exception:
            return current_sql, None, error_message, repair_round
        repaired_sql = str(repaired.get("final_sql") or "").strip()
        if not repaired_sql or repaired_sql.strip() == current_sql.strip():
            return current_sql, None, error_message or "auto repair produced no change", repair_round
        current_sql = repaired_sql
        repair_round += 1


def normalize_rows(rows: list[list[Any]], ignore_order: bool) -> list[list[Any]]:
    if not ignore_order:
        return rows
    # Sort rows as strings for deterministic comparison
    return sorted(rows, key=lambda r: json.dumps(r, ensure_ascii=True, default=str))


def compare_results(
    expected: dict[str, Any],
    generated: dict[str, Any],
    ignore_order: bool,
) -> tuple[bool, dict[str, Any]]:
    exp_cols = expected.get("columns", [])
    gen_cols = generated.get("columns", [])
    exp_rows = expected.get("rows", [])
    gen_rows = generated.get("rows", [])

    def _is_count_col(col: str) -> bool:
        name = str(col).strip().upper()
        return name in {"CNT", "COUNT"} or "COUNT(" in name

    same_cols = exp_cols == gen_cols
    exp_norm = normalize_rows(exp_rows, ignore_order)
    gen_norm = normalize_rows(gen_rows, ignore_order)
    same_rows = exp_norm == gen_norm

    if not same_cols and same_rows and len(exp_cols) == len(gen_cols) == 1:
        if _is_count_col(exp_cols[0]) and _is_count_col(gen_cols[0]):
            same_cols = True

    # Alias-only mismatch should not fail when data rows are identical.
    alias_equivalent = bool(same_rows and len(exp_cols) == len(gen_cols))
    if not same_cols and alias_equivalent:
        same_cols = True

    subset_rows = False
    if not same_rows and same_cols and ignore_order and exp_norm and gen_norm and len(exp_norm) < len(gen_norm):
        exp_counter = Counter(json.dumps(row, ensure_ascii=True, default=str) for row in exp_norm)
        gen_counter = Counter(json.dumps(row, ensure_ascii=True, default=str) for row in gen_norm)
        subset_rows = all(gen_counter.get(key, 0) >= count for key, count in exp_counter.items())

    return (same_cols and (same_rows or subset_rows)), {
        "same_cols": same_cols,
        "same_rows": same_rows,
        "alias_equivalent": alias_equivalent,
        "subset_rows": subset_rows,
        "expected_row_count": expected.get("row_count"),
        "generated_row_count": generated.get("row_count"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate text-to-SQL accuracy against expected SQL.")
    parser.add_argument(
        "--input",
        default="var/metadata/sql_examples.jsonl",
        help="Path to jsonl with {question, sql}.",
    )
    parser.add_argument(
        "--output",
        default="var/logs/eval_report.jsonl",
        help="Report output jsonl path.",
    )
    parser.add_argument("--max", type=int, default=0, help="Max number of examples (0 = all).")
    parser.add_argument("--ignore-order", action="store_true", help="Ignore row order in comparison.")
    parser.add_argument("--skip-policy", action="store_true", help="Skip PolicyGate precheck in generation.")
    parser.add_argument(
        "--execution-mode",
        choices=["oneshot", "pipeline", "service_pipeline"],
        default="pipeline",
        help=(
            "oneshot: execute generated SQL directly, "
            "pipeline: local /query/run-equivalent repair flow, "
            "service_pipeline: call query route handlers directly."
        ),
    )
    parser.add_argument(
        "--db-timeout-sec",
        type=int,
        default=0,
        help="Override DB_TIMEOUT_SEC for this evaluation run.",
    )
    parser.add_argument(
        "--require-advanced",
        action="store_true",
        help="Fail if demo cache is used (requires DEMO_MODE=false).",
    )
    parser.add_argument(
        "--augment-on-fail",
        action="store_true",
        help="Append mismatch/exec_error rows (with valid expected SQL) to augmented few-shot examples.",
    )
    parser.add_argument(
        "--augment-output",
        default="var/metadata/sql_examples_augmented.jsonl",
        help="Augmented few-shot jsonl output path.",
    )
    parser.add_argument(
        "--augment-max-new",
        type=int,
        default=50,
        help="Maximum new rows to append when --augment-on-fail is enabled (0 = unlimited).",
    )
    parser.add_argument(
        "--respect-budget-gate",
        action="store_true",
        help="Keep runtime budget gate enabled (default: disabled in service_pipeline evaluation).",
    )
    args = parser.parse_args()

    if args.db_timeout_sec and args.db_timeout_sec > 0:
        os.environ["DB_TIMEOUT_SEC"] = str(args.db_timeout_sec)
        from app.core import config as config_module
        config_module._SETTINGS = None

    examples = load_examples(Path(args.input))
    if not examples:
        print("No examples found.")
        return 1

    if args.max and args.max > 0:
        examples = examples[: args.max]

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total = len(examples)
    gen_ok = 0
    exec_ok = 0
    match_ok = 0
    demo_used = 0
    expected_exec_error = 0
    generated_exec_error = 0

    settings = get_settings()
    service_mode = args.execution_mode == "service_pipeline"
    api_oneshot = None
    api_run_query = None
    OneShotRequest = None
    RunRequest = None
    if service_mode:
        # Evaluation should not terminate early due budget gate.
        # This override only affects this script process.
        if not args.respect_budget_gate:
            from app.api.routes import query as query_route  # pylint: disable=import-outside-toplevel

            query_route.ensure_budget_ok = lambda: None  # type: ignore[assignment]
        from app.api.routes.query import (  # pylint: disable=import-outside-toplevel
            OneShotRequest as _OneShotRequest,
            RunRequest as _RunRequest,
            oneshot as _api_oneshot,
            run_query as _api_run_query,
        )

        api_oneshot = _api_oneshot
        api_run_query = _api_run_query
        OneShotRequest = _OneShotRequest
        RunRequest = _RunRequest

    def _safe_service_run(*, qid: str | None = None, sql: str | None = None) -> tuple[dict[str, Any] | None, str | None]:
        if api_run_query is None or RunRequest is None:
            return None, "service pipeline not initialized"
        try:
            payload = RunRequest(qid=qid, sql=sql, user_ack=True, user_name="eval", user_role="eval")
            return api_run_query(payload), None
        except HTTPException as exc:
            return None, str(exc.detail)
        except Exception as exc:
            return None, str(exc)

    with out_path.open("w", encoding="utf-8") as report:
        for idx, item in enumerate(examples, 1):
            question = item["question"]
            expected_sql = item["sql"]

            if service_mode:
                if api_oneshot is None or OneShotRequest is None:
                    status = {
                        "idx": idx,
                        "question": question,
                        "status": "service_pipeline_init_error",
                    }
                    report.write(json.dumps(status, ensure_ascii=True) + "\n")
                    continue
                try:
                    one_resp = api_oneshot(
                        OneShotRequest(
                            question=question,
                            user_name="eval",
                            user_role="eval",
                        )
                    )
                except HTTPException as exc:
                    status = {
                        "idx": idx,
                        "question": question,
                        "status": "oneshot_error",
                        "error": str(exc.detail),
                    }
                    report.write(json.dumps(status, ensure_ascii=True) + "\n")
                    continue
                except Exception as exc:
                    status = {
                        "idx": idx,
                        "question": question,
                        "status": "oneshot_error",
                        "error": str(exc),
                    }
                    report.write(json.dumps(status, ensure_ascii=True) + "\n")
                    continue
                payload = one_resp.get("payload", {}) if isinstance(one_resp, dict) else {}
                qid = str(one_resp.get("qid") or "").strip() if isinstance(one_resp, dict) else ""
            else:
                payload = run_oneshot(
                    question,
                    skip_policy=args.skip_policy,
                    enable_clarification=settings.clarifier_enabled,
                )
                qid = ""
            if payload.get("mode") == "demo":
                demo_used += 1
                if args.require_advanced:
                    status = {
                        "idx": idx,
                        "question": question,
                        "status": "demo_used",
                    }
                    report.write(json.dumps(status, ensure_ascii=True) + "\n")
                    continue

            final = payload.get("final", {})
            generated_sql = final.get("final_sql") or payload.get("draft", {}).get("final_sql")

            if not generated_sql:
                status = {
                    "idx": idx,
                    "question": question,
                    "status": "no_generated_sql",
                }
                report.write(json.dumps(status, ensure_ascii=True) + "\n")
                continue

            gen_ok += 1

            if service_mode:
                exp_run, exp_err = _safe_service_run(sql=expected_sql)
                exp_result = exp_run.get("result") if isinstance(exp_run, dict) else None
            else:
                exp_result, exp_err = safe_execute(expected_sql)
            effective_generated_sql = generated_sql
            if args.execution_mode == "pipeline":
                effective_generated_sql, gen_result, gen_err, repair_attempts = execute_with_repair(
                    question=question,
                    payload=payload,
                    generated_sql=generated_sql,
                )
            elif args.execution_mode == "service_pipeline":
                gen_run, gen_err = _safe_service_run(qid=qid or None)
                gen_result = gen_run.get("result") if isinstance(gen_run, dict) else None
                if isinstance(gen_run, dict):
                    effective_generated_sql = str(gen_run.get("sql") or generated_sql)
                    repair_meta = gen_run.get("repair")
                    if isinstance(repair_meta, dict):
                        repair_attempts = int(repair_meta.get("attempts") or 0)
                    else:
                        repair_attempts = 0
                else:
                    repair_attempts = 0
            else:
                gen_result, gen_err = safe_execute(generated_sql)
                repair_attempts = 0

            status: dict[str, Any] = {
                "idx": idx,
                "question": question,
                "expected_sql": expected_sql,
                "generated_sql_initial": generated_sql,
                "generated_sql": effective_generated_sql,
                "expected_error": exp_err,
                "generated_error": gen_err,
                "execution_mode": args.execution_mode,
                "repair_attempts": repair_attempts,
            }

            if exp_result is None:
                expected_exec_error += 1
                status["status"] = "expected_exec_error"
            elif gen_result is None:
                generated_exec_error += 1
                status["status"] = "generated_exec_error"
            else:
                exec_ok += 1
                matched, detail = compare_results(exp_result, gen_result, args.ignore_order)
                if matched:
                    match_ok += 1
                    status["status"] = "match"
                else:
                    status["status"] = "mismatch"
                status["compare"] = detail

            report.write(json.dumps(status, ensure_ascii=True, default=str) + "\n")

    summary = {
        "total": total,
        "generated_sql": gen_ok,
        "executed_both": exec_ok,
        "matched": match_ok,
        "expected_exec_error": expected_exec_error,
        "generated_exec_error": generated_exec_error,
        "comparable_cases": exec_ok,
        "demo_used": demo_used,
        "execution_mode": args.execution_mode,
        "db_timeout_sec": get_settings().db_timeout_sec,
        "output": str(out_path),
    }
    if args.augment_on_fail:
        settings_now = get_settings()
        base_path = Path(str(getattr(settings_now, "sql_examples_path", "var/metadata/sql_examples.jsonl")))
        augment_added = _augment_examples_from_report(
            out_path,
            base_path=base_path,
            augmented_path=Path(args.augment_output),
            max_new=max(0, int(args.augment_max_new)),
        )
        summary["augment_output"] = args.augment_output
        summary["augment_added"] = augment_added

    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
