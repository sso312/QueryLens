"""Evaluation helpers for pipeline quality metrics."""
from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import Any, Dict, List

import pandas as pd

from src.agent.analysis_agent import analyze_and_visualize


@dataclass
class EvalCase:
    name: str
    user_query: str
    sql: str
    rows: List[Dict[str, Any]]


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def evaluate_cases(cases: List[EvalCase]) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    for case in cases:
        df = pd.DataFrame(case.rows)
        response = analyze_and_visualize(case.user_query, case.sql, df, request_id=f"eval-{case.name}")
        renderable = any(card.figure_json for card in response.analyses)
        results.append(
            {
                "name": case.name,
                "renderable": renderable,
                "fallback_used": response.fallback_used,
                "total_latency_ms": response.total_latency_ms or 0.0,
                "failure_count": len(response.failure_reasons),
                "attempt_count": response.attempt_count,
            }
        )

    success = sum(1 for r in results if r["renderable"])
    fallback = sum(1 for r in results if r["fallback_used"])
    failure_free = sum(1 for r in results if r["failure_count"] == 0)
    latency_values = [float(r["total_latency_ms"]) for r in results]

    summary = {
        "case_count": len(results),
        "render_success_rate_pct": _safe_rate(success, len(results)),
        "fallback_rate_pct": _safe_rate(fallback, len(results)),
        "failure_free_rate_pct": _safe_rate(failure_free, len(results)),
        "avg_latency_ms": round(mean(latency_values), 2) if latency_values else 0.0,
        "max_latency_ms": round(max(latency_values), 2) if latency_values else 0.0,
        "results": results,
    }
    return summary

