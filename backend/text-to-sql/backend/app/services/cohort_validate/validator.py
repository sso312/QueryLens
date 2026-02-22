from __future__ import annotations

from typing import Any

from .invariants import summarize_invariants
from .negative_sampling import summarize_negative_samples
from .stepwise_counts import summarize_stepwise


def summarize_validation(report: dict[str, Any]) -> dict[str, Any]:
    status = str((report or {}).get("status") or "skipped").strip().lower()
    invariant_summary = summarize_invariants(report or {})
    stepwise_summary = summarize_stepwise(report or {})
    negative_summary = summarize_negative_samples(report or {})
    return {
        "status": status,
        "validation_pass": status == "passed" and invariant_summary.get("failed", 1) == 0,
        "invariants": invariant_summary,
        "stepwise": stepwise_summary,
        "negative_sampling": negative_summary,
    }
