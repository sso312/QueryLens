from __future__ import annotations

from typing import Any


def summarize_invariants(report: dict[str, Any]) -> dict[str, Any]:
    invariants = report.get("invariants") if isinstance(report.get("invariants"), list) else []
    total = len(invariants)
    failed = 0
    for item in invariants:
        if not isinstance(item, dict):
            continue
        if not bool(item.get("passed", False)):
            failed += 1
    return {"total": total, "failed": failed, "passed": max(0, total - failed)}
