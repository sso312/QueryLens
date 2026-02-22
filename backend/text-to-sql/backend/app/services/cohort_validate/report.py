from __future__ import annotations

from typing import Any


def build_validation_markdown(report: dict[str, Any], *, accuracy_mode: bool) -> str:
    status = str((report or {}).get("status") or "skipped").strip().lower()
    invariants = report.get("invariants") if isinstance(report.get("invariants"), list) else []
    anomalies = report.get("anomalies") if isinstance(report.get("anomalies"), list) else []

    lines: list[str] = []
    lines.append(f"- accuracy_mode: {bool(accuracy_mode)}")
    lines.append(f"- status: {status}")
    lines.append(f"- invariants: {len(invariants)}")
    lines.append(f"- anomalies: {len(anomalies)}")
    return "\n".join(lines)
