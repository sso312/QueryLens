from __future__ import annotations

from typing import Any


def summarize_stepwise(report: dict[str, Any]) -> dict[str, Any]:
    counts = report.get("stepwise_counts") if isinstance(report.get("stepwise_counts"), list) else []
    anomalies = report.get("anomalies") if isinstance(report.get("anomalies"), list) else []
    return {
        "step_count": len(counts),
        "anomaly_count": len(anomalies),
    }
