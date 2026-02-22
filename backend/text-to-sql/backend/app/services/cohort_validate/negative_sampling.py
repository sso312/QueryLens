from __future__ import annotations

from typing import Any


def summarize_negative_samples(report: dict[str, Any]) -> dict[str, Any]:
    samples = report.get("negative_samples") if isinstance(report.get("negative_samples"), list) else []
    return {"sample_count": len(samples)}
