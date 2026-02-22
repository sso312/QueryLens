"""Post-process helpers for chart rule engine plans."""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def dedupe_plans(plans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique: List[Dict[str, Any]] = []
    seen = set()
    for plan in plans:
        spec = plan.get("chart_spec") or {}
        key = (
            spec.get("chart_type"),
            spec.get("x"),
            spec.get("y"),
            spec.get("group"),
            spec.get("secondary_group"),
            spec.get("agg"),
            spec.get("size"),
            spec.get("animation_frame"),
            spec.get("mode"),
            spec.get("bar_mode"),
            spec.get("orientation"),
            tuple(spec.get("series_cols", []) or []),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(plan)
    return unique


def bar_preferred_chart_type(style: Dict[str, bool]) -> Optional[str]:
    if not style.get("requested"):
        return None
    if style.get("percent") and style.get("horizontal"):
        return "bar_hpercent"
    if style.get("percent"):
        return "bar_percent"
    if style.get("stacked") and style.get("horizontal"):
        return "bar_hstack"
    if style.get("stacked"):
        return "bar_stacked"
    if style.get("horizontal"):
        return "bar_hgroup"
    if style.get("grouped") or style.get("detailed"):
        return "bar_grouped"
    return "bar_basic"


def prioritize_bar_plans(
    plans: List[Dict[str, Any]],
    style: Dict[str, bool],
) -> List[Dict[str, Any]]:
    preferred = bar_preferred_chart_type(style)
    if not preferred:
        return plans

    base_order = {
        "bar_basic": 0,
        "bar": 0,
        "bar_grouped": 1,
        "bar_stacked": 2,
        "bar_hgroup": 3,
        "bar_hstack": 4,
        "bar_percent": 5,
        "bar_hpercent": 6,
    }

    def _rank(plan: Dict[str, Any]) -> tuple[int, int, int]:
        spec = plan.get("chart_spec") or {}
        chart_type = str(spec.get("chart_type") or "").lower()
        is_bar = chart_type.startswith("bar")
        prefer_rank = 0 if chart_type == preferred else 1
        style_rank = base_order.get(chart_type, 999)
        return (0 if is_bar else 1, prefer_rank, style_rank)

    return sorted(plans, key=_rank)


def prioritize_requested_chart(
    plans: List[Dict[str, Any]],
    preferred_chart: Optional[str],
) -> List[Dict[str, Any]]:
    preferred = str(preferred_chart or "").strip().lower()
    if not preferred:
        return plans

    def _rank(plan: Dict[str, Any]) -> tuple[int]:
        spec = plan.get("chart_spec") or {}
        chart_type = str(spec.get("chart_type") or "").lower()
        return (0 if chart_type == preferred else 1,)

    return sorted(plans, key=_rank)
