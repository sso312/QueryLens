from __future__ import annotations

import copy
from typing import Any

from .policy_store import load_policy
from .question_templates import top_questions


def _inject_signal_map_ambiguities(
    ambiguities: list[dict[str, Any]],
    *,
    schema_map: dict[str, Any],
    spec: dict[str, Any],
) -> list[dict[str, Any]]:
    out = list(ambiguities)
    seen = {str(item.get("id") or "").strip() for item in out if isinstance(item, dict)}

    signal_map = schema_map.get("signal_map") if isinstance(schema_map.get("signal_map"), dict) else {}
    requirements = spec.get("requirements") if isinstance(spec.get("requirements"), list) else []
    for req in requirements:
        if not isinstance(req, dict):
            continue
        if str(req.get("type") or "").strip().lower() != "measurement_required":
            continue
        signals = req.get("signals") if isinstance(req.get("signals"), list) else []
        for signal in signals:
            key = str(signal or "").strip()
            if not key:
                continue
            if key in signal_map and isinstance(signal_map.get(key), dict) and signal_map.get(key, {}).get("itemids"):
                continue
            amb_id = f"amb_signal_map_{key}"
            if amb_id in seen:
                continue
            seen.add(amb_id)
            out.append(
                {
                    "id": amb_id,
                    "question": f"측정 시그널 '{key}' 의 itemid 매핑이 필요합니다.",
                    "options": ["SchemaMap에 itemids 입력", "해당 signal 제외"],
                    "default_policy": "require_user_choice",
                    "status": "unresolved",
                }
            )
    return out


def resolve_ambiguities(
    *,
    spec: dict[str, Any],
    schema_map: dict[str, Any],
    pdf_hash: str,
    limit: int = 3,
) -> dict[str, Any]:
    out = copy.deepcopy(spec or {})
    ambiguities = out.get("ambiguities") if isinstance(out.get("ambiguities"), list) else []
    ambiguities = _inject_signal_map_ambiguities(ambiguities, schema_map=schema_map, spec=out)

    policy = load_policy(pdf_hash)
    selections = policy.get("selections") if isinstance(policy.get("selections"), dict) else {}

    unresolved: list[dict[str, Any]] = []
    for item in ambiguities:
        if not isinstance(item, dict):
            continue
        amb_id = str(item.get("id") or "").strip()
        if not amb_id:
            continue
        chosen = selections.get(amb_id)
        options = item.get("options") if isinstance(item.get("options"), list) else []
        if chosen is not None and (not options or chosen in options):
            item["status"] = "resolved"
            item["resolved_value"] = chosen
        status = str(item.get("status") or "unresolved").strip().lower()
        if status != "resolved":
            item["status"] = "unresolved"
            unresolved.append(item)

    out["ambiguities"] = ambiguities
    return {
        "spec": out,
        "ambiguities": unresolved,
        "questions": top_questions(unresolved, limit=limit),
        "blocked": bool(unresolved),
    }
