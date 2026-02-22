from __future__ import annotations

import copy
from typing import Any


def _has_evidence(item: dict[str, Any]) -> bool:
    refs = item.get("evidence_refs")
    return isinstance(refs, list) and len(refs) > 0


def enforce_condition_evidence(spec: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    out = copy.deepcopy(spec or {})
    ambiguities = out.get("ambiguities") if isinstance(out.get("ambiguities"), list) else []

    total = 0
    with_evidence = 0
    seen_amb: set[str] = {
        str(item.get("id") or "").strip()
        for item in ambiguities
        if isinstance(item, dict)
    }

    for section in ("inclusion", "exclusion", "requirements"):
        items = out.get(section)
        if not isinstance(items, list):
            continue
        kept: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            total += 1
            if _has_evidence(item):
                with_evidence += 1
                kept.append(item)
                continue
            cond_id = str(item.get("id") or f"{section}_{total}").strip()
            amb_id = f"amb_missing_evidence_{cond_id}"
            if amb_id not in seen_amb:
                seen_amb.add(amb_id)
                ambiguities.append(
                    {
                        "id": amb_id,
                        "question": f"{cond_id} 조건의 evidence_refs가 없습니다. 근거 확인이 필요합니다.",
                        "options": ["재추출", "수동 근거 입력"],
                        "default_policy": "require_user_choice",
                        "status": "unresolved",
                    }
                )
        out[section] = kept

    out["ambiguities"] = ambiguities
    coverage = float(with_evidence / max(1, total))
    summary = {
        "condition_count": total,
        "evidence_condition_count": with_evidence,
        "coverage": round(coverage, 4),
    }
    return out, summary
