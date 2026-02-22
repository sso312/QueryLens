from __future__ import annotations

from typing import Any

from .anti_patterns import find_anti_patterns
from .base import CompilerGuardResult


def apply_oracle_compiler_guards(compiled: dict[str, Any], *, accuracy_mode: bool) -> dict[str, Any]:
    out = dict(compiled or {})
    sql = str(out.get("cohort_sql") or "")
    violations = find_anti_patterns(sql)

    result = CompilerGuardResult()
    for item in violations:
        code = str(item.get("code") or "")
        blocking = bool(item.get("blocking"))
        msg = str(item.get("message") or code)
        result.violations.append({"code": code, "message": msg})
        if blocking and accuracy_mode:
            result.blocked = True
        else:
            result.warnings.append(msg)

    if result.violations:
        out["compiler_guard"] = {
            "blocked": result.blocked,
            "violations": result.violations,
            "warnings": result.warnings,
        }

    existing_warn = out.get("warning") if isinstance(out.get("warning"), list) else []
    merged_warn = list(existing_warn)
    for msg in result.warnings:
        if msg not in merged_warn:
            merged_warn.append(msg)
    if merged_warn:
        out["warning"] = merged_warn
    if result.blocked:
        out["blocked"] = True
    return out
