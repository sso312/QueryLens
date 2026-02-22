from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CompilerGuardResult:
    blocked: bool = False
    violations: list[dict[str, str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
