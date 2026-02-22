from __future__ import annotations

from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def project_root() -> Path:
    return _PROJECT_ROOT


def project_path(relative: str | Path) -> Path:
    path = Path(relative)
    if path.is_absolute():
        return path
    return _PROJECT_ROOT / path

