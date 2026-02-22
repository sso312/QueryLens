from __future__ import annotations

from typing import Any

from app.services.runtime.state_store import get_state_store


_KEY_PREFIX = "cohort_ambiguity_policy::"


def load_policy(pdf_hash: str) -> dict[str, Any]:
    store = get_state_store()
    payload = store.get(f"{_KEY_PREFIX}{pdf_hash}") if store else None
    return payload if isinstance(payload, dict) else {}


def save_policy(pdf_hash: str, policy: dict[str, Any]) -> bool:
    store = get_state_store()
    if not store or not pdf_hash:
        return False
    return store.set(f"{_KEY_PREFIX}{pdf_hash}", policy if isinstance(policy, dict) else {})
