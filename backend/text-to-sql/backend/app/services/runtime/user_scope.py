from __future__ import annotations

import re


_INVALID_USER_CHAR_PATTERN = re.compile(r"[^a-zA-Z0-9._:@-]+")
_MAX_USER_ID_LEN = 128


def normalize_user_id(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    normalized = _INVALID_USER_CHAR_PATTERN.sub("_", text)
    normalized = re.sub(r"_+", "_", normalized).strip("._:@-")
    if not normalized:
        return ""
    return normalized[:_MAX_USER_ID_LEN]


def scoped_state_key(base_key: str, user_id: str | None) -> str:
    normalized = normalize_user_id(user_id)
    if not normalized:
        return base_key
    return f"{base_key}::user::{normalized}"
