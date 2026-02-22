from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from app.core.paths import project_path
from app.services.runtime.request_context import get_request_user_id
from app.services.runtime.state_store import get_state_store
from app.services.runtime.user_scope import normalize_user_id, scoped_state_key


BASE_PATH = project_path("var/metadata")
CONNECTION_PATH = BASE_PATH / "connection_settings.json"
TABLE_SCOPE_PATH = BASE_PATH / "table_scope.json"
CONNECTION_KEY = "connection_settings"
TABLE_SCOPE_KEY = "table_scope"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def _resolve_user_id(user_id: str | None = None) -> str:
    explicit = normalize_user_id(user_id)
    if explicit:
        return explicit
    return normalize_user_id(get_request_user_id())


def _scoped_path(path: Path, user_id: str | None = None) -> Path:
    normalized = normalize_user_id(user_id)
    if not normalized:
        return path
    return path.with_name(f"{path.stem}__user__{normalized}{path.suffix}")


def load_connection_settings(
    user_id: str | None = None,
    *,
    include_global_fallback: bool = False,
) -> dict[str, Any]:
    resolved_user = _resolve_user_id(user_id)
    store = get_state_store()
    data: dict[str, Any] | None = None
    if store.enabled:
        if resolved_user:
            user_key = scoped_state_key(CONNECTION_KEY, resolved_user)
            data = store.get(user_key)
            if isinstance(data, dict) and data:
                return data
            if include_global_fallback:
                data = store.get(CONNECTION_KEY)
                if isinstance(data, dict) and data:
                    return data
        else:
            data = store.get(CONNECTION_KEY)
            if isinstance(data, dict) and data:
                return data

    if resolved_user:
        scoped_data = _load_json(_scoped_path(CONNECTION_PATH, resolved_user))
        if scoped_data:
            return scoped_data
        if not include_global_fallback:
            return {}
    return _load_json(CONNECTION_PATH)


def save_connection_settings(payload: dict[str, Any], user_id: str | None = None) -> None:
    resolved_user = normalize_user_id(user_id)
    store = get_state_store()
    if store.enabled:
        if resolved_user:
            user_key = scoped_state_key(CONNECTION_KEY, resolved_user)
            if store.set(user_key, payload):
                return
        elif store.set(CONNECTION_KEY, payload):
            return
    target_path = _scoped_path(CONNECTION_PATH, resolved_user) if resolved_user else CONNECTION_PATH
    _save_json(target_path, payload)


def load_table_scope(
    user_id: str | None = None,
    *,
    include_global_fallback: bool = False,
) -> list[str]:
    resolved_user = _resolve_user_id(user_id)
    store = get_state_store()
    data: dict[str, Any] | None = None
    if store.enabled:
        if resolved_user:
            user_key = scoped_state_key(TABLE_SCOPE_KEY, resolved_user)
            data = store.get(user_key)
            if isinstance(data, dict) and "selected_ids" in data:
                raw = data.get("selected_ids")
                if isinstance(raw, list):
                    return [str(item) for item in raw if isinstance(item, (str, int))]
            if include_global_fallback:
                data = store.get(TABLE_SCOPE_KEY)
                if isinstance(data, dict) and "selected_ids" in data:
                    raw = data.get("selected_ids")
                    if isinstance(raw, list):
                        return [str(item) for item in raw if isinstance(item, (str, int))]
        else:
            data = store.get(TABLE_SCOPE_KEY)
            if isinstance(data, dict) and "selected_ids" in data:
                raw = data.get("selected_ids")
                if isinstance(raw, list):
                    return [str(item) for item in raw if isinstance(item, (str, int))]

    if resolved_user:
        scoped_path = _scoped_path(TABLE_SCOPE_PATH, resolved_user)
        if scoped_path.exists():
            data = _load_json(scoped_path)
            if isinstance(data, dict) and "selected_ids" in data:
                raw = data.get("selected_ids")
                if isinstance(raw, list):
                    return [str(item) for item in raw if isinstance(item, (str, int))]
        if not include_global_fallback:
            return []
        data = _load_json(TABLE_SCOPE_PATH)
    else:
        data = _load_json(TABLE_SCOPE_PATH)
    raw = data.get("selected_ids", [])
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw if isinstance(item, (str, int))]


def save_table_scope(selected_ids: list[str], user_id: str | None = None) -> None:
    resolved_user = normalize_user_id(user_id)
    payload = {"selected_ids": selected_ids}
    store = get_state_store()
    if store.enabled:
        if resolved_user:
            user_key = scoped_state_key(TABLE_SCOPE_KEY, resolved_user)
            if store.set(user_key, payload):
                return
        elif store.set(TABLE_SCOPE_KEY, payload):
            return
    target_path = _scoped_path(TABLE_SCOPE_PATH, resolved_user) if resolved_user else TABLE_SCOPE_PATH
    _save_json(target_path, payload)
