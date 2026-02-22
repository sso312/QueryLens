from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from app.core.config import get_settings
from app.services.oracle.connection import reset_pool
from app.services.oracle.metadata_extractor import extract_metadata
from app.services.runtime.settings_store import (
    load_connection_settings as fetch_connection_settings,
    load_table_scope as fetch_table_scope,
    save_connection_settings as persist_connection_settings,
    save_table_scope as persist_table_scope,
)
from app.services.runtime.request_context import use_request_user

router = APIRouter()


class ConnectionSettings(BaseModel):
    host: str
    port: str
    database: str
    username: str
    password: str | None = None
    sslMode: str | None = None
    defaultSchema: str | None = None


class TableScopeSettings(BaseModel):
    selected_ids: list[str] = []


def _normalize_connection_settings_payload(
    payload: dict[str, str | None],
    previous: dict[str, str] | None = None,
) -> dict[str, str]:
    prev = previous or {}
    normalized: dict[str, str] = {}

    for key in ("host", "port", "database", "username"):
        normalized[key] = str(payload.get(key) or "").strip()

    ssl_mode = str(payload.get("sslMode") or "").strip()
    if ssl_mode:
        normalized["sslMode"] = ssl_mode

    default_schema = str(payload.get("defaultSchema") or "").strip()
    if default_schema:
        normalized["defaultSchema"] = default_schema

    password_raw = payload.get("password")
    if password_raw is None:
        password = str(prev.get("password") or "")
    else:
        password = str(password_raw).strip()
        if not password:
            password = str(prev.get("password") or "")
    if not password:
        raise HTTPException(
            status_code=400,
            detail="Password is required for Oracle authentication.",
        )
    normalized["password"] = password
    return normalized


def _validate_connection_settings_payload(payload: dict[str, str]) -> None:
    host = str(payload.get("host") or "").strip()
    port = str(payload.get("port") or "").strip()
    database = str(payload.get("database") or "").strip()
    username = str(payload.get("username") or "").strip()

    missing = [
        name
        for name, value in (
            ("host", host),
            ("port", port),
            ("database", database),
            ("username", username),
        )
        if not value
    ]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required connection fields: {', '.join(missing)}",
        )

    try:
        port_number = int(port)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Port must be a valid integer") from exc
    if port_number < 1 or port_number > 65535:
        raise HTTPException(status_code=400, detail="Port must be between 1 and 65535")

    if host.lower() == "mimic-iv.hospital.edu":
        raise HTTPException(
            status_code=400,
            detail="mimic-iv.hospital.edu is a demo placeholder. Enter your real Oracle host.",
        )


@router.get("/connection")
def get_connection_settings(user: str | None = Query(default=None)):
    use_global_fallback = not bool(str(user or "").strip())
    return fetch_connection_settings(user, include_global_fallback=use_global_fallback) or {}


@router.post("/connection")
def save_connection_settings(req: ConnectionSettings, user: str | None = Query(default=None)):
    previous = fetch_connection_settings(user, include_global_fallback=False) or {}
    payload = _normalize_connection_settings_payload(
        req.model_dump(exclude_none=True),
        previous=previous,
    )
    _validate_connection_settings_payload(payload)
    persist_connection_settings(payload, user)
    reset_pool(user)

    settings = get_settings()
    owner = str(
        payload.get("defaultSchema")
        or previous.get("defaultSchema")
        or settings.oracle_default_schema
        or ""
    ).strip()

    if not owner:
        return {
            "ok": True,
            "metadata_synced": False,
            "owner": None,
            "reason": "ORACLE_DEFAULT_SCHEMA is not configured",
        }

    try:
        with use_request_user(user):
            sync_result = extract_metadata(owner)
    except Exception as exc:
        detail = str(getattr(exc, "detail", exc)).strip()
        return {
            "ok": True,
            "metadata_synced": False,
            "owner": owner.upper(),
            "reason": detail or "metadata sync failed",
        }

    tables_synced = int(sync_result.get("tables") or 0)
    if tables_synced <= 0:
        return {
            "ok": True,
            "metadata_synced": False,
            "owner": owner.upper(),
            "reason": "No tables found for the configured schema owner",
        }

    effective_owner = str(sync_result.get("effective_owner") or owner).strip().upper()
    if effective_owner:
        current_default = str(payload.get("defaultSchema") or previous.get("defaultSchema") or "").strip().upper()
        if current_default != effective_owner:
            payload["defaultSchema"] = effective_owner
            persist_connection_settings(payload, user)
            reset_pool(user)

    return {
        "ok": True,
        "metadata_synced": True,
        "owner": effective_owner or owner.upper(),
        "tables": tables_synced,
    }


@router.get("/table-scope")
def get_table_scope(user: str | None = Query(default=None)):
    use_global_fallback = not bool(str(user or "").strip())
    selected_ids = fetch_table_scope(user, include_global_fallback=use_global_fallback)
    return {"selected_ids": selected_ids}


@router.post("/table-scope")
def save_table_scope(req: TableScopeSettings, user: str | None = Query(default=None)):
    persist_table_scope(req.selected_ids, user)
    return {"ok": True}
