from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.core.config import get_settings
from app.services.logging_store.store import read_events, write_events
from app.services.runtime.user_scope import normalize_user_id


router = APIRouter()


def _format_ts(ts: int | None) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _format_duration(duration_ms: int | None) -> str:
    if duration_ms is None:
        return "0.00초"
    try:
        return f"{duration_ms / 1000:.2f}초"
    except Exception:
        return "0.00초"


def _normalize_terms(items: Any) -> list[dict[str, str]]:
    if not isinstance(items, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            term = str(item.get("term") or item.get("name") or "").strip()
            version = str(item.get("version") or "").strip()
            if term:
                normalized.append({"term": term, "version": version})
        elif isinstance(item, str) and item.strip():
            normalized.append({"term": item.strip(), "version": ""})
    return normalized


def _normalize_metrics(items: Any) -> list[dict[str, str]]:
    if not isinstance(items, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("metric") or item.get("term") or "").strip()
            version = str(item.get("version") or "").strip()
            if name:
                normalized.append({"name": name, "version": version})
        elif isinstance(item, str) and item.strip():
            normalized.append({"name": item.strip(), "version": ""})
    return normalized


def _build_log_uid(event: dict[str, Any], line_no: int, fallback_id: int) -> str:
    ts = _safe_int(event.get("ts") or 0)
    base_id = str(event.get("id") or event.get("qid") or f"audit-{fallback_id}")
    return f"{ts}:{line_no}:{base_id}"


def _event_user_id(event: dict[str, Any]) -> str:
    user = event.get("user") if isinstance(event.get("user"), dict) else {}
    user_id = normalize_user_id(str(user.get("id") or ""))
    if user_id:
        return user_id
    return normalize_user_id(str(user.get("name") or ""))


def _event_matches_user(event: dict[str, Any], requested_user: str) -> bool:
    if not requested_user:
        return True
    return _event_user_id(event) == requested_user


def _is_visible_audit_event(event: dict[str, Any]) -> bool:
    if event.get("type") != "audit":
        return False
    # `query_oneshot` is an intent/planning step, not a final SQL execution.
    # Exclude it from audit list to keep one log row per executed query.
    event_name = str(event.get("event") or "").strip().lower()
    if event_name == "query_oneshot":
        return False
    return True


def _event_to_log(event: dict[str, Any], fallback_id: int) -> dict[str, Any]:
    ts = _safe_int(event.get("ts") or 0)
    user = event.get("user") if isinstance(event.get("user"), dict) else {}
    user_id = str(user.get("id") or "").strip()
    user_name = str(user.get("name") or "사용자")
    user_role = str(user.get("role") or "연구원")
    question = str(event.get("question") or "직접 SQL 실행")
    sql = str(event.get("sql") or "")
    status = str(event.get("status") or "success")
    duration_ms = event.get("duration_ms")
    rows_returned = _safe_int(event.get("rows_returned") or 0)

    audit_no = _safe_int(event.get("_audit_no"), fallback_id)
    base_id = str(event.get("id") or event.get("qid") or f"audit-{audit_no}")
    line_no = _safe_int(event.get("_line_no"), fallback_id)
    log_uid = _build_log_uid(event, line_no, audit_no)

    log = {
        "id": log_uid,
        "baseId": base_id,
        "timestamp": _format_ts(ts),
        "ts": ts,
        "user": {"id": user_id or None, "name": user_name, "role": user_role},
        "query": {"original": question, "sql": sql},
        "appliedTerms": _normalize_terms(event.get("applied_terms")),
        "appliedMetrics": _normalize_metrics(event.get("applied_metrics")),
        "execution": {
            "duration": _format_duration(duration_ms if isinstance(duration_ms, int) else None),
            "rowsReturned": rows_returned,
            "status": status,
        },
    }

    summary = event.get("result_summary")
    download_url = event.get("result_download_url")
    if summary or download_url:
        log["resultSnapshot"] = {
            "summary": str(summary or ""),
            "downloadUrl": str(download_url or ""),
        }

    return log


@router.get("/logs")
def audit_logs(limit: int = Query(200, ge=1, le=2000), user: str | None = Query(default=None)):
    settings = get_settings()
    events = read_events(settings.events_log_path)
    requested_user = normalize_user_id(user)
    audit_events: list[dict[str, Any]] = []
    audit_no = 1
    for idx, event in enumerate(events):
        if not _is_visible_audit_event(event):
            continue
        if requested_user and not _event_matches_user(event, requested_user):
            continue
        item = dict(event)
        item["_line_no"] = idx
        item["_audit_no"] = audit_no
        audit_events.append(item)
        audit_no += 1

    audit_events.sort(key=lambda item: _safe_int(item.get("ts") or 0), reverse=True)

    total = len(audit_events)
    success_count = sum(1 for event in audit_events if event.get("status") == "success")
    today = datetime.now().date()
    today_count = 0
    user_names: set[str] = set()
    for event in audit_events:
        ts = _safe_int(event.get("ts") or 0)
        if ts:
            try:
                if datetime.fromtimestamp(ts).date() == today:
                    today_count += 1
            except Exception:
                pass
        user = event.get("user")
        if isinstance(user, dict):
            name = user.get("name")
            if name:
                user_names.add(str(name))

    success_rate = round((success_count / total) * 100, 1) if total else 0.0

    sliced = audit_events[:limit]
    logs = [_event_to_log(event, idx) for idx, event in enumerate(sliced, start=1)]

    return {
        "logs": logs,
        "stats": {
            "total": total,
            "today": today_count,
            "active_users": len(user_names),
            "success_rate": success_rate,
        },
    }


@router.delete("/logs/{log_id}")
def delete_audit_log(log_id: str, user: str | None = Query(default=None)):
    settings = get_settings()
    events = read_events(settings.events_log_path)
    requested_user = normalize_user_id(user)
    target_line: int | None = None
    fallback_id = 1
    for line_no, event in enumerate(events):
        if not _is_visible_audit_event(event):
            continue
        if requested_user and not _event_matches_user(event, requested_user):
            continue
        candidate_uid = _build_log_uid(event, line_no, fallback_id)
        if candidate_uid == log_id:
            target_line = line_no
            break
        fallback_id += 1

    if target_line is None or target_line < 0 or target_line >= len(events):
        raise HTTPException(status_code=404, detail="Audit log not found")

    next_events = [event for i, event in enumerate(events) if i != target_line]
    write_events(settings.events_log_path, next_events)
    return {"ok": True}
