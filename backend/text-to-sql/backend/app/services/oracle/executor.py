from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from typing import Any
from pathlib import Path

from fastapi import HTTPException

from app.core.config import get_settings
from app.core.paths import project_path
from app.services.oracle.connection import acquire_connection, resolve_call_timeout_ms
from app.services.runtime.settings_store import load_connection_settings

_FROM_JOIN_TABLE_WITH_SCHEMA_RE = re.compile(
    r"\b(from|join)\s+(\"?[A-Za-z0-9_$#]+\"?)\s*\.\s*(\"?[A-Za-z0-9_$#]+\"?)",
    re.IGNORECASE,
)
_CLIENT_TIMEOUT_MARKERS = ("DPY-4024", "DPI-1067", "ORA-03156")

logger = logging.getLogger(__name__)


def _sanitize_sql(sql: str) -> str:
    return sql.strip().rstrip(";")


def _safe_close(resource: Any) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except Exception:
        # A broken Oracle handle can raise during close(). Do not mask
        # the original execution error with cleanup failures.
        pass


def _load_metadata_owner() -> str:
    path: Path = project_path("var/metadata/schema_catalog.json")
    if not path.exists():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return ""
    return str((data or {}).get("owner") or "").strip()


def _is_ora_00942(exc: Exception) -> bool:
    return "ORA-00942" in str(exc).upper()


def _valid_schema_name(schema: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_$#]+", schema))


def _normalize_identifier(identifier: str) -> str:
    value = str(identifier or "").strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]
    return value.strip().upper()


def _strip_non_target_schema_prefixes(
    sql: str,
    *,
    target_schemas: set[str],
) -> tuple[str, bool]:
    normalized_targets = {
        _normalize_identifier(schema)
        for schema in target_schemas
        if _valid_schema_name(str(schema or "").strip())
    }
    changed = False

    def _replace(match: re.Match[str]) -> str:
        nonlocal changed
        keyword = match.group(1)
        schema = match.group(2)
        table = match.group(3)
        if _normalize_identifier(schema) in normalized_targets:
            return match.group(0)
        changed = True
        return f"{keyword} {table}"

    rewritten = _FROM_JOIN_TABLE_WITH_SCHEMA_RE.sub(_replace, sql)
    return rewritten, changed


def _classify_db_error(exc: Exception) -> str:
    message = str(exc).upper()
    if any(marker in message for marker in _CLIENT_TIMEOUT_MARKERS):
        return "CLIENT_TIMEOUT"
    if "ORA-" in message:
        return "DB_ERROR"
    return "EXEC_ERROR"


def execute_sql(
    sql: str,
    *,
    accuracy_mode: bool = False,
    db_call_timeout_ms: int | None = None,
    query_tag: str | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    overrides = load_connection_settings()
    text = _sanitize_sql(sql)
    query_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    effective_timeout_ms = (
        max(1_000, int(db_call_timeout_ms))
        if db_call_timeout_ms is not None
        else resolve_call_timeout_ms(accuracy_mode=accuracy_mode)
    )
    started = time.perf_counter()
    tag = str(query_tag or "default").strip() or "default"
    # Keep executor policy aligned with precheck_sql:
    # allow plain SELECT and CTE-based read-only queries (WITH ... SELECT ...).
    if not re.match(r"^\s*(select|with)\b", text, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
    if re.match(r"^\s*with\b", text, re.IGNORECASE) and not re.search(r"\bselect\b", text, re.IGNORECASE):
        raise HTTPException(status_code=400, detail="CTE query must include SELECT")

    logger.info(
        "Oracle SQL start tag=%s qh=%s timeout_ms=%s accuracy_mode=%s",
        tag,
        query_hash,
        effective_timeout_ms,
        bool(accuracy_mode),
    )
    conn = acquire_connection(accuracy_mode=accuracy_mode)
    try:
        try:
            conn.call_timeout = effective_timeout_ms
        except Exception:
            pass
        session_schema = str(
            overrides.get("defaultSchema")
            or settings.oracle_default_schema
            or ""
        ).strip()
        fallback_schema = _load_metadata_owner()

        def _run_once(schema_name: str, sql_text: str) -> dict[str, Any]:
            if schema_name and _valid_schema_name(schema_name):
                schema_cur = conn.cursor()
                try:
                    schema_cur.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema_name}")
                finally:
                    _safe_close(schema_cur)

            # Best-effort full result count for UI badges.
            total_count: int | None = None
            if bool(getattr(settings, "db_precount_enabled", False)):
                count_cur = conn.cursor()
                try:
                    count_cur.execute(f"SELECT COUNT(*) FROM ({sql_text})")
                    count_row = count_cur.fetchone()
                    if count_row and len(count_row) > 0 and count_row[0] is not None:
                        total_count = int(count_row[0])
                except Exception:
                    total_count = None
                finally:
                    _safe_close(count_cur)

            run_cur = conn.cursor()
            try:
                run_cur.execute(sql_text)
                columns = [d[0] for d in run_cur.description] if run_cur.description else []
                row_cap_limit = max(0, int(getattr(settings, "row_cap", 0) or 0))
                row_cap_reached = False
                if row_cap_limit > 0:
                    rows = run_cur.fetchmany(row_cap_limit + 1)
                    if len(rows) > row_cap_limit:
                        rows = rows[:row_cap_limit]
                        row_cap_reached = True
                else:
                    rows = run_cur.fetchall()
                return {
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                    "row_cap": row_cap_limit if row_cap_reached else None,
                    "total_count": total_count,
                    "query_hash": query_hash,
                    "db_call_timeout_ms": effective_timeout_ms,
                    "accuracy_mode": bool(accuracy_mode),
                }
            finally:
                _safe_close(run_cur)

        try:
            result = _run_once(session_schema, text)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            logger.info(
                "Oracle SQL ok tag=%s qh=%s elapsed_ms=%s rows=%s total_count=%s timeout_ms=%s accuracy_mode=%s",
                tag,
                query_hash,
                elapsed_ms,
                int(result.get("row_count") or 0),
                result.get("total_count"),
                effective_timeout_ms,
                bool(accuracy_mode),
            )
            result["elapsed_ms"] = elapsed_ms
            return result
        except Exception as exc:
            # If default schema is stale/misconfigured, retry once with
            # the metadata owner inferred during table sync. Also handle
            # stale schema prefixes baked into SQL (e.g. old_owner.TABLE).
            last_exc: Exception = exc
            if (
                _is_ora_00942(exc)
                and fallback_schema
                and fallback_schema.upper() != session_schema.upper()
            ):
                try:
                    return _run_once(fallback_schema, text)
                except Exception as retry_exc:
                    last_exc = retry_exc
            if _is_ora_00942(last_exc):
                rewritten_sql, changed = _strip_non_target_schema_prefixes(
                    text,
                    target_schemas={session_schema, fallback_schema},
                )
                if changed:
                    try:
                        rewrite_schema = fallback_schema or session_schema
                        result = _run_once(rewrite_schema, rewritten_sql)
                        elapsed_ms = int((time.perf_counter() - started) * 1000)
                        logger.info(
                            "Oracle SQL ok(rewrite) tag=%s qh=%s elapsed_ms=%s rows=%s total_count=%s timeout_ms=%s accuracy_mode=%s",
                            tag,
                            query_hash,
                            elapsed_ms,
                            int(result.get("row_count") or 0),
                            result.get("total_count"),
                            effective_timeout_ms,
                            bool(accuracy_mode),
                        )
                        result["elapsed_ms"] = elapsed_ms
                        return result
                    except Exception as rewrite_exc:
                        last_exc = rewrite_exc
            raise last_exc
    except Exception as exc:  # pragma: no cover - depends on driver
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        raw_exc: Exception
        if isinstance(exc, HTTPException):
            raw_exc = Exception(str(exc.detail))
            message = str(exc.detail)
        else:
            raw_exc = exc
            message = str(exc)
        error_class = _classify_db_error(raw_exc)
        if error_class == "CLIENT_TIMEOUT" and elapsed_ms >= int(effective_timeout_ms * 0.9):
            logger.warning(
                "Oracle SQL timeout_near_limit tag=%s qh=%s elapsed_ms=%s timeout_ms=%s accuracy_mode=%s error=%s",
                tag,
                query_hash,
                elapsed_ms,
                effective_timeout_ms,
                bool(accuracy_mode),
                message,
            )
        logger.error(
            "Oracle SQL failed tag=%s qh=%s class=%s elapsed_ms=%s timeout_ms=%s accuracy_mode=%s error=%s",
            tag,
            query_hash,
            error_class,
            elapsed_ms,
            effective_timeout_ms,
            bool(accuracy_mode),
            message,
        )
        raise HTTPException(
            status_code=400,
            detail=(
                f"{error_class}: {message} "
                f"(query_hash={query_hash}, elapsed_ms={elapsed_ms}, timeout_ms={effective_timeout_ms})"
            ),
        ) from exc
    finally:
        _safe_close(conn)
