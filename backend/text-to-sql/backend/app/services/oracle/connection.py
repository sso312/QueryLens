from __future__ import annotations

import os
import threading
from typing import Any
from pathlib import Path
import logging

from fastapi import HTTPException

from app.core.config import get_settings
from app.services.runtime.request_context import get_request_user_id
from app.services.runtime.settings_store import load_connection_settings
from app.services.runtime.user_scope import normalize_user_id

try:
    import oracledb  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    oracledb = None


_POOLS: dict[str, Any] = {}
_POOL_LOCK = threading.Lock()
_CLIENT_INIT = False
_CLIENT_LOCK = threading.Lock()
logger = logging.getLogger(__name__)
_SSL_RETRY_ERROR_MARKERS = (
    "ORA-28759",
    "ORA-288",
)


def _pool_create_error(exc: Exception) -> HTTPException:
    detail = str(exc).strip()
    upper = detail.upper()
    if "ORA-01017" in upper:
        return HTTPException(
            status_code=503,
            detail=(
                "Oracle authentication failed (ORA-01017). "
                "Check username/password and save connection settings again."
            ),
        )
    if "DPY-4011" in upper:
        return HTTPException(
            status_code=503,
            detail=(
                "Oracle connection closed by database/network (DPY-4011). "
                "Check DSN(service name), SSL mode(tcps/wallet), and DB-side access rules."
            ),
        )
    return HTTPException(status_code=503, detail=f"Oracle pool create failed: {detail or exc}")


def _build_dsn(
    *,
    host: str,
    port: str,
    database: str,
    ssl_mode: str,
) -> tuple[str, str]:
    host_v = str(host or "").strip()
    port_v = str(port or "").strip()
    db_v = str(database or "").strip()
    ssl_v = str(ssl_mode or "").strip().lower()
    if not host_v or not port_v or not db_v:
        return "", ""
    if ssl_v in {"require", "verify-ca", "verify-full"}:
        return f"tcps://{host_v}:{port_v}/{db_v}", f"{host_v}:{port_v}/{db_v}"
    dsn = f"{host_v}:{port_v}/{db_v}"
    return dsn, dsn


def _env_connection_profile(settings: Any) -> dict[str, str]:
    user = str(getattr(settings, "oracle_user", "") or "").strip()
    password = str(getattr(settings, "oracle_password", "") or "")
    dsn = str(getattr(settings, "oracle_dsn", "") or "").strip()
    ssl_mode = str(os.getenv("ORACLE_SSL_MODE", "disable") or "disable").strip().lower()
    tcp_fallback = ""

    if not dsn:
        env_host = str(os.getenv("ORACLE_HOST", "") or "").strip()
        env_port = str(os.getenv("ORACLE_PORT", "") or "").strip()
        env_service = str(os.getenv("ORACLE_SERVICE_NAME", "") or "").strip()
        dsn, tcp_fallback = _build_dsn(
            host=env_host,
            port=env_port,
            database=env_service,
            ssl_mode=ssl_mode,
        )

    if not user or not password or not dsn:
        return {}
    return {
        "user": user,
        "password": password,
        "dsn": dsn,
        "ssl_mode": ssl_mode or "disable",
        "tcp_fallback_dsn": tcp_fallback,
    }


def _create_pool_with_retry(lib: Any, pool_kwargs: dict[str, Any], *, ssl_mode: str, tcp_fallback_dsn: str) -> Any:
    try:
        return lib.create_pool(**pool_kwargs)
    except Exception as exc:
        upper_msg = str(exc).upper()
        if (
            str(pool_kwargs.get("dsn", "")).lower().startswith("tcps://")
            and tcp_fallback_dsn
            and ssl_mode == "require"
            and any(marker in upper_msg for marker in _SSL_RETRY_ERROR_MARKERS)
        ):
            retry_kwargs = dict(pool_kwargs)
            retry_kwargs["dsn"] = tcp_fallback_dsn
            return lib.create_pool(**retry_kwargs)
        raise


def _has_client_lib(lib_path: Path) -> bool:
    return any(
        next(lib_path.glob(pattern), None) is not None
        for pattern in ("libclntsh.so*", "oci.dll", "libclntsh.dylib")
    )


def _candidate_client_dirs() -> list[Path]:
    candidates: list[Path] = []
    env_dir = os.getenv("ORACLE_LIB_DIR", "").strip()
    if env_dir:
        candidates.append(Path(env_dir))

    here = Path(__file__).resolve()
    repo_root = here.parents[5] if len(here.parents) > 5 else here.parents[-1]
    text_to_sql_root = here.parents[4] if len(
        here.parents) > 4 else here.parents[-1]
    for rel in (
        Path("oracle/instantclient_23_26"),
        Path("backend/query-visualization/oracle/instantclient_23_26"),
        Path("backend/text-to-sql/oracle/instantclient_23_26"),
    ):
        candidates.append(repo_root / rel)
        candidates.append(text_to_sql_root / rel)

    deduped: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _require_oracledb() -> Any:
    if oracledb is None:
        raise HTTPException(
            status_code=500, detail="oracledb library is not installed")
    return oracledb


def _oracle_driver_mode() -> str:
    raw = str(os.getenv("ORACLE_DRIVER_MODE", "thin") or "thin").strip().lower()
    if raw in {"thin", "thick", "auto"}:
        return raw
    return "thin"


def resolve_call_timeout_ms(*, accuracy_mode: bool = False) -> int:
    """Resolve per-connection Oracle call timeout (milliseconds)."""
    settings = get_settings()
    base_ms = max(180_000, int(getattr(settings, "db_timeout_sec", 180) or 180) * 1000)
    if not accuracy_mode:
        return base_ms
    accuracy_ms = max(
        180_000,
        int(getattr(settings, "db_timeout_sec_accuracy", 180) or 180) * 1000,
    )
    return max(base_ms, accuracy_ms)


def _init_oracle_client() -> None:
    global _CLIENT_INIT
    if _CLIENT_INIT:
        return
    driver_mode = _oracle_driver_mode()
    if driver_mode == "thin":
        # Thin mode does not require Oracle Instant Client.
        return
    lib = _require_oracledb()
    config_dir = os.getenv("ORACLE_TNS_ADMIN", "").strip()
    strict_thick = driver_mode == "thick"
    selected_path: Path | None = None
    for candidate in _candidate_client_dirs():
        if not candidate.exists():
            continue
        if _has_client_lib(candidate):
            selected_path = candidate
            break
    if selected_path is None:
        if strict_thick:
            raise HTTPException(
                status_code=500,
                detail=(
                    "Oracle client init failed. ORACLE_DRIVER_MODE=thick but no Oracle client library "
                    "was found. Set ORACLE_DRIVER_MODE=thin or provide ORACLE_LIB_DIR with Instant Client."
                ),
            )
        # Auto mode: keep thin mode if no Instant Client is available.
        return
    lib_dir = str(selected_path)
    if not config_dir:
        candidate_tns = selected_path / "network" / "admin"
        if candidate_tns.exists():
            config_dir = str(candidate_tns)
    with _CLIENT_LOCK:
        if _CLIENT_INIT:
            return
        try:
            if config_dir:
                lib.init_oracle_client(lib_dir=lib_dir, config_dir=config_dir)
            else:
                lib.init_oracle_client(lib_dir=lib_dir)
        except Exception as exc:  # pragma: no cover - depends on client install
            if strict_thick:
                raise HTTPException(
                    status_code=500,
                    detail=f"Oracle client init failed. Check ORACLE_LIB_DIR/ORACLE_TNS_ADMIN. {exc}",
                ) from exc
            # Auto mode: client init failed, fall back to thin mode.
            return
        _CLIENT_INIT = True


def _resolve_user_id(user_id: str | None = None) -> str:
    explicit = normalize_user_id(user_id)
    if explicit:
        return explicit
    return normalize_user_id(get_request_user_id())


def _pool_key(user_id: str | None = None) -> str:
    resolved_user = _resolve_user_id(user_id)
    if resolved_user:
        return f"user::{resolved_user}"
    return "__global__"


def _close_pool(pool: Any) -> None:
    try:
        pool.close()
    except Exception:
        pass


def get_pool(user_id: str | None = None):
    key = _pool_key(user_id)
    with _POOL_LOCK:
        pool = _POOLS.get(key)
        if pool is not None:
            return pool

    settings = get_settings()
    resolved_user = _resolve_user_id(user_id)
    overrides = load_connection_settings(
        resolved_user or None,
        include_global_fallback=not bool(resolved_user),
    )
    host = str(overrides.get("host") or "").strip()
    port = str(overrides.get("port") or "").strip()
    database = str(overrides.get("database") or "").strip()
    ssl_mode = str(overrides.get("sslMode") or "").strip().lower()
    dsn_override = str(overrides.get("dsn") or "").strip()

    env_profile = _env_connection_profile(settings)
    prefer_env = bool(env_profile)

    dsn = ""
    tcp_fallback_dsn = ""
    source_label = "connection_settings"
    if prefer_env:
        dsn = str(env_profile.get("dsn") or "").strip()
        ssl_mode = str(env_profile.get("ssl_mode") or "disable").strip().lower()
        tcp_fallback_dsn = str(env_profile.get("tcp_fallback_dsn") or "").strip()
        source_label = "env"
    else:
        if dsn_override:
            dsn = dsn_override
        elif host and port and database:
            dsn, tcp_fallback_dsn = _build_dsn(
                host=host,
                port=port,
                database=database,
                ssl_mode=ssl_mode,
            )
        else:
            dsn = str(getattr(settings, "oracle_dsn", "") or "").strip()
            source_label = "env"

    if not dsn:
        raise HTTPException(
            status_code=503,
            detail="Oracle connection is not configured. Save connection settings first.",
        )
    _init_oracle_client()
    lib = _require_oracledb()
    with _POOL_LOCK:
        existing = _POOLS.get(key)
        if existing is not None:
            return existing

        if prefer_env:
            username = str(env_profile.get("user") or "").strip()
            password = str(env_profile.get("password") or "")
        else:
            username = str(overrides.get("username") or settings.oracle_user or "").strip()
            password_value = overrides.get("password")
            if password_value is None:
                password_value = settings.oracle_password
            password = str(password_value or "")
        if password != password.strip():
            password = password.strip()
        if not username:
            raise HTTPException(
                status_code=503,
                detail="Oracle username is not configured. Save connection settings first.",
            )
        if not password:
            raise HTTPException(
                status_code=503,
                detail="Oracle password is empty. Save connection settings with a valid password.",
            )
        pool_kwargs = {
            "user": username,
            "password": password,
            "dsn": dsn,
            "min": settings.oracle_pool_min,
            "max": settings.oracle_pool_max,
            "increment": settings.oracle_pool_inc,
            "timeout": settings.oracle_pool_timeout_sec,
        }
        try:
            created = _create_pool_with_retry(
                lib,
                pool_kwargs,
                ssl_mode=ssl_mode,
                tcp_fallback_dsn=tcp_fallback_dsn,
            )
        except Exception as exc:
            # If env profile was preferred but failed, try user/runtime settings once.
            if prefer_env and (dsn_override or (host and port and database)):
                try:
                    fallback_dsn = dsn_override
                    fallback_tcp = ""
                    fallback_ssl_mode = str(overrides.get("sslMode") or "").strip().lower()
                    if not fallback_dsn:
                        fallback_dsn, fallback_tcp = _build_dsn(
                            host=host,
                            port=port,
                            database=database,
                            ssl_mode=fallback_ssl_mode,
                        )
                    if fallback_dsn:
                        fallback_username = str(overrides.get("username") or "").strip()
                        fallback_password = str(overrides.get("password") or "").strip()
                        if fallback_username and fallback_password:
                            fallback_kwargs = {
                                "user": fallback_username,
                                "password": fallback_password,
                                "dsn": fallback_dsn,
                                "min": settings.oracle_pool_min,
                                "max": settings.oracle_pool_max,
                                "increment": settings.oracle_pool_inc,
                                "timeout": settings.oracle_pool_timeout_sec,
                            }
                            created = _create_pool_with_retry(
                                lib,
                                fallback_kwargs,
                                ssl_mode=fallback_ssl_mode,
                                tcp_fallback_dsn=fallback_tcp,
                            )
                            source_label = "connection_settings"
                        else:
                            raise _pool_create_error(exc) from exc
                    else:
                        raise _pool_create_error(exc) from exc
                except Exception as retry_exc:
                    retry_http = _pool_create_error(retry_exc)
                    raise HTTPException(
                        status_code=retry_http.status_code,
                        detail=f"{retry_http.detail} (source={source_label})",
                    ) from retry_exc
            else:
                err_http = _pool_create_error(exc)
                raise HTTPException(
                    status_code=err_http.status_code,
                    detail=f"{err_http.detail} (source={source_label})",
                ) from exc
        _POOLS[key] = created
        return created


def reset_pool(user_id: str | None = None) -> None:
    key = _pool_key(user_id)
    with _POOL_LOCK:
        pool = _POOLS.pop(key, None)
        if pool is not None:
            _close_pool(pool)
            return

        # Preserve legacy behavior for no-user contexts.
        if key == "__global__":
            pools = list(_POOLS.values())
            _POOLS.clear()
            for item in pools:
                _close_pool(item)


def acquire_connection(user_id: str | None = None, *, accuracy_mode: bool = False):
    pool = get_pool(user_id)
    try:
        conn = pool.acquire()
        try:
            conn.call_timeout = resolve_call_timeout_ms(accuracy_mode=accuracy_mode)
        except Exception:
            pass
        return conn
    except Exception as exc:  # pragma: no cover - depends on driver
        message = str(exc)
        upper = message.upper()
        recoverable = any(
            marker in upper
            for marker in (
                "DPY-4011",
                "DPY-6005",
                "DPI-1080",
                "CONNECTION WAS CLOSED",
                "CONNECTION RESET",
                "EOF OCCURRED",
            )
        )
        if recoverable:
            # Recover stale/disconnected pools once before failing the request.
            reset_pool(user_id)
            try:
                conn = get_pool(user_id).acquire()
                try:
                    conn.call_timeout = resolve_call_timeout_ms(accuracy_mode=accuracy_mode)
                except Exception:
                    pass
                return conn
            except Exception as retry_exc:
                raise HTTPException(
                    status_code=503,
                    detail=f"Oracle pool unavailable: {retry_exc}",
                ) from retry_exc
        if "DPY-4011" in upper:
            logger.warning("Oracle acquire failed with DPY-4011 (accuracy_mode=%s): %s", accuracy_mode, message)
        raise HTTPException(
            status_code=503, detail=f"Oracle pool unavailable: {exc}") from exc


def pool_status(user_id: str | None = None) -> dict[str, Any]:
    pool = get_pool(user_id)
    try:
        conn = pool.acquire()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM dual")
        cur.fetchone()
        cur.close()
        conn.close()
    except Exception as exc:  # pragma: no cover - depends on driver
        raise HTTPException(
            status_code=503, detail=f"Oracle connection check failed: {exc}") from exc
    return {
        "open": True,
        "busy": getattr(pool, "busy", None),
        "open_connections": getattr(pool, "open", None),
        "max": getattr(pool, "max", None),
    }
