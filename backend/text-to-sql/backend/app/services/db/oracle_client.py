from __future__ import annotations

from typing import Any


try:
    import oracledb  # type: ignore
except Exception:  # pragma: no cover
    oracledb = None


def get_connection(dsn: str, user: str, password: str, *, accuracy_mode: bool) -> Any:
    if oracledb is None:
        raise RuntimeError("oracledb is not installed")
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    conn.call_timeout = 180_000 if accuracy_mode else 60_000
    return conn
