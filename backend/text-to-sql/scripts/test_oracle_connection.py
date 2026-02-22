from __future__ import annotations

from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "backend"))

from app.core.config import get_settings
from app.services.oracle.connection import acquire_connection


def _mask(value: str) -> str:
    if not value:
        return "(empty)"
    if len(value) <= 4:
        return "***"
    return f"{value[:2]}***{value[-2:]}"


def main() -> int:
    settings = get_settings()
    print("Oracle env/config snapshot:")
    print("  ORACLE_DSN =", settings.oracle_dsn or "(empty)")
    print("  ORACLE_USER =", settings.oracle_user or "(empty)")
    print("  ORACLE_DEFAULT_SCHEMA =", settings.oracle_default_schema or "(empty)")
    print("  ORACLE_TNS_ADMIN =", os.getenv("ORACLE_TNS_ADMIN", "(empty)"))
    print("  ORACLE_LIB_DIR =", os.getenv("ORACLE_LIB_DIR", "(empty)"))
    print("  LD_LIBRARY_PATH =", os.getenv("LD_LIBRARY_PATH", "(empty)"))
    print("  ORACLE_PASSWORD =", _mask(settings.oracle_password))

    try:
        conn = acquire_connection()
    except Exception as exc:
        print("FAIL: connection acquisition failed")
        print(f"  {type(exc).__name__}: {exc}")
        return 1

    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM dual")
        row = cur.fetchone()
        cur.close()
        print("OK: query succeeded, row =", row)
    except Exception as exc:
        print("FAIL: query failed")
        print(f"  {type(exc).__name__}: {exc}")
        return 1
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
