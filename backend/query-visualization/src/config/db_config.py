"""Load Oracle configuration from environment variables"""
from __future__ import annotations

import os
from typing import Dict, Optional

import oracledb


# DSN 생성 함수
# DSN: Data Source Name (Oracle DB 접속 대상 정보를 담은 문자열)
# 입력: 없음
# 출력: DSN 문자열 또는 None
def _build_dsn() -> Optional[str]:
    # 1) ORACLE_DSN이 있으면 그대로 사용
    dsn = os.getenv("ORACLE_DSN")
    if dsn:
        return dsn

    # 2) 없으면 HOST/PORT/SERVICE_NAME으로 DSN 생성
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT")
    service_name = os.getenv("ORACLE_SERVICE_NAME")
    if host and port and service_name:
        return oracledb.makedsn(host, int(port), service_name=service_name)
    return None

# 입력: 없음
# 출력: Oracle connection parameters: user, password, dsn
# 계정/비밀번호/DSN을 환경변수에서 읽어온다
def get_oracle_config() -> Dict[str, str]:
    """Return Oracle connection parameters: user, password, dsn."""
    # 계정/비밀번호/DSN을 환경변수에서 읽어온다
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    dsn = _build_dsn()

    if not user or not password or not dsn:
        # 누락된 항목을 모아 에러 메시지로 보여준다
        missing = [
            name
            for name, val in (
                ("ORACLE_USER", user),
                ("ORACLE_PASSWORD", password),
                ("ORACLE_DSN or ORACLE_HOST/PORT/SERVICE", dsn),
            )
            if not val
        ]
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    return {"user": user, "password": password, "dsn": dsn}
