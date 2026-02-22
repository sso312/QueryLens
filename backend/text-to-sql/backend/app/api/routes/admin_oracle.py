import asyncio

from fastapi import APIRouter, HTTPException, Query

from app.core.config import get_settings
from app.services.oracle.connection import pool_status
from app.services.runtime.request_context import use_request_user

router = APIRouter()


def _pool_status_for_user(user: str | None) -> dict:
    with use_request_user(user):
        return pool_status(user)


@router.get("/pool/status")
async def oracle_pool_status(user: str | None = Query(default=None)):
    timeout_sec = max(1, int(get_settings().oracle_healthcheck_timeout_sec))
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_pool_status_for_user, user),
            timeout=timeout_sec,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Oracle connection check timed out after {timeout_sec}s",
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Oracle connection check failed: {exc}",
        ) from exc
