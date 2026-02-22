import asyncio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os

from app.core.config import get_settings
from app.api.routes import (
    admin_budget,
    admin_metadata,
    admin_oracle,
    admin_settings,
    audit,
    chat,
    cohort,
    dashboard,
    pdf,
    query,
    report,
)

app = FastAPI(title="RAG SQL Demo API", version="0.1.0")


@app.middleware("http")
async def request_timeout_middleware(request: Request, call_next):
    # Keep API timeout above DB timeout. If upstream proxy (nginx/gunicorn) exists,
    # make sure its timeout is also >= this value to avoid earlier disconnects.
    timeout_sec = max(190, int(get_settings().api_request_timeout_sec))
    try:
        return await asyncio.wait_for(call_next(request), timeout=timeout_sec)
    except asyncio.TimeoutError:
        return JSONResponse(
            status_code=504,
            content={"detail": f"Request timeout after {timeout_sec}s"},
        )

origins = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000",
    ).split(",")
    if origin.strip()
]
if origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}

app.include_router(admin_metadata.router, prefix="/admin/metadata", tags=["admin-metadata"])
app.include_router(admin_metadata.rag_router, prefix="/admin/rag", tags=["admin-rag"])
app.include_router(admin_settings.router, prefix="/admin/settings", tags=["admin-settings"])
app.include_router(audit.router, prefix="/audit", tags=["audit"])
app.include_router(chat.router, prefix="/chat", tags=["chat"])
app.include_router(cohort.router, prefix="/cohort", tags=["cohort"])
app.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
app.include_router(query.router, prefix="/query", tags=["query"])
app.include_router(report.router, prefix="/report", tags=["report"])
app.include_router(admin_budget.router, prefix="/admin/budget", tags=["admin-budget"])
app.include_router(admin_oracle.router, prefix="/admin/oracle", tags=["admin-oracle"])
app.include_router(pdf.router, prefix="/pdf", tags=["pdf"])
