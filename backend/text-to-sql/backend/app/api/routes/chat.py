from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app.services.runtime.state_store import get_state_store

router = APIRouter()


class ChatHistoryRequest(BaseModel):
    user: str
    state: dict | None = None


@router.get("/history")
def get_history(user: str):
    store = get_state_store()
    if not store.enabled:
        return {"state": None}
    state = store.get(f"chat::{user}")
    return {"state": state}


@router.post("/history")
def save_history(req: ChatHistoryRequest):
    store = get_state_store()
    if not store.enabled:
        return {"ok": False, "detail": "MongoDB is not configured"}
    key = f"chat::{req.user}"
    if req.state is None:
        store.delete(key)
    else:
        store.set(key, req.state)
    return {"ok": True}
