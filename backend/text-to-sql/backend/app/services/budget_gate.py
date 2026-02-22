from fastapi import HTTPException

from app.services.cost_tracker import get_cost_tracker


def ensure_budget_ok() -> None:
    status = get_cost_tracker().status()
    if status.get("over_limit"):
        raise HTTPException(status_code=402, detail="Budget limit exceeded")
