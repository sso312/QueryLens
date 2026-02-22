from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.cost_tracker import get_cost_tracker

router = APIRouter()


@router.get("/status")
def budget_status():
    return get_cost_tracker().status()


class BudgetConfigRequest(BaseModel):
    budget_limit_krw: int | None = Field(default=None, ge=0)
    cost_alert_threshold_krw: int | None = Field(default=None, ge=0)


@router.get("/config")
def budget_config():
    status = get_cost_tracker().status()
    return {
        "budget_limit_krw": status["budget_limit_krw"],
        "cost_alert_threshold_krw": status["cost_alert_threshold_krw"],
    }


@router.post("/config")
def update_budget_config(req: BudgetConfigRequest):
    tracker = get_cost_tracker()
    tracker.update_config(req.budget_limit_krw, req.cost_alert_threshold_krw)
    return tracker.status()
