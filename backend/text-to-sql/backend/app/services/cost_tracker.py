from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import json
import time
from typing import Any

from app.core.config import get_settings
from app.services.logging_store.store import append_event
from app.services.runtime.state_store import get_state_store


@dataclass
class CostState:
    total_krw: int = 0
    last_updated: int = 0


@dataclass
class BudgetConfig:
    budget_limit_krw: int | None = None
    cost_alert_threshold_krw: int | None = None


@dataclass
class CostTracker:
    state_path: str
    events_log_path: str
    config_path: str
    state: CostState = field(default_factory=CostState)
    config: BudgetConfig = field(default_factory=BudgetConfig)

    @property
    def _state_key(self) -> str:
        return "cost_state"

    @property
    def _config_key(self) -> str:
        return "budget_config"

    def load(self) -> None:
        store = get_state_store()
        if store.enabled:
            data = store.get(self._state_key)
            if isinstance(data, dict):
                self.state.total_krw = int(data.get("total_krw", 0) or 0)
                self.state.last_updated = int(data.get("last_updated", 0) or 0)
                return
        path = Path(self.state_path)
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return
        self.state.total_krw = int(data.get("total_krw", 0))
        self.state.last_updated = int(data.get("last_updated", 0))

    def load_config(self) -> None:
        store = get_state_store()
        if store.enabled:
            data = store.get(self._config_key)
            if isinstance(data, dict):
                if "budget_limit_krw" in data and data.get("budget_limit_krw") is not None:
                    self.config.budget_limit_krw = int(data["budget_limit_krw"])
                if "cost_alert_threshold_krw" in data and data.get("cost_alert_threshold_krw") is not None:
                    self.config.cost_alert_threshold_krw = int(data["cost_alert_threshold_krw"])
                return
        path = Path(self.config_path)
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return
        if "budget_limit_krw" in data:
            self.config.budget_limit_krw = int(data["budget_limit_krw"])
        if "cost_alert_threshold_krw" in data:
            self.config.cost_alert_threshold_krw = int(data["cost_alert_threshold_krw"])

    def persist(self) -> None:
        payload = {
            "total_krw": self.state.total_krw,
            "last_updated": int(time.time()),
        }
        store = get_state_store()
        if store.enabled and store.set(self._state_key, payload):
            return
        path = Path(self.state_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

    def persist_config(self) -> None:
        payload = {}
        if self.config.budget_limit_krw is not None:
            payload["budget_limit_krw"] = self.config.budget_limit_krw
        if self.config.cost_alert_threshold_krw is not None:
            payload["cost_alert_threshold_krw"] = self.config.cost_alert_threshold_krw
        store = get_state_store()
        if store.enabled and store.set(self._config_key, payload):
            return
        path = Path(self.config_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

    def add_cost(self, krw: int, meta: dict[str, Any] | None = None) -> None:
        if krw <= 0:
            return
        self.state.total_krw += int(krw)
        self.state.last_updated = int(time.time())
        self.persist()
        append_event(self.events_log_path, {
            "type": "cost",
            "krw": int(krw),
            "total_krw": self.state.total_krw,
            "meta": meta or {},
        })

    def status(self) -> dict[str, Any]:
        settings = get_settings()
        budget_limit = self.config.budget_limit_krw or settings.budget_limit_krw
        alert_limit = self.config.cost_alert_threshold_krw or settings.cost_alert_threshold_krw
        return {
            "total_krw": self.state.total_krw,
            "budget_limit_krw": budget_limit,
            "cost_alert_threshold_krw": alert_limit,
            "over_limit": self.state.total_krw >= budget_limit,
            "over_alert": self.state.total_krw >= alert_limit,
        }

    def update_config(self, budget_limit_krw: int | None, cost_alert_threshold_krw: int | None) -> None:
        if budget_limit_krw is not None:
            self.config.budget_limit_krw = int(budget_limit_krw)
        if cost_alert_threshold_krw is not None:
            self.config.cost_alert_threshold_krw = int(cost_alert_threshold_krw)
        self.persist_config()


_TRACKER: CostTracker | None = None


def get_cost_tracker() -> CostTracker:
    global _TRACKER
    if _TRACKER is None:
        settings = get_settings()
        _TRACKER = CostTracker(
            state_path=settings.cost_state_path,
            events_log_path=settings.events_log_path,
            config_path=settings.budget_config_path,
        )
        _TRACKER.load()
        _TRACKER.load_config()
    return _TRACKER
