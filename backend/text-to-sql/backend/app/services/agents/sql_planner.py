from __future__ import annotations

import json
from typing import Any

from app.core.config import get_settings
from app.services.agents.json_utils import extract_json_object
from app.services.agents.llm_client import LLMClient
from app.services.agents.prompts import PLANNER_SYSTEM_PROMPT


def plan_query_intent(
    question: str,
    context: dict[str, Any],
    *,
    question_en: str | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    client = LLMClient()
    payload: dict[str, Any] = {"question": question, "context": context}
    if question_en and question_en.strip() and question_en.strip() != question.strip():
        payload["question_en"] = question_en.strip()
    messages = [
        {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
    ]
    response = client.chat(
        messages=messages,
        model=settings.planner_model,
        max_tokens=max(150, int(getattr(settings, "llm_max_output_tokens_planner", settings.llm_max_output_tokens))),
        expect_json=True,
    )
    parsed = extract_json_object(response["content"])
    if not isinstance(parsed.get("intent"), dict):
        parsed["intent"] = {}
    assumptions = parsed.get("assumptions")
    if not isinstance(assumptions, list):
        parsed["assumptions"] = []
    parsed["usage"] = response.get("usage", {})
    return parsed
