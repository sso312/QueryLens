from __future__ import annotations

import json
from typing import Any

from app.core.config import get_settings
from app.services.agents.json_utils import extract_json_object
from app.services.agents.llm_client import LLMClient
from app.services.agents.prompts import EXPERT_SYSTEM_PROMPT, ERROR_REPAIR_SYSTEM_PROMPT


def review_sql(
    question: str,
    context: dict[str, Any],
    draft: dict[str, Any],
    *,
    question_en: str | None = None,
    planner_intent: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    client = LLMClient()
    payload: dict[str, Any] = {"question": question, "context": context, "draft": draft}
    if question_en and question_en.strip() and question_en.strip() != question.strip():
        payload["question_en"] = question_en.strip()
    if isinstance(planner_intent, dict) and planner_intent:
        payload["planner_intent"] = planner_intent
    messages = [
        {"role": "system", "content": EXPERT_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps(payload, ensure_ascii=True),
        },
    ]
    response = client.chat(
        messages=messages,
        model=settings.expert_model,
        max_tokens=max(200, int(getattr(settings, "llm_max_output_tokens_expert", settings.llm_max_output_tokens))),
        expect_json=True,
    )
    payload = extract_json_object(response["content"])
    payload["usage"] = response.get("usage", {})
    return payload


def repair_sql_after_error(
    question: str,
    context: dict[str, Any],
    failed_sql: str,
    error_message: str,
    *,
    question_en: str | None = None,
    planner_intent: dict[str, Any] | None = None,
    structured_error: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    client = LLMClient()
    payload: dict[str, Any] = {
        "question": question,
        "context": context,
        "failed_sql": failed_sql,
        "error_message": error_message,
    }
    if isinstance(structured_error, dict) and structured_error:
        payload["error_detail"] = structured_error
    if question_en and question_en.strip() and question_en.strip() != question.strip():
        payload["question_en"] = question_en.strip()
    if isinstance(planner_intent, dict) and planner_intent:
        payload["planner_intent"] = planner_intent
    messages = [
        {"role": "system", "content": ERROR_REPAIR_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps(payload, ensure_ascii=True),
        },
    ]
    response = client.chat(
        messages=messages,
        model=settings.expert_model,
        max_tokens=max(200, int(getattr(settings, "llm_max_output_tokens_repair", settings.llm_max_output_tokens))),
        expect_json=True,
    )
    payload = extract_json_object(response["content"])
    payload["usage"] = response.get("usage", {})
    return payload
