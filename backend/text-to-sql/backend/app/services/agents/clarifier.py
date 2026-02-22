from __future__ import annotations

import json
from typing import Any

from app.core.config import get_settings
from app.services.agents.json_utils import extract_json_object
from app.services.agents.llm_client import LLMClient
from app.services.agents.prompts import CLARIFIER_SYSTEM_PROMPT


def _normalize_conversation(conversation: list[dict[str, Any]] | None) -> list[dict[str, str]]:
    if not conversation:
        return []
    normalized: list[dict[str, str]] = []
    for turn in conversation[-10:]:
        role = str(turn.get("role") or "").strip().lower()
        content = str(turn.get("content") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        normalized.append({"role": role, "content": content[:2000]})
    return normalized


def evaluate_question_clarity(
    question: str,
    *,
    conversation: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    client = LLMClient()
    messages = [
        {"role": "system", "content": CLARIFIER_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps(
                {
                    "latest_question": question,
                    "conversation": _normalize_conversation(conversation),
                },
                ensure_ascii=False,
            ),
        },
    ]
    response = client.chat(
        messages=messages,
        model=settings.expert_model,
        max_tokens=max(150, int(getattr(settings, "llm_max_output_tokens_clarifier", settings.llm_max_output_tokens))),
        expect_json=True,
    )
    payload = extract_json_object(response["content"])
    payload["usage"] = response.get("usage", {})
    return payload
