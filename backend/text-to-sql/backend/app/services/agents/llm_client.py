from __future__ import annotations

from typing import Any

from app.core.config import get_settings
from app.services.runtime.request_context import get_request_llm_model

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None


class LLMClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._settings = settings
        if OpenAI is None:
            raise RuntimeError("openai library is not installed")
        self.client = OpenAI(
            api_key=settings.openai_api_key or None,
            base_url=settings.openai_base_url or None,
            organization=settings.openai_org or None,
            timeout=settings.llm_timeout_sec,
        )

    def chat(
        self,
        messages: list[dict[str, str]],
        model: str,
        max_tokens: int,
        *,
        expect_json: bool = False,
    ) -> dict[str, Any]:
        requested_model = get_request_llm_model()
        kwargs: dict[str, Any] = {
            "model": requested_model or model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": self._settings.llm_temperature,
        }
        if expect_json:
            kwargs["response_format"] = {"type": "json_object"}
        try:
            response = self.client.chat.completions.create(**kwargs)
        except TypeError:
            # Fallback for SDK/providers that do not support response_format.
            kwargs.pop("response_format", None)
            response = self.client.chat.completions.create(**kwargs)
        choices = list(getattr(response, "choices", []) or [])
        if not choices:
            raise RuntimeError("LLM response has no choices")
        message = getattr(choices[0], "message", None)
        raw_content = getattr(message, "content", "")
        if isinstance(raw_content, str):
            content = raw_content
        elif raw_content is None:
            content = ""
        elif isinstance(raw_content, list):
            parts: list[str] = []
            for item in raw_content:
                if isinstance(item, dict):
                    parts.append(str(item.get("text") or ""))
                else:
                    parts.append(str(getattr(item, "text", "") or ""))
            content = "".join(parts)
        else:
            content = str(raw_content)
        usage_obj = getattr(response, "usage", None)
        usage = {
            "prompt_tokens": getattr(usage_obj, "prompt_tokens", 0) if usage_obj is not None else 0,
            "completion_tokens": getattr(usage_obj, "completion_tokens", 0) if usage_obj is not None else 0,
            "total_tokens": getattr(usage_obj, "total_tokens", 0) if usage_obj is not None else 0,
        }
        return {"content": content, "usage": usage}
