from __future__ import annotations

import json
import re
from typing import Any


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)


def extract_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("LLM response is empty")

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    fence_match = _JSON_FENCE_RE.search(raw)
    if fence_match:
        candidate = fence_match.group(1).strip()
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed

    # Fallback: find balanced JSON object candidates and parse the first valid one.
    starts = [i for i, ch in enumerate(raw) if ch == "{"]
    for start in starts:
        depth = 0
        for idx in range(start, len(raw)):
            ch = raw[idx]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = raw[start : idx + 1]
                    try:
                        parsed = json.loads(candidate)
                    except json.JSONDecodeError:
                        break
                    if isinstance(parsed, dict):
                        return parsed
                    break

    raise ValueError("LLM response is not valid JSON")
