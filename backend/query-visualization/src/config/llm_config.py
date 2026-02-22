"""GPT 설정 로딩 유틸."""
from __future__ import annotations

import os

# .env에서 읽은 GPT API 키
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# .env에서 읽은 기본 모델명
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
