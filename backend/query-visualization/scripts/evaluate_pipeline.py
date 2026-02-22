from __future__ import annotations

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.metrics.evaluator import EvalCase, evaluate_cases


def _default_cases() -> list[EvalCase]:
    return [
        EvalCase(
            name="trend_basic",
            user_query="ICU 월별 mortality_rate 추세 보여줘",
            sql="SELECT icu_admit_month, mortality_rate, gender FROM sample",
            rows=[
                {"icu_admit_month": "2024-01", "mortality_rate": 0.12, "gender": "M"},
                {"icu_admit_month": "2024-02", "mortality_rate": 0.10, "gender": "F"},
                {"icu_admit_month": "2024-03", "mortality_rate": 0.15, "gender": "M"},
            ],
        ),
        EvalCase(
            name="distribution_basic",
            user_query="age 분포를 보여줘",
            sql="SELECT age, gender FROM sample",
            rows=[
                {"age": 65, "gender": "M"},
                {"age": 72, "gender": "F"},
                {"age": 58, "gender": "M"},
                {"age": 60, "gender": "F"},
            ],
        ),
        EvalCase(
            name="fallback_empty",
            user_query="빈 결과에서 가능한 시각화만 보여줘",
            sql="SELECT * FROM sample WHERE 1=0",
            rows=[],
        ),
    ]


def main() -> None:
    summary = evaluate_cases(_default_cases())
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

