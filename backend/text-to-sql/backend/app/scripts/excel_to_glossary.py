from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input Excel file")
    parser.add_argument("--output", required=True, help="Output jsonl file")
    parser.add_argument("--term-col", default="term")
    parser.add_argument("--def-col", default="definition")
    args = parser.parse_args()

    if pd is None:
        raise RuntimeError("pandas is required to read Excel files")

    df = pd.read_excel(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            term = str(row.get(args.term_col, "")).strip()
            definition = str(row.get(args.def_col, "")).strip()
            if not term:
                continue
            payload = {"term": term, "definition": definition}
            f.write(json.dumps(payload, ensure_ascii=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
