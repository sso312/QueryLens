# Query Visualization Eval Sets

## Files

- `query_visualization_eval_aside.jsonl`
  - Source: `c:\Users\KDT_03\Downloads\aside.txt`
  - Scope: aside 텍스트의 질문+정답 SQL 블록 기반 + L2 누락 5문항 보강
  - Count: 100 cases (`L1=50`, `L2=40`, `L3=10`)

## Regenerate

```bash
python query-visualization/scripts/build_eval_set_from_aside.py
```

L2 누락 5문항 보강:

```bash
python query-visualization/scripts/append_l2_missing_eval_cases.py
```

Optional arguments:

```bash
python query-visualization/scripts/build_eval_set_from_aside.py \
  --source "c:\Users\KDT_03\Downloads\aside.txt" \
  --output "query-visualization/data/eval_sets/query_visualization_eval_aside.jsonl"
```

## JSONL Schema

Each line is one case:

- `id`: case id (`aside_qv_001` ...)
- `source`: fixed source label (`aside.txt`)
- `source_line`: line number of the matched question heading
- `level`: `L1` | `L2` | `L3`
- `section`: section letter (`A/B/C/D`) or `unknown`
- `section_title`: section heading text
- `question_no`: numbered heading id (`1`, `2`, `1-1` ...)
- `user_query`: query text for visualization evaluation
- `sql`: reference SQL
  - Stored as one-line normalized SQL (no `\n`, comments stripped) for stable execution.
- `expected_chart_type`: primary expected chart type (`line`, `bar`, `hist`, `box`)
- `accepted_chart_types`: acceptable alternatives
- `chart_rationale`: heuristic rationale for chart expectation
