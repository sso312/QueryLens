# Metadata Guide

`text-to-sql/var/metadata` contains both runtime assets and evaluation datasets.

## Runtime Core (required)
- `schema_catalog.json`: table/column catalog.
- `join_graph.json`: FK join edges.
- `join_templates.jsonl`, `sql_templates.jsonl`: template SQL for retrieval/indexing.
- `sql_examples.jsonl`, `sql_examples_augmented.jsonl`: few-shot SQL examples.
- `table_value_profiles.jsonl`: DB-scan based low-cardinality value profiles per table/column.
- `table_value_profile_summary.json`: table-level coverage summary for the profile file.
- `glossary_docs.jsonl`: term-definition docs.
- `diagnosis_icd_map.jsonl`, `procedure_icd_map.jsonl`: ICD concept mappings.
- `label_intent_profiles.jsonl`, `column_value_docs.jsonl`: intent/value hints.
- `sql_postprocess_rules.json`, `sql_postprocess_schema_hints.json`, `sql_error_repair_rules.json`: postprocess/repair rules.

## Runtime Local State (mutable)
- `connection_settings.json`: DB connection settings saved from UI.
- `table_scope.json`: allowed table scope saved from UI.

These files are environment-specific and should be managed carefully in shared branches.

## Evaluation Assets
- `mimic_eval_questions.jsonl`, `mimic_eval_new_v1.jsonl`
- split/chunk variants: `mimic_eval_chunk_*`, `mimic_eval_half*`, `mimic_eval_half_remaining.jsonl`
- smoke/demo: `demo_questions.jsonl`, `confirm_smoke_1.jsonl`

## Recommended Maintenance
- Validate JSON/JSONL shape:
  - `python text-to-sql/scripts/validate_assets.py`
- Clean and dedupe metadata docs:
  - `python text-to-sql/scripts/clean_rag_metadata.py --metadata-dir text-to-sql/var/metadata`
- Build table value profiles from live DB:
  - `docker exec compose-api-1 python scripts/build_table_value_profiles.py --metadata-dir var/metadata --output var/metadata/table_value_profiles.jsonl --max-distinct 40 --top-n 20`
