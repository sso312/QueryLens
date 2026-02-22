# text-to-sql: differences vs origin/feature only

Comparison base: `origin/feature` (`898579b`)
Local `HEAD` is also `898579b`; below is the current local working-tree diff only.

## Modified (M)
- `text-to-sql/README.md`
- `text-to-sql/backend/app/core/config.py`
- `text-to-sql/backend/app/services/agents/orchestrator.py`
- `text-to-sql/backend/app/services/agents/prompts.py`
- `text-to-sql/backend/app/services/cost_tracker.py`
- `text-to-sql/backend/app/services/logging_store/store.py`
- `text-to-sql/backend/app/services/oracle/connection.py`
- `text-to-sql/backend/app/services/policy/gate.py`
- `text-to-sql/backend/app/services/rag/indexer.py`
- `text-to-sql/backend/app/services/rag/retrieval.py`
- `text-to-sql/deploy/compose/docker-compose.yml`
- `text-to-sql/deploy/docker/Dockerfile.ui`
- `text-to-sql/docs/PROJECT_OVERVIEW_KO.md`
- `text-to-sql/scripts/clean_rag_metadata.py`
- `text-to-sql/var/logs/cost_state.json`
- `text-to-sql/var/logs/events.jsonl`
- `text-to-sql/var/metadata/join_graph.json`
- `text-to-sql/var/metadata/schema_catalog.json`
- `text-to-sql/var/metadata/sql_error_repair_rules.json`
- `text-to-sql/var/metadata/sql_examples_augmented.jsonl`

## Deleted (D)
- `text-to-sql/ui/app/admin/page.tsx`
- `text-to-sql/ui/app/ask/page.tsx`
- `text-to-sql/ui/app/globals.css`
- `text-to-sql/ui/app/layout.tsx`
- `text-to-sql/ui/app/page.tsx`
- `text-to-sql/ui/app/results/[qid]/page.tsx`
- `text-to-sql/ui/app/review/[qid]/page.tsx`
- `text-to-sql/ui/next.config.js`
- `text-to-sql/ui/package.json`
- `text-to-sql/var/metadata/mimic_eval_chunk_2b_2.jsonl`
